/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! Nexmark TCP producer for the `statistic_overhead` benchmark.
//!
//! Streams the two Nexmark streams that Query 8 needs — `person` and `auction` — as CSV lines,
//! one stream per port (`port_base+0` = person, `port_base+1` = auction). Schemas match
//! `nes-systests/benchmark/Nexmark_with_varsized.test`:
//!
//!   person:  id,name,email_address,credit_card,city,state,timestamp,extra
//!   auction: timestamp,id,initialbid,reserve,expires,seller,category
//!
//! Event time is a *virtual clock*, a pure function of the global Nexmark event index:
//!
//!     kind(i) = i % 50  ->  0 = person, 1..=3 = auction, 4..=49 = bid  (Nexmark's 1:3:46 mix)
//!     ts_ms(i) = i * 1000 / events_per_sec
//!
//! Bid indices are reserved but not materialized (Q8 does not read `bid`); keeping them in the index
//! space preserves Nexmark's person/auction event-time spacing. Add a bid writer when a query needs
//! one.
//!
//! The offered load is set with `--tuples-per-sec` (the wire rate one consumer of every materialized
//! stream sees) and delivered by pacing each stream against that virtual clock — see `Args`. Do not
//! reintroduce a per-connection tuples/sec pacer: it desynchronises the streams and breaks windowed
//! joins. `--tuples-per-sec 0` disables pacing entirely, which starves concurrent queries.
//!
//! Every connection is stateless and derives everything from its own counter, so N consumers on one
//! port each get an identical, reproducible stream. Designed to run as a sidecar joined to the
//! worker's netns via `--network container:<worker>` so the worker dials 127.0.0.1:<port>.

use std::io;
use std::net::IpAddr;

use clap::Parser;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;

/// Nexmark's canonical event mix: 1 person : 3 auctions : 46 bids.
const EVENTS_PER_GROUP: u64 = 50;
const PERSONS_PER_GROUP: u64 = 1;
const AUCTIONS_PER_GROUP: u64 = 3;

/// INT32 in the NES schema — keep generated ids well inside the positive range.
const MAX_INT32_ID: u64 = 2_000_000_000;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "nes-nexmark-gen",
    about = "Nexmark person/auction TCP CSV producer for NES benchmarks"
)]
struct Args {
    /// First port to bind. port_base+0 serves `person`, port_base+1 serves `auction`.
    #[arg(long, default_value_t = 9200)]
    port_base: u16,

    /// Bind address. Inside a shared netns 0.0.0.0 covers 127.0.0.1 too.
    #[arg(long, default_value = "0.0.0.0")]
    bind: IpAddr,

    /// PRNG seed. Same seed -> identical stream.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Virtual clock: Nexmark events per second of *event* time. This is what decides how many
    /// tuples fall into one window, hence how much join state is live:
    ///   persons_per_window  = events_per_sec * window_sec / 50
    ///   auctions_per_window = 3 * persons_per_window
    /// Lower it if a run hits BUFFER_EXHAUSTION.
    #[arg(long, default_value_t = 100_000)]
    events_per_sec: u64,

    /// Size of the join-key domain: `person.id` cycles through [0, person_domain) and
    /// `auction.seller` is drawn uniformly from it.
    ///
    /// Nexmark uses monotonically growing person ids; recycling them over a bounded domain is a
    /// deliberate simplification. It buys two things the experiment needs: the join actually
    /// produces output within a window, and the equi-width histogram gets static, well-defined
    /// bounds [0, person_domain). Set it very large to approximate Nexmark's monotone ids.
    ///
    /// Set it to persons-per-window (= events_per_sec * window_sec / 50) for ~1 matching person per
    /// auction. The default pairs with --events-per-sec 100000 and a 10 s window.
    #[arg(long, default_value_t = 20_000)]
    person_domain: u64,

    /// Offered load in tuples/sec, as seen by ONE consumer reading every materialized stream
    /// (person + auction, i.e. 4 of every 50 Nexmark events). 0 = unlimited.
    ///
    /// This is the wire rate, but it is NOT how the pacer works: it is converted to an event-time
    /// replay speed and every stream is then paced on event time. That indirection is load-bearing,
    /// not ceremony. Nexmark's mix is 1 person to 3 auctions, so pacing each connection at the same
    /// tuples/sec advances person's event clock 3x faster than auction's; a windowed join then
    /// buffers the fast side forever waiting for the slow side's watermark and the run dies with
    /// BUFFER_EXHAUSTION. Deriving one event-time speed and sharing it keeps every stream on one
    /// clock whatever its share of the mix.
    ///
    /// Unlimited is unsafe for a second, independent reason: NebulaStream's buffer pool is global
    /// with no per-query fairness, so the first query's sources drain it and the rest starve.
    #[arg(long, default_value_t = 0)]
    tuples_per_sec: u64,
}

/// Materialized tuples per group of 50 Nexmark events (bids are reserved but not emitted).
const MATERIALIZED_PER_GROUP: u64 = PERSONS_PER_GROUP + AUCTIONS_PER_GROUP;

impl Args {
    /// Milliseconds of event time per millisecond of wall clock. 0 = unlimited.
    ///
    /// One consumer reading every materialized stream sees MATERIALIZED_PER_GROUP tuples per group
    /// of EVENTS_PER_GROUP events, so `tuples_per_sec` of wire rate corresponds to
    /// `tuples_per_sec * EVENTS_PER_GROUP / MATERIALIZED_PER_GROUP` events of event time per second.
    fn time_scale(&self) -> f64 {
        if self.tuples_per_sec == 0 {
            return 0.0;
        }
        self.tuples_per_sec as f64 * EVENTS_PER_GROUP as f64
            / (MATERIALIZED_PER_GROUP as f64 * self.events_per_sec as f64)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Stream {
    Person,
    Auction,
}

/// SplitMix64 — stateless, deterministic, good avalanche. Used as a hash of the event index so
/// every derived field is a pure function of that index (no per-connection RNG state).
fn splitmix64(index: u64) -> u64 {
    let mut z = index.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Global Nexmark event index of the `k`-th person event.
fn person_event_index(k: u64) -> u64 {
    k * EVENTS_PER_GROUP
}

/// Global Nexmark event index of the `k`-th auction event. Auctions occupy slots 1..=3 of each
/// group of 50.
fn auction_event_index(k: u64) -> u64 {
    (k / AUCTIONS_PER_GROUP) * EVENTS_PER_GROUP + PERSONS_PER_GROUP + (k % AUCTIONS_PER_GROUP)
}

/// Virtual event time in milliseconds for a global event index.
fn event_time_ms(index: u64, events_per_sec: u64) -> u64 {
    index.saturating_mul(1000) / events_per_sec
}

/// Small fixed vocabularies. Values must not contain the CSV field delimiter.
const FIRST_NAMES: [&str; 8] = [
    "ada", "grace", "alan", "edsger", "barbara", "linus", "ken", "jean",
];
const LAST_NAMES: [&str; 8] = [
    "lovelace", "hopper", "turing", "dijkstra", "liskov", "torvalds", "thompson", "bartik",
];
const CITIES: [&str; 8] = [
    "berlin", "hamburg", "munich", "cologne", "leipzig", "dresden", "bremen", "essen",
];
const STATES: [&str; 8] = ["be", "hh", "by", "nw", "sn", "sn", "hb", "nw"];

/// Nexmark's `extra` is a long padding field; a fixed 64-byte filler keeps the tuple realistically
/// wide without making the producer the bottleneck.
const EXTRA_FILLER: &str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

struct LineWriter {
    buf: Vec<u8>,
    itoa: itoa::Buffer,
}

impl LineWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(512),
            itoa: itoa::Buffer::new(),
        }
    }

    fn start(&mut self) {
        self.buf.clear();
    }

    fn sep(&mut self) {
        self.buf.push(b',');
    }

    fn num(&mut self, value: u64) {
        let s = self.itoa.format(value);
        self.buf.extend_from_slice(s.as_bytes());
    }

    fn text(&mut self, value: &str) {
        self.buf.extend_from_slice(value.as_bytes());
    }

    fn end(&mut self) {
        self.buf.push(b'\n');
    }
}

/// person: id,name,email_address,credit_card,city,state,timestamp,extra
/// Returns the tuple's event time in ms, which is what the pacer schedules against.
fn write_person(w: &mut LineWriter, k: u64, args: &Args) -> u64 {
    let index = person_event_index(k);
    let ts = event_time_ms(index, args.events_per_sec);
    let h = splitmix64(index ^ args.seed);
    let first = FIRST_NAMES[(h % FIRST_NAMES.len() as u64) as usize];
    let last = LAST_NAMES[((h >> 8) % LAST_NAMES.len() as u64) as usize];
    let city = CITIES[((h >> 16) % CITIES.len() as u64) as usize];
    let state = STATES[((h >> 24) % STATES.len() as u64) as usize];

    w.start();
    w.num(k % args.person_domain); // id
    w.sep();
    w.text(first); // name — no delimiter, so "first_last"
    w.text("_");
    w.text(last);
    w.sep();
    w.text(first); // email_address
    w.text("@");
    w.text(last);
    w.text(".example");
    w.sep();
    w.num(1_000_000_000_000_000 + h % 9_000_000_000_000_000); // credit_card, always 16 digits
    w.sep();
    w.text(city);
    w.sep();
    w.text(state);
    w.sep();
    w.num(ts); // timestamp
    w.sep();
    w.text(EXTRA_FILLER);
    w.end();
    ts
}

/// auction: timestamp,id,initialbid,reserve,expires,seller,category
/// Returns the tuple's event time in ms, which is what the pacer schedules against.
fn write_auction(w: &mut LineWriter, k: u64, args: &Args) -> u64 {
    let index = auction_event_index(k);
    let h = splitmix64(index ^ args.seed);
    let ts = event_time_ms(index, args.events_per_sec);
    // Each field takes a distinct bit range of the hash; `seller` uses the low bits, so drawing
    // the others from there too would make them visibly correlated with the join key.
    let initial_bid = (h >> 32) % 1_000;

    w.start();
    w.num(ts); // timestamp
    w.sep();
    w.num(k % MAX_INT32_ID); // id
    w.sep();
    w.num(initial_bid); // initialbid
    w.sep();
    w.num(initial_bid + (h >> 20) % 10_000); // reserve
    w.sep();
    w.num(ts + 10_000); // expires
    w.sep();
    w.num(h % args.person_domain); // seller — the join key
    w.sep();
    w.num((h >> 40) % 10); // category
    w.end();
    ts
}

async fn handle_connection(
    sock: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    port: u16,
    stream: Stream,
    args: Args,
) {
    eprintln!(
        "accept port={} peer={} stream={:?} tuples_per_sec={} time_scale={}",
        port,
        peer,
        stream,
        args.tuples_per_sec,
        args.time_scale()
    );
    let _ = sock.set_nodelay(true);
    let mut writer = BufWriter::with_capacity(64 * 1024, sock);

    let mut w = LineWriter::new();
    let mut k: u64 = 0;

    // Event-time pacing. A tuple whose event time is T is due at start + T/time_scale. Measured
    // against a fixed start instant so the schedule cannot drift, and only actually sleeping once
    // we are more than a millisecond early — otherwise this would sleep per tuple.
    let start = std::time::Instant::now();
    let time_scale = args.time_scale();
    let paced = time_scale > 0.0;
    let slack = std::time::Duration::from_millis(1);

    loop {
        let event_time_ms = match stream {
            Stream::Person => write_person(&mut w, k, &args),
            Stream::Auction => write_auction(&mut w, k, &args),
        };

        if let Err(e) = writer.write_all(&w.buf).await {
            match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof => {
                    eprintln!("disconnect port={} peer={} tuples={}", port, peer, k);
                }
                _ => {
                    eprintln!(
                        "write error port={} peer={} kind={:?}: {}",
                        port,
                        peer,
                        e.kind(),
                        e
                    );
                }
            }
            return;
        }

        k = k.wrapping_add(1);

        if paced {
            let due =
                std::time::Duration::from_secs_f64(event_time_ms as f64 / (1000.0 * time_scale));
            let elapsed = start.elapsed();
            if due > elapsed + slack {
                // Flush before sleeping, or the BufWriter would hold this batch back and the
                // consumer would see a bursty input cadence instead of a steady one.
                if writer.flush().await.is_err() {
                    eprintln!("disconnect port={} peer={} tuples={}", port, peer, k);
                    return;
                }
                tokio::time::sleep(due - elapsed).await;
            }
        }
    }
}

async fn run_listener(listener: TcpListener, port: u16, stream: Stream, args: Args) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let args = args.clone();
                tokio::spawn(async move {
                    handle_connection(sock, peer, port, stream, args).await;
                });
            }
            Err(e) => {
                eprintln!("accept error port={}: {}", port, e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    eprintln!(
        "nexmark generator: seed={} events_per_sec={} person_domain={} tuples_per_sec={} (time_scale={})",
        args.seed,
        args.events_per_sec,
        args.person_domain,
        args.tuples_per_sec,
        args.time_scale()
    );

    let mut listeners = Vec::new();
    for (offset, stream) in [(0u16, Stream::Person), (1u16, Stream::Auction)] {
        let port = args.port_base + offset;
        let addr = std::net::SocketAddr::new(args.bind, port);
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            eprintln!("bind failed addr={}: {}", addr, e);
            e
        })?;
        listeners.push((port, stream, listener));
    }

    // READY handshake — the Python runner blocks on this line before submitting queries.
    println!("READY");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    for (port, stream, _) in &listeners {
        eprintln!("listening on {}:{} -> {:?}", args.bind, port, stream);
    }

    for (port, stream, listener) in listeners {
        let args = args.clone();
        tokio::spawn(async move { run_listener(listener, port, stream, args).await });
    }

    // Wait for SIGINT (Ctrl-C) or SIGTERM (docker stop) and shut down.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { eprintln!("SIGINT received, shutting down"); }
        _ = sigterm.recv() => { eprintln!("SIGTERM received, shutting down"); }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_args() -> Args {
        Args {
            port_base: 9200,
            bind: "0.0.0.0".parse().unwrap(),
            seed: 42,
            events_per_sec: 100_000,
            person_domain: 20_000,
            tuples_per_sec: 0,
        }
    }

    /// The two index maps must partition the group of 50 exactly as Nexmark specifies: one person
    /// in slot 0, three auctions in slots 1..=3, and never a collision between them.
    #[test]
    fn event_indices_follow_the_nexmark_mix() {
        for k in 0..1_000u64 {
            assert_eq!(person_event_index(k) % EVENTS_PER_GROUP, 0);
            let a = auction_event_index(k) % EVENTS_PER_GROUP;
            assert!((1..=3).contains(&a), "auction {k} landed in slot {a}");
        }
        let persons: Vec<u64> = (0..100).map(person_event_index).collect();
        let auctions: Vec<u64> = (0..300).map(auction_event_index).collect();
        assert!(auctions.iter().all(|a| !persons.contains(a)));
        // Auction indices are strictly increasing, i.e. event time never goes backwards.
        assert!(auctions.windows(2).all(|w| w[0] < w[1]));
    }

    /// The whole experiment dies quietly if Q8 produces no output, so assert that the person ids and
    /// the auction sellers occurring in one 10 s window actually overlap.
    #[test]
    fn join_keys_overlap_within_one_window() {
        let args = test_args();
        let window_ms = 10_000u64;
        let mut person_ids = std::collections::HashSet::new();
        let mut matched = 0usize;
        let mut total = 0usize;

        let mut k = 0u64;
        while event_time_ms(person_event_index(k), args.events_per_sec) < window_ms {
            person_ids.insert(k % args.person_domain);
            k += 1;
        }
        // ~1 person per key over the domain, by construction of the default settings.
        assert_eq!(person_ids.len(), args.person_domain as usize);

        let mut k = 0u64;
        while event_time_ms(auction_event_index(k), args.events_per_sec) < window_ms {
            let seller = splitmix64(auction_event_index(k) ^ args.seed) % args.person_domain;
            if person_ids.contains(&seller) {
                matched += 1;
            }
            total += 1;
            k += 1;
        }
        assert!(total > 0, "no auctions in the first window");
        assert_eq!(matched, total, "every seller must reference a live person");
    }

    /// The wire-rate knob must convert to the event-time speed the pacer actually uses.
    #[test]
    fn tuples_per_sec_converts_to_event_time_speed() {
        let mut args = test_args();
        assert_eq!(
            args.time_scale(),
            0.0,
            "0 must mean unlimited, not a division by zero"
        );

        // One query reading both streams gets 4 of every 50 events. At events_per_sec=100_000,
        // 200_000 tup/s is therefore 25x real-time replay - the operating point from calibration.
        args.tuples_per_sec = 200_000;
        assert!(
            (args.time_scale() - 25.0).abs() < 1e-9,
            "got {}",
            args.time_scale()
        );

        // And the split across the two streams follows the 1:3 mix.
        let persons_per_sec = args.tuples_per_sec / MATERIALIZED_PER_GROUP * PERSONS_PER_GROUP;
        let auctions_per_sec = args.tuples_per_sec / MATERIALIZED_PER_GROUP * AUCTIONS_PER_GROUP;
        assert_eq!(persons_per_sec, 50_000);
        assert_eq!(auctions_per_sec, 150_000);
    }

    /// Regression test for the bug that killed the first calibration run.
    ///
    /// Both streams must cover the SAME span of event time in the same number of paced seconds.
    /// Because the pacer schedules on event time, the tuple counts per unit of event time differ
    /// (1 person : 3 auctions) while the event-time span does not. Pacing on tuples/sec instead
    /// made person's clock run 3x ahead of auction's, so the windowed join could never close a
    /// window and the engine died with BUFFER_EXHAUSTION.
    #[test]
    fn both_streams_advance_event_time_together() {
        let args = test_args();
        let span_ms = 10_000u64;

        let persons = (0..)
            .take_while(|&k| event_time_ms(person_event_index(k), args.events_per_sec) < span_ms)
            .count();
        let auctions = (0..)
            .take_while(|&k| event_time_ms(auction_event_index(k), args.events_per_sec) < span_ms)
            .count();

        // The Nexmark mix, recovered from the event-time span rather than assumed.
        let ratio = auctions as f64 / persons as f64;
        assert!(
            (ratio - 3.0).abs() < 0.01,
            "expected 1:3 person:auction, got 1:{ratio}"
        );

        // The last tuple of each stream lands in the same window, i.e. neither clock ran ahead.
        let last_person =
            event_time_ms(person_event_index(persons as u64 - 1), args.events_per_sec);
        let last_auction = event_time_ms(
            auction_event_index(auctions as u64 - 1),
            args.events_per_sec,
        );
        assert!(
            last_person.abs_diff(last_auction) < 10,
            "streams desynchronised: person at {last_person} ms, auction at {last_auction} ms"
        );
    }

    /// CSV over TCP is comma-delimited and newline-terminated: no field may contain either.
    #[test]
    fn rendered_lines_are_well_formed_csv() {
        let args = test_args();
        let mut w = LineWriter::new();
        for k in 0..1_000u64 {
            write_person(&mut w, k, &args);
            let line = std::str::from_utf8(&w.buf).unwrap();
            assert!(line.ends_with('\n'));
            assert_eq!(line.trim_end().split(',').count(), 8, "person: {line}");

            write_auction(&mut w, k, &args);
            let line = std::str::from_utf8(&w.buf).unwrap();
            assert!(line.ends_with('\n'));
            assert_eq!(line.trim_end().split(',').count(), 7, "auction: {line}");
        }
    }
}
