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

//! ClusterMonitoring TCP producer for the `statistic_overhead` benchmark. Sibling of
//! `rust-tcp-generator`, which serves a synthetic 3-field schema; this one emits the real benchmark
//! schema so the queries under test are the ones from the systests.
//!
//! One stream on `port_base`, matching `monitoringClusterData` in
//! `nes-systests/benchmark/ClusterMonitoring.test`, with one deliberate difference: `constraints` is
//! emitted as 0/1 for an INT16 column rather than a BOOLEAN. Query 2 never reads it, and this avoids
//! depending on how the CSV parser spells booleans.
//!
//!   creationTS,jobId,taskId,machineId,eventType,userId,category,priority,cpu,ram,disk,constraints
//!
//! Event time is a *virtual clock*, a pure function of the tuple index: `ts_ms = k * 1000 /
//! events_per_sec`. Wall-clock event time would be unusable, because the offered load is what the
//! benchmark varies — a burst would collapse into one window.
//!
//! The offered load is set with `--tuples-per-sec` and delivered by pacing against that virtual
//! clock: a tuple whose event time is T is due at `start + T/time_scale`. Leaving it at 0 disables
//! pacing, which is unsafe here — NebulaStream's buffer pool is global with no per-query fairness,
//! so the first query's source drains it and every other query starves with BUFFER_EXHAUSTION.
//!
//! Every connection is stateless and derives everything from its own counter, so N consumers on the
//! port each get an identical, reproducible stream — which is what lets ten queries share one
//! generator and still see the same data.

use std::io;
use std::net::IpAddr;

use clap::Parser;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;

/// Fraction of tuples carrying eventType == 3, the value ClusterMonitoring Q2 filters on. One in
/// four keeps a useful share of the stream flowing into the aggregation while leaving the WHERE
/// clause real work to do.
const EVENT_TYPE_MATCH_MODULUS: u64 = 4;
const EVENT_TYPE_MATCH: u64 = 3;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "nes-bench-gen",
    about = "ClusterMonitoring TCP CSV producer for NES benchmarks"
)]
struct Args {
    /// Port to bind.
    #[arg(long, default_value_t = 9200)]
    port_base: u16,

    /// Bind address. Inside a shared netns 0.0.0.0 covers 127.0.0.1 too.
    #[arg(long, default_value = "0.0.0.0")]
    bind: IpAddr,

    /// PRNG seed. Same seed -> identical stream.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Virtual clock: tuples per second of *event* time. Together with the query's window size this
    /// decides how many tuples fall into one window.
    #[arg(long, default_value_t = 100_000)]
    events_per_sec: u64,

    /// Size of the grouping-key domain: `jobId` cycles through [0, job_domain), which is also the
    /// equi-width histogram's max_value and the number of groups the aggregation tracks per open
    /// window. Lower it if aggregation state gets tight.
    #[arg(long, default_value_t = 10_000)]
    job_domain: u64,

    /// Offered load in tuples/sec per consumer. 0 = unlimited, which starves concurrent queries —
    /// see the module docs.
    #[arg(long, default_value_t = 0)]
    tuples_per_sec: u64,
}

impl Args {
    /// Milliseconds of event time per millisecond of wall clock. 0 = unlimited.
    ///
    /// One tuple per event here, so the wire rate maps straight onto the event-time speed.
    fn time_scale(&self) -> f64 {
        if self.tuples_per_sec == 0 {
            return 0.0;
        }
        self.tuples_per_sec as f64 / self.events_per_sec as f64
    }
}

/// SplitMix64 — stateless, deterministic, good avalanche. Used as a hash of the tuple index so
/// every derived field is a pure function of that index (no per-connection RNG state).
fn splitmix64(index: u64) -> u64 {
    let mut z = index.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Virtual event time in milliseconds for a tuple index.
fn event_time_ms(index: u64, events_per_sec: u64) -> u64 {
    index.saturating_mul(1000) / events_per_sec
}

struct LineWriter {
    buf: Vec<u8>,
    itoa: itoa::Buffer,
    ryu: ryu::Buffer,
}

impl LineWriter {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
            itoa: itoa::Buffer::new(),
            ryu: ryu::Buffer::new(),
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

    /// Callers keep values in [0.1, 1.0) so ryu never falls back to exponent notation, which the CSV
    /// parser would not accept.
    fn float(&mut self, value: f64) {
        let s = self.ryu.format(value);
        self.buf.extend_from_slice(s.as_bytes());
    }

    fn end(&mut self) {
        self.buf.push(b'\n');
    }
}

/// creationTS,jobId,taskId,machineId,eventType,userId,category,priority,cpu,ram,disk,constraints
/// Returns the tuple's event time in ms, which is what the pacer schedules against.
fn write_tuple(w: &mut LineWriter, k: u64, args: &Args) -> u64 {
    let h = splitmix64(k ^ args.seed);
    let ts = event_time_ms(k, args.events_per_sec);
    // cpu/ram/disk in [0.1, 1.0). The lower bound is not cosmetic: ryu switches to exponent notation
    // for very small magnitudes (0.000002 prints as "2e-6"), which the CSV parser would reject.
    let unit = |bits: u64| (100_000 + bits % 900_000) as f64 / 1_000_000.0;

    w.start();
    w.num(ts); // creationTS
    w.sep();
    w.num(k % args.job_domain); // jobId — the GROUP BY key and the histogram's subject
    w.sep();
    w.num(k); // taskId
    w.sep();
    w.num((h >> 8) % 100_000); // machineId
    w.sep();
    w.num(if h % EVENT_TYPE_MATCH_MODULUS == 0 {
        EVENT_TYPE_MATCH
    } else {
        (h >> 16) % 3 // any value other than the one Q2 selects
    });
    w.sep();
    w.num((h >> 24) % 1_000); // userId
    w.sep();
    w.num((h >> 32) % 10); // category
    w.sep();
    w.num((h >> 36) % 10); // priority
    w.sep();
    w.float(unit(h >> 40)); // cpu
    w.sep();
    w.float(unit(h >> 44)); // ram
    w.sep();
    w.float(unit(h >> 48)); // disk
    w.sep();
    w.num((h >> 52) & 1); // constraints, emitted 0/1 for an INT16 column
    w.end();
    ts
}

async fn handle_connection(sock: tokio::net::TcpStream, peer: std::net::SocketAddr, args: Args) {
    eprintln!(
        "accept peer={} tuples_per_sec={} time_scale={}",
        peer,
        args.tuples_per_sec,
        args.time_scale()
    );
    let _ = sock.set_nodelay(true);
    let mut writer = BufWriter::with_capacity(64 * 1024, sock);

    let mut w = LineWriter::new();
    let mut k: u64 = 0;

    // Event-time pacing. A tuple whose event time is T is due at start + T/time_scale. Measured
    // against a fixed start instant so the schedule cannot drift, and only actually sleeping once we
    // are more than a millisecond early — otherwise this would sleep per tuple.
    let start = std::time::Instant::now();
    let time_scale = args.time_scale();
    let paced = time_scale > 0.0;
    let slack = std::time::Duration::from_millis(1);

    loop {
        let event_time_ms = write_tuple(&mut w, k, &args);

        if let Err(e) = writer.write_all(&w.buf).await {
            match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof => {
                    eprintln!("disconnect peer={} tuples={}", peer, k);
                }
                _ => {
                    eprintln!("write error peer={} kind={:?}: {}", peer, e.kind(), e);
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
                    eprintln!("disconnect peer={} tuples={}", peer, k);
                    return;
                }
                tokio::time::sleep(due - elapsed).await;
            }
        }
    }
}

async fn run_listener(listener: TcpListener, args: Args) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let args = args.clone();
                tokio::spawn(async move {
                    handle_connection(sock, peer, args).await;
                });
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    eprintln!(
        "generator: seed={} events_per_sec={} job_domain={} tuples_per_sec={} (time_scale={})",
        args.seed,
        args.events_per_sec,
        args.job_domain,
        args.tuples_per_sec,
        args.time_scale()
    );

    let addr = std::net::SocketAddr::new(args.bind, args.port_base);
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        eprintln!("bind failed addr={}: {}", addr, e);
        e
    })?;

    // READY handshake — the Python runner blocks on this line before submitting queries.
    println!("READY");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    eprintln!("listening on {}", addr);

    tokio::spawn(async move { run_listener(listener, args).await });

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
            job_domain: 10_000,
            tuples_per_sec: 0,
        }
    }

    /// Every column ClusterMonitoring Q2 depends on has to hold: 12 well-formed CSV fields, jobId
    /// inside the histogram's declared bounds, a monotonic event clock, and enough eventType == 3
    /// tuples to get through the WHERE clause.
    #[test]
    fn tuples_are_well_formed() {
        let args = test_args();
        let mut w = LineWriter::new();
        let mut matches = 0usize;
        let mut previous_ts = 0u64;

        for k in 0..10_000u64 {
            let ts = write_tuple(&mut w, k, &args);
            let line = std::str::from_utf8(&w.buf).unwrap();
            assert!(line.ends_with('\n'));
            let fields: Vec<&str> = line.trim_end().split(',').collect();
            assert_eq!(fields.len(), 12, "{line}");

            assert_eq!(
                fields[0].parse::<u64>().unwrap(),
                ts,
                "creationTS must be the paced time"
            );
            assert!(ts >= previous_ts, "event time went backwards at k={k}");
            previous_ts = ts;

            let job_id: u64 = fields[1].parse().unwrap();
            assert!(job_id < args.job_domain, "jobId {job_id} outside the histogram bounds");

            if fields[4].parse::<u64>().unwrap() == EVENT_TYPE_MATCH {
                matches += 1;
            }
            // cpu/ram/disk must be plain decimals — ryu must not have used exponent form.
            for field in &fields[8..11] {
                assert!(!field.contains('e'), "float in exponent notation: {field}");
                let value: f64 = field.parse().unwrap();
                assert!((0.1..1.0).contains(&value), "float out of range: {value}");
            }
        }

        // ~25% by construction; assert loosely so the hash's exact spread doesn't make this brittle.
        let fraction = matches as f64 / 10_000.0;
        assert!((0.20..0.30).contains(&fraction), "eventType==3 fraction was {fraction}");
    }

    /// jobId must cycle through the whole domain, or the histogram would only ever see part of its
    /// declared range and the group count would not match what the query actually aggregates.
    #[test]
    fn job_ids_cover_the_domain() {
        let args = test_args();
        let mut seen = std::collections::HashSet::new();
        for k in 0..args.job_domain {
            seen.insert(k % args.job_domain);
        }
        assert_eq!(seen.len(), args.job_domain as usize);
    }

    /// One tuple per event, so the wire rate maps straight onto the event-time speed. 0 must mean
    /// unlimited rather than dividing by zero.
    #[test]
    fn wire_rate_maps_to_event_time_speed() {
        let mut args = test_args();
        assert_eq!(args.time_scale(), 0.0, "0 must mean unlimited");

        args.tuples_per_sec = 200_000;
        assert!((args.time_scale() - 2.0).abs() < 1e-9, "got {}", args.time_scale());

        args.tuples_per_sec = 50_000;
        assert!((args.time_scale() - 0.5).abs() < 1e-9, "got {}", args.time_scale());
    }
}
