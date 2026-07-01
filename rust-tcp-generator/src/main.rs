//! TCP producer for the over-time Prometheus benchmark.
//!
//! Listens on a contiguous port range, accepts arbitrary number of clients per port,
//! and on each accepted connection streams CSV lines `id,value,timestamp\n` as fast
//! as the consumer can read. `id` and `value` are drawn from a precomputed lookup
//! table (deterministic from --seed); `timestamp` is a per-connection monotonic
//! counter starting at 0 and incremented by 1 per emitted tuple.
//!
//! Designed to run as a sidecar to NebulaStream's combined-runtime container, joined
//! to the worker's network namespace via `--network container:<worker>`, so the worker
//! can dial 127.0.0.1:<port>.

use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "nes-tcp-gen",
    about = "TCP CSV row producer for NES benchmarks"
)]
struct Args {
    /// First port to bind. The producer listens on [port_base, port_base+num_ports).
    #[arg(long, default_value_t = 9100)]
    port_base: u16,

    /// Number of contiguous ports to bind starting at port_base.
    #[arg(long, default_value_t = 100)]
    num_ports: u16,

    /// Bind address. Inside a shared netns 0.0.0.0 covers both 127.0.0.1 and any
    /// other host-bound interfaces.
    #[arg(long, default_value = "0.0.0.0")]
    bind: IpAddr,

    /// Size of the (id, value) lookup table. Larger -> more variety, more startup work.
    #[arg(long, default_value_t = 65_536)]
    lookup_size: usize,

    /// PRNG seed for the lookup table. Same seed -> identical id/value sequence.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Target emission rate per connection in tuples/sec. 0 = unlimited (stream as fast as the
    /// consumer reads, the original behaviour). A finite rate decouples window-close cadence from
    /// raw throughput so windowed statistic queries close windows on a predictable wall-clock
    /// schedule (rate / window_size_in_tuples closes per second).
    #[arg(long, default_value_t = 0)]
    rate: u64,
}

/// Xorshift64* — small, deterministic, plenty of statistical quality for benchmark data.
struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x9E3779B97F4A7C15 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

fn build_lookup(seed: u64, size: usize) -> Vec<(u64, u64)> {
    let mut rng = Xorshift64::new(seed);
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        // id in [0, 4999], value in [0, 499_999] — matches the schema's intended range.
        let id = rng.next_u64() % 5_000;
        let value = rng.next_u64() % 500_000;
        out.push((id, value));
    }
    out
}

async fn handle_connection(
    sock: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    port: u16,
    lookup: Arc<Vec<(u64, u64)>>,
    rate: u64,
) {
    eprintln!("accept port={} peer={} rate={}", port, peer, rate);
    let _ = sock.set_nodelay(true);
    let mut writer = BufWriter::with_capacity(64 * 1024, sock);

    let mut ts: u64 = 0;
    let mut idx: usize = 0;
    let lookup_len = lookup.len();

    // Pacing for rate>0: emit in batches of ~1ms worth of tuples, flush, then sleep until the next
    // batch is due (against a fixed start instant, so pacing doesn't drift). rate==0 disables it.
    let batch = if rate == 0 { 0 } else { (rate / 1000).max(1) };
    let start = std::time::Instant::now();
    let mut emitted: u64 = 0;

    let mut line = [0u8; 64];
    let mut id_buf = itoa::Buffer::new();
    let mut val_buf = itoa::Buffer::new();
    let mut ts_buf = itoa::Buffer::new();

    loop {
        let (id, value) = lookup[idx % lookup_len];
        let id_s = id_buf.format(id).as_bytes();
        let val_s = val_buf.format(value).as_bytes();
        let ts_s = ts_buf.format(ts).as_bytes();

        let mut n = 0;
        line[n..n + id_s.len()].copy_from_slice(id_s);
        n += id_s.len();
        line[n] = b',';
        n += 1;
        line[n..n + val_s.len()].copy_from_slice(val_s);
        n += val_s.len();
        line[n] = b',';
        n += 1;
        line[n..n + ts_s.len()].copy_from_slice(ts_s);
        n += ts_s.len();
        line[n] = b'\n';
        n += 1;

        if let Err(e) = writer.write_all(&line[..n]).await {
            match e.kind() {
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::UnexpectedEof => {
                    eprintln!("disconnect port={} peer={} tuples={}", port, peer, ts);
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

        ts = ts.wrapping_add(1);
        idx = idx.wrapping_add(1);

        if rate > 0 {
            emitted += 1;
            if emitted % batch == 0 {
                // Flush so the consumer sees this batch before we sleep (BufWriter would otherwise
                // hold it), keeping the input cadence steady.
                if writer.flush().await.is_err() {
                    eprintln!("disconnect port={} peer={} tuples={}", port, peer, ts);
                    return;
                }
                let target = std::time::Duration::from_secs_f64(emitted as f64 / rate as f64);
                let elapsed = start.elapsed();
                if target > elapsed {
                    tokio::time::sleep(target - elapsed).await;
                }
            }
        }
    }
}

async fn run_listener(listener: TcpListener, port: u16, lookup: Arc<Vec<(u64, u64)>>, rate: u64) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let lookup = lookup.clone();
                tokio::spawn(async move {
                    handle_connection(sock, peer, port, lookup, rate).await;
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
    let lookup = Arc::new(build_lookup(args.seed, args.lookup_size));
    eprintln!(
        "lookup built: size={} seed={} (id range [0,5000), value range [0,500000))",
        lookup.len(),
        args.seed
    );

    let mut listeners = Vec::with_capacity(args.num_ports as usize);
    for i in 0..args.num_ports {
        let port = args.port_base + i;
        let addr = std::net::SocketAddr::new(args.bind, port);
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            eprintln!("bind failed addr={}: {}", addr, e);
            e
        })?;
        listeners.push((port, listener));
    }

    // READY handshake — the Python runner blocks on this line before submitting queries.
    println!("READY");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    eprintln!(
        "listening on {}:{}..{}",
        args.bind,
        args.port_base,
        args.port_base + args.num_ports - 1
    );

    for (port, listener) in listeners {
        let lookup = lookup.clone();
        let rate = args.rate;
        tokio::spawn(async move { run_listener(listener, port, lookup, rate).await });
    }

    // Wait for SIGINT (Ctrl-C) or SIGTERM (docker stop) and shut down.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { eprintln!("SIGINT received, shutting down"); }
        _ = sigterm.recv() => { eprintln!("SIGTERM received, shutting down"); }
    }

    Ok(())
}
