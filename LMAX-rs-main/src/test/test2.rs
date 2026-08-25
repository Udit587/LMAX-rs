#![allow(non_snake_case)]

pub mod ring_buffer;
pub mod single_producer;
pub mod sequence;
pub mod order;
pub mod buffer_slot;
pub mod consumer;
pub mod sequence_barrier;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};

use ring_buffer::ringBuffer::RingBuffer;
use buffer_slot::inputSlot::InputSlot;
use sequence::sequence::Sequence;
use sequence_barrier::sequenceBarrier::SequenceBarrier;
use single_producer::singleProducer::SingleProducer;
use consumer::jounrnalConsumer::JournalConsumer;
use consumer::replicatorConsumer::Replicator;
use consumer::unmarshallerConsumer::Unmarshaller;
use consumer::blp::BusinessLogicProcessor;

fn main() {
    test_benchmark2();
}

// ================================================================
// RDTSC — reads CPU time stamp counter directly, ~3 cycles overhead
// no syscall, no vDSO — the only sub-100ns clock on x86
// ================================================================
#[inline(always)]
fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

// calibrate TSC ticks → nanoseconds using Instant as reference
fn calibrate_tsc_ns_per_tick() -> f64 {
    // warm up branch predictor + instruction cache
    for _ in 0..1000 { let _ = rdtsc(); }

    let t0 = Instant::now();
    let c0 = rdtsc();
    std::thread::sleep(Duration::from_millis(200));
    let c1 = rdtsc();
    let t1 = Instant::now();

    let nanos = t1.duration_since(t0).as_nanos() as f64;
    let ticks = (c1 - c0) as f64;
    nanos / ticks
}

#[inline(always)]
fn publish_with_tsc(producer: &mut SingleProducer, producer_seq: &Arc<Sequence>) {
    let ts = rdtsc().to_le_bytes();
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&ts);
    producer.publish(&raw, producer_seq);
}

// ================================================================
// Test 4 — Latency + Throughput Benchmark
// Uses RDTSC for sub-nanosecond timestamp resolution.
// Both producer and BLP read from the same TSC — delta is pure
// in-process ring buffer propagation time.
// ================================================================
fn test_benchmark() {
    println!("\n=== TEST 4: Latency & Throughput Benchmark ===\n");

    const BUFFER_SIZE: usize = 64 * 1024;  // large — no producer backpressure
    const MSG_COUNT:   usize = 200_000;
    const WARMUP:      usize = 20_000;
    const TOTAL:       usize = MSG_COUNT + WARMUP;

    println!("Calibrating TSC...");
    let ns_per_tick = calibrate_tsc_ns_per_tick();
    println!("TSC rate: {:.3} ns/tick  ({:.0} MHz)\n", ns_per_tick, 1000.0 / ns_per_tick);

    let ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(BUFFER_SIZE));

    let journal_seq      = Sequence::new(-1);
    let replicator_seq   = Sequence::new(-1);
    let unmarshaller_seq = Sequence::new(-1);
    let blp_seq          = Sequence::new(-1);
    let producer_seq     = Sequence::new(-1);

    let journal_barrier      = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let replicator_barrier   = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let unmarshaller_barrier = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let blp_barrier = SequenceBarrier::new(vec![
        Arc::clone(&journal_seq),
        Arc::clone(&replicator_seq),
        Arc::clone(&unmarshaller_seq),
    ]);

    let mut producer = SingleProducer::new(Arc::clone(&ring), Arc::clone(&blp_seq));

    // lock-free pre-allocated storage — zero allocation / zero locking in hot path
    let latencies: Arc<Vec<AtomicU64>> = Arc::new(
        (0..MSG_COUNT).map(|_| AtomicU64::new(0)).collect()
    );

    // --- fast consumers: pure spin, no work ---
    { let r=Arc::clone(&ring); let s=Arc::clone(&journal_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; journal_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&replicator_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; replicator_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&unmarshaller_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; unmarshaller_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }

    // --- BLP: reads TSC timestamp, computes latency ---
    {
        let r    = Arc::clone(&ring);
        let s    = Arc::clone(&blp_seq);
        let lats = Arc::clone(&latencies);

        thread::spawn(move || {
            loop {
                let n = s.get() + 1;
                blp_barrier.wait_for(n);

                let slot = unsafe { r.slot_ref(n) };
                let ts_bytes: [u8; 8] = slot.raw_bytes[0..8].try_into().unwrap();
                let sent_tsc   = u64::from_le_bytes(ts_bytes);
                let now_tsc    = rdtsc();
                let delta_tsc  = now_tsc.saturating_sub(sent_tsc);
                let latency_ns = (delta_tsc as f64 * ns_per_tick) as u64;

                if n >= WARMUP as i64 {
                    let idx = (n - WARMUP as i64) as usize;
                    if idx < lats.len() {
                        lats[idx].store(latency_ns, Ordering::Relaxed);
                    }
                }

                s.set(n);
                if n >= TOTAL as i64 - 1 { break; }
            }
        });
    }

    // --- warmup ---
    println!("Warming up ({} messages)...", WARMUP);
    for _ in 0..WARMUP {
        publish_with_tsc(&mut producer, &producer_seq);
    }
    loop {
        if blp_seq.get() >= WARMUP as i64 - 1 { break; }
        std::hint::spin_loop();
    }

    // --- benchmark ---
    println!("Benchmarking ({} messages)...", MSG_COUNT);
    let bench_start = Instant::now();

    for _ in 0..MSG_COUNT {
        publish_with_tsc(&mut producer, &producer_seq);
    }

    loop {
        if blp_seq.get() >= TOTAL as i64 - 1 { break; }
        std::hint::spin_loop();
    }

    let elapsed = bench_start.elapsed();

    // --- collect off hot path, sort ---
    let mut lats: Vec<u64> = latencies
        .iter()
        .map(|a| a.load(Ordering::Relaxed))
        .collect();
    lats.sort_unstable();

    let count      = lats.len();
    let min        = lats[0];
    let max        = lats[count - 1];
    let mean       = lats.iter().sum::<u64>() / count as u64;
    let p50        = lats[count * 50  / 100];
    let p90        = lats[count * 90  / 100];
    let p95        = lats[count * 95  / 100];
    let p99        = lats[count * 99  / 100];
    let p999       = lats[count * 999 / 1000];
    let throughput = MSG_COUNT as f64 / elapsed.as_secs_f64();

    println!("\n╔══════════════════════════════════════╗");
    println!("║     BENCHMARK RESULTS                ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Messages     : {:>10}             ║", MSG_COUNT);
    println!("║ Total time   : {:>10.2?}           ║", elapsed);
    println!("║ Throughput   : {:>10.0} msg/sec    ║", throughput);
    println!("╠══════════════════════════════════════╣");
    println!("║ Latency (producer → BLP)             ║");
    println!("║ Min          : {:>10} ns           ║", min);
    println!("║ Mean         : {:>10} ns           ║", mean);
    println!("║ p50          : {:>10} ns           ║", p50);
    println!("║ p90          : {:>10} ns           ║", p90);
    println!("║ p95          : {:>10} ns           ║", p95);
    println!("║ p99          : {:>10} ns           ║", p99);
    println!("║ p99.9        : {:>10} ns           ║", p999);
    println!("║ Max          : {:>10} ns           ║", max);
    println!("╚══════════════════════════════════════╝");

    println!("\nLatency Distribution:");
    let buckets: &[(&str, u64, u64)] = &[
        ("   < 200ns",       0,       200),
        ("  200-500ns",    200,       500),
        (" 500ns-1µs",     500,     1_000),
        ("     1-5µs",   1_000,     5_000),
        ("    5-10µs",   5_000,    10_000),
        ("   10-50µs",  10_000,    50_000),
        ("  50-100µs",  50_000,   100_000),
        ("   > 100µs", 100_000, u64::MAX),
    ];
    for (label, low, high) in buckets {
        let c   = lats.iter().filter(|&&x| x >= *low && x < *high).count();
        let pct = c as f64 / count as f64 * 100.0;
        let bar = "█".repeat((pct / 1.5) as usize);
        println!("  {} | {:>6.2}% | {}", label, pct, bar);
    }

    // jitter = spread between consecutive latency samples
    let mut jitter: Vec<u64> = lats.windows(2)
        .map(|w| w[1].saturating_sub(w[0]))
        .collect();
    jitter.sort_unstable();
    println!("\nJitter p99  : {} ns", jitter[jitter.len() * 99  / 100]);
    println!("Jitter p99.9: {} ns", jitter[jitter.len() * 999 / 1000]);
}
fn test_benchmark2() {
    println!("\n=== TEST 4: Latency & Throughput Benchmark (Ping-Pong) ===\n");

    const BUFFER_SIZE: usize = 1024;
    const MSG_COUNT:   usize = 100_000;
    const WARMUP:      usize = 10_000;

    println!("Calibrating TSC...");
    let ns_per_tick = calibrate_tsc_ns_per_tick();
    println!("TSC rate: {:.3} ns/tick  ({:.0} MHz)\n", ns_per_tick, 1000.0 / ns_per_tick);

    let ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(BUFFER_SIZE));

    let journal_seq      = Sequence::new(-1);
    let replicator_seq   = Sequence::new(-1);
    let unmarshaller_seq = Sequence::new(-1);
    let blp_seq          = Sequence::new(-1);
    let producer_seq     = Sequence::new(-1);

    let journal_barrier      = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let replicator_barrier   = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let unmarshaller_barrier = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let blp_barrier = SequenceBarrier::new(vec![
        Arc::clone(&journal_seq),
        Arc::clone(&replicator_seq),
        Arc::clone(&unmarshaller_seq),
    ]);

    let mut producer = SingleProducer::new(Arc::clone(&ring), Arc::clone(&blp_seq));

    // fast consumers
    { let r=Arc::clone(&ring); let s=Arc::clone(&journal_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; journal_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&replicator_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; replicator_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&unmarshaller_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; unmarshaller_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }

    // BLP
    { let r=Arc::clone(&ring); let s=Arc::clone(&blp_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; blp_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }

    let total = MSG_COUNT + WARMUP;
    let mut latencies: Vec<u64> = Vec::with_capacity(MSG_COUNT);

    // ping-pong: publish one, wait for BLP to ack, record TSC delta
    for i in 0..total {
        let sent_tsc = rdtsc();
        publish_with_tsc(&mut producer, &producer_seq);

        // spin-wait for THIS exact message to be processed by BLP
        let expected = i as i64;
        loop {
            if blp_seq.get() >= expected { break; }
            std::hint::spin_loop();
        }

        let now_tsc    = rdtsc();
        let delta_tsc  = now_tsc.saturating_sub(sent_tsc);
        let latency_ns = (delta_tsc as f64 * ns_per_tick) as u64;

        if i >= WARMUP {
            latencies.push(latency_ns);
        }
    }

    // throughput — separate bulk run
    println!("Running throughput test ({} messages)...", MSG_COUNT);
    let bulk_start = Instant::now();
    for _ in 0..MSG_COUNT {
        publish_with_tsc(&mut producer, &producer_seq);
    }
    loop {
        if blp_seq.get() >= (total + MSG_COUNT) as i64 - 1 { break; }
        std::hint::spin_loop();
    }
    let throughput = MSG_COUNT as f64 / bulk_start.elapsed().as_secs_f64();

    latencies.sort_unstable();
    let count  = latencies.len();
    let min    = latencies[0];
    let max    = latencies[count - 1];
    let mean   = latencies.iter().sum::<u64>() / count as u64;
    let p50    = latencies[count * 50  / 100];
    let p90    = latencies[count * 90  / 100];
    let p95    = latencies[count * 95  / 100];
    let p99    = latencies[count * 99  / 100];
    let p999   = latencies[count * 999 / 1000];

    println!("\n╔══════════════════════════════════════╗");
    println!("║     BENCHMARK RESULTS                ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Throughput   : {:>10.0} msg/sec    ║", throughput);
    println!("╠══════════════════════════════════════╣");
    println!("║ Latency — ping-pong (1 msg at a time)║");
    println!("║ Min          : {:>10} ns           ║", min);
    println!("║ Mean         : {:>10} ns           ║", mean);
    println!("║ p50          : {:>10} ns           ║", p50);
    println!("║ p90          : {:>10} ns           ║", p90);
    println!("║ p95          : {:>10} ns           ║", p95);
    println!("║ p99          : {:>10} ns           ║", p99);
    println!("║ p99.9        : {:>10} ns           ║", p999);
    println!("║ Max          : {:>10} ns           ║", max);
    println!("╚══════════════════════════════════════╝");

    println!("\nLatency Distribution:");
    let buckets: &[(&str, u64, u64)] = &[
        ("   < 200ns",       0,       200),
        ("  200-500ns",    200,       500),
        (" 500ns-1µs",     500,     1_000),
        ("     1-5µs",   1_000,     5_000),
        ("    5-10µs",   5_000,    10_000),
        ("   10-50µs",  10_000,    50_000),
        ("  50-100µs",  50_000,   100_000),
        ("   > 100µs", 100_000, u64::MAX),
    ];
    for (label, low, high) in buckets {
        let c   = latencies.iter().filter(|&&x| x >= *low && x < *high).count();
        let pct = c as f64 / count as f64 * 100.0;
        let bar = "█".repeat((pct / 1.5) as usize);
        println!("  {} | {:>6.2}% | {}", label, pct, bar);
    }

    let mut jitter: Vec<u64> = latencies.windows(2)
        .map(|w| w[1].saturating_sub(w[0]))
        .collect();
    jitter.sort_unstable();
    println!("\nJitter p99  : {} ns", jitter[jitter.len() * 99  / 100]);
    println!("Jitter p99.9: {} ns", jitter[jitter.len() * 999 / 1000]);
}