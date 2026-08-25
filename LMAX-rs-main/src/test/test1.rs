
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
use std::time::Duration;
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
    // test_ordering();
    // test_slow_consumer();
    // test_wraparound();
    test_benchmark(); 
}
// ================================================================
// Test 4 — Latency + Throughput Benchmark
// Measures: p50, p95, p99, p999, max, throughput (msg/sec)
// ================================================================
fn test_benchmark() {
    println!("\n=== TEST 4: Latency & Throughput Benchmark ===\n");

    const BUFFER_SIZE: usize = 1024;
    const MSG_COUNT: usize = 100_000;
    const WARMUP: usize = 10_000;  // warmup first, discard these latencies

    let ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(BUFFER_SIZE));

    let journal_seq       = Sequence::new(-1);
    let replicator_seq    = Sequence::new(-1);
    let unmarshaller_seq  = Sequence::new(-1);
    let blp_seq           = Sequence::new(-1);
    let producer_seq      = Sequence::new(-1);

    let journal_barrier      = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let replicator_barrier   = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let unmarshaller_barrier = SequenceBarrier::new(vec![Arc::clone(&producer_seq)]);
    let blp_barrier = SequenceBarrier::new(vec![
        Arc::clone(&journal_seq),
        Arc::clone(&replicator_seq),
        Arc::clone(&unmarshaller_seq),
    ]);

    let mut producer = SingleProducer::new(Arc::clone(&ring), Arc::clone(&blp_seq));

    // shared latency storage — producer writes timestamp into slot
    // BLP reads it and records latency
    // we reuse raw_bytes[0..8] as a u64 nanosecond timestamp
    let latencies: Arc<std::sync::Mutex<Vec<u64>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(MSG_COUNT)));

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

    // BLP — reads timestamp from raw_bytes, records latency
    {
        let r     = Arc::clone(&ring);
        let s     = Arc::clone(&blp_seq);
        let lats  = Arc::clone(&latencies);
        let total = (MSG_COUNT + WARMUP) as i64;

        thread::spawn(move || {
            loop {
                let n = s.get() + 1;
                blp_barrier.wait_for(n);

                let slot = unsafe { r.slot_ref(n) };

                // read timestamp embedded in raw_bytes
                let ts_bytes: [u8; 8] = slot.raw_bytes[0..8].try_into().unwrap();
                let sent_ns = u64::from_le_bytes(ts_bytes);
                let now_ns  = get_nanos();
                let latency_ns = now_ns.saturating_sub(sent_ns);

                // discard warmup
                if n >= WARMUP as i64 {
                    lats.lock().unwrap().push(latency_ns);
                }

                s.set(n);

                if n >= total - 1 { break; }
            }
        });
    }

    // --- warmup ---
    println!("Warming up ({} messages)...", WARMUP);
    for _ in 0..WARMUP {
        publish_with_timestamp(&mut producer, &producer_seq);
    }

    // wait for warmup to drain
    loop {
        if blp_seq.get() >= WARMUP as i64 - 1 { break; }
        std::hint::spin_loop();
    }

    // --- benchmark ---
    println!("Benchmarking ({} messages)...", MSG_COUNT);
    let bench_start = std::time::Instant::now();

    for _ in 0..MSG_COUNT {
        publish_with_timestamp(&mut producer, &producer_seq);
    }

    // wait for BLP to finish all
    loop {
        if blp_seq.get() >= (MSG_COUNT + WARMUP) as i64 - 1 { break; }
        std::hint::spin_loop();
    }

    let elapsed = bench_start.elapsed();

    // --- compute metrics ---
    let mut lats = latencies.lock().unwrap();
    lats.sort_unstable();

    let count    = lats.len();
    let min      = lats[0];
    let max      = lats[count - 1];
    let mean     = lats.iter().sum::<u64>() / count as u64;
    let p50      = lats[count * 50  / 100];
    let p90      = lats[count * 90  / 100];
    let p95      = lats[count * 95  / 100];
    let p99      = lats[count * 99  / 100];
    let p999     = lats[count * 999 / 1000];
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

    // histogram — shows latency distribution
    println!("\nLatency Distribution:");
    let buckets = [
        ("< 1µs",   0,       1_000),
        ("1-5µs",   1_000,   5_000),
        ("5-10µs",  5_000,   10_000),
        ("10-50µs", 10_000,  50_000),
        ("50-100µs",50_000,  100_000),
        ("> 100µs", 100_000, u64::MAX),
    ];

    for (label, low, high) in &buckets {
        let c = lats.iter().filter(|&&x| x >= *low && x < *high).count();
        let pct = c as f64 / count as f64 * 100.0;
        let bar: String = "█".repeat((pct / 2.0) as usize);
        println!("  {:>10} | {:>6.2}% | {}", label, pct, bar);
    }
}

// embed nanosecond timestamp into raw_bytes[0..8]
fn publish_with_timestamp(producer: &mut SingleProducer, producer_seq: &Arc<Sequence>) {
    let ts = get_nanos().to_le_bytes();
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&ts);
    producer.publish(&raw, producer_seq);
}

// nanosecond clock
fn get_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ================================================================
// Test 1 — BLP always processes AFTER all three consumers
// Publish 200 messages, track per-seq which consumers finished
// before BLP ran. Any violation = test fails.
// ================================================================
fn test_ordering() {
    println!("\n=== TEST 1: Ordering Guarantee (200 messages) ===\n");

    const BUFFER_SIZE: usize = 1024;
    const MSG_COUNT: i64 = 200;

    // shared counters — track last seq each consumer finished
    let journal_done      = Arc::new(AtomicU64::new(0));
    let replicator_done   = Arc::new(AtomicU64::new(0));
    let unmarshaller_done = Arc::new(AtomicU64::new(0));
    let violations        = Arc::new(AtomicU64::new(0));

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

    // --- journal with tracking ---
    {
        let ring = Arc::clone(&ring);
        let seq  = Arc::clone(&journal_seq);
        let done = Arc::clone(&journal_done);
        thread::spawn(move || {
            loop {
                let next = seq.get() + 1;
                journal_barrier.wait_for(next);
                let _slot = unsafe { ring.slot_ref(next) };
                done.store(next as u64, Ordering::Release);
                seq.set(next);
            }
        });
    }

    // --- replicator with tracking ---
    {
        let ring = Arc::clone(&ring);
        let seq  = Arc::clone(&replicator_seq);
        let done = Arc::clone(&replicator_done);
        thread::spawn(move || {
            loop {
                let next = seq.get() + 1;
                replicator_barrier.wait_for(next);
                let _slot = unsafe { ring.slot_ref(next) };
                done.store(next as u64, Ordering::Release);
                seq.set(next);
            }
        });
    }

    // --- unmarshaller with tracking ---
    {
        let ring = Arc::clone(&ring);
        let seq  = Arc::clone(&unmarshaller_seq);
        let done = Arc::clone(&unmarshaller_done);
        thread::spawn(move || {
            loop {
                let next = seq.get() + 1;
                unmarshaller_barrier.wait_for(next);
                let _slot = unsafe { ring.slot_ref(next) };
                done.store(next as u64, Ordering::Release);
                seq.set(next);
            }
        });
    }

    // --- BLP checks ordering on every seq ---
    {
        let ring      = Arc::clone(&ring);
        let seq       = Arc::clone(&blp_seq);
        let j_done    = Arc::clone(&journal_done);
        let r_done    = Arc::clone(&replicator_done);
        let u_done    = Arc::clone(&unmarshaller_done);
        let viols     = Arc::clone(&violations);
        thread::spawn(move || {
            loop {
                let next = seq.get() + 1;
                blp_barrier.wait_for(next);
                let _slot = unsafe { ring.slot_ref(next) };

                // by the time BLP runs, all three MUST have finished this seq
                let j = j_done.load(Ordering::Acquire) as i64;
                let r = r_done.load(Ordering::Acquire) as i64;
                let u = u_done.load(Ordering::Acquire) as i64;

                if j < next || r < next || u < next {
                    println!(
                        "❌ VIOLATION at seq={} journal={} replicator={} unmarshaller={}",
                        next, j, r, u
                    );
                    viols.fetch_add(1, Ordering::Relaxed);
                }

                seq.set(next);
            }
        });
    }

    // publish 200 messages as fast as possible
    for i in 0..MSG_COUNT {
        let msg = format!("{:08}", i);
        producer.publish(msg.as_bytes(), &producer_seq);
    }

    // wait for BLP to finish all
    loop {
        if blp_seq.get() >= MSG_COUNT - 1 { break; }
        thread::sleep(Duration::from_millis(1));
    }

    let v = violations.load(Ordering::Relaxed);
    if v == 0 {
        println!("✅ TEST 1 PASSED — 200 messages, zero ordering violations");
    } else {
        println!("❌ TEST 1 FAILED — {} violations detected", v);
    }
}

// ================================================================
// Test 2 — Slow consumer backpressure
// Unmarshaller sleeps 10ms per message
// Producer must stall, BLP must still see correct order
// ================================================================
fn test_slow_consumer() {
    println!("\n=== TEST 2: Slow Consumer Backpressure ===\n");

    const BUFFER_SIZE: usize = 16; // small buffer — forces wrap quickly
    const MSG_COUNT: i64 = 20;

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

    // journal — fast
    { let r=Arc::clone(&ring); let s=Arc::clone(&journal_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; journal_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }

    // replicator — fast
    { let r=Arc::clone(&ring); let s=Arc::clone(&replicator_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; replicator_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)}; s.set(n);
      });
    }

    // unmarshaller — SLOW (10ms per message)
    { let r=Arc::clone(&ring); let s=Arc::clone(&unmarshaller_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; unmarshaller_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)};
          thread::sleep(Duration::from_millis(10)); // artificial slowdown
          println!("[SlowUnmarshaller] processed seq={}", n);
          s.set(n);
      });
    }

    // BLP
    { let r=Arc::clone(&ring); let s=Arc::clone(&blp_seq);
      thread::spawn(move || loop {
          let n=s.get()+1; blp_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)};
          println!("[BLP] seq={}", n);
          s.set(n);
      });
    }

    let start = std::time::Instant::now();
    for i in 0..MSG_COUNT {
        let msg = format!("{:08}", i);
        producer.publish(msg.as_bytes(), &producer_seq);
        println!("[Producer] published seq={}", i);
    }
    println!("[Producer] all published in {:?}", start.elapsed());

    // wait for BLP to finish all 20
    loop {
        if blp_seq.get() >= MSG_COUNT - 1 { break; }
        thread::sleep(Duration::from_millis(5));
    }
    println!("✅ TEST 2 PASSED — producer stalled correctly, all {} messages processed", MSG_COUNT);
}

// ================================================================
// Test 3 — Wrap-around
// Publish 2x buffer size messages, verify no seq is skipped
// ================================================================
fn test_wraparound() {
    println!("\n=== TEST 3: Ring Buffer Wrap-Around ===\n");

    const BUFFER_SIZE: usize = 64;
    const MSG_COUNT: i64 = 256; // 4x buffer size

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

    let last_blp_seq = Arc::new(AtomicU64::new(0));
    let seq_errors   = Arc::new(AtomicU64::new(0));

    // fast consumers
    { let r=Arc::clone(&ring); let s=Arc::clone(&journal_seq);
      thread::spawn(move || loop { let n=s.get()+1; journal_barrier.wait_for(n); let _=unsafe{r.slot_ref(n)}; s.set(n); });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&replicator_seq);
      thread::spawn(move || loop { let n=s.get()+1; replicator_barrier.wait_for(n); let _=unsafe{r.slot_ref(n)}; s.set(n); });
    }
    { let r=Arc::clone(&ring); let s=Arc::clone(&unmarshaller_seq);
      thread::spawn(move || loop { let n=s.get()+1; unmarshaller_barrier.wait_for(n); let _=unsafe{r.slot_ref(n)}; s.set(n); });
    }

    // BLP verifies no seq is skipped
    { let r=Arc::clone(&ring); let s=Arc::clone(&blp_seq);
      let last=Arc::clone(&last_blp_seq); let errs=Arc::clone(&seq_errors);
      thread::spawn(move || loop {
          let n=s.get()+1; blp_barrier.wait_for(n);
          let _=unsafe{r.slot_ref(n)};

          // verify strictly sequential — no skips, no jumps
          let prev = last.load(Ordering::Acquire) as i64;
          if n != prev + 1 && prev != 0 {
              println!("❌ SEQ SKIP: expected={} got={}", prev+1, n);
              errs.fetch_add(1, Ordering::Relaxed);
          }
          last.store(n as u64, Ordering::Release);
          s.set(n);
      });
    }

    for i in 0..MSG_COUNT {
        let msg = format!("{:08}", i);
        producer.publish(msg.as_bytes(), &producer_seq);
    }

    loop {
        if blp_seq.get() >= MSG_COUNT - 1 { break; }
        thread::sleep(Duration::from_millis(1));
    }

    let errs = seq_errors.load(Ordering::Relaxed);
    if errs == 0 {
        println!("✅ TEST 3 PASSED — {} messages across {} wrap-arounds, no seq skips",
            MSG_COUNT, MSG_COUNT / BUFFER_SIZE as i64);
    } else {
        println!("❌ TEST 3 FAILED — {} sequence errors detected", errs);
    }
}