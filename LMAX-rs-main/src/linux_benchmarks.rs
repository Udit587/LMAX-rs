// ============================================================
//  linux_benchmarks.rs  —  drop into src/
//
//  Add to main.rs (top level, outside mod tests):
//      #[cfg(target_os = "linux")]
//      mod linux_benchmarks;
//
//  Cargo.toml [dependencies]:
//      libc = "0.2"
//
//  Run:
//      cargo test benchmark_linux --release -- --nocapture
// ============================================================

#![cfg(target_os = "linux")]
#![allow(non_snake_case)]

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::buffer_slot::inputSlot::InputSlot;
use crate::buffer_slot::outputSlot::OutputSlot;
use crate::consumer::blp::BusinessLogicProcessor;
use crate::consumer::outputConsumer::OutputConsumer;
use crate::consumer::unmarshallerConsumer::Unmarshaller;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;
use crate::single_producer::singleProducer::SingleProducer;

#[inline(always)]
fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn pin_thread(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut f: F) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return;
        }
        std::hint::spin_loop();
    }
    panic!("timeout waiting for pipeline to drain");
}

fn print_dist(v: &[u64]) {
    let n = v.len() as f64;
    let pct = |t: u64| v.iter().filter(|&&l| l < t).count() as f64 / n * 100.0;
    println!("Latency distribution:");
    println!("  <100 ns:   {:.2}%", pct(100));
    println!("  <500 ns:   {:.2}%", pct(500));
    println!("  <1 µs:     {:.2}%", pct(1_000));
    println!("  <2 µs:     {:.2}%", pct(2_000));
    println!("  <5 µs:     {:.2}%", pct(5_000));
    println!("  <10 µs:    {:.2}%", pct(10_000));
    println!("  <100 µs:   {:.2}%", pct(100_000));
}

fn collect_latencies(
    output_ring: &RingBuffer<OutputSlot>,
    output_sequence: &Sequence,
    send_times: &[u64],
) -> Vec<u64> {
    let mut out = Vec::with_capacity(send_times.len());
    for seq in 0..=output_sequence.get() {
        let slot = unsafe { output_ring.slot_ref(seq) };
        if slot.timestamp_ns > 0 {
            let idx = seq as usize;
            if idx < send_times.len()
                && send_times[idx] > 0
                && slot.timestamp_ns >= send_times[idx]
            {
                out.push(slot.timestamp_ns - send_times[idx]);
            }
        }
    }
    out
}

fn make_place_bytes(client_id: u64, client_order_id: u64, price: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(1u8);
    b.extend_from_slice(&client_id.to_le_bytes());
    b.extend_from_slice(&client_order_id.to_le_bytes());
    b.extend_from_slice(b"BTCUSD\0\0");
    b.push(0u8);
    b.push(0u8);
    b.push(1u8);
    b.extend_from_slice(&price.to_le_bytes());
    b.extend_from_slice(&10u64.to_le_bytes());
    b.push(0u8);
    b
}

fn make_cancel_bytes(client_id: u64, client_order_id: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(2u8);
    b.extend_from_slice(&client_id.to_le_bytes());
    b.extend_from_slice(&client_order_id.to_le_bytes());
    b
}

fn make_sided_place(client_id: u64, coid: u64, side: u8, price: u64, qty: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(1u8);
    b.extend_from_slice(&client_id.to_le_bytes());
    b.extend_from_slice(&coid.to_le_bytes());
    b.extend_from_slice(b"BTCUSD\0\0");
    b.push(side);
    b.push(0u8);
    b.push(1u8);
    b.extend_from_slice(&price.to_le_bytes());
    b.extend_from_slice(&qty.to_le_bytes());
    b.push(0u8);
    b
}

#[test]
fn benchmark_linux_pure_place_pinned() {
    pin_thread(0);

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(131072));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let blp_input_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&unmarshaller_sequence),
    );
    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&blp_input_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        131072,
    );
    let output_consumer = OutputConsumer::new(
        Arc::clone(&output_ring),
        Arc::clone(&output_gating_sequence),
    );

    let _u = unmarshaller.run();
    let _b = blp.run();
    let _oc = output_consumer.run();

    thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 100_000usize;
    let mut send_times = vec![0u64; total];

    let wall_start = Instant::now();

    for i in 0..total {
        let bytes = make_place_bytes(1, i as u64, 100 + (i % 50) as u64);
        send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    wait_until(Duration::from_secs(30), || {
        output_gating_sequence.get() >= total as i64 - 1
    });
    thread::sleep(Duration::from_millis(20));

    let wall_elapsed = wall_start.elapsed();
    let mut latencies_ns = collect_latencies(&output_ring, &output_gating_sequence, &send_times);
    latencies_ns.sort_unstable();

    let count = latencies_ns.len();
    assert!(count > 0, "no latency samples collected");

    let mean = latencies_ns.iter().sum::<u64>() / count as u64;
    let min = latencies_ns[0];
    let p50 = latencies_ns[count * 50 / 100];
    let p90 = latencies_ns[count * 90 / 100];
    let p99 = latencies_ns[count * 99 / 100];
    let p999 = latencies_ns[count * 999 / 1000];
    let max = *latencies_ns.last().unwrap();

    println!("\n=== [Linux/WSL] Pure Place Benchmark — CPU pinned ===");
    println!("Messages:       {}", total);
    println!("Samples:        {}", count);
    println!("Wall time:      {:.2?}", wall_elapsed);
    println!(
        "Throughput:     {:.0} ops/sec",
        total as f64 / wall_elapsed.as_secs_f64()
    );
    println!("─────────────────────────────────────────────────────");
    println!("Min latency:    {} ns", min);
    println!("Mean latency:   {} ns", mean);
    println!("p50:            {} ns", p50);
    println!("p90:            {} ns", p90);
    println!("p99:            {} ns", p99);
    println!("p999:           {} ns", p999);
    println!("Max:            {} ns", max);
    println!("─────────────────────────────────────────────────────");
    print_dist(&latencies_ns);
}

#[test]
fn benchmark_linux_mixed_workload_pinned() {
    pin_thread(0);

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(131072));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let blp_input_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&unmarshaller_sequence),
    );
    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&blp_input_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        131072,
    );
    let output_consumer = OutputConsumer::new(
        Arc::clone(&output_ring),
        Arc::clone(&output_gating_sequence),
    );

    let _u = unmarshaller.run();
    let _b = blp.run();
    let _oc = output_consumer.run();

    thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 100_000usize;
    let mut send_times = vec![0u64; total];
    let mut placed_ids: Vec<u64> = Vec::with_capacity(500);
    let mut next_client_order_id = 0u64;
    let mid = 120u64;

    let wall_start = Instant::now();

    for i in 0..total {
        let coid = next_client_order_id;
        next_client_order_id += 1;
        let bucket = i % 10;

        let bytes = match bucket {
            0 if !placed_ids.is_empty() => {
                let target_coid = placed_ids.remove(0);
                make_cancel_bytes(1, target_coid)
            }
            1 | 2 => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 {
                    placed_ids.remove(0);
                }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid + 5)
                } else {
                    (1u8, mid - 5)
                };
                make_sided_place(1, coid, side, price, 5)
            }
            _ => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 {
                    placed_ids.remove(0);
                }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid - 10 - (i as u64 % 5))
                } else {
                    (1u8, mid + 10 + (i as u64 % 5))
                };
                make_sided_place(1, coid, side, price, 3)
            }
        };

        send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    wait_until(Duration::from_secs(30), || {
        output_gating_sequence.get() >= total as i64 - 1
    });
    thread::sleep(Duration::from_millis(20));

    let wall_elapsed = wall_start.elapsed();
    let mut latencies_ns = collect_latencies(&output_ring, &output_gating_sequence, &send_times);
    latencies_ns.sort_unstable();

    let count = latencies_ns.len();
    assert!(count > 0, "no latency samples collected");

    let mean = latencies_ns.iter().sum::<u64>() / count as u64;
    let min = latencies_ns[0];
    let p50 = latencies_ns[count * 50 / 100];
    let p90 = latencies_ns[count * 90 / 100];
    let p99 = latencies_ns[count * 99 / 100];
    let p999 = latencies_ns[count * 999 / 1000];
    let max = *latencies_ns.last().unwrap();

    let passive_count = (0..total).filter(|i| i % 10 >= 3).count();
    let aggressive_count = (0..total).filter(|i| i % 10 == 1 || i % 10 == 2).count();
    let cancel_count = (0..total).filter(|i| i % 10 == 0).count();

    println!("\n=== [Linux/WSL] Mixed Workload Benchmark — CPU pinned ===");
    println!("Messages:         {}", total);
    println!("  Passive (70%):  {}", passive_count);
    println!("  Aggressive(20%):{}", aggressive_count);
    println!("  Cancel (10%):   {}", cancel_count);
    println!("Samples:          {}", count);
    println!("Output events:    {}", output_gating_sequence.get() + 1);
    println!("Wall time:        {:.2?}", wall_elapsed);
    println!(
        "Throughput:       {:.0} ops/sec",
        total as f64 / wall_elapsed.as_secs_f64()
    );
    println!("────────────────────────────────────────────────────────");
    println!("Min latency:      {} ns", min);
    println!("Mean latency:     {} ns", mean);
    println!("p50:              {} ns", p50);
    println!("p90:              {} ns", p90);
    println!("p99:              {} ns", p99);
    println!("p999:             {} ns", p999);
    println!("Max:              {} ns", max);
    println!("────────────────────────────────────────────────────────");
    print_dist(&latencies_ns);
}

#[test]
fn benchmark_linux_burst_5x100k_pinned() {
    pin_thread(0);

    let burst_size = 100_000usize;
    let num_bursts = 5usize;
    let mid = 120u64;

    println!(
        "\n=== [Linux/WSL] Burst Load Benchmark ({}×{}) — CPU pinned ===",
        num_bursts, burst_size
    );
    println!(
        "{:<8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Burst", "Throughput", "Mean", "p50", "p90", "p99", "p999", "Max"
    );
    println!("{}", "─".repeat(82));

    let mut global_latencies: Vec<u64> = Vec::with_capacity(burst_size * num_bursts);

    for burst in 0..num_bursts {
        let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
        let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(524288));

        let producer_sequence = Arc::new(Sequence::new(-1));
        let unmarshaller_sequence = Arc::new(Sequence::new(-1));
        let blp_input_sequence = Arc::new(Sequence::new(-1));
        let output_sequence = Arc::new(Sequence::new(-1));
        let output_gating_sequence = Arc::new(Sequence::new(-1));

        let unmarshaller_barrier =
            Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
        let blp_barrier =
            Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

        let unmarshaller = Unmarshaller::new(
            Arc::clone(&input_ring),
            Arc::clone(&unmarshaller_barrier),
            Arc::clone(&unmarshaller_sequence),
        );
        let blp = BusinessLogicProcessor::new(
            Arc::clone(&input_ring),
            Arc::clone(&blp_barrier),
            Arc::clone(&blp_input_sequence),
            Arc::clone(&output_ring),
            Arc::clone(&output_sequence),
            Arc::clone(&output_gating_sequence),
            100,
            1,
            524288,
        );
        let output_consumer = OutputConsumer::new(
            Arc::clone(&output_ring),
            Arc::clone(&output_gating_sequence),
        );

        let _u = unmarshaller.run();
        let _b = blp.run();
        let _oc = output_consumer.run();

        thread::sleep(Duration::from_millis(10));

        let mut producer = SingleProducer::new(
            Arc::clone(&input_ring),
            Arc::clone(&blp_input_sequence),
        );

        let mut send_times = vec![0u64; burst_size];
        let mut next_coid = (burst * burst_size) as u64;
        let mut placed_ids: Vec<u64> = Vec::with_capacity(500);

        let burst_start = Instant::now();

        for i in 0..burst_size {
            let coid = next_coid;
            next_coid += 1;
            let bucket = i % 10;

            let bytes = match bucket {
                0 if !placed_ids.is_empty() => {
                    let target = placed_ids.remove(0);
                    make_cancel_bytes(1, target)
                }
                1 | 2 => {
                    placed_ids.push(coid);
                    if placed_ids.len() > 500 {
                        placed_ids.remove(0);
                    }
                    let (side, price) = if i % 2 == 0 {
                        (0u8, mid + 5)
                    } else {
                        (1u8, mid - 5)
                    };
                    make_sided_place(1, coid, side, price, 5)
                }
                _ => {
                    placed_ids.push(coid);
                    if placed_ids.len() > 500 {
                        placed_ids.remove(0);
                    }
                    let (side, price) = if i % 2 == 0 {
                        (0u8, mid - 10 - (i as u64 % 5))
                    } else {
                        (1u8, mid + 10 + (i as u64 % 5))
                    };
                    make_sided_place(1, coid, side, price, 3)
                }
            };

            send_times[i] = now_ns();
            producer.publish(&bytes, &producer_sequence);
        }

        wait_until(Duration::from_secs(30), || {
            output_gating_sequence.get() >= burst_size as i64 - 1
        });
        thread::sleep(Duration::from_millis(20));

        let burst_elapsed = burst_start.elapsed();
        let mut burst_latencies =
            collect_latencies(&output_ring, &output_gating_sequence, &send_times);
        burst_latencies.sort_unstable();

        let count = burst_latencies.len();
        if count == 0 {
            println!("burst {}: no samples collected", burst + 1);
            continue;
        }

        global_latencies.extend_from_slice(&burst_latencies);

        let mean = burst_latencies.iter().sum::<u64>() / count as u64;
        let p50 = burst_latencies[count * 50 / 100];
        let p90 = burst_latencies[count * 90 / 100];
        let p99 = burst_latencies[count * 99 / 100];
        let p999 = burst_latencies[count * 999 / 1000];
        let max = *burst_latencies.last().unwrap();
        let tput = burst_size as f64 / burst_elapsed.as_secs_f64();

        println!(
            "{:<8} {:>10.0}/s {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            burst + 1, tput, mean, p50, p90, p99, p999, max
        );
    }

    global_latencies.sort_unstable();
    let count = global_latencies.len();

    if count > 0 {
        let mean = global_latencies.iter().sum::<u64>() / count as u64;
        let p50 = global_latencies[count * 50 / 100];
        let p90 = global_latencies[count * 90 / 100];
        let p99 = global_latencies[count * 99 / 100];
        let p999 = global_latencies[count * 999 / 1000];
        let max = *global_latencies.last().unwrap();

        println!("{}", "─".repeat(82));
        println!(
            "{:<8} {:>12} {:>10}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            "TOTAL", "", mean, p50, p90, p99, p999, max
        );
    }
}

#[test]
fn benchmark_linux_latency_over_time_pinned() {
    pin_thread(0);

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(524288));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let blp_input_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&unmarshaller_sequence),
    );
    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&blp_input_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        524288,
    );
    let output_consumer = OutputConsumer::new(
        Arc::clone(&output_ring),
        Arc::clone(&output_gating_sequence),
    );

    let _u = unmarshaller.run();
    let _b = blp.run();
    let _oc = output_consumer.run();

    thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 100_000usize;
    let batch_size = 10_000usize;
    let num_batches = total / batch_size;
    let mid = 120u64;

    let mut all_send_times = vec![0u64; total];
    let mut next_coid = 0u64;
    let mut placed_ids: Vec<u64> = Vec::with_capacity(500);

    for i in 0..total {
        let coid = next_coid;
        next_coid += 1;
        let bucket = i % 10;

        let bytes = match bucket {
            0 if !placed_ids.is_empty() => {
                let target = placed_ids.remove(0);
                make_cancel_bytes(1, target)
            }
            1 | 2 => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 {
                    placed_ids.remove(0);
                }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid + 5)
                } else {
                    (1u8, mid - 5)
                };
                make_sided_place(1, coid, side, price, 5)
            }
            _ => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 {
                    placed_ids.remove(0);
                }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid - 10 - (i as u64 % 5))
                } else {
                    (1u8, mid + 10 + (i as u64 % 5))
                };
                make_sided_place(1, coid, side, price, 3)
            }
        };

        all_send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    wait_until(Duration::from_secs(30), || {
        output_gating_sequence.get() >= total as i64 - 1
    });
    thread::sleep(Duration::from_millis(20));

    println!(
        "\n=== [Linux/WSL] Latency Over Time ({} batches of {}) — CPU pinned ===",
        num_batches, batch_size
    );
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Batch", "Mean", "p50", "p90", "p99", "p999", "Max"
    );
    println!("{}", "─".repeat(70));

    let mut global_latencies: Vec<u64> = Vec::with_capacity(total);

    for batch in 0..num_batches {
        let seq_start = (batch * batch_size) as i64;
        let seq_end = seq_start + batch_size as i64;

        let mut batch_latencies: Vec<u64> = Vec::with_capacity(batch_size);

        for seq in seq_start..seq_end.min(output_gating_sequence.get() + 1) {
            let slot = unsafe { output_ring.slot_ref(seq) };
            if slot.timestamp_ns > 0 {
                let idx = seq as usize;
                if idx < all_send_times.len()
                    && all_send_times[idx] > 0
                    && slot.timestamp_ns >= all_send_times[idx]
                {
                    let lat = slot.timestamp_ns - all_send_times[idx];
                    batch_latencies.push(lat);
                    global_latencies.push(lat);
                }
            }
        }

        batch_latencies.sort_unstable();

        let count = batch_latencies.len();
        if count == 0 {
            println!("batch {:>2}: no samples", batch + 1);
            continue;
        }

        let mean = batch_latencies.iter().sum::<u64>() / count as u64;
        let p50 = batch_latencies[count * 50 / 100];
        let p90 = batch_latencies[count * 90 / 100];
        let p99 = batch_latencies[count * 99 / 100];
        let p999 = batch_latencies[count * 999 / 1000];
        let max = *batch_latencies.last().unwrap();

        let marker = if batch == 0 { "  ← cold start" } else { "" };
        println!(
            "{:<10} {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns{}",
            batch + 1, mean, p50, p90, p99, p999, max, marker
        );
    }

    global_latencies.sort_unstable();
    let count = global_latencies.len();

    if count > 0 {
        let mean = global_latencies.iter().sum::<u64>() / count as u64;
        let p50 = global_latencies[count * 50 / 100];
        let p90 = global_latencies[count * 90 / 100];
        let p99 = global_latencies[count * 99 / 100];
        let p999 = global_latencies[count * 999 / 1000];
        let max = *global_latencies.last().unwrap();

        println!("{}", "─".repeat(70));
        println!(
            "{:<10} {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            "TOTAL", mean, p50, p90, p99, p999, max
        );
    }
}

#[test]
fn benchmark_linux_producer_ceiling_1m() {
    pin_thread(0);

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(65536));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(131072));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let blp_input_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier =
        Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&unmarshaller_sequence),
    );
    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&blp_input_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        131072,
    );
    let output_consumer = OutputConsumer::new(
        Arc::clone(&output_ring),
        Arc::clone(&output_gating_sequence),
    );

    let _u = unmarshaller.run();
    let _b = blp.run();
    let _oc = output_consumer.run();

    thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 1_000_000usize;

    let wall = Instant::now();
    for i in 0..total {
        let bytes = make_place_bytes(1, i as u64, 100 + (i % 50) as u64);
        producer.publish(&bytes, &producer_sequence);
    }

    wait_until(Duration::from_secs(30), || {
        output_gating_sequence.get() >= total as i64 - 1
    });

    let elapsed = wall.elapsed();
    let tput = total as f64 / elapsed.as_secs_f64();

    println!("\n=== [Linux/WSL] Producer Ceiling — 1M messages, CPU pinned ===");
    println!("Messages    : {}", total);
    println!("Wall time   : {:.3?}", elapsed);
    println!("Throughput  : {:.0} ops/sec  ({:.2}M/s)", tput, tput / 1_000_000.0);
    println!(
        "ns/message  : {:.1}",
        elapsed.as_nanos() as f64 / total as f64
    );
}