#![allow(non_snake_case)]

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

pub mod ring_buffer;
pub mod sequence;
pub mod sequence_barrier;
pub mod buffer_slot;
pub mod order;
pub mod blp;
pub mod consumer;
pub mod output_event;
pub mod single_producer;
pub mod util;
// #[cfg(target_os = "linux")]
// mod linux_benchmarks;

use crate::buffer_slot::inputSlot::InputSlot;
use crate::buffer_slot::outputSlot::OutputSlot;
use crate::consumer::blp::BusinessLogicProcessor;
use crate::consumer::unmarshallerConsumer::Unmarshaller;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;

static START_INSTANT: OnceLock<Instant> = OnceLock::new();

pub fn now_ns() -> u64 {
    let start = START_INSTANT.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

fn main() {
    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(1024));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(1024));

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
        4096,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    println!("LMAX pipeline started. Run benchmark with:");
    println!("cargo test benchmark_lmax_end_to_end_latency --release -- --nocapture");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_event::outputEvent::OutputEvent;
    use crate::single_producer::singleProducer::SingleProducer;
    use hdrhistogram::Histogram;
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::thread;
    use std::time::{Duration, Instant};

    fn wait_until<F>(timeout: Duration, mut condition: F)
    where
        F: FnMut() -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timeout waiting for condition");
    }

    fn make_place_bytes(client_id: u64, client_order_id: u64, price: u64) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&client_id.to_le_bytes());
        b.extend_from_slice(&client_order_id.to_le_bytes());
        b.extend_from_slice(b"BTCUSD\0\0");
        b.push(0u8); // BUY
        b.push(0u8); // LIMIT
        b.push(1u8); // price present
        b.extend_from_slice(&price.to_le_bytes());
        b.extend_from_slice(&10u64.to_le_bytes());
        b.push(0u8); // GTC
        b
    }

    #[test]
    fn benchmark_lmax_end_to_end_latency() {
        let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(1024));
        let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(1024));

        let producer_sequence = Arc::new(Sequence::new(-1));
        let unmarshaller_sequence = Arc::new(Sequence::new(-1));
        let blp_input_sequence = Arc::new(Sequence::new(-1));
        let output_sequence = Arc::new(Sequence::new(-1));
        let output_gating_sequence = Arc::new(Sequence::new(-1));

        let unmarshaller_barrier =
            Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
        let blp_barrier =
            Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));
        let output_barrier =
            Arc::new(SequenceBarrier::new(vec![Arc::clone(&output_sequence)]));

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
            4096,
        );

        let _u = unmarshaller.run();
        let _b = blp.run();

        let total_messages = 100_000usize;
        let consumed = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let max_latency = Arc::new(AtomicU64::new(0));
        let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

        let out_ring = Arc::clone(&output_ring);
        let out_barrier = Arc::clone(&output_barrier);
        let out_gating = Arc::clone(&output_gating_sequence);
        let consumed_clone = Arc::clone(&consumed);
        let done_clone = Arc::clone(&done);
        let max_clone = Arc::clone(&max_latency);
        let hist_clone = Arc::clone(&hist);

        let consumer_handle = thread::spawn(move || {
            let mut next_seq = 0i64;

            while consumed_clone.load(Ordering::Acquire) < total_messages {
                out_barrier.wait_for(next_seq);

                let slot = unsafe { out_ring.slot_ref(next_seq) };

                if let Some(_event) = slot.event {
                    let end_ns = now_ns();
                    let start_ns = slot.timestamp_ns;
                    let latency = end_ns.saturating_sub(start_ns).max(1);

                    {
                        let mut h = hist_clone.lock().unwrap();
                        let _ = h.record(latency);
                    }

                    let mut prev = max_clone.load(Ordering::Relaxed);
                    while latency > prev {
                        match max_clone.compare_exchange(
                            prev,
                            latency,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(actual) => prev = actual,
                        }
                    }

                    consumed_clone.fetch_add(1, Ordering::Release);
                }

                out_gating.set(next_seq);
                next_seq += 1;
            }

            done_clone.store(true, Ordering::Release);
        });

        let mut producer = SingleProducer::new(
            Arc::clone(&input_ring),
            Arc::clone(&blp_input_sequence),
        );

        let start_wall = Instant::now();

        for i in 0..total_messages {
            let bytes = make_place_bytes(1, 1_000_000 + i as u64, 100 + (i as u64 % 16));
            producer.publish(&bytes, &producer_sequence);
        }

        wait_until(Duration::from_secs(20), || done.load(Ordering::Acquire));

        let elapsed = start_wall.elapsed();
        consumer_handle.join().unwrap();

        let h = hist.lock().unwrap();
        let throughput = total_messages as f64 / elapsed.as_secs_f64();

        println!("\n=== LMAX End-to-End Latency Benchmark ===");
        println!("messages   : {}", total_messages);
        println!("elapsed    : {:.3?}", elapsed);
        println!("throughput : {:.0} msg/s", throughput);
        println!("mean       : {:.0} ns", h.mean());
        println!("p50        : {} ns", h.value_at_quantile(0.50));
        println!("p90        : {} ns", h.value_at_quantile(0.90));
        println!("p99        : {} ns", h.value_at_quantile(0.99));
        println!("p99.9      : {} ns", h.value_at_quantile(0.999));
        println!("max        : {} ns", max_latency.load(Ordering::Relaxed));

        assert_eq!(consumed.load(Ordering::Acquire), total_messages);
        assert_eq!(unmarshaller_sequence.get(), total_messages as i64 - 1);
        assert_eq!(blp_input_sequence.get(), total_messages as i64 - 1);
    }
    #[test]
fn benchmark_full_pipeline_throughput_and_latency() {
    use crate::util::time::now_ns;
    let _ = now_ns();

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
        100, 1, 131072,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    // small sleep to let threads start up
    std::thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 100_000usize;
    let mut send_times = vec![0u64; total];

    let wall_start = std::time::Instant::now();

    for i in 0..total {
        let bytes = make_place_bytes(
            1,
            i as u64,
            100 + (i % 50) as u64,
        );
        send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    // wait for BLP to finish all inputs
    wait_until(Duration::from_secs(30), || {
        blp_input_sequence.get() >= total as i64 - 1
    });

    // small sleep to let last output events flush
    std::thread::sleep(Duration::from_millis(50));

    let wall_elapsed = wall_start.elapsed();

    // collect latencies
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(total);

    for seq in 0..=output_sequence.get() {
        let slot = unsafe { output_ring.slot_ref(seq) };
        if slot.timestamp_ns > 0 {
            let idx = seq as usize;
            if idx < send_times.len()
                && send_times[idx] > 0
                && slot.timestamp_ns >= send_times[idx]
            {
                latencies_ns.push(slot.timestamp_ns - send_times[idx]);
            }
        }
    }

    latencies_ns.sort_unstable();

    let count = latencies_ns.len();
    assert!(count > 0, "no latency samples collected — check timestamp threading");

    let mean = latencies_ns.iter().sum::<u64>() / count as u64;
    let p50  = latencies_ns[count * 50 / 100];
    let p90  = latencies_ns[count * 90 / 100];
    let p99  = latencies_ns[count * 99 / 100];
    let p999 = latencies_ns[count * 999 / 1000];
    let max  = *latencies_ns.last().unwrap();
    let min  = latencies_ns[0];

    let throughput = total as f64 / wall_elapsed.as_secs_f64();

    println!("=== LMAX Pipeline Benchmark ===");
    println!("Messages:       {}", total);
    println!("Samples:        {}", count);
    println!("Wall time:      {:.2?}", wall_elapsed);
    println!("Throughput:     {:.0} ops/sec", throughput);
    println!("───────────────────────────────");
    println!("Min latency:    {} ns", min);
    println!("Mean latency:   {} ns", mean);
    println!("p50:            {} ns", p50);
    println!("p90:            {} ns", p90);
    println!("p99:            {} ns", p99);
    println!("p999:           {} ns", p999);
    println!("Max:            {} ns", max);
    println!("───────────────────────────────");

    let under_100ns = latencies_ns.iter().filter(|&&l| l < 100).count();
    let under_500ns = latencies_ns.iter().filter(|&&l| l < 500).count();
    let under_1us   = latencies_ns.iter().filter(|&&l| l < 1_000).count();
    let under_2us   = latencies_ns.iter().filter(|&&l| l < 2_000).count();
    let under_5us   = latencies_ns.iter().filter(|&&l| l < 5_000).count();
    let under_10us  = latencies_ns.iter().filter(|&&l| l < 10_000).count();
    let under_100us = latencies_ns.iter().filter(|&&l| l < 100_000).count();

    println!("Latency distribution:");
    println!("  <100 ns:   {:.2}%", under_100ns  as f64 / count as f64 * 100.0);
    println!("  <500 ns:   {:.2}%", under_500ns  as f64 / count as f64 * 100.0);
    println!("  <1 µs:     {:.2}%", under_1us    as f64 / count as f64 * 100.0);
    println!("  <2 µs:     {:.2}%", under_2us    as f64 / count as f64 * 100.0);
    println!("  <5 µs:     {:.2}%", under_5us    as f64 / count as f64 * 100.0);
    println!("  <10 µs:    {:.2}%", under_10us   as f64 / count as f64 * 100.0);
    println!("  <100 µs:   {:.2}%", under_100us  as f64 / count as f64 * 100.0);
}
#[test]
fn benchmark_full_pipeline_mixed_workload() {
    use crate::util::time::now_ns;
    let _ = now_ns();

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
        100, 1, 131072,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    std::thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total = 100_000usize;
    let mut send_times = vec![0u64; total];

    // track placed client_order_ids so cancels have something to cancel
    // we keep a simple ring of the last 500 placed order ids
    let mut placed_ids: Vec<u64> = Vec::with_capacity(500);
    let mut next_client_order_id = 0u64;

    // mid price is 120, spread ±20 ticks
    // passive orders: priced away from mid (won't cross)
    // aggressive orders: priced to cross the opposite side
    let mid = 120u64;

    let wall_start = std::time::Instant::now();

    for i in 0..total {
        let coid = next_client_order_id;
        next_client_order_id += 1;

        // 70% normal passive, 20% aggressive crossing, 10% cancel
        let bucket = i % 10;

        let bytes = match bucket {
            // 10% cancel
            0 if !placed_ids.is_empty() => {
                // cancel oldest tracked order
                let target_coid = placed_ids.remove(0);
                let mut b = Vec::new();
                b.push(2u8);                                        // cancel tag
                b.extend_from_slice(&1u64.to_le_bytes());           // client_id
                b.extend_from_slice(&target_coid.to_le_bytes());    // client_order_id
                b
            }

            // 20% aggressive — crosses the spread
            1 | 2 => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 { placed_ids.remove(0); }
                let (side, price) = if i % 2 == 0 {
                    // aggressive buy — price above mid, hits resting sells
                    (0u8, mid + 5)
                } else {
                    // aggressive sell — price below mid, hits resting buys
                    (1u8, mid - 5)
                };
                let mut b = Vec::new();
                b.push(1u8);
                b.extend_from_slice(&1u64.to_le_bytes());
                b.extend_from_slice(&coid.to_le_bytes());
                b.extend_from_slice(b"BTCUSD\0\0");
                b.push(side);
                b.push(0u8);   // LIMIT
                b.push(1u8);   // price present
                b.extend_from_slice(&price.to_le_bytes());
                b.extend_from_slice(&5u64.to_le_bytes());  // qty 5
                b.push(0u8);   // GTC
                b
            }

            // 70% passive — priced away from mid, rests in book
            _ => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 { placed_ids.remove(0); }
                let (side, price) = if i % 2 == 0 {
                    // passive buy — below mid
                    (0u8, mid - 10 - (i as u64 % 5))
                } else {
                    // passive sell — above mid
                    (1u8, mid + 10 + (i as u64 % 5))
                };
                let mut b = Vec::new();
                b.push(1u8);
                b.extend_from_slice(&1u64.to_le_bytes());
                b.extend_from_slice(&coid.to_le_bytes());
                b.extend_from_slice(b"BTCUSD\0\0");
                b.push(side);
                b.push(0u8);   // LIMIT
                b.push(1u8);   // price present
                b.extend_from_slice(&price.to_le_bytes());
                b.extend_from_slice(&3u64.to_le_bytes());  // qty 3
                b.push(0u8);   // GTC
                b
            }
        };

        send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    // wait for BLP to finish all inputs
    wait_until(Duration::from_secs(30), || {
        blp_input_sequence.get() >= total as i64 - 1
    });

    std::thread::sleep(Duration::from_millis(50));

    let wall_elapsed = wall_start.elapsed();

    // collect latencies
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(total);

    for seq in 0..=output_sequence.get() {
        let slot = unsafe { output_ring.slot_ref(seq) };
        if slot.timestamp_ns > 0 {
            let idx = seq as usize;
            if idx < send_times.len()
                && send_times[idx] > 0
                && slot.timestamp_ns >= send_times[idx]
            {
                latencies_ns.push(slot.timestamp_ns - send_times[idx]);
            }
        }
    }

    latencies_ns.sort_unstable();

    let count = latencies_ns.len();
    assert!(count > 0, "no latency samples collected");

    let mean = latencies_ns.iter().sum::<u64>() / count as u64;
    let p50  = latencies_ns[count * 50 / 100];
    let p90  = latencies_ns[count * 90 / 100];
    let p99  = latencies_ns[count * 99 / 100];
    let p999 = latencies_ns[count * 999 / 1000];
    let max  = *latencies_ns.last().unwrap();
    let min  = latencies_ns[0];

    let throughput = total as f64 / wall_elapsed.as_secs_f64();

    // count actual workload breakdown
    let passive_count   = (0..total).filter(|i| i % 10 >= 3).count();
    let aggressive_count = (0..total).filter(|i| i % 10 == 1 || i % 10 == 2).count();
    let cancel_count    = (0..total).filter(|i| i % 10 == 0).count();

    println!("=== LMAX Pipeline Mixed Workload Benchmark ===");
    println!("Messages:         {}", total);
    println!("  Passive (70%):  {}", passive_count);
    println!("  Aggressive(20%):{}", aggressive_count);
    println!("  Cancel (10%):   {}", cancel_count);
    println!("Samples:          {}", count);
    println!("Output events:    {}", output_sequence.get() + 1);
    println!("Wall time:        {:.2?}", wall_elapsed);
    println!("Throughput:       {:.0} ops/sec", throughput);
    println!("──────────────────────────────────────────────");
    println!("Min latency:      {} ns", min);
    println!("Mean latency:     {} ns", mean);
    println!("p50:              {} ns", p50);
    println!("p90:              {} ns", p90);
    println!("p99:              {} ns", p99);
    println!("p999:             {} ns", p999);
    println!("Max:              {} ns", max);
    println!("──────────────────────────────────────────────");

    let under_100ns = latencies_ns.iter().filter(|&&l| l < 100).count();
    let under_500ns = latencies_ns.iter().filter(|&&l| l < 500).count();
    let under_1us   = latencies_ns.iter().filter(|&&l| l < 1_000).count();
    let under_2us   = latencies_ns.iter().filter(|&&l| l < 2_000).count();
    let under_5us   = latencies_ns.iter().filter(|&&l| l < 5_000).count();
    let under_10us  = latencies_ns.iter().filter(|&&l| l < 10_000).count();
    let under_100us = latencies_ns.iter().filter(|&&l| l < 100_000).count();

    println!("Latency distribution:");
    println!("  <100 ns:   {:.2}%", under_100ns  as f64 / count as f64 * 100.0);
    println!("  <500 ns:   {:.2}%", under_500ns  as f64 / count as f64 * 100.0);
    println!("  <1 µs:     {:.2}%", under_1us    as f64 / count as f64 * 100.0);
    println!("  <2 µs:     {:.2}%", under_2us    as f64 / count as f64 * 100.0);
    println!("  <5 µs:     {:.2}%", under_5us    as f64 / count as f64 * 100.0);
    println!("  <10 µs:    {:.2}%", under_10us   as f64 / count as f64 * 100.0);
    println!("  <100 µs:   {:.2}%", under_100us  as f64 / count as f64 * 100.0);
}
#[test]
fn benchmark_full_pipeline_burst_5x100k() {
    use crate::util::time::now_ns;
    let _ = now_ns();

    let burst_size  = 100_000usize;
    let num_bursts  = 5usize;
    let mid         = 120u64;

    println!("=== LMAX Pipeline Burst Load Benchmark ({}×{}) ===", num_bursts, burst_size);
    println!("{:<8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Burst", "Throughput", "Mean", "p50", "p90", "p99", "p999", "Max");
    println!("{}", "─".repeat(82));

    let mut global_latencies: Vec<u64> = Vec::with_capacity(burst_size * num_bursts);

    for burst in 0..num_bursts {
        // fresh pipeline per burst — avoids output ring overflow and offset confusion
        let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
        let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(524288));

        let producer_sequence       = Arc::new(Sequence::new(-1));
        let unmarshaller_sequence   = Arc::new(Sequence::new(-1));
        let blp_input_sequence      = Arc::new(Sequence::new(-1));
        let output_sequence         = Arc::new(Sequence::new(-1));
        let output_gating_sequence  = Arc::new(Sequence::new(-1));

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
            100, 1, 524288,
        );

        let _u = unmarshaller.run();
        let _b = blp.run();

        std::thread::sleep(Duration::from_millis(10));

        let mut producer = SingleProducer::new(
            Arc::clone(&input_ring),
            Arc::clone(&blp_input_sequence),
        );

        let mut send_times = vec![0u64; burst_size];
        let mut next_coid = (burst * burst_size) as u64;
        let mut placed_ids: Vec<u64> = Vec::with_capacity(500);

        let burst_start = std::time::Instant::now();

        for i in 0..burst_size {
            let coid = next_coid;
            next_coid += 1;

            let bucket = i % 10;

            let bytes = match bucket {
                0 if !placed_ids.is_empty() => {
                    let target = placed_ids.remove(0);
                    let mut b = Vec::new();
                    b.push(2u8);
                    b.extend_from_slice(&1u64.to_le_bytes());
                    b.extend_from_slice(&target.to_le_bytes());
                    b
                }
                1 | 2 => {
                    placed_ids.push(coid);
                    if placed_ids.len() > 500 { placed_ids.remove(0); }
                    let (side, price) = if i % 2 == 0 {
                        (0u8, mid + 5)
                    } else {
                        (1u8, mid - 5)
                    };
                    let mut b = Vec::new();
                    b.push(1u8);
                    b.extend_from_slice(&1u64.to_le_bytes());
                    b.extend_from_slice(&coid.to_le_bytes());
                    b.extend_from_slice(b"BTCUSD\0\0");
                    b.push(side);
                    b.push(0u8);
                    b.push(1u8);
                    b.extend_from_slice(&price.to_le_bytes());
                    b.extend_from_slice(&5u64.to_le_bytes());
                    b.push(0u8);
                    b
                }
                _ => {
                    placed_ids.push(coid);
                    if placed_ids.len() > 500 { placed_ids.remove(0); }
                    let (side, price) = if i % 2 == 0 {
                        (0u8, mid - 10 - (i as u64 % 5))
                    } else {
                        (1u8, mid + 10 + (i as u64 % 5))
                    };
                    let mut b = Vec::new();
                    b.push(1u8);
                    b.extend_from_slice(&1u64.to_le_bytes());
                    b.extend_from_slice(&coid.to_le_bytes());
                    b.extend_from_slice(b"BTCUSD\0\0");
                    b.push(side);
                    b.push(0u8);
                    b.push(1u8);
                    b.extend_from_slice(&price.to_le_bytes());
                    b.extend_from_slice(&3u64.to_le_bytes());
                    b.push(0u8);
                    b
                }
            };

            send_times[i] = now_ns();
            producer.publish(&bytes, &producer_sequence);
        }

        // wait for BLP to finish all inputs for this burst
        wait_until(Duration::from_secs(30), || {
            blp_input_sequence.get() >= burst_size as i64 - 1
        });

        std::thread::sleep(Duration::from_millis(20));

        let burst_elapsed = burst_start.elapsed();

        // collect latencies — output slots map 1:1 with input sequence
        let mut burst_latencies: Vec<u64> = Vec::with_capacity(burst_size);

        for seq in 0..=output_sequence.get() {
            let slot = unsafe { output_ring.slot_ref(seq) };
            if slot.timestamp_ns > 0 {
                let idx = seq as usize;
                if idx < send_times.len()
                    && send_times[idx] > 0
                    && slot.timestamp_ns >= send_times[idx]
                {
                    let lat = slot.timestamp_ns - send_times[idx];
                    burst_latencies.push(lat);
                    global_latencies.push(lat);
                }
            }
        }

        burst_latencies.sort_unstable();

        let count = burst_latencies.len();
        if count == 0 {
            println!("burst {}: no samples collected", burst + 1);
            continue;
        }

        let mean = burst_latencies.iter().sum::<u64>() / count as u64;
        let p50  = burst_latencies[count * 50 / 100];
        let p90  = burst_latencies[count * 90 / 100];
        let p99  = burst_latencies[count * 99 / 100];
        let p999 = burst_latencies[count * 999 / 1000];
        let max  = *burst_latencies.last().unwrap();
        let throughput = burst_size as f64 / burst_elapsed.as_secs_f64();

        println!("{:<8} {:>10.0}/s {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            burst + 1, throughput, mean, p50, p90, p99, p999, max);
    }

    // global summary
    global_latencies.sort_unstable();
    let count = global_latencies.len();

    if count > 0 {
        let mean = global_latencies.iter().sum::<u64>() / count as u64;
        let p50  = global_latencies[count * 50 / 100];
        let p90  = global_latencies[count * 90 / 100];
        let p99  = global_latencies[count * 99 / 100];
        let p999 = global_latencies[count * 999 / 1000];
        let max  = *global_latencies.last().unwrap();

        println!("{}", "─".repeat(82));
        println!("{:<8} {:>12} {:>10}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            "TOTAL", "", mean, p50, p90, p99, p999, max);
    }
}

#[test]
fn benchmark_full_pipeline_latency_over_time() {
    use crate::util::time::now_ns;
    let _ = now_ns();

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(4096));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(524288));

    let producer_sequence       = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence   = Arc::new(Sequence::new(-1));
    let blp_input_sequence      = Arc::new(Sequence::new(-1));
    let output_sequence         = Arc::new(Sequence::new(-1));
    let output_gating_sequence  = Arc::new(Sequence::new(-1));

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
        100, 1, 524288,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    std::thread::sleep(Duration::from_millis(10));

    let mut producer = SingleProducer::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_input_sequence),
    );

    let total      = 100_000usize;
    let batch_size = 10_000usize;
    let num_batches = total / batch_size;
    let mid        = 120u64;

    let mut all_send_times = vec![0u64; total];
    let mut next_coid = 0u64;
    let mut placed_ids: Vec<u64> = Vec::with_capacity(500);

    let wall_start = std::time::Instant::now();

    for i in 0..total {
        let coid = next_coid;
        next_coid += 1;

        let bucket = i % 10;

        let bytes = match bucket {
            0 if !placed_ids.is_empty() => {
                let target = placed_ids.remove(0);
                let mut b = Vec::new();
                b.push(2u8);
                b.extend_from_slice(&1u64.to_le_bytes());
                b.extend_from_slice(&target.to_le_bytes());
                b
            }
            1 | 2 => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 { placed_ids.remove(0); }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid + 5)
                } else {
                    (1u8, mid - 5)
                };
                let mut b = Vec::new();
                b.push(1u8);
                b.extend_from_slice(&1u64.to_le_bytes());
                b.extend_from_slice(&coid.to_le_bytes());
                b.extend_from_slice(b"BTCUSD\0\0");
                b.push(side);
                b.push(0u8);
                b.push(1u8);
                b.extend_from_slice(&price.to_le_bytes());
                b.extend_from_slice(&5u64.to_le_bytes());
                b.push(0u8);
                b
            }
            _ => {
                placed_ids.push(coid);
                if placed_ids.len() > 500 { placed_ids.remove(0); }
                let (side, price) = if i % 2 == 0 {
                    (0u8, mid - 10 - (i as u64 % 5))
                } else {
                    (1u8, mid + 10 + (i as u64 % 5))
                };
                let mut b = Vec::new();
                b.push(1u8);
                b.extend_from_slice(&1u64.to_le_bytes());
                b.extend_from_slice(&coid.to_le_bytes());
                b.extend_from_slice(b"BTCUSD\0\0");
                b.push(side);
                b.push(0u8);
                b.push(1u8);
                b.extend_from_slice(&price.to_le_bytes());
                b.extend_from_slice(&3u64.to_le_bytes());
                b.push(0u8);
                b
            }
        };

        all_send_times[i] = now_ns();
        producer.publish(&bytes, &producer_sequence);
    }

    wait_until(Duration::from_secs(30), || {
        blp_input_sequence.get() >= total as i64 - 1
    });

    std::thread::sleep(Duration::from_millis(50));

    let _wall_elapsed = wall_start.elapsed();

    println!("=== LMAX Pipeline Latency Over Time ({} batches of {}) ===",
        num_batches, batch_size);
    println!("{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Batch", "Mean", "p50", "p90", "p99", "p999", "Max");
    println!("{}", "─".repeat(70));

    let mut global_latencies: Vec<u64> = Vec::with_capacity(total);

    for batch in 0..num_batches {
        let seq_start = (batch * batch_size) as i64;
        let seq_end   = seq_start + batch_size as i64;

        let mut batch_latencies: Vec<u64> = Vec::with_capacity(batch_size);

        for seq in seq_start..seq_end.min(output_sequence.get() + 1) {
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
        let p50  = batch_latencies[count * 50 / 100];
        let p90  = batch_latencies[count * 90 / 100];
        let p99  = batch_latencies[count * 99 / 100];
        let p999 = batch_latencies[count * 999 / 1000];
        let max  = *batch_latencies.last().unwrap();

        println!("{:<10} {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            batch + 1, mean, p50, p90, p99, p999, max);
    }

    global_latencies.sort_unstable();
    let count = global_latencies.len();

    if count > 0 {
        let mean = global_latencies.iter().sum::<u64>() / count as u64;
        let p50  = global_latencies[count * 50 / 100];
        let p90  = global_latencies[count * 90 / 100];
        let p99  = global_latencies[count * 99 / 100];
        let p999 = global_latencies[count * 999 / 1000];
        let max  = *global_latencies.last().unwrap();

        println!("{}", "─".repeat(70));
        println!("{:<10} {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
            "TOTAL", mean, p50, p90, p99, p999, max);
    }
}
}