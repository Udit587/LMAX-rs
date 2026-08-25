# LMAX Disruptor — Matching Engine - Rust

A low-latency, high-throughput order matching engine built on the LMAX Disruptor
pattern in Rust. This project implements the core pipeline architecture described
in the original LMAX technical papers, with a bitmap + arena allocator matching
engine as the business logic processor.

---

## Architecture
```text
Input Ring Buffer
                    ┌─────────────────────────────────┐
                    │         Ring Slots [0..N]        │
                    └─────────────────────────────────┘
                          ▲               │
                          │               │
                    Single Producer       │
                    (wire bytes in)       │
                                          │
                ┌─────────────────────────┼─────────────────────────┐
                │                         │                         │
           Unmarshaller            Replicator*              Journal*
           (parses bytes           (hot standby)           (durable log)
            → Command)
                │                         │                         │
                └─────────────────────────┼─────────────────────────┘
                                          │ (all three must advance
                                          │  before BLP can proceed)
                                          ▼
                                Business Logic Processor
                                (order book matching engine)
                                          │
                                          ▼
                                Output Ring Buffer
                                ┌─────────────────┐
                                │  OutputEvents   │
                                └─────────────────┘
                                          │
                                          ▼
                                  Output Consumer
                                  (drains slots,
                                   advances gating seq)
```
> `*` Replicator and Journal are stubbed — crash recovery and replication
> are planned for a future layer.

---

## Key Design Decisions

### Disruptor pipeline

The input ring buffer has a single producer and four consumers —
Unmarshaller, Replicator, Journal, and BLP. BLP waits for all three
upstream consumers to advance their sequences before processing each
slot. This is the core sequencing guarantee of the LMAX architecture.

### Bitmap + arena matching engine

The order book uses a flat array of price levels indexed by tick,
with a bitmap to track which ticks have resting orders. Best bid/ask
lookup is O(1) via a single BSR/BSF instruction on the bitmap word.
Orders are stored in a pre-allocated arena with generational IDs for
O(1) ABA-safe cancellation. No heap allocation occurs on the hot path.

### Single producer

The input ring has exactly one producer. Backpressure is enforced by
spinning until the slowest consumer (BLP) has advanced far enough that
the slot being written to is safe to overwrite.

### Zero-copy sequence discipline

All inter-thread coordination happens through atomic sequence numbers.
Slots are accessed via raw pointers — the sequence discipline enforces
that no two threads ever access the same slot concurrently. No locks,
no mutexes, no channels.

---

## Order Lifecycle
```text
Client wire bytes
│
▼
Unmarshaller ──► Command::Place / Cancel / Modify
│
▼
BLP (BusinessLogicProcessor)
│
├── process_place  ──► OrderAccepted / OrderRejected / Fill
├── process_cancel ──► OrderCancelled / CancelRejected
└── process_modify ──► OrderModified / ModifyRejected
│
▼
Output Ring Buffer
│
▼
Output Consumer
```

### Supported commands

| Command | Wire tag | Fields |
|---------|----------|--------|
| Place   | `0x01`   | client\_id, client\_order\_id, symbol, side, order\_type, price?, quantity, time\_in\_force |
| Cancel  | `0x02`   | client\_id, client\_order\_id |
| Modify  | `0x03`   | client\_id, client\_order\_id, new\_price?, new\_qty? |

### Output events

| Event | Trigger |
|-------|---------|
| `OrderAccepted` | Limit order rested in book |
| `OrderRejected` | Invalid price, qty, arena full, or client limit exceeded |
| `Fill` | Two orders crossed |
| `OrderCancelled` | Cancel succeeded, or market order unfilled residual |
| `CancelRejected` | Order not found or already filled |
| `OrderModified` | Modify succeeded |
| `ModifyRejected` | Order not found, invalid new price/qty |

---

## Test Coverage

### Benchmark Hardware
- **Windows 11**, Intel Core i5-12450H (8 cores, 4.4GHz turbo)
- **Rust stable**, `--release` (LTO + codegen-units=1)
- **No special tuning** — stock Windows timer granularity (~100ns)

### Pipeline mechanics

| Test | What it proves |
|------|----------------|
| `diagnostic_ring_wraparound_correctness` | Ring slots reused correctly after consumer advances |
| `diagnostic_ring_wraparound_backpressure` | Producer blocks when ring is full, unblocks when consumer advances |
| `diagnostic_blp_waits_for_all_three` | BLP does not advance until all three upstream sequences advance |
| `diagnostic_full_pipeline_one_step` | End-to-end place order through full pipeline |
| `diagnostic_full_pipeline_two_steps_in_order` | Two sequential messages preserve order |
| `diagnostic_output_consumer_advances_gating_sequence` | Output consumer correctly advances gating sequence |
| `diagnostic_output_consumer_wraparound` | Output consumer handles ring wraparound |
| `diagnostic_full_pipeline_cancel_reaches_output` | Cancel flows end-to-end through unmarshaller → BLP → output ring |
| `diagnostic_full_pipeline_modify_reaches_output` | Modify flows end-to-end through unmarshaller → BLP → output ring |

### Business logic

| Test | What it proves |
|------|----------------|
| `diagnostic_process_place_emits_accept_event_directly` | Place order emits OrderAccepted |
| `diagnostic_cancel_produces_cancelled_event` | Cancel by client\_order\_id emits OrderCancelled |
| `diagnostic_cancel_unknown_order_produces_reject` | Cancel of unknown order emits CancelRejected |
| `diagnostic_cancel_after_full_fill_rejected` | Cancel of fully filled order emits CancelRejected |
| `diagnostic_modify_price_change_loses_queue_priority` | Price change causes order to lose queue priority |
| `diagnostic_modify_qty_reduction_keeps_priority` | Qty reduction in place keeps queue priority |
| `diagnostic_modify_unknown_order_rejected` | Modify of unknown order emits ModifyRejected |
| `diagnostic_matching_two_crossing_orders_produce_fill` | Two crossing orders emit Fill event |
| `diagnostic_partial_fill_leaves_residual` | Partial fill leaves correct residual in book |
| `diagnostic_market_order_rejects_on_empty_book` | Market order on empty book emits OrderCancelled |
| `diagnostic_client_open_order_limit_enforced` | Client order limit enforced correctly |

### Stress tests

| Test | What it proves |
|------|----------------|
| `diagnostic_stress_burst_1000_messages_with_single_producer` | 1000 messages, order preserved, no drops |
| `diagnostic_stress_burst_with_delayed_blp` | Pipeline correct under producer/consumer speed mismatch |
| `diagnostic_stress_burst_with_true_blp_stall` | Pipeline drains correctly after BLP stall |
| `soak_test_mixed_workload_under_pressure` | 10,000 mixed place/cancel/modify messages, output fully drained |

---

## Benchmark Results

All benchmarks run on:
- Windows 11, Intel Core i5-12450H
- Rust `--release`
- 100,000 messages per run

> Note: Windows `Instant` has ~100ns timer resolution. True p50 may be
> slightly lower than reported. Values below 100ns snap to 0ns or 100ns.

### Pure place benchmark

100,000 limit orders, all buys at prices 100–149.

| Metric        | Value        |
|--------------|-------------|
| Throughput   | 698,566 ops/sec |
| Mean latency | 155 ns      |
| p50          | 100 ns      |
| p90          | 200 ns      |
| p99          | 200 ns      |
| p999         | 400 ns      |
| Max          | 3,564 µs (OS scheduler spike) |


####Latency distribution:
| Range     | Percentage |
|----------|-----------|
| <100 ns  | 11.43%    |
| <500 ns  | 99.94%    |
| <1 µs    | 99.97%    |
| <100 µs  | 100.00%   |
---

### Mixed workload benchmark

70% passive limit orders, 20% aggressive crossing orders, 10% cancels.

| Metric        | Value        |
|--------------|-------------|
| Throughput   | 1,078,061 ops/sec |
| Mean latency | 63 ns       |
| p50          | 100 ns      |
| p90          | 100 ns      |
| p99          | 100 ns      |
| p999         | 200 ns      |
| Max          | 59 µs       |

####Latency distribution:
| Range     | Percentage |
|----------|-----------|
| <100 ns  | 38.57%    |
| <500 ns  | 99.98%    |
| <1 µs    | 99.99%    |
| <100 µs  | 100.00%   |

---

### Burst load — 5 × 100,000 messages

Same 70/20/10 mixed workload, 5 consecutive bursts.
| Burst | Throughput (ops/sec) | Mean | p50 | p90 | p99 | p999 | Max |
|------|----------------------|------|-----|-----|-----|------|------|
| 1 | 1,288,348 | 173 ns | 100 ns | 100 ns | 100 ns | 200 ns | 12.5 ms |
| 2 | 1,239,171 | 211 ns | 100 ns | 100 ns | 100 ns | 100 ns | 14.9 ms |
| 3 | 1,214,600 | 201 ns | 100 ns | 100 ns | 100 ns | 100 ns | 14.5 ms |
| 4 | 918,684   | 338 ns | 100 ns | 100 ns | 100 ns | 100 ns | 13.1 ms |
| 5 | 568,813   | 810 ns | 100 ns | 100 ns | 100 ns | 100 ns | 47.5 ms |
| **TOTAL** | — | **347 ns** | 100 ns | 100 ns | 100 ns | 100 ns | 47.5 ms |

p50, p90, p99, and p999 are completely flat across all 5 bursts.
Throughput degradation in bursts 4–5 is a benchmark artifact — each
burst spawns fresh threads while previous burst threads remain alive
and compete for CPU cores. In a real deployment with a single
long-running pipeline this degradation does not occur.

### Latency over time — 10 batches of 10,000
| Batch | Mean | p50 | p90 | p99 | p999 | Max |
|------|------|-----|-----|-----|------|------|
| 1 | 587 ns | 0 ns | 100 ns | 100 ns | 200 ns | 5.3 ms |
| 2 | 54 ns  | 100 ns | 100 ns | 100 ns | 200 ns | 2.3 µs |
| 3 | 44 ns  | 0 ns | 100 ns | 100 ns | 200 ns | 400 ns |
| 4 | 61 ns  | 100 ns | 100 ns | 100 ns | 300 ns | 4.4 µs |
| 5 | 64 ns  | 100 ns | 100 ns | 100 ns | 300 ns | 18.0 µs |
| 6 | 67 ns  | 100 ns | 100 ns | 300 ns | 800 ns | 14.0 µs |
| 7 | 58 ns  | 100 ns | 100 ns | 100 ns | 200 ns | 700 ns |
| 8 | 56 ns  | 100 ns | 100 ns | 100 ns | 100 ns | 400 ns |
| 9 | 54 ns  | 100 ns | 100 ns | 100 ns | 200 ns | 500 ns |
| 10| 53 ns  | 100 ns | 100 ns | 100 ns | 100 ns | 200 ns |
| **TOTAL** | **110 ns** | 100 ns | 100 ns | 100 ns | 400 ns | 5.3 ms |

p99 is flat at 100ns from batch 2 onwards. Batch 1 spike is cold
start — caches and OS scheduler settling.

### Comparison — standalone matching engine vs full pipeline

| Metric | Standalone bitmap | Pipeline pure | Pipeline mixed |
|--------|-------------------|---------------|----------------|
| Throughput | 6,055,039/s | 698,566/s | 1,078,061/s |
| Mean | 117ns | 155ns | 63ns |
| p50 | 100ns | 100ns | 100ns |
| p90 | 200ns | 200ns | 100ns |
| p99 | 600ns | 200ns | 100ns |
| p999 | 1,600ns | 400ns | 200ns |
| Max | 210µs | 3.5ms | 59µs |
| p99 stability | stable | stable | stable |

The throughput difference between standalone and pipeline is the cost
of disruptor coordination — three threads, two sequence barriers,
atomic sequence tracking. The standalone number is an upper bound with
zero coordination overhead.

p99 and p999 are better in the pipeline mixed workload than in the
standalone benchmark because the mixed workload keeps the book
shallower — cancels remove resting orders and aggressive orders get
matched immediately, reducing average matching sweep length.

## Performance Context

1.3M msg/sec end-to-end (publish→unmarshaller→BLP→consume) compares to:

| System | Ops/sec | Notes |
|--------|---------|-------|
| **Your LMAX** | 1.3M | Full pipeline, Windows i5 |
| **Your bitmap** | 6.0M | Pure matching logic only |
| **Java Disruptor** | 6–50M | Single producer → single consumer, Linux [1] |
| **Production HFT** | 10–100M | Dedicated hardware, FPGA [2] |

**Pipeline adds ~4x coordination overhead** vs pure matching, which is expected.

---

## Project Structure
```text
src/
├── main.rs                    — pipeline wiring and all tests
├── ring_buffer/
│   └── ringBuffer.rs          — ring buffer with power-of-two indexing
├── sequence/
│   └── sequence.rs            — atomic sequence number
├── sequence_barrier/
│   └── sequenceBarrier.rs     — multi-dependency barrier (min of N sequences)
├── single_producer/
│   └── singleProducer.rs      — single producer with backpressure
├── buffer_slot/
│   ├── inputSlot.rs           — input ring slot (raw bytes + parsed command)
│   └── outputSlot.rs          — output ring slot (output event + timestamp)
├── consumer/
│   ├── unmarshallerConsumer.rs — parses wire bytes → Command
│   ├── blp.rs                 — business logic processor
│   ├── outputConsumer.rs      — drains output ring
│   ├── jounrnalConsumer.rs    — stubbed (crash recovery — future layer)
│   └── replicatorConsumer.rs  — stubbed (hot standby — future layer)
├── blp/
│   ├── book.rs                — order book (bids + asks)
│   ├── handlers.rs            — process_place, process_cancel, process_modify
│   ├── matching.rs            — match_aggressor (crossing logic)
│   ├── publisher.rs           — publish_output to output ring
│   ├── arena.rs               — generational arena allocator
│   └── order_id.rs            — OrderId (index + generation)
├── order/
│   ├── command.rs             — Command enum (Place/Cancel/Modify)
│   ├── order.rs               — Order struct
│   ├── side.rs                — Side (BUY/SELL)
│   ├── orderType.rs           — OrderType (LIMIT/MARKET)
│   └── timeInForce.rs         — TimeInForce (GTC/IOC/FOK)
├── output_event/
│   ├── outputEvent.rs         — OutputEvent enum
│   └── rejectReason.rs        — RejectReason enum
└── util/
    └── time.rs                — now_ns() monotonic timestamp---
```
## Running Tests

```bash
# run all tests
cargo test -- --nocapture

# run a specific test
cargo test diagnostic_full_pipeline_one_step -- --nocapture

# run all benchmarks
cargo test benchmark --release -- --nocapture
```

---

## What's Next

This project is built in deliberate layers. The core matching engine
and pipeline are complete. Future layers:

- [ ] **Crash recovery** — journal consumer writes WAL, engine replays
      on restart to rebuild book state
- [ ] **Replication** — replicator consumer sends raw input bytes to
      hot standby node
- [ ] **Network layer** — TCP receiver feeds the input ring, output
      consumer routes events back to client connections
- [ ] **Multi-symbol** — routing layer dispatches to per-symbol BLPs
- [ ] **Risk engine** — position limits, fat-finger checks

---

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
- [Mechanical Sympathy — Martin Thompson](https://mechanical-sympathy.blogspot.com/)
- [Disruptor technical paper](https://lmax-exchange.github.io/disruptor/disruptor.html)
- [QuantCup winning solution](https://gist.github.com/druska/d6ce3f2bac74db08ee9007cdf98106ef)
- [Arenas in Rust](https://manishearth.github.io/blog/2021/03/15/arenas-in-rust/)
- [x86 BSR/BSF instructions](https://www.felixcloutier.com/x86/bsr)
