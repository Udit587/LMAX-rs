#![allow(non_snake_case)]

use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub mod ring_buffer;
pub mod sequence;
pub mod sequence_barrier;
pub mod buffer_slot;
pub mod order;
pub mod blp;
pub mod consumer;
pub mod output_event;

use blp::matching::ClientAccount;
use consumer::blp::BusinessLogicProcessor;
use buffer_slot::inputSlot::InputSlot;
use buffer_slot::outputSlot::OutputSlot;
use order::command::Command;
use order::orderType::OrderType;
use order::side::Side;
use order::timeInForce::TimeInForce;
use ring_buffer::ringBuffer::RingBuffer;
use sequence::sequence::Sequence;
use sequence_barrier::sequenceBarrier::SequenceBarrier;
use output_event::outputEvent::OutputEvent;

fn main() {
    let input_capacity = 1024;
    let output_capacity = 1024;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(input_capacity));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(output_capacity));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let blp_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let input_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let output_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&blp_sequence)]));

    let _bp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&input_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    ).run();

    let _ = output_barrier;

    let commands = vec![
        Command::Place {
            client_id: 1,
            client_order_id: 11,
            symbol: *b"BTCUSD\0\0",
            side: Side::BUY,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 10,
            time_in_force: TimeInForce::GTC,
        },
        Command::Place {
            client_id: 2,
            client_order_id: 22,
            symbol: *b"BTCUSD\0\0",
            side: Side::SELL,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 6,
            time_in_force: TimeInForce::GTC,
        },
        Command::Place {
            client_id: 3,
            client_order_id: 33,
            symbol: *b"BTCUSD\0\0",
            side: Side::SELL,
            order_type: OrderType::LIMIT,
            price: Some(102),
            quantity: 4,
            time_in_force: TimeInForce::GTC,
        },
        Command::Cancel {
            client_id: 1,
            order_id: 1,
        },
    ];

    let mut producer_seq = -1i64;
    for cmd in commands {
        producer_seq += 1;
        write_command_slot(&input_ring, producer_seq as usize, cmd);
        producer_sequence.set(producer_seq);
    }

    thread::sleep(Duration::from_millis(500));

    println!("Test run complete.");
}

fn write_command_slot(ring: &Arc<RingBuffer<InputSlot>>, seq: usize, cmd: Command) {
    let slot = unsafe { ring.slot_mut_ref(seq as i64) };
    slot.command = Some(cmd);
    slot.len = 0;
    slot.raw_bytes = [0u8; 256];
}