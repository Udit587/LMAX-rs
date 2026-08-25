
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::blp::book::OrderBook;
use crate::blp::handlers::{process_cancel, process_modify, process_place, OrderIndex};
use crate::blp::matching::ClientAccount;
use crate::blp::publisher::publish_output;
use crate::buffer_slot::inputSlot::InputSlot;
use crate::buffer_slot::outputSlot::OutputSlot;
use crate::order::command::Command;
use crate::output_event::outputEvent::OutputEvent;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;

pub struct BusinessLogicProcessor {
    input_ring: Arc<RingBuffer<InputSlot>>,
    input_barrier: Arc<SequenceBarrier>,
    input_sequence: Arc<Sequence>,

    output_ring: Arc<RingBuffer<OutputSlot>>,
    output_sequence: Arc<Sequence>,
    output_gating_sequence: Arc<Sequence>,

    base_price: u64,
    tick_size: u64,
    arena_capacity: usize,
}

impl BusinessLogicProcessor {
    pub fn new(
        input_ring: Arc<RingBuffer<InputSlot>>,
        input_barrier: Arc<SequenceBarrier>,
        input_sequence: Arc<Sequence>,
        output_ring: Arc<RingBuffer<OutputSlot>>,
        output_sequence: Arc<Sequence>,
        output_gating_sequence: Arc<Sequence>,
        base_price: u64,
        tick_size: u64,
        arena_capacity: usize,
    ) -> Self {
        Self {
            input_ring,
            input_barrier,
            input_sequence,
            output_ring,
            output_sequence,
            output_gating_sequence,
            base_price,
            tick_size,
            arena_capacity,
        }
    }


pub fn run(self) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut book = OrderBook::new(self.base_price, self.tick_size, self.arena_capacity);
        let mut clients: HashMap<u64, ClientAccount> = HashMap::new();
        let mut order_index: OrderIndex = HashMap::new();
        let mut next_order_id: u64 = 1;

        loop {
            let next_seq = self.input_sequence.get() + 1;
            // println!("blp: waiting for input seq {}", next_seq);
            self.input_barrier.wait_for(next_seq);
            // println!("blp: barrier released for seq {}", next_seq);

            // capture both command and timestamp in one slot read
            let (command, input_ts) = {
                let slot = unsafe { self.input_ring.slot_ref(next_seq) };
                // println!("blp: reading slot at seq {}", next_seq);

                match slot.command {
                    Some(cmd) => {
                        //println!("blp: found command at seq {} -> {:?}", next_seq, cmd);
                        (cmd, slot.timestamp_ns)   // capture timestamp here
                    }
                    None => {
                        //println!("blp: slot.command is None at seq {}, advancing input sequence", next_seq);
                        self.input_sequence.set(next_seq);
                        continue;
                    }
                }
            };

            let mut events: [Option<OutputEvent>; 16] = [None; 16];
            let mut count = 0usize;

            let mut emit = |event: OutputEvent| {
                if count < events.len() {
                    //println!("blp: emit buffered event at local index {} -> {:?}", count, event);
                    events[count] = Some(event);
                    count += 1;
                } else {
                    //println!("blp: event buffer full at seq {}, dropping extra event -> {:?}", next_seq, event);
                }
            };

            //println!("blp: processing command at seq {}", next_seq);
            match command {
                Command::Place {
                    client_id,
                    client_order_id,
                    symbol,
                    side,
                    order_type,
                    price,
                    quantity,
                    ..
                } => {
                    // println!(
                    //     "blp: PLACE seq={} client_id={} client_order_id={} symbol={:?} side={:?} order_type={:?} price={:?} quantity={}",
                    //     next_seq, client_id, client_order_id, symbol, side, order_type, price, quantity
                    // );
                    process_place(
                        &mut book, &mut clients, &mut order_index, &mut next_order_id,
                        client_id, client_order_id, symbol, side, order_type, price, quantity,
                        &mut emit,
                    );
                }

                Command::Cancel { client_id, client_order_id } => {
                    //println!("blp: CANCEL seq={} client_id={} client_order_id={}", next_seq, client_id, client_order_id);
                    process_cancel(
                        &mut book, &mut clients, &mut order_index,
                        client_id, client_order_id,
                        &mut emit,
                    );
                }

                Command::Modify { client_id, client_order_id, new_price, new_qty } => {
                    // println!(
                    //     "blp: MODIFY seq={} client_id={} client_order_id={} new_price={:?} new_qty={:?}",
                    //     next_seq, client_id, client_order_id, new_price, new_qty
                    // );
                    process_modify(
                        &mut book, &mut clients, &mut order_index,
                        client_id, client_order_id, new_price, new_qty,
                        &mut emit,
                    );
                }
            }

            // println!("blp: finished processing seq {}, generated {} event(s)", next_seq, count);
            self.input_sequence.set(next_seq);
            // println!("blp: advanced input sequence to {}", next_seq);

            for i in 0..count {
                if let Some(event) = events[i] {
                    let before_out = self.output_sequence.get();
                    let gate = self.output_gating_sequence.get();
                    // println!(
                    //     "blp: publishing output event {} for input seq {} (before_out_seq={}, gate_seq={}) -> {:?}",
                    //     i, next_seq, before_out, gate, event
                    // );

                    publish_output(
                        &self.output_ring,
                        &self.output_sequence,
                        &self.output_gating_sequence,
                        event,
                        input_ts,   // thread timestamp through
                    );

                    let after_out = self.output_sequence.get();
                    // println!(
                    //     "blp: published output event {} for input seq {} (after_out_seq={})",
                    //     i, next_seq, after_out
                    // );
                }
            }

            //println!("blp: loop complete for seq {}", next_seq);
        }
    })
}
}