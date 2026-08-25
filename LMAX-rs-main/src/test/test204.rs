// #![allow(non_snake_case)]

// use std::sync::Arc;

// pub mod ring_buffer;
// pub mod sequence;
// pub mod sequence_barrier;
// pub mod buffer_slot;
// pub mod order;
// pub mod blp;
// pub mod consumer;
// pub mod output_event;

// use buffer_slot::inputSlot::InputSlot;
// use buffer_slot::outputSlot::OutputSlot;
// use consumer::blp::BusinessLogicProcessor;
// use ring_buffer::ringBuffer::RingBuffer;
// use sequence::sequence::Sequence;
// use sequence_barrier::sequenceBarrier::SequenceBarrier;

// fn main() {
//     println!("Run correctness tests with: cargo test -- --nocapture");
// }

// #[allow(dead_code)]
// fn build_test_runtime(
//     input_capacity: usize,
//     output_capacity: usize,
// ) -> (
//     Arc<RingBuffer<InputSlot>>,
//     Arc<RingBuffer<OutputSlot>>,
//     Arc<Sequence>,
//     Arc<Sequence>,
//     Arc<Sequence>,
//     Arc<Sequence>,
//     Arc<Sequence>,
//     Arc<SequenceBarrier>,
//     BusinessLogicProcessor,
// ) {
//     let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(input_capacity));
//     let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(output_capacity));

//     let producer_sequence = Sequence::new(-1);
//     let unmarshaller_sequence = Sequence::new(-1);
//     let blp_sequence = Sequence::new(-1);
//     let output_sequence = Sequence::new(-1);
//     let output_gating_sequence = Sequence::new(-1);

//     let input_barrier = SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]);
//     let _output_barrier = SequenceBarrier::new(vec![Arc::clone(&blp_sequence)]);

//     let blp = BusinessLogicProcessor::new(
//         Arc::clone(&input_ring),
//         Arc::clone(&input_barrier),
//         Arc::clone(&unmarshaller_sequence),
//         Arc::clone(&output_ring),
//         Arc::clone(&output_sequence),
//         Arc::clone(&output_gating_sequence),
//         100,
//         1,
//         4096,
//     );

//     (
//         input_ring,
//         output_ring,
//         producer_sequence,
//         unmarshaller_sequence,
//         blp_sequence,
//         output_sequence,
//         output_gating_sequence,
//         input_barrier,
//         blp,
//     )
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     use crate::order::command::Command;
//     use crate::order::orderType::OrderType;
//     use crate::order::side::Side;
//     use crate::order::timeInForce::TimeInForce;

//     fn btcusd() -> [u8; 8] {
//         *b"BTCUSD\0\0"
//     }

//     #[test]
//     fn command_objects_are_built_correctly() {
//         let buy = Command::Place {
//             client_id: 1,
//             client_order_id: 11,
//             symbol: btcusd(),
//             side: Side::BUY,
//             order_type: OrderType::LIMIT,
//             price: Some(101),
//             quantity: 10,
//             time_in_force: TimeInForce::GTC,
//         };

//         let sell = Command::Place {
//             client_id: 2,
//             client_order_id: 22,
//             symbol: btcusd(),
//             side: Side::SELL,
//             order_type: OrderType::LIMIT,
//             price: Some(101),
//             quantity: 6,
//             time_in_force: TimeInForce::GTC,
//         };

//         match buy {
//             Command::Place {
//                 client_id,
//                 client_order_id,
//                 symbol,
//                 side,
//                 order_type,
//                 price,
//                 quantity,
//                 time_in_force,
//             } => {
//                 assert_eq!(client_id, 1);
//                 assert_eq!(client_order_id, 11);
//                 assert_eq!(symbol, btcusd());
//                 assert!(matches!(side, Side::BUY));
//                 assert!(matches!(order_type, OrderType::LIMIT));
//                 assert_eq!(price, Some(101));
//                 assert_eq!(quantity, 10);
//                 assert!(matches!(time_in_force, TimeInForce::GTC));
//             }
//             _ => panic!("expected place command"),
//         }

//         match sell {
//             Command::Place {
//                 client_id,
//                 client_order_id,
//                 symbol,
//                 side,
//                 order_type,
//                 price,
//                 quantity,
//                 time_in_force,
//             } => {
//                 assert_eq!(client_id, 2);
//                 assert_eq!(client_order_id, 22);
//                 assert_eq!(symbol, btcusd());
//                 assert!(matches!(side, Side::SELL));
//                 assert!(matches!(order_type, OrderType::LIMIT));
//                 assert_eq!(price, Some(101));
//                 assert_eq!(quantity, 6);
//                 assert!(matches!(time_in_force, TimeInForce::GTC));
//             }
//             _ => panic!("expected place command"),
//         }
//     }

//     #[test]
//     fn cancel_command_fields_are_correct() {
//         let cancel = Command::Cancel {
//             client_id: 7,
//             order_id: 99,
//         };

//         match cancel {
//             Command::Cancel { client_id, order_id } => {
//                 assert_eq!(client_id, 7);
//                 assert_eq!(order_id, 99);
//             }
//             _ => panic!("expected cancel command"),
//         }
//     }

//     #[test]
//     fn runtime_builds_without_panicking() {
//         let (
//             _input_ring,
//             _output_ring,
//             producer_sequence,
//             unmarshaller_sequence,
//             blp_sequence,
//             output_sequence,
//             output_gating_sequence,
//             _input_barrier,
//             _blp,
//         ) = build_test_runtime(64, 64);

//         assert_eq!(producer_sequence.get(), -1);
//         assert_eq!(unmarshaller_sequence.get(), -1);
//         assert_eq!(blp_sequence.get(), -1);
//         assert_eq!(output_sequence.get(), -1);
//         assert_eq!(output_gating_sequence.get(), -1);
//     }

//     #[test]
//     fn input_slot_accepts_command_write() {
//         let (input_ring, _output_ring, _producer_sequence, ..) = build_test_runtime(16, 16);

//         let cmd = Command::Place {
//             client_id: 1,
//             client_order_id: 55,
//             symbol: btcusd(),
//             side: Side::BUY,
//             order_type: OrderType::LIMIT,
//             price: Some(100),
//             quantity: 5,
//             time_in_force: TimeInForce::GTC,
//         };

//         let slot = unsafe { input_ring.slot_mut_ref(0) };
//         slot.command = Some(cmd);
//         slot.len = 0;
//         slot.raw_bytes = [0u8; 256];

//         let written = unsafe { input_ring.slot_ref(0) };
//         assert!(written.command.is_some());

//         match written.command.as_ref().unwrap() {
//             Command::Place {
//                 client_id,
//                 client_order_id,
//                 symbol,
//                 side,
//                 order_type,
//                 price,
//                 quantity,
//                 time_in_force,
//             } => {
//                 assert_eq!(*client_id, 1);
//                 assert_eq!(*client_order_id, 55);
//                 assert_eq!(*symbol, btcusd());
//                 assert!(matches!(side, Side::BUY));
//                 assert!(matches!(order_type, OrderType::LIMIT));
//                 assert_eq!(*price, Some(100));
//                 assert_eq!(*quantity, 5);
//                 assert!(matches!(time_in_force, TimeInForce::GTC));
//             }
//             _ => panic!("expected place command in slot"),
//         }
//     }
// }
#![allow(non_snake_case)]

use std::collections::HashMap;

pub mod ring_buffer;
pub mod sequence;
pub mod sequence_barrier;
pub mod buffer_slot;
pub mod order;
pub mod blp;
pub mod consumer;
pub mod output_event;

use blp::arena::SlotState;
use blp::book::OrderBook;
use blp::handlers::{process_cancel, process_place, OrderIndex};
use blp::matching::ClientAccount;
use order::command::Command;
use order::orderType::OrderType;
use order::side::Side;
use order::timeInForce::TimeInForce;
use output_event::outputEvent::OutputEvent;

fn main() {
    println!("Run tests with: cargo test -- --nocapture");
}

fn new_book() -> OrderBook {
    OrderBook::new(100, 1, 1024)
}

fn new_state() -> (OrderBook, HashMap<u64, ClientAccount>, OrderIndex, u64, Vec<OutputEvent>) {
    let book = new_book();
    let clients = HashMap::new();
    let order_index = HashMap::new();
    let next_order_id = 1u64;
    let events = Vec::new();
    (book, clients, order_index, next_order_id, events)
}

fn btcusd() -> [u8; 8] {
    *b"BTCUSD\0\0"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_buy_is_accepted_when_book_is_empty() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            1,
            11,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(101),
            10,
            &mut |e| events.push(e),
        );

        assert_eq!(next_order_id, 2);
        assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(1));
        assert_eq!(order_index.len(), 1);
        assert_eq!(book.bids.best_bid(), Some(1));
        assert!(book.asks.best_ask().is_none());

        match &events[..] {
            [OutputEvent::OrderAccepted { order_id, client_id, client_order_id, symbol, side, price, quantity }] => {
                assert_eq!(*order_id, 1);
                assert_eq!(*client_id, 1);
                assert_eq!(*client_order_id, 11);
                assert_eq!(*symbol, btcusd());
                assert!(matches!(side, Side::BUY));
                assert_eq!(*price, 101);
                assert_eq!(*quantity, 10);
            }
            _ => panic!("expected one OrderAccepted event"),
        }
    }

    #[test]
fn buy_then_sell_matches_and_leaves_correct_remainder() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        11,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(101),
        10,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        2,
        22,
        btcusd(),
        Side::SELL,
        OrderType::LIMIT,
        Some(101),
        6,
        &mut |e| events.push(e),
    );

    assert_eq!(next_order_id, 3);
    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(1));
    assert_eq!(clients.get(&2).map(|c| c.open_orders), Some(0));
    assert_eq!(order_index.len(), 1);

    let buy_locator = order_index.get(&1).copied().expect("buy order should remain indexed");
    let slot_idx = book.arena.validate(buy_locator.oid).expect("buy slot should be valid");

    match &book.arena.order_store[slot_idx].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.client_id, 1);
            assert_eq!(order.order_id, 1);
            assert_eq!(order.price, 101);
            assert_eq!(order.quantity, 10);
            assert_eq!(order.leaves_qty, 4);
            assert_eq!(order.filled_qty, 6);
        }
        _ => panic!("expected occupied slot for resting buy order"),
    }

    assert_eq!(book.bids.best_bid(), Some(1));
    assert!(book.asks.best_ask().is_none());

    assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderAccepted { order_id: 1, .. })));
    assert!(!events.iter().any(|e| matches!(e, OutputEvent::OrderAccepted { order_id: 2, .. })));
}
   
    #[test]
    fn cancel_removes_live_order_and_updates_client_count() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            7,
            77,
            btcusd(),
            Side::SELL,
            OrderType::LIMIT,
            Some(105),
            9,
            &mut |e| events.push(e),
        );

        assert_eq!(clients.get(&7).map(|c| c.open_orders), Some(1));
        assert_eq!(order_index.len(), 1);

        process_cancel(
            &mut book,
            &mut clients,
            &mut order_index,
            7,
            1,
            &mut |e| events.push(e),
        );

        assert_eq!(clients.get(&7).map(|c| c.open_orders), Some(0));
        assert!(order_index.is_empty());
        assert!(book.asks.best_ask().is_none());

        assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderCancelled { order_id: 1, client_id: 7, leaves_qty: 9 })));
    }

    #[test]
    fn cancel_rejected_for_wrong_owner() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            10,
            101,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(100),
            5,
            &mut |e| events.push(e),
        );

        process_cancel(
            &mut book,
            &mut clients,
            &mut order_index,
            99,
            1,
            &mut |e| events.push(e),
        );

        assert_eq!(clients.get(&10).map(|c| c.open_orders), Some(1));
        assert_eq!(order_index.len(), 1);
        assert!(events.iter().any(|e| matches!(e, OutputEvent::CancelRejected { order_id: 1, client_id: 99 })));
    }
}