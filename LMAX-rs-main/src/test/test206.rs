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
use std::sync::Arc;
use blp::arena::SlotState;
use blp::book::OrderBook;
use blp::handlers::{process_cancel, process_modify, process_place, OrderIndex};
use blp::matching::ClientAccount;
use order::command::Command;
use order::orderType::OrderType;
use order::side::Side;
use order::timeInForce::TimeInForce;
use output_event::outputEvent::OutputEvent;
use output_event::rejectReason::RejectReason;
use crate::blp::matching::MAX_OPEN_ORDERS;
use crate::blp::order_id::OrderId;
use crate::buffer_slot::inputSlot::InputSlot;
use crate::buffer_slot::outputSlot::OutputSlot;
use crate::sequence::sequence::Sequence;
use crate::sequence_barrier::sequenceBarrier::SequenceBarrier;
use crate::ring_buffer::ringBuffer::RingBuffer;
use crate::consumer::blp::BusinessLogicProcessor;
use crate::consumer::unmarshallerConsumer::Unmarshaller;
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
    fn command_objects_are_built_correctly() {
        let buy = Command::Place {
            client_id: 1,
            client_order_id: 11,
            symbol: btcusd(),
            side: Side::BUY,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 10,
            time_in_force: TimeInForce::GTC,
        };

        let sell = Command::Place {
            client_id: 2,
            client_order_id: 22,
            symbol: btcusd(),
            side: Side::SELL,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 6,
            time_in_force: TimeInForce::GTC,
        };

        match buy {
            Command::Place {
                client_id,
                client_order_id,
                symbol,
                side,
                order_type,
                price,
                quantity,
                time_in_force,
            } => {
                assert_eq!(client_id, 1);
                assert_eq!(client_order_id, 11);
                assert_eq!(symbol, btcusd());
                assert!(matches!(side, Side::BUY));
                assert!(matches!(order_type, OrderType::LIMIT));
                assert_eq!(price, Some(101));
                assert_eq!(quantity, 10);
                assert!(matches!(time_in_force, TimeInForce::GTC));
            }
            _ => panic!("expected place command"),
        }

        match sell {
            Command::Place {
                client_id,
                client_order_id,
                symbol,
                side,
                order_type,
                price,
                quantity,
                time_in_force,
            } => {
                assert_eq!(client_id, 2);
                assert_eq!(client_order_id, 22);
                assert_eq!(symbol, btcusd());
                assert!(matches!(side, Side::SELL));
                assert!(matches!(order_type, OrderType::LIMIT));
                assert_eq!(price, Some(101));
                assert_eq!(quantity, 6);
                assert!(matches!(time_in_force, TimeInForce::GTC));
            }
            _ => panic!("expected place command"),
        }
    }

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

    #[test]
    fn invalid_quantity_is_rejected() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            1,
            1,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(101),
            0,
            &mut |e| events.push(e),
        );

        assert_eq!(next_order_id, 1);
        assert!(order_index.is_empty());
        assert!(clients.is_empty());
        assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderRejected { reason: RejectReason::InvalidQuantity, .. })));
    }

    #[test]
fn invalid_price_is_rejected() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        1,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        None,
        5,
        &mut |e| events.push(e),
    );

    assert_eq!(next_order_id, 2);
    assert!(order_index.is_empty());
    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(0));
    assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderRejected {
        reason: RejectReason::InvalidPrice,
        ..
    })));
}

    #[test]
fn market_order_partially_fills_and_cancels_leftover() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        1,
        btcusd(),
        Side::SELL,
        OrderType::LIMIT,
        Some(101),
        4,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        2,
        2,
        btcusd(),
        Side::BUY,
        OrderType::MARKET,
        None,
        10,
        &mut |e| events.push(e),
    );

    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(0));
    assert_eq!(clients.get(&2).map(|c| c.open_orders), Some(0));
    assert!(order_index.is_empty());
    assert!(book.asks.best_ask().is_none());
    assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderCancelled {
        order_id: 2,
        client_id: 2,
        leaves_qty: 6,
    })));
}
   
    #[test]
    fn modify_smaller_quantity_updates_in_place() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            1,
            1,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(100),
            10,
            &mut |e| events.push(e),
        );

        process_modify(
            &mut book,
            &mut clients,
            &mut order_index,
            1,
            1,
            None,
            Some(6),
            &mut |e| events.push(e),
        );

        assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(1));
        let loc = order_index.get(&1).copied().unwrap();
        let idx = book.arena.validate(loc.oid).unwrap();

        match &book.arena.order_store[idx].state {
            SlotState::Occupied { order, .. } => {
                assert_eq!(order.leaves_qty, 6);
                assert_eq!(order.filled_qty, 4);
            }
            _ => panic!("expected occupied slot after modify"),
        }

        assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderModified { order_id: 1, client_id: 1, new_price: 100, new_qty: 6 })));
    }

    #[test]
    fn modify_price_change_requeues_order() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            1,
            1,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(100),
            10,
            &mut |e| events.push(e),
        );

        process_modify(
            &mut book,
            &mut clients,
            &mut order_index,
            1,
            1,
            Some(102),
            Some(10),
            &mut |e| events.push(e),
        );

        let loc = order_index.get(&1).copied().unwrap();
        let idx = book.arena.validate(loc.oid).unwrap();

        match &book.arena.order_store[idx].state {
            SlotState::Occupied { order, .. } => {
                assert_eq!(order.price, 102);
                assert_eq!(order.leaves_qty, 10);
            }
            _ => panic!("expected requeued order"),
        }

        assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderModified { order_id: 1, client_id: 1, new_price: 102, new_qty: 10 })));
    }

    #[test]
    fn client_open_order_limit_is_enforced() {
        let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

        for i in 0..MAX_OPEN_ORDERS {
            process_place(
                &mut book,
                &mut clients,
                &mut order_index,
                &mut next_order_id,
                1,
                i as u64 + 1,
                btcusd(),
                Side::BUY,
                OrderType::LIMIT,
                Some(100 + i as u64),
                1,
                &mut |e| events.push(e),
            );
        }

        process_place(
            &mut book,
            &mut clients,
            &mut order_index,
            &mut next_order_id,
            1,
            9999,
            btcusd(),
            Side::BUY,
            OrderType::LIMIT,
            Some(999),
            1,
            &mut |e| events.push(e),
        );

        assert!(events.iter().any(|e| matches!(e, OutputEvent::OrderRejected { reason: RejectReason::ClientOrderLimitExceeded, .. })));
    }

    #[test]
fn cancel_rejected_for_missing_order() {
    let (mut book, mut clients, mut order_index, _next_order_id, mut events) = new_state();

    process_cancel(
        &mut book,
        &mut clients,
        &mut order_index,
        42,
        999,
        &mut |e| events.push(e),
    );

    assert!(events.iter().any(|e| matches!(
        e,
        OutputEvent::CancelRejected {
            order_id: 999,
            client_id: 42
        }
    )));
}

#[test]
fn modify_rejected_for_missing_order() {
    let (mut book, mut clients, mut order_index, _next_order_id, mut events) = new_state();

    process_modify(
        &mut book,
        &mut clients,
        &mut order_index,
        7,
        999,
        Some(101),
        Some(5),
        &mut |e| events.push(e),
    );

    assert!(events.iter().any(|e| matches!(
        e,
        OutputEvent::ModifyRejected {
            order_id: 999,
            client_id: 7
        }
    )));
}

#[test]
fn modify_rejected_when_new_qty_is_zero() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        1,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(100),
        10,
        &mut |e| events.push(e),
    );

    process_modify(
        &mut book,
        &mut clients,
        &mut order_index,
        1,
        1,
        None,
        Some(0),
        &mut |e| events.push(e),
    );

    assert!(events.iter().any(|e| matches!(
        e,
        OutputEvent::ModifyRejected {
            order_id: 1,
            client_id: 1
        }
    )));

    let loc = order_index.get(&1).copied().unwrap();
    let idx = book.arena.validate(loc.oid).unwrap();

    match &book.arena.order_store[idx].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.price, 100);
            assert_eq!(order.leaves_qty, 10);
            assert_eq!(order.filled_qty, 0);
        }
        _ => panic!("expected original order to remain unchanged"),
    }
}

#[test]
fn out_of_range_price_is_rejected() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        77,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(99),
        5,
        &mut |e| events.push(e),
    );

    assert!(order_index.is_empty());
    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(0));
    assert!(events.iter().any(|e| matches!(
        e,
        OutputEvent::OrderRejected {
            client_id: 1,
            client_order_id: 77,
            reason: RejectReason::InvalidPrice
        }
    )));
}

#[test]
fn two_resting_buys_same_price_both_remain_before_match() {
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
        5,
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
        Side::BUY,
        OrderType::LIMIT,
        Some(101),
        7,
        &mut |e| events.push(e),
    );

    assert_eq!(order_index.len(), 2);
    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(1));
    assert_eq!(clients.get(&2).map(|c| c.open_orders), Some(1));
    assert_eq!(book.bids.best_bid(), Some(1));

    let loc1 = order_index.get(&1).copied().unwrap();
    let idx1 = book.arena.validate(loc1.oid).unwrap();

    let loc2 = order_index.get(&2).copied().unwrap();
    let idx2 = book.arena.validate(loc2.oid).unwrap();

    match &book.arena.order_store[idx1].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.client_id, 1);
            assert_eq!(order.leaves_qty, 5);
        }
        _ => panic!("expected first buy to exist"),
    }

    match &book.arena.order_store[idx2].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.client_id, 2);
            assert_eq!(order.leaves_qty, 7);
        }
        _ => panic!("expected second buy to exist"),
    }
}

#[test]
fn same_price_match_consumes_resting_orders_in_time_priority() {
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
        5,
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
        Side::BUY,
        OrderType::LIMIT,
        Some(101),
        7,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        3,
        33,
        btcusd(),
        Side::SELL,
        OrderType::LIMIT,
        Some(101),
        6,
        &mut |e| events.push(e),
    );

    assert_eq!(order_index.len(), 1);
    assert!(order_index.get(&1).is_none());

    let loc2 = order_index.get(&2).copied().expect("second resting order should remain");
    let idx2 = book.arena.validate(loc2.oid).unwrap();

    match &book.arena.order_store[idx2].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.client_id, 2);
            assert_eq!(order.order_id, 2);
            assert_eq!(order.price, 101);
            assert_eq!(order.quantity, 7);
            assert_eq!(order.leaves_qty, 6);
            assert_eq!(order.filled_qty, 1);
        }
        _ => panic!("expected second order to remain with partial fill"),
    }

    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(0));
    assert_eq!(clients.get(&2).map(|c| c.open_orders), Some(1));
    assert_eq!(clients.get(&3).map(|c| c.open_orders), Some(0));
}

#[test]
fn one_aggressor_matches_multiple_resting_orders() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        1,
        btcusd(),
        Side::SELL,
        OrderType::LIMIT,
        Some(101),
        3,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        2,
        2,
        btcusd(),
        Side::SELL,
        OrderType::LIMIT,
        Some(101),
        4,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        3,
        3,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(101),
        5,
        &mut |e| events.push(e),
    );

    assert_eq!(order_index.len(), 1);
    assert!(order_index.get(&1).is_none());

    let loc2 = order_index.get(&2).copied().expect("second sell should still be resting");
    let idx2 = book.arena.validate(loc2.oid).unwrap();

    match &book.arena.order_store[idx2].state {
        SlotState::Occupied { order, .. } => {
            assert_eq!(order.client_id, 2);
            assert_eq!(order.leaves_qty, 2);
            assert_eq!(order.filled_qty, 2);
        }
        _ => panic!("expected second sell to remain partially filled"),
    }

    assert_eq!(clients.get(&1).map(|c| c.open_orders), Some(0));
    assert_eq!(clients.get(&2).map(|c| c.open_orders), Some(1));
    assert_eq!(clients.get(&3).map(|c| c.open_orders), Some(0));
}

#[test]
fn best_bid_moves_down_after_top_level_removed() {
    let (mut book, mut clients, mut order_index, mut next_order_id, mut events) = new_state();

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        1,
        1,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(103),
        2,
        &mut |e| events.push(e),
    );

    process_place(
        &mut book,
        &mut clients,
        &mut order_index,
        &mut next_order_id,
        2,
        2,
        btcusd(),
        Side::BUY,
        OrderType::LIMIT,
        Some(101),
        2,
        &mut |e| events.push(e),
    );

    assert_eq!(book.bids.best_bid(), Some(3));

    process_cancel(
        &mut book,
        &mut clients,
        &mut order_index,
        1,
        1,
        &mut |e| events.push(e),
    );

    assert_eq!(book.bids.best_bid(), Some(1));
}
#[test]
fn full_pipeline_place_limit_order_emits_accept_event() {
    use crate::consumer::unmarshallerConsumer::Unmarshaller;
    use std::thread;
    use std::time::{Duration, Instant};

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(16));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(16));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&producer_sequence),
    );

    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    let cmd = {
        let mut bytes = Vec::new();
        bytes.push(1u8);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&11u64.to_le_bytes());
        bytes.extend_from_slice(b"BTCUSD\0\0");
        bytes.push(0u8);
        bytes.push(0u8);
        bytes.push(1u8);
        bytes.extend_from_slice(&101u64.to_le_bytes());
        bytes.extend_from_slice(&10u64.to_le_bytes());
        bytes.push(0u8);
        bytes
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..cmd.len()].copy_from_slice(&cmd);
        slot.len = cmd.len();
        slot.command = None;
    }

    producer_sequence.set(0);

    let deadline = Instant::now() + Duration::from_millis(500);
    while output_sequence.get() < 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    let published = output_sequence.get();
    assert!(published >= 0, "expected at least one published output event");

    let mut found = false;

    for seq in 0..=published {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(OutputEvent::OrderAccepted {
            client_id,
            client_order_id,
            symbol,
            side,
            price,
            quantity,
            ..
        }) = out_slot.event
        {
            if client_id == 1
                && client_order_id == 11
                && symbol == *b"BTCUSD\0\0"
                && matches!(side, Side::BUY)
                && price == 101
                && quantity == 10
            {
                found = true;
            }
        }
        output_gating_sequence.set(seq);
    }

    assert!(found, "expected OrderAccepted event in published output range");
}

#[test]
fn full_pipeline_buy_then_sell_emits_fill_and_remainder_state() {
    use crate::consumer::unmarshallerConsumer::Unmarshaller;
    use std::thread;
    use std::time::{Duration, Instant};

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(16));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(32));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&producer_sequence),
    );

    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    let _u = unmarshaller.run();
    let _b = blp.run();

    let buy = {
        let mut bytes = Vec::new();
        bytes.push(1u8);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&11u64.to_le_bytes());
        bytes.extend_from_slice(b"BTCUSD\0\0");
        bytes.push(0u8);
        bytes.push(0u8);
        bytes.push(1u8);
        bytes.extend_from_slice(&101u64.to_le_bytes());
        bytes.extend_from_slice(&10u64.to_le_bytes());
        bytes.push(0u8);
        bytes
    };

    let sell = {
        let mut bytes = Vec::new();
        bytes.push(1u8);
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&22u64.to_le_bytes());
        bytes.extend_from_slice(b"BTCUSD\0\0");
        bytes.push(1u8);
        bytes.push(0u8);
        bytes.push(1u8);
        bytes.extend_from_slice(&101u64.to_le_bytes());
        bytes.extend_from_slice(&6u64.to_le_bytes());
        bytes.push(0u8);
        bytes
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..buy.len()].copy_from_slice(&buy);
        slot.len = buy.len();
        slot.command = None;
    }
    producer_sequence.set(0);

    thread::sleep(Duration::from_millis(50));

    {
        let slot = unsafe { input_ring.slot_mut_ref(1) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..sell.len()].copy_from_slice(&sell);
        slot.len = sell.len();
        slot.command = None;
    }
    producer_sequence.set(1);

    let deadline = Instant::now() + Duration::from_millis(700);
    while output_sequence.get() < 1 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    let published = output_sequence.get();
    assert!(published >= 1, "expected at least two published output events");

    let mut saw_accept = false;
    let mut saw_fill = false;

    for seq in 0..=published {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(event) = out_slot.event {
            match event {
                OutputEvent::OrderAccepted {
                    client_id,
                    client_order_id,
                    symbol,
                    side,
                    price,
                    quantity,
                    ..
                } => {
                    if client_id == 1
                        && client_order_id == 11
                        && symbol == *b"BTCUSD\0\0"
                        && matches!(side, Side::BUY)
                        && price == 101
                        && quantity == 10
                    {
                        saw_accept = true;
                    }
                }
                OutputEvent::Fill {
                    aggressor_client_id,
                    resting_client_id,
                    symbol,
                    price,
                    quantity,
                    aggressor_side,
                    ..
                } => {
                    if aggressor_client_id == 2
                        && resting_client_id == 1
                        && symbol == *b"BTCUSD\0\0"
                        && price == 101
                        && quantity == 6
                        && matches!(aggressor_side, Side::SELL)
                    {
                        saw_fill = true;
                    }
                }
                _ => {}
            }
        }
        output_gating_sequence.set(seq);
    }

    assert!(saw_accept, "expected to observe accepted resting buy in output ring");
    assert!(saw_fill, "expected to observe fill event in output ring");
}
#[test]
fn diagnostic_unmarshaller_parses_place_message() {
    use crate::consumer::unmarshallerConsumer::Unmarshaller;
    use std::sync::Arc;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(8));
    let producer_sequence = Arc::new(Sequence::new(-1));
    let input_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&input_barrier),
        Arc::clone(&producer_sequence),
    );

    let bytes = {
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&11u64.to_le_bytes());
        b.extend_from_slice(b"BTCUSD\0\0");
        b.push(0u8);
        b.push(0u8);
        b.push(1u8);
        b.extend_from_slice(&101u64.to_le_bytes());
        b.extend_from_slice(&10u64.to_le_bytes());
        b.push(0u8);
        b
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..bytes.len()].copy_from_slice(&bytes);
        slot.len = bytes.len();
        slot.command = None;
    }

    producer_sequence.set(0);

    let _handle = unmarshaller.run();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let slot = unsafe { input_ring.slot_ref(0) };
    assert!(slot.command.is_some(), "unmarshaller did not parse command");
}
#[test]
fn diagnostic_blp_publishes_accept_event() {
    use crate::consumer::blp::BusinessLogicProcessor;
    use std::sync::Arc;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(8));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(8));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let input_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));
    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&input_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.command = Some(Command::Place {
            client_id: 1,
            client_order_id: 11,
            symbol: *b"BTCUSD\0\0",
            side: Side::BUY,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 10,
            time_in_force: TimeInForce::GTC,
        });
    }

    unmarshaller_sequence.set(0);

    let _handle = blp.run();
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert!(output_sequence.get() >= 0, "BLP did not publish any output");

    let mut found = false;
    for seq in 0..=output_sequence.get() {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(OutputEvent::OrderAccepted { client_id, client_order_id, .. }) = out_slot.event {
            if client_id == 1 && client_order_id == 11 {
                found = true;
            }
        }
    }

    assert!(found, "BLP ran, but no accept event was published");
}
#[test]
fn diagnostic_full_pipeline_one_step() {
    use crate::consumer::unmarshallerConsumer::Unmarshaller;
    use crate::consumer::blp::BusinessLogicProcessor;
    use std::sync::Arc;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(16));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(16));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&producer_sequence),
    );

    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    let bytes = {
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&11u64.to_le_bytes());
        b.extend_from_slice(b"BTCUSD\0\0");
        b.push(0u8);
        b.push(0u8);
        b.push(1u8);
        b.extend_from_slice(&101u64.to_le_bytes());
        b.extend_from_slice(&10u64.to_le_bytes());
        b.push(0u8);
        b
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..bytes.len()].copy_from_slice(&bytes);
        slot.len = bytes.len();
        slot.command = None;
    }

    producer_sequence.set(0);

    let _u = unmarshaller.run();
    let _b = blp.run();

    std::thread::sleep(std::time::Duration::from_millis(150));

    assert!(unsafe { input_ring.slot_ref(0).command.is_some() }, "unmarshaller did not set command");
    assert!(unsafe { output_sequence.get() } >= 0, "BLP did not publish output");

    let mut saw_accept = false;
    for seq in 0..=unsafe { output_sequence.get() } {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(OutputEvent::OrderAccepted { client_id, client_order_id, .. }) = out_slot.event {
            if client_id == 1 && client_order_id == 11 {
                saw_accept = true;
            }
        }
    }

    assert!(saw_accept, "full pipeline did not produce accept event");
}
#[test]
fn diagnostic_unmarshaller_parses_place_message() {
    use crate::consumer::unmarshallerConsumer::Unmarshaller;
    use std::sync::Arc;
    use std::time::Duration;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(8));
    let producer_sequence = Arc::new(Sequence::new(-1));
    let input_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&input_barrier),
        Arc::clone(&producer_sequence),
    );

    let bytes = {
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&11u64.to_le_bytes());
        b.extend_from_slice(b"BTCUSD\0\0");
        b.push(0u8);
        b.push(0u8);
        b.push(1u8);
        b.extend_from_slice(&101u64.to_le_bytes());
        b.extend_from_slice(&10u64.to_le_bytes());
        b.push(0u8);
        b
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..bytes.len()].copy_from_slice(&bytes);
        slot.len = bytes.len();
        slot.command = None;
    }

    let _handle = unmarshaller.run();
    producer_sequence.set(0);

    std::thread::sleep(Duration::from_millis(50));

    let slot = unsafe { input_ring.slot_ref(0) };
    assert!(slot.command.is_some(), "unmarshaller did not parse command");
}
#[test]
fn diagnostic_blp_publishes_accept_event() {
    use crate::consumer::blp::BusinessLogicProcessor;
    use std::sync::Arc;
    use std::time::Duration;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(8));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(8));

    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let input_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&input_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.command = Some(Command::Place {
            client_id: 1,
            client_order_id: 11,
            symbol: *b"BTCUSD\0\0",
            side: Side::BUY,
            order_type: OrderType::LIMIT,
            price: Some(101),
            quantity: 10,
            time_in_force: TimeInForce::GTC,
        });
    }

    let _handle = blp.run();
    unmarshaller_sequence.set(0);

    std::thread::sleep(Duration::from_millis(50));

    assert!(output_sequence.get() >= 0, "BLP did not publish any output");

    let mut found = false;
    for seq in 0..=output_sequence.get() {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(OutputEvent::OrderAccepted { client_id, client_order_id, .. }) = out_slot.event {
            if client_id == 1 && client_order_id == 11 {
                found = true;
            }
        }
    }

    assert!(found, "BLP ran, but no accept event was published");
}
#[test]
fn diagnostic_full_pipeline_one_step() {
    use crate::consumer::blp::BusinessLogicProcessor;
    use crate::consumer::unmarshaller::Unmarshaller;
    use std::sync::Arc;
    use std::time::Duration;

    let input_ring: Arc<RingBuffer<InputSlot>> = Arc::new(RingBuffer::new(16));
    let output_ring: Arc<RingBuffer<OutputSlot>> = Arc::new(RingBuffer::new(16));

    let producer_sequence = Arc::new(Sequence::new(-1));
    let unmarshaller_sequence = Arc::new(Sequence::new(-1));
    let output_sequence = Arc::new(Sequence::new(-1));
    let output_gating_sequence = Arc::new(Sequence::new(-1));

    let unmarshaller_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&producer_sequence)]));
    let blp_barrier = Arc::new(SequenceBarrier::new(vec![Arc::clone(&unmarshaller_sequence)]));

    let unmarshaller = Unmarshaller::new(
        Arc::clone(&input_ring),
        Arc::clone(&unmarshaller_barrier),
        Arc::clone(&producer_sequence),
    );

    let blp = BusinessLogicProcessor::new(
        Arc::clone(&input_ring),
        Arc::clone(&blp_barrier),
        Arc::clone(&unmarshaller_sequence),
        Arc::clone(&output_ring),
        Arc::clone(&output_sequence),
        Arc::clone(&output_gating_sequence),
        100,
        1,
        4096,
    );

    let bytes = {
        let mut b = Vec::new();
        b.push(1u8);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&11u64.to_le_bytes());
        b.extend_from_slice(b"BTCUSD\0\0");
        b.push(0u8);
        b.push(0u8);
        b.push(1u8);
        b.extend_from_slice(&101u64.to_le_bytes());
        b.extend_from_slice(&10u64.to_le_bytes());
        b.push(0u8);
        b
    };

    {
        let slot = unsafe { input_ring.slot_mut_ref(0) };
        slot.raw_bytes = [0u8; 256];
        slot.raw_bytes[..bytes.len()].copy_from_slice(&bytes);
        slot.len = bytes.len();
        slot.command = None;
    }

    let _u = unmarshaller.run();
    let _b = blp.run();

    producer_sequence.set(0);

    std::thread::sleep(Duration::from_millis(150));

    assert!(input_ring.slot_ref(0).command.is_some(), "unmarshaller did not set command");
    assert!(output_sequence.get() >= 0, "BLP did not publish output");

    let mut saw_accept = false;
    for seq in 0..=output_sequence.get() {
        let out_slot = unsafe { output_ring.slot_ref(seq) };
        if let Some(OutputEvent::OrderAccepted { client_id, client_order_id, .. }) = out_slot.event {
            if client_id == 1 && client_order_id == 11 {
                saw_accept = true;
            }
        }
    }

    assert!(saw_accept, "full pipeline did not produce accept event");
}
}