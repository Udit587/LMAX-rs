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
use blp::handlers::{process_cancel, process_modify, process_place, OrderIndex};
use blp::matching::ClientAccount;
use order::command::Command;
use order::orderType::OrderType;
use order::side::Side;
use order::timeInForce::TimeInForce;
use output_event::outputEvent::OutputEvent;
use output_event::rejectReason::RejectReason;
use crate::blp::matching::MAX_OPEN_ORDERS;

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

}