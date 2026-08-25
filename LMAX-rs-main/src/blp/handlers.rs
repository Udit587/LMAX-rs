use std::collections::HashMap;

use crate::blp::arena::SlotState;
use crate::blp::book::OrderBook;
//use crate::blp::handlers::OrderIndex;
use crate::blp::matching::{match_aggressor, ClientAccount, MAX_OPEN_ORDERS};
use crate::order::order::Order;
use crate::order::orderType::OrderType;
use crate::order::side::Side;
use crate::order::timeInForce::TimeInForce;
use crate::output_event::{outputEvent::OutputEvent, rejectReason::RejectReason};
use crate::blp::order_id::OrderId;
#[derive(Clone, Copy)]
pub struct OrderLocator {
    pub oid: OrderId,
    pub tick: usize,
    pub side: Side,
     pub internal_id: u64,
}

pub type OrderIndex = HashMap<(u64, u64), OrderLocator>;

pub fn process_place(
    book: &mut OrderBook,
    clients: &mut HashMap<u64, ClientAccount>,
    order_index: &mut OrderIndex,
    next_order_id: &mut u64,
    client_id: u64,
    client_order_id: u64,
    symbol: [u8; 8],
    side: Side,
    order_type: OrderType,
    price: Option<u64>,
    quantity: u64,
    emit: &mut impl FnMut(OutputEvent),
) {
    if quantity == 0 {
        emit(OutputEvent::OrderRejected {
            client_id,
            client_order_id,
            reason: RejectReason::InvalidQuantity,
        });
        return;
    }

    {
        let account = clients.entry(client_id).or_default();
        if account.open_orders >= MAX_OPEN_ORDERS {
            emit(OutputEvent::OrderRejected {
                client_id,
                client_order_id,
                reason: RejectReason::ClientOrderLimitExceeded,
            });
            return;
        }
    }

    let order_id = *next_order_id;
    *next_order_id += 1;

    match order_type {
        OrderType::MARKET => {
            let leaves_qty = match_aggressor(
                book,
                order_index,
                clients,
                order_id,
                client_id,
                symbol,
                side,
                None,
                quantity,
                emit,
            );

            if leaves_qty > 0 {
                emit(OutputEvent::OrderCancelled {
                    order_id,
                    client_id,
                    leaves_qty,
                });
            }
        }

        OrderType::LIMIT => {
            let price = match price {
                Some(p) => p,
                None => {
                    emit(OutputEvent::OrderRejected {
                        client_id,
                        client_order_id,
                        reason: RejectReason::InvalidPrice,
                    });
                    return;
                }
            };

            let tick_idx = match book.price_to_tick(price) {
                Some(t) => t,
                None => {
                    emit(OutputEvent::OrderRejected {
                        client_id,
                        client_order_id,
                        reason: RejectReason::InvalidPrice,
                    });
                    return;
                }
            };

            let leaves_qty = match_aggressor(
                book,
                order_index,
                clients,
                order_id,
                client_id,
                symbol,
                side,
                Some(price),
                quantity,
                emit,
            );

            if leaves_qty > 0 {
                let order = Order {
                    order_id,
                    client_order_id,
                    client_id,
                    symbol,
                    side,
                    order_type,
                    time_in_force: TimeInForce::GTC,
                    price,
                    quantity,
                    leaves_qty,
                    filled_qty: quantity - leaves_qty,
                };

                let result = match side {
                    Side::BUY => book.bids.add_order(tick_idx, order, &mut book.arena),
                    Side::SELL => book.asks.add_order(tick_idx, order, &mut book.arena),
                };

                match result {
                    Ok(oid) => {
                        let account = clients.entry(client_id).or_default();
                        account.open_orders += 1;

                        order_index.insert((client_id, client_order_id), OrderLocator {
                            oid,
                            tick: tick_idx,
                            side,
                            internal_id: order_id,
                        });


                        emit(OutputEvent::OrderAccepted {
                            order_id,
                            client_id,
                            client_order_id,
                            symbol,
                            side,
                            price,
                            quantity: leaves_qty,
                        });
                    }
                    Err(_) => {
                        emit(OutputEvent::OrderRejected {
                            client_id,
                            client_order_id,
                            reason: RejectReason::ArenaFull,
                        });
                    }
                }
            }
        }
    }
}


pub fn process_cancel(
    book: &mut OrderBook,
    clients: &mut HashMap<u64, ClientAccount>,
    order_index: &mut OrderIndex,
    client_id: u64,
    client_order_id: u64,          // renamed from order_id
    emit: &mut impl FnMut(OutputEvent),
) {
    let locator = match order_index.get(&(client_id, client_order_id)).copied() {
        Some(v) => v,
        None => {
            emit(OutputEvent::CancelRejected { order_id: client_order_id, client_id });
            return;
        }
    };

    let slot_idx = match book.arena.validate(locator.oid) {
        Some(idx) => idx,
        None => {
            order_index.remove(&(client_id, client_order_id));
            emit(OutputEvent::CancelRejected { order_id: client_order_id, client_id });
            return;
        }
    };

    let (owner, leaves_qty) = match &book.arena.order_store[slot_idx].state {
        SlotState::Occupied { order, .. } => (order.client_id, order.leaves_qty),
        _ => {
            emit(OutputEvent::CancelRejected { order_id: client_order_id, client_id });
            return;
        }
    };

    if owner != client_id {
        emit(OutputEvent::CancelRejected { order_id: client_order_id, client_id });
        return;
    }

    match locator.side {
        Side::BUY => book.bids.remove_order(locator.tick, slot_idx, &mut book.arena),
        Side::SELL => book.asks.remove_order(locator.tick, slot_idx, &mut book.arena),
    }

    order_index.remove(&(client_id, client_order_id));

    if let Some(ac) = clients.get_mut(&client_id) {
        ac.open_orders = ac.open_orders.saturating_sub(1);
    }

    emit(OutputEvent::OrderCancelled {
        order_id: locator.internal_id,   // the real internal u64
        client_id,
        leaves_qty,
    });
}


pub fn process_modify(
    book: &mut OrderBook,
    clients: &mut HashMap<u64, ClientAccount>,
    order_index: &mut OrderIndex,
    client_id: u64,
    client_order_id: u64,
    new_price: Option<u64>,
    new_qty: Option<u64>,
    emit: &mut impl FnMut(OutputEvent),
) {
    let locator = match order_index.get(&(client_id, client_order_id)).copied() {
        Some(v) => v,
        None => {
            emit(OutputEvent::ModifyRejected { order_id: client_order_id, client_id });
            return;
        }
    };

    let slot_idx = match book.arena.validate(locator.oid) {
        Some(idx) => idx,
        None => {
            order_index.remove(&(client_id, client_order_id));
            emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
            return;
        }
    };

    let (owner, current_price, current_qty, current_leaves, current_filled) =
        match &book.arena.order_store[slot_idx].state {
            SlotState::Occupied { order, .. } => (
                order.client_id,
                order.price,
                order.quantity,
                order.leaves_qty,
                order.filled_qty,
            ),
            _ => {
                emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
                return;
            }
        };

    if owner != client_id {
        emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
        return;
    }

    let target_price = new_price.unwrap_or(current_price);
    let target_leaves = new_qty.unwrap_or(current_leaves);

    if target_leaves == 0 {
        emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
        return;
    }

    let price_changed = target_price != current_price;
    let qty_increased = target_leaves > current_leaves;

    // simple case: price unchanged and qty reduced — no need to re-queue
    if !price_changed && !qty_increased {
        let diff = current_leaves - target_leaves;

        match &mut book.arena.order_store[slot_idx].state {
            SlotState::Occupied { order, .. } => {
                order.leaves_qty = target_leaves;
                order.filled_qty = current_filled + diff;
            }
            _ => {
                emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
                return;
            }
        }

        match locator.side {
            Side::BUY => {
                if let Some(pl) = book.bids.price_levels[locator.tick].as_mut() {
                    pl.total_qty = pl.total_qty.saturating_sub(diff);
                }
            }
            Side::SELL => {
                if let Some(pl) = book.asks.price_levels[locator.tick].as_mut() {
                    pl.total_qty = pl.total_qty.saturating_sub(diff);
                }
            }
        }

        emit(OutputEvent::OrderModified {
            order_id: locator.internal_id,
            client_id,
            new_price: target_price,
            new_qty: target_leaves,
        });

        return;
    }

    // complex case: price changed or qty increased — must remove, re-insert, lose queue priority
    let (symbol, side, order_type, prev_quantity, prev_filled) =
        match &book.arena.order_store[slot_idx].state {
            SlotState::Occupied { order, .. } => (
                order.symbol,
                order.side,
                order.order_type,
                order.quantity,
                order.filled_qty,
            ),
            _ => {
                emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
                return;
            }
        };

    match locator.side {
        Side::BUY => book.bids.remove_order(locator.tick, slot_idx, &mut book.arena),
        Side::SELL => book.asks.remove_order(locator.tick, slot_idx, &mut book.arena),
    }

    order_index.remove(&(client_id, client_order_id));

    let new_tick = match book.price_to_tick(target_price) {
        Some(t) => t,
        None => {
            // price level doesn't exist in this book's tick range
            if let Some(ac) = clients.get_mut(&client_id) {
                ac.open_orders = ac.open_orders.saturating_sub(1);
            }
            emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
            return;
        }
    };

    let new_order = Order {
        order_id: locator.internal_id,
        client_order_id,
        client_id,
        symbol,
        side,
        order_type,
        time_in_force: TimeInForce::GTC,
        price: target_price,
        quantity: prev_quantity,
        leaves_qty: target_leaves,
        filled_qty: prev_filled,
    };

    let result = match side {
        Side::BUY => book.bids.add_order(new_tick, new_order, &mut book.arena),
        Side::SELL => book.asks.add_order(new_tick, new_order, &mut book.arena),
    };

    match result {
        Ok(new_oid) => {
            order_index.insert((client_id, client_order_id), OrderLocator {
                oid: new_oid,
                tick: new_tick,
                side,
                internal_id: locator.internal_id,  // preserve original internal id
            });

            emit(OutputEvent::OrderModified {
                order_id: locator.internal_id,
                client_id,
                new_price: target_price,
                new_qty: target_leaves,
            });
        }
        Err(_) => {
            // arena is full — order was removed but couldn't be re-inserted
            // decrement open_orders since the order no longer exists
            if let Some(ac) = clients.get_mut(&client_id) {
                ac.open_orders = ac.open_orders.saturating_sub(1);
            }
            emit(OutputEvent::ModifyRejected { order_id: locator.internal_id, client_id });
        }
    }
}