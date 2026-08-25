use std::collections::HashMap;

use crate::blp::arena::SlotState;
use crate::blp::book::OrderBook;
use crate::blp::handlers::OrderIndex;
use crate::order::side::Side;
use crate::output_event::outputEvent::OutputEvent;

#[derive(Default)]
pub struct ClientAccount {
    pub open_orders: u32,
}

pub const MAX_OPEN_ORDERS: u32 = 1000;

pub fn match_aggressor(
    book: &mut OrderBook,
    order_index: &mut OrderIndex,
    clients: &mut HashMap<u64, ClientAccount>,
    aggressor_order_id: u64,
    aggressor_client_id: u64,
    symbol: [u8; 8],
    side: Side,
    limit_price: Option<u64>,
    mut leaves_qty: u64,
    emit: &mut impl FnMut(OutputEvent),
) -> u64 {
    loop {
        if leaves_qty == 0 {
            break;
        }

        let best_tick = match side {
            Side::BUY => book.asks.best_ask(),
            Side::SELL => book.bids.best_bid(),
        };

        let best_tick = match best_tick {
            Some(t) => t,
            None => break,
        };

        if let Some(limit_price) = limit_price {
            let best_price = book.tick_to_price(best_tick);
            let crosses = match side {
                Side::BUY => limit_price >= best_price,
                Side::SELL => limit_price <= best_price,
            };
            if !crosses {
                break;
            }
        }

        let resting_slot_idx = match side {
            Side::BUY => book.asks.get_price_level(best_tick).and_then(|pl| pl.head),
            Side::SELL => book.bids.get_price_level(best_tick).and_then(|pl| pl.head),
        };

        let resting_slot_idx = match resting_slot_idx {
            Some(idx) => idx,
            None => break,
        };

        let (resting_order_id, resting_client_id, resting_client_order_id, resting_qty, fill_price) =
            match &book.arena.order_store[resting_slot_idx].state {
                SlotState::Occupied { order, .. } => (
                    order.order_id,
                    order.client_id,
                    order.client_order_id,
                    order.leaves_qty,
                    order.price,
                ),
                _ => break,
            };

        let fill_qty = leaves_qty.min(resting_qty);

        emit(OutputEvent::Fill {
            aggressor_order_id,
            resting_order_id,
            aggressor_client_id,
            resting_client_id,
            symbol,
            price: fill_price,
            quantity: fill_qty,
            aggressor_side: side,
        });

        leaves_qty -= fill_qty;

        if fill_qty == resting_qty {
            // resting order fully filled — remove from book and index
            match side {
                Side::BUY => book.asks.remove_order(best_tick, resting_slot_idx, &mut book.arena),
                Side::SELL => book.bids.remove_order(best_tick, resting_slot_idx, &mut book.arena),
            }

            order_index.remove(&(resting_client_id, resting_client_order_id));

            if let Some(ac) = clients.get_mut(&resting_client_id) {
                ac.open_orders = ac.open_orders.saturating_sub(1);
            }
        } else {
            // resting order partially filled — update qty in place, keep in book
            match &mut book.arena.order_store[resting_slot_idx].state {
                SlotState::Occupied { order, .. } => {
                    order.leaves_qty -= fill_qty;
                    order.filled_qty += fill_qty;
                }
                _ => break,
            }

            match side {
                Side::BUY => {
                    if let Some(pl) = book.asks.price_levels[best_tick].as_mut() {
                        pl.total_qty = pl.total_qty.saturating_sub(fill_qty);
                    }
                }
                Side::SELL => {
                    if let Some(pl) = book.bids.price_levels[best_tick].as_mut() {
                        pl.total_qty = pl.total_qty.saturating_sub(fill_qty);
                    }
                }
            }

            if leaves_qty == 0 {
                break;
            }
        }
    }

    leaves_qty
}