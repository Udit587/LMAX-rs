use crate::blp::arena::Arena;
use crate::blp::bitmap::BitMap;
use crate::blp::order_id::OrderId;
use crate::blp::price_level::PriceLevel;
use crate::order::order::Order;

pub struct HalfBook {
    pub bitmap: BitMap,
    pub price_levels: Box<[Option<PriceLevel>]>,
}

impl HalfBook {
    pub fn new() -> Self {
        let price_levels = (0..64 * 64 * 64)
            .map(|_| None)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            bitmap: BitMap::new(),
            price_levels,
        }
    }

    pub fn add_order(
        &mut self,
        tick_idx: usize,
        order: Order,
        arena: &mut Arena,
    ) -> Result<OrderId, Order> {
        if self.price_levels[tick_idx].is_none() {
            self.price_levels[tick_idx] = Some(PriceLevel::new());
        }

        let price_level = self.price_levels[tick_idx].as_mut().unwrap();
        let result = arena.alloc_order(order, price_level);

        if result.is_ok() {
            self.bitmap.set_bit(tick_idx);
        } else if price_level.order_count == 0 {
            self.price_levels[tick_idx] = None;
        }

        result
    }

    pub fn remove_order(&mut self, tick_idx: usize, slot_idx: usize, arena: &mut Arena) {
        if let Some(price_level) = self.price_levels[tick_idx].as_mut() {
            arena.free_order(slot_idx, price_level);
            if price_level.order_count == 0 {
                self.price_levels[tick_idx] = None;
                self.bitmap.clear_bit(tick_idx);
            }
        }
    }

    pub fn best_ask(&self) -> Option<usize> {
        self.bitmap.best_ask()
    }

    pub fn best_bid(&self) -> Option<usize> {
        self.bitmap.best_bid()
    }

    pub fn get_price_level(&self, tick_idx: usize) -> Option<&PriceLevel> {
        self.price_levels[tick_idx].as_ref()
    }
}

pub struct OrderBook {
    pub bids: HalfBook,
    pub asks: HalfBook,
    pub arena: Arena,
    pub base_price: u64,
    pub tick_size: u64,
}

impl OrderBook {
    pub fn new(base_price: u64, tick_size: u64, capacity: usize) -> Self {
        Self {
            bids: HalfBook::new(),
            asks: HalfBook::new(),
            arena: Arena::new(capacity),
            base_price,
            tick_size,
        }
    }

    pub fn price_to_tick(&self, price: u64) -> Option<usize> {
        if price < self.base_price {
            return None;
        }

        let tick = ((price - self.base_price) / self.tick_size) as usize;
        if tick >= 64 * 64 * 64 {
            return None;
        }

        Some(tick)
    }

    pub fn tick_to_price(&self, tick: usize) -> u64 {
        self.base_price + (tick as u64 * self.tick_size)
    }
}