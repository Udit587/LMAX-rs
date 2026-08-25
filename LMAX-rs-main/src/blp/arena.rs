use crate::blp::order_id::OrderId;
use crate::blp::price_level::PriceLevel;
use crate::order::order::Order;

pub struct Slot {
    pub generation: u32,
    pub state: SlotState,
}

pub enum SlotState {
    Occupied {
        order: Order,
        prev: Option<usize>,
        next: Option<usize>,
    },
    Free {
        next_free: Option<usize>,
    },
}

impl Slot {
    pub fn new_free(next_free: Option<usize>) -> Self {
        Self {
            generation: 0,
            state: SlotState::Free { next_free },
        }
    }
}

pub struct Arena {
    pub order_store: Vec<Slot>,
    pub free_order_head: Option<usize>,
    pub order_count: usize,
}

impl Arena {
    pub fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        for i in 0..capacity - 1 {
            vec.push(Slot::new_free(Some(i + 1)));
        }
        vec.push(Slot::new_free(None));

        Self {
            order_store: vec,
            free_order_head: Some(0),
            order_count: 0,
        }
    }

    pub fn validate(&self, order_id: OrderId) -> Option<usize> {
        let idx = order_id.index as usize;
        if idx >= self.order_store.len() {
            return None;
        }
        if self.order_store[idx].generation != order_id.generation {
            return None;
        }

        match self.order_store[idx].state {
            SlotState::Occupied { .. } => Some(idx),
            SlotState::Free { .. } => None,
        }
    }

    pub fn alloc_order(
        &mut self,
        order: Order,
        price_level: &mut PriceLevel,
    ) -> Result<OrderId, Order> {
        match self.free_order_head {
            None => Err(order),
            Some(idx) => {
                let next_free = match self.order_store[idx].state {
                    SlotState::Free { next_free } => next_free,
                    _ => None,
                };

                let generation = self.order_store[idx].generation;

                self.order_store[idx].state = SlotState::Occupied {
                    order,
                    prev: price_level.tail,
                    next: None,
                };

                if let Some(tail_idx) = price_level.tail {
                    if let SlotState::Occupied { ref mut next, .. } =
                        self.order_store[tail_idx].state
                    {
                        *next = Some(idx);
                    }
                }

                if price_level.head.is_none() {
                    price_level.head = Some(idx);
                }

                if let SlotState::Occupied { ref order, .. } = self.order_store[idx].state {
                    price_level.total_qty += order.leaves_qty;
                }

                price_level.tail = Some(idx);
                price_level.order_count += 1;
                self.free_order_head = next_free;
                self.order_count += 1;

                Ok(OrderId {
                    index: idx as u32,
                    generation,
                })
            }
        }
    }

    pub fn free_order(&mut self, idx: usize, price_level: &mut PriceLevel) {
        let (prev1, next1) = match self.order_store[idx].state {
            SlotState::Occupied { prev, next, .. } => (prev, next),
            SlotState::Free { .. } => return,
        };

        if let SlotState::Occupied { ref order, .. } = self.order_store[idx].state {
            price_level.total_qty = price_level.total_qty.saturating_sub(order.leaves_qty);
        }

        if let Some(prev_idx) = prev1 {
            if let SlotState::Occupied { ref mut next, .. } =
                self.order_store[prev_idx].state
            {
                *next = next1;
            }
        }

        if let Some(next_idx) = next1 {
            if let SlotState::Occupied { ref mut prev, .. } =
                self.order_store[next_idx].state
            {
                *prev = prev1;
            }
        }

        if price_level.head == Some(idx) {
            price_level.head = next1;
        }
        if price_level.tail == Some(idx) {
            price_level.tail = prev1;
        }

        price_level.order_count -= 1;

        self.order_store[idx].generation += 1;
        self.order_store[idx].state = SlotState::Free {
            next_free: self.free_order_head,
        };
        self.free_order_head = Some(idx);
        self.order_count -= 1;
    }
}