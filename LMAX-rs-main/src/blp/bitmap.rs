pub struct BitMap {
    l0: u64,
    l1: [u64; 64],
    l2: [u64; 64 * 64],
}

impl BitMap {
    pub fn new() -> Self {
        Self {
            l0: 0,
            l1: [0; 64],
            l2: [0; 64 * 64],
        }
    }

    pub fn set_bit(&mut self, n: usize) {
        let l0_bit = n >> 12;
        let l1_bit = (n >> 6) & 63;
        let l2_bit = n & 63;

        self.l2[l0_bit * 64 + l1_bit] |= 1u64 << l2_bit;
        self.l1[l0_bit]               |= 1u64 << l1_bit;
        self.l0                       |= 1u64 << l0_bit;
    }

    pub fn clear_bit(&mut self, n: usize) {
        let l0_bit = n >> 12;
        let l1_bit = (n >> 6) & 63;
        let l2_bit = n & 63;

        self.l2[l0_bit * 64 + l1_bit] &= !(1u64 << l2_bit);

        if self.l2[l0_bit * 64 + l1_bit] == 0 {
            self.l1[l0_bit] &= !(1u64 << l1_bit);
        }
        if self.l1[l0_bit] == 0 {
            self.l0 &= !(1u64 << l0_bit);
        }
    }

    pub fn best_ask(&self) -> Option<usize> {
        if self.l0 == 0 {
            return None;
        }

        let i0 = self.l0.trailing_zeros() as usize;
        let i1 = self.l1[i0].trailing_zeros() as usize;
        let i2 = self.l2[i0 * 64 + i1].trailing_zeros() as usize;

        Some((i0 << 12) | (i1 << 6) | i2)
    }

    pub fn best_bid(&self) -> Option<usize> {
        if self.l0 == 0 {
            return None;
        }

        let i0 = 63 - self.l0.leading_zeros() as usize;
        let i1 = 63 - self.l1[i0].leading_zeros() as usize;
        let i2 = 63 - self.l2[i0 * 64 + i1].leading_zeros() as usize;

        Some((i0 << 12) | (i1 << 6) | i2)
    }
}