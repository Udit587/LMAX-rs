use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

#[repr(align(128))]
pub struct CacheAligned<T>(pub T);

pub struct RingBuffer<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    pub cursor: CacheAligned<AtomicI64>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        assert!(capacity > 0, "Capacity must be greater than 0");

        let mut vec = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            buffer: vec.into_boxed_slice(),
            mask: capacity - 1,
            cursor: CacheAligned(AtomicI64::new(-1)), // -1 = nothing published yet
        }
    }

    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    pub unsafe fn slot_mut(&self, seq: i64) -> *mut MaybeUninit<T> {
        let idx = (seq as usize) & self.mask;
        self.buffer[idx].get()
    }

    pub unsafe fn slot_ref(&self, seq: i64) -> &T {
        let idx = (seq as usize) & self.mask;
        unsafe{(*self.buffer[idx].get()).assume_init_ref()}
    }

    pub unsafe fn slot_ref_idx(&self, idx: usize) -> &T {
        unsafe{(*self.buffer[idx & self.mask].get()).assume_init_ref()}
    }

    // Only Unmarshaller should call this — gives mutable reference to an initialized slot
    pub unsafe fn slot_mut_ref(&self, seq: i64) -> &mut T {
        let idx = (seq as usize) & self.mask;
        unsafe{(*self.buffer[idx].get()).assume_init_mut()}
    }
}

// ✅ NEW — without this, T's destructor never runs → memory leak
impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        let cursor = self.cursor.0.load(Ordering::Acquire);
        if cursor < 0 {
            // nothing was ever published, nothing to drop
            return;
        }
        // drop all slots from 0..=cursor (these were written by the producer)
        // if buffer wrapped around, we drop the whole buffer
        let published = ((cursor as usize) + 1).min(self.capacity());
        for i in 0..published {
            unsafe {
                self.buffer[i].get_mut().assume_init_drop();
            }
        }
    }
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}