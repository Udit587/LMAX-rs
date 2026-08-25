
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[repr(C)]  //guaratess layout: pad0 | seq | pad1
pub struct Sequence{
    pad0: [u8;120],
    seq: AtomicI64,
    pad1:[u8;120]
}


impl Sequence{
    pub fn new(initial: i64)->Arc<Self>{
        Arc::new(
            Self{
                pad0:[0u8;120],
                seq:AtomicI64::new(initial),
                pad1: [0u8;120],
            }
        )
    }

    pub fn get(&self)->i64{
        self.seq.load(Ordering::Acquire)
    }

    pub fn set(&self,val: i64){
        self.seq.store(val,Ordering::Release)
    }
}