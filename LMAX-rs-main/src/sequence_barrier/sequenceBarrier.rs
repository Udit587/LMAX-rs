
use std::sync::Arc;
use crate::sequence::sequence::Sequence;

pub struct SequenceBarrier{
    dependencies: Vec<Arc<Sequence>>
}

impl SequenceBarrier{
    pub fn new(deps: Vec<Arc<Sequence>>)->Arc<Self>{
        Arc::new(Self{
            dependencies: deps
        })
    }

    pub fn wait_for(&self, seq: i64){
        loop{
            let min=self.dependencies.iter().map(|s| s.get()).min().unwrap_or(i64::MAX);

            if min>=seq{
                break;
            }
            std::hint::spin_loop();
        }
    }
}