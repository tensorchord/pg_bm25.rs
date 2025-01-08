use enum_dispatch::enum_dispatch;
use fixed_block_partition::FixedBlockPartition;
use variable_block_partition::VariableBlockPartition;

use crate::options::PartitionOption;

mod fixed_block_partition;
mod variable_block_partition;

#[enum_dispatch]
pub trait BlockPartitionTrait {
    fn partitions(&self) -> &[u32];
    fn max_doc(&self) -> &[u32];
    fn add_doc(&mut self, score: f32);
    fn reset(&mut self);
    fn make_partitions(&mut self);
}

#[enum_dispatch(BlockPartitionTrait)]
pub enum BlockPartition {
    FixedBlockPartition,
    VariableBlockPartition,
}

const FIXED_BLOCK_SIZE: u32 = 128;
const VARIABLE_EPS1: f32 = 0.01;
const VARIABLE_EPS2: f32 = 0.4;

impl BlockPartition {
    pub fn new(option: PartitionOption) -> Self {
        match option {
            PartitionOption::Fixed => FixedBlockPartition::new(FIXED_BLOCK_SIZE).into(),
            PartitionOption::Variable(options) => {
                VariableBlockPartition::new(options.lambda, VARIABLE_EPS1, VARIABLE_EPS2).into()
            }
        }
    }
}
