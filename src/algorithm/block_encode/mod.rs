use std::num::NonZero;

use delta_bitpack::{DeltaBitpackDecode, DeltaBitpackEncode};
use elias_fano::{EliasFanoDecode, EliasFanoEncode};
use enum_dispatch::enum_dispatch;

use crate::options::EncodeOption;

mod delta_bitpack;
mod elias_fano;

#[enum_dispatch]
pub trait BlockEncodeTrait {
    fn encode(
        &mut self,
        offset: Option<NonZero<u32>>,
        docids: &mut [u32],
        freqs: &mut [u32],
    ) -> &[u8];
}

#[enum_dispatch]
pub trait BlockDecodeTrait {
    fn decode(&mut self, data: &[u8], offset: Option<NonZero<u32>>, doc_cnt: u32);
    fn next(&mut self) -> bool;
    fn seek(&mut self, target: u32) -> bool;
    fn docid(&self) -> u32;
    fn freq(&self) -> u32;
}

#[enum_dispatch(BlockEncodeTrait)]
pub enum BlockEncode {
    DeltaBitpackEncode,
    EliasFanoEncode,
}

#[enum_dispatch(BlockDecodeTrait)]
pub enum BlockDecode {
    DeltaBitpackDecode,
    EliasFanoDecode,
}

impl BlockEncode {
    pub fn new(option: EncodeOption) -> Self {
        match option {
            EncodeOption::DeltaBitpack => DeltaBitpackEncode::new().into(),
            EncodeOption::EliasFano => EliasFanoEncode::new().into(),
        }
    }
}

impl BlockDecode {
    pub fn new(option: EncodeOption) -> Self {
        match option {
            EncodeOption::DeltaBitpack => DeltaBitpackDecode::new().into(),
            EncodeOption::EliasFano => EliasFanoDecode::new().into(),
        }
    }
}
