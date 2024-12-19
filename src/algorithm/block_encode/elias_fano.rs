#![allow(unused_variables)]

use std::num::NonZeroU32;

use super::{BlockDecodeTrait, BlockEncodeTrait};

pub struct EliasFanoEncode {}

impl EliasFanoEncode {
    pub fn new() -> Self {
        Self {}
    }
}

impl BlockEncodeTrait for EliasFanoEncode {
    fn encode(
        &mut self,
        last_docid: Option<NonZeroU32>,
        docids: &mut [u32],
        freqs: &mut [u32],
    ) -> (u16, &[u8]) {
        todo!()
    }
}

pub struct EliasFanoDecode {}

impl EliasFanoDecode {
    pub fn new() -> Self {
        Self {}
    }
}

impl BlockDecodeTrait for EliasFanoDecode {
    fn decode(
        &mut self,
        data: &[u8],
        auxiliary: u16,
        last_docid: Option<NonZeroU32>,
        doc_cnt: u32,
    ) {
        todo!()
    }

    fn size(&self, auxiliary: u16, doc_cnt: u32) -> usize {
        todo!()
    }

    fn next(&mut self) -> bool {
        todo!()
    }

    fn seek(&mut self, target: u32) -> bool {
        todo!()
    }

    fn docid(&self) -> u32 {
        todo!()
    }

    fn freq(&self) -> u32 {
        todo!()
    }
}
