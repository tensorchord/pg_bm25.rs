mod reader;
mod serializer;
mod writer;

use bytemuck::{Pod, Zeroable};
pub use reader::{PostingReader, PostingTermInfoReader};
pub use serializer::{InvertedAppender, InvertedSerialize, InvertedSerializer};
pub use writer::InvertedWriter;

use crate::utils::compress_block::compressed_block_size;

pub const TERMINATED_DOC: u32 = u32::MAX;

pub const COMPRESSION_BLOCK_SIZE: usize =
    <bitpacking::BitPacker4x as bitpacking::BitPacker>::BLOCK_LEN;

#[derive(Clone, Copy)]
pub struct PostingTermInfo {
    pub doc_count: u32,
    pub skip_info_blkno: pgrx::pg_sys::BlockNumber,
    pub skip_info_last_blkno: pgrx::pg_sys::BlockNumber,
    pub block_data_blkno: pgrx::pg_sys::BlockNumber,
}

impl PostingTermInfo {
    pub fn empty() -> Self {
        Self {
            doc_count: 0,
            skip_info_blkno: pgrx::pg_sys::InvalidBlockNumber,
            skip_info_last_blkno: pgrx::pg_sys::InvalidBlockNumber,
            block_data_blkno: pgrx::pg_sys::InvalidBlockNumber,
        }
    }
}

unsafe impl Zeroable for PostingTermInfo {}
unsafe impl Pod for PostingTermInfo {}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct SkipBlockFlags: u8 {
        const UNFULLED = 1 << 0;
        const PAGE_CHANGED = 1 << 1;
    }
}

impl Default for SkipBlockFlags {
    fn default() -> Self {
        SkipBlockFlags::empty()
    }
}

// for unfulled block, docid_bits and tf_bits are combined into a single u16 to store the block size
#[derive(Clone, Copy, Default, Debug)]
pub struct SkipBlock {
    last_doc: u32,
    blockwand_tf: u32,
    docid_bits: u8,
    tf_bits: u8,
    blockwand_fieldnorm_id: u8,
    flag: SkipBlockFlags,
}

unsafe impl Zeroable for SkipBlock {}
unsafe impl Pod for SkipBlock {}

impl SkipBlock {
    // unfulled block will return invalid block size
    pub fn block_size(&self) -> usize {
        if !self.flag.contains(SkipBlockFlags::UNFULLED) {
            compressed_block_size(self.docid_bits) + compressed_block_size(self.tf_bits)
        } else {
            ((self.docid_bits as usize) << 8) | (self.tf_bits as usize)
        }
    }
}
