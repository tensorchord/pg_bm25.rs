mod reader;
mod serializer;
mod writer;

use bytemuck::{Pod, Zeroable};
pub use reader::{InvertedReader, Posting, PostingReader, TermDictReader, TermInfoReader};
pub use serializer::InvertedSerializer;
pub use writer::PostingsWriter;

pub const COMPRESSION_BLOCK_SIZE: usize =
    <bitpacking::BitPacker4x as bitpacking::BitPacker>::BLOCK_LEN;

#[derive(Clone, Copy, Default)]
pub struct TermInfo {
    pub docs: u32,
    pub postings_blkno: pgrx::pg_sys::BlockNumber,
}

unsafe impl Zeroable for TermInfo {}
unsafe impl Pod for TermInfo {}

#[repr(C, align(4))]
#[derive(Clone, Copy, Default)]
struct SkipBlock {
    last_doc: u32,
    tf_sum: u32,
    blockwand_tf: u32,
    docid_bits: u8,
    tf_bits: u8,
    blockwand_fieldnorm_id: u8,
}

unsafe impl Zeroable for SkipBlock {}
unsafe impl Pod for SkipBlock {}
