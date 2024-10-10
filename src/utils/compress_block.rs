use bitpacking::{BitPacker, BitPacker4x};

use crate::postings::COMPRESSION_BLOCK_SIZE;

use super::vint;

const COMPRESSED_BLOCK_MAX_BYTES: usize = COMPRESSION_BLOCK_SIZE * std::mem::size_of::<u32>();

pub struct BlockEncoder {
    bitpacker: BitPacker4x,
    output: [u8; COMPRESSED_BLOCK_MAX_BYTES],
}

impl BlockEncoder {
    pub fn new() -> Self {
        Self {
            bitpacker: BitPacker4x::new(),
            output: [0; COMPRESSED_BLOCK_MAX_BYTES],
        }
    }

    pub fn compress_block_sorted(&mut self, block: &[u32], offset: u32) -> (u8, &[u8]) {
        let offset = if offset == 0u32 { None } else { Some(offset) };

        let num_bits = self.bitpacker.num_bits_strictly_sorted(offset, block);
        let written_size =
            self.bitpacker
                .compress_strictly_sorted(offset, block, &mut self.output[..], num_bits);
        (num_bits, &self.output[..written_size])
    }

    pub fn compress_block_unsorted(&mut self, block: &[u32]) -> (u8, &[u8]) {
        let num_bits = self.bitpacker.num_bits(block);
        let written_size = self
            .bitpacker
            .compress(block, &mut self.output[..], num_bits);
        (num_bits, &self.output[..written_size])
    }

    pub fn compress_vint_sorted(&mut self, block: &[u32], mut offset: u32) -> &[u8] {
        let mut byte_written = 0;
        for &v in block {
            let mut to_encode: u32 = v - offset;
            offset = v;
            loop {
                let next_byte: u8 = (to_encode % 128u32) as u8;
                to_encode /= 128u32;
                if to_encode == 0u32 {
                    self.output[byte_written] = next_byte | 128u8;
                    byte_written += 1;
                    break;
                } else {
                    self.output[byte_written] = next_byte;
                    byte_written += 1;
                }
            }
        }
        &self.output[..byte_written]
    }

    pub fn compress_vint_unsorted(&mut self, block: &[u32]) -> &[u8] {
        let mut byte_written = 0;
        for &v in block {
            let mut to_encode: u32 = v;
            loop {
                let next_byte: u8 = (to_encode % 128u32) as u8;
                to_encode /= 128u32;
                if to_encode == 0u32 {
                    self.output[byte_written] = next_byte | 128u8;
                    byte_written += 1;
                    break;
                } else {
                    self.output[byte_written] = next_byte;
                    byte_written += 1;
                }
            }
        }
        &self.output[..byte_written]
    }
}

pub struct BlockDecoder {
    bitpacker: BitPacker4x,
    output: [u32; COMPRESSION_BLOCK_SIZE],
    len: usize,
}

impl BlockDecoder {
    pub fn new() -> Self {
        Self {
            bitpacker: BitPacker4x::new(),
            output: [0; COMPRESSION_BLOCK_SIZE],
            len: 0,
        }
    }

    pub fn decompress_block_sorted(&mut self, block: &[u8], num_bits: u8, offset: u32) -> usize {
        let offset = if offset == 0u32 { None } else { Some(offset) };
        self.len = COMPRESSION_BLOCK_SIZE;
        self.bitpacker
            .decompress_strictly_sorted(offset, block, &mut self.output, num_bits)
    }

    pub fn decompress_block_unsorted(&mut self, block: &[u8], num_bits: u8) -> usize {
        self.len = COMPRESSION_BLOCK_SIZE;
        self.bitpacker.decompress(block, &mut self.output, num_bits)
    }

    pub fn decompress_vint_sorted(&mut self, mut block: &[u8], offset: u32, count: u32) -> usize {
        let count = count as usize;
        let start = block;
        self.len = count;
        let mut res = offset;
        for i in 0..count {
            res += vint::decode_vint32(&mut block);
            self.output[i] = res;
        }
        block.as_ptr() as usize - start.as_ptr() as usize
    }

    pub fn decompress_vint_unsorted(&mut self, mut block: &[u8], count: u32) -> usize {
        let count = count as usize;
        let start = block;
        self.len = count;
        for i in 0..count {
            self.output[i] = vint::decode_vint32(&mut block);
        }
        block.as_ptr() as usize - start.as_ptr() as usize
    }

    #[inline]
    pub fn output(&self) -> &[u32] {
        &self.output[..self.len]
    }

    #[inline]
    pub(crate) fn output_mut(&mut self) -> &mut [u32] {
        &mut self.output[..self.len]
    }
}

pub fn compressed_block_size(num_bits: u8) -> usize {
    (num_bits as usize) * COMPRESSION_BLOCK_SIZE / 8
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_block_encoder() {
        let mut encoder = BlockEncoder::new();
        let block = (0..128).collect::<Vec<_>>();
        let (num_bits, compressed) = encoder.compress_block_sorted(&block, 0);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_block_sorted(compressed, num_bits, 0);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_unsorted() {
        let mut encoder = BlockEncoder::new();
        let block = (0..128).collect::<Vec<_>>();
        let (num_bits, compressed) = encoder.compress_block_unsorted(&block);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_block_unsorted(compressed, num_bits);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_vint_sorted() {
        let mut encoder = BlockEncoder::new();
        let block = (0..100).collect::<Vec<_>>();
        let compressed = encoder.compress_vint_sorted(&block, 0);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_vint_sorted(compressed, 0, 100);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_vint_unsorted() {
        let mut encoder = BlockEncoder::new();
        let block = (0..100).collect::<Vec<_>>();
        let compressed = encoder.compress_vint_unsorted(&block);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_vint_unsorted(compressed, 100);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_random() {
        let mut encoder = BlockEncoder::new();
        let mut block = rand::seq::index::sample(&mut rand::thread_rng(), 100000, 128)
            .into_iter()
            .map(|i| i as u32)
            .collect::<Vec<_>>();
        block.sort_unstable();
        let (num_bits, compressed) = encoder.compress_block_sorted(&block, 0);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_block_sorted(compressed, num_bits, 0);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_unsorted_random() {
        let mut encoder = BlockEncoder::new();
        let block = rand::seq::index::sample(&mut rand::thread_rng(), 100000, 128)
            .into_iter()
            .map(|i| i as u32)
            .collect::<Vec<_>>();
        let (num_bits, compressed) = encoder.compress_block_unsorted(&block);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_block_unsorted(compressed, num_bits);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_vint_sorted_random() {
        let mut encoder = BlockEncoder::new();
        let mut block = rand::seq::index::sample(&mut rand::thread_rng(), 100000, 100)
            .into_iter()
            .map(|i| i as u32)
            .collect::<Vec<_>>();
        block.sort_unstable();
        let compressed = encoder.compress_vint_sorted(&block, 0);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_vint_sorted(compressed, 0, 100);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }

    #[test]
    fn test_block_encoder_vint_unsorted_random() {
        let mut encoder = BlockEncoder::new();
        let block = rand::seq::index::sample(&mut rand::thread_rng(), 100000, 100)
            .into_iter()
            .map(|i| i as u32)
            .collect::<Vec<_>>();
        let compressed = encoder.compress_vint_unsorted(&block);
        let mut decoder = BlockDecoder::new();
        let bytes = decoder.decompress_vint_unsorted(compressed, 100);
        assert_eq!(decoder.output(), block);
        assert_eq!(bytes, compressed.len());
    }
}
