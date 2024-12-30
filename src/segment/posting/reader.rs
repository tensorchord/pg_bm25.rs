use std::{fmt::Debug, io::Read, mem::MaybeUninit};

use crate::{
    page::{page_read, PageReader, VirtualPageReader},
    segment::{field_norm::id_to_fieldnorm, posting::SkipBlockFlags},
    utils::compress_block::BlockDecoder,
    weight::Bm25Weight,
};

use super::{PostingTermInfo, SkipBlock, COMPRESSION_BLOCK_SIZE, TERMINATED_DOC};

pub struct PostingTermInfoReader(VirtualPageReader);

impl PostingTermInfoReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(VirtualPageReader::new(index, blkno))
    }

    pub fn read(&self, term_id: u32) -> PostingTermInfo {
        let mut buf = MaybeUninit::uninit();
        self.0.read_at(
            term_id * std::mem::size_of::<PostingTermInfo>() as u32,
            unsafe {
                std::slice::from_raw_parts_mut(
                    buf.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<PostingTermInfo>(),
                )
            },
        );
        unsafe { buf.assume_init() }
    }

    pub fn write(&mut self, term_id: u32, info: PostingTermInfo) {
        self.0.update_at(
            term_id * std::mem::size_of::<PostingTermInfo>() as u32,
            std::mem::size_of::<PostingTermInfo>() as u32,
            |data| {
                data.copy_from_slice(bytemuck::bytes_of(&info));
            },
        );
    }
}

pub struct PostingReader<const WITH_FREQ: bool> {
    index: pgrx::pg_sys::Relation,
    skip_blocks: Box<[SkipBlock]>,
    doc_count: u32,
    // decoders
    doc_decoder: BlockDecoder,
    freq_decoder: BlockDecoder,
    // skip cursor
    block_data_reader: VirtualPageReader,
    cur_page: pgrx::pg_sys::BlockNumber,
    page_offset: usize,
    cur_block: usize,
    block_offset: usize,
    remain_doc_cnt: u32,
    block_decoded: bool,
}

impl<const WITH_FREQ: bool> Debug for PostingReader<WITH_FREQ> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingReader")
            .field("with_freq", &WITH_FREQ)
            .field("doc_count", &self.doc_count)
            .field("cur_page", &self.cur_page)
            .field("page_offset", &self.page_offset)
            .field("cur_block", &self.cur_block)
            .field("block_offset", &self.block_offset)
            .field("remain_doc_cnt", &self.remain_doc_cnt)
            .field("block_decoded", &self.block_decoded)
            .finish()
    }
}

// This api is used in 2 ways:
// - advance_block + advance_cur to move forward, manually call decode_block
// - shallow_seek + seek to move to a specific doc_id, advance to move forward. it will decode_block automatically
impl<const WITH_FREQ: bool> PostingReader<WITH_FREQ> {
    pub fn new(index: pgrx::pg_sys::Relation, term_info: PostingTermInfo) -> Self {
        assert!(term_info.doc_count > 0);
        assert!(term_info.skip_info_blkno != pgrx::pg_sys::InvalidBlockNumber);
        assert!(term_info.block_data_blkno != pgrx::pg_sys::InvalidBlockNumber);
        let mut skip_info_reader = PageReader::new(index, term_info.skip_info_blkno);
        let block_cnt = (term_info.doc_count).div_ceil(COMPRESSION_BLOCK_SIZE as u32);

        // for memory alignment
        let mut buf: Box<[MaybeUninit<SkipBlock>]> = Box::new_uninit_slice(block_cnt as usize);
        let slice_mut = unsafe {
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut u8,
                block_cnt as usize * std::mem::size_of::<SkipBlock>(),
            )
        };
        skip_info_reader.read_exact(slice_mut).unwrap();
        drop(skip_info_reader);
        let skip_blocks = unsafe { buf.assume_init() };

        let block_data_reader = VirtualPageReader::new(index, term_info.block_data_blkno);

        Self {
            index,
            doc_count: term_info.doc_count,
            skip_blocks,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::new(),
            block_data_reader,
            cur_page: 0,
            page_offset: 0,
            cur_block: 0,
            block_offset: 0,
            remain_doc_cnt: term_info.doc_count,
            block_decoded: false,
        }
    }

    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }

    pub fn advance_block(&mut self) -> bool {
        debug_assert!(!self.completed());
        self.cur_block += 1;
        self.block_decoded = false;
        self.remain_doc_cnt -= std::cmp::min(COMPRESSION_BLOCK_SIZE as u32, self.remain_doc_cnt);
        self.update_page_cursor();
        if self.completed() {
            return false;
        }
        true
    }

    pub fn advance_cur(&mut self) -> bool {
        debug_assert!(self.block_decoded);
        if self.block_offset < COMPRESSION_BLOCK_SIZE.min(self.remain_doc_cnt as usize) {
            self.block_offset += 1;
        }
        if self.block_offset == COMPRESSION_BLOCK_SIZE.min(self.remain_doc_cnt as usize) {
            return false;
        }
        true
    }

    pub fn advance(&mut self) -> bool {
        if self.completed() {
            return false;
        }
        if self.advance_cur() {
            return true;
        }
        if self.advance_block() {
            self.decode_block();
            true
        } else {
            false
        }
    }

    pub fn shallow_seek(&mut self, doc_id: u32) -> bool {
        if self.completed() {
            return false;
        }
        while self.skip_blocks[self.cur_block].last_doc < doc_id {
            if !self.advance_block() {
                return false;
            }
        }
        true
    }

    pub fn seek(&mut self, doc_id: u32) -> u32 {
        if self.completed() {
            self.block_offset = 128;
            return TERMINATED_DOC;
        }
        if !self.shallow_seek(doc_id) {
            return TERMINATED_DOC;
        }
        if !self.block_decoded {
            self.decode_block();
        }
        self.block_offset = self.doc_decoder.output().partition_point(|&v| v < doc_id);
        self.doc_id()
    }

    pub fn doc_id(&self) -> u32 {
        if self.completed() && self.block_offset == 128 {
            return TERMINATED_DOC;
        }
        self.doc_decoder.output()[self.block_offset]
    }

    pub fn term_freq(&self) -> u32 {
        debug_assert!(!self.completed());
        debug_assert!(self.block_decoded);
        const {
            assert!(WITH_FREQ);
        }
        self.freq_decoder.output()[self.block_offset]
    }

    pub fn block_max_score(&self, bm25_weight: &Bm25Weight) -> f32 {
        if self.completed() {
            return 0.0;
        }
        let fieldnorm_id = self.skip_blocks[self.cur_block].blockwand_fieldnorm_id;
        let fieldnorm = id_to_fieldnorm(fieldnorm_id);
        let tf = self.skip_blocks[self.cur_block].blockwand_tf;
        bm25_weight.score(fieldnorm, tf)
    }

    pub fn last_doc_in_block(&self) -> u32 {
        if self.completed() {
            return TERMINATED_DOC;
        }
        self.skip_blocks[self.cur_block].last_doc
    }

    pub fn completed(&self) -> bool {
        self.remain_doc_cnt == 0
    }

    pub fn decode_block(&mut self) {
        debug_assert!(!self.completed());
        if self.block_decoded {
            return;
        }
        let skip = &self.skip_blocks[self.cur_block];
        let last_doc = if self.cur_block == 0 {
            0
        } else {
            self.skip_blocks[self.cur_block - 1].last_doc
        };

        let page = page_read(
            self.index,
            self.block_data_reader.get_block_id(self.cur_page),
        );

        if self.remain_doc_cnt < COMPRESSION_BLOCK_SIZE as u32 {
            debug_assert!(skip.flag.contains(SkipBlockFlags::UNFULLED));
            let bytes = self.doc_decoder.decompress_vint_sorted(
                &page.data()[self.page_offset + std::mem::size_of::<u32>()..],
                last_doc,
                self.remain_doc_cnt,
            );
            if WITH_FREQ {
                self.freq_decoder.decompress_vint_unsorted(
                    &page.data()[(self.page_offset + std::mem::size_of::<u32>() + bytes)..],
                    self.remain_doc_cnt,
                );
                self.freq_decoder
                    .output_mut()
                    .iter_mut()
                    .for_each(|v| *v += 1);
            }
        } else {
            debug_assert!(!skip.flag.contains(SkipBlockFlags::UNFULLED));
            let bytes = self.doc_decoder.decompress_block_sorted(
                &page.data()[self.page_offset..],
                skip.docid_bits,
                last_doc,
            );
            if WITH_FREQ {
                self.freq_decoder.decompress_block_unsorted(
                    &page.data()[(self.page_offset + bytes)..],
                    skip.tf_bits,
                );
                self.freq_decoder
                    .output_mut()
                    .iter_mut()
                    .for_each(|v| *v += 1);
            }
        }
        self.block_offset = 0;
        self.block_decoded = true;
    }

    fn update_page_cursor(&mut self) {
        self.page_offset += self.skip_blocks[self.cur_block - 1].block_size();

        if self.completed() {
            self.page_offset = 0;
            return;
        }

        if self.skip_blocks[self.cur_block]
            .flag
            .contains(SkipBlockFlags::PAGE_CHANGED)
        {
            self.cur_page += 1;
            self.page_offset = 0;
        }
    }
}
