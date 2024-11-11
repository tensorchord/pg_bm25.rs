use std::{fmt::Debug, mem::MaybeUninit};

use crate::{
    page::{bm25_page_size, page_read, PageReadGuard, VirtualPageReader},
    segment::field_norm::id_to_fieldnorm,
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
}

pub struct PostingReader {
    index: pgrx::pg_sys::Relation,
    skip_blocks: Box<[SkipBlock]>,
    doc_count: u32,
    // decoders
    doc_decoder: BlockDecoder,
    freq_decoder: BlockDecoder,
    // skip cursor
    virtual_reader: VirtualPageReader,
    cur_page: pgrx::pg_sys::BlockNumber,
    page_offset: usize,
    page_inner: Option<PageReadGuard>,
    cur_block: usize,
    block_offset: usize,
    remain_doc_cnt: u32,
    block_decoded: bool,
}

impl Debug for PostingReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingReader")
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
impl PostingReader {
    pub fn new(index: pgrx::pg_sys::Relation, term_info: PostingTermInfo) -> Self {
        assert!(term_info.postings_blkno != pgrx::pg_sys::InvalidBlockNumber);
        let reader = VirtualPageReader::new(index, term_info.postings_blkno);
        let block_cnt = (term_info.doc_count).div_ceil(COMPRESSION_BLOCK_SIZE as u32);
        let mut buf: Box<[MaybeUninit<SkipBlock>]> = Box::new_uninit_slice(block_cnt as usize);
        let slice_mut = unsafe {
            std::slice::from_raw_parts_mut(
                buf.as_mut_ptr() as *mut u8,
                block_cnt as usize * std::mem::size_of::<SkipBlock>(),
            )
        };
        let mut offset = 0;
        while offset < slice_mut.len() {
            let len = bm25_page_size().min(slice_mut.len() - offset);
            reader.read_at(offset as u32, &mut slice_mut[offset..][..len]);
            offset += bm25_page_size();
        }
        let skip_blocks = unsafe { buf.assume_init() };

        let mut cur_page = (slice_mut.len() / bm25_page_size()) as u32;
        let mut page_offset = slice_mut.len() % bm25_page_size();

        if term_info.doc_count >= COMPRESSION_BLOCK_SIZE as u32 {
            let first_block_size = skip_blocks[0].block_size();
            if page_offset + first_block_size > bm25_page_size() {
                cur_page += 1;
                page_offset = 0;
            }
        }

        Self {
            index,
            doc_count: term_info.doc_count,
            skip_blocks,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::new(),
            virtual_reader: reader,
            cur_page,
            page_offset,
            page_inner: None,
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

        let page = self.page_inner.get_or_insert_with(|| {
            page_read(self.index, self.virtual_reader.get_block_id(self.cur_page))
        });

        if self.remain_doc_cnt < COMPRESSION_BLOCK_SIZE as u32 {
            let bytes = self.doc_decoder.decompress_vint_sorted(
                &page.data()[self.page_offset..],
                last_doc,
                self.remain_doc_cnt,
            );
            self.freq_decoder.decompress_vint_unsorted(
                &page.data()[(self.page_offset + bytes)..],
                self.remain_doc_cnt,
            );
            self.freq_decoder
                .output_mut()
                .iter_mut()
                .for_each(|v| *v += 1);
        } else {
            let bytes = self.doc_decoder.decompress_block_sorted(
                &page.data()[self.page_offset..],
                skip.docid_bits,
                last_doc,
            );
            self.freq_decoder.decompress_block_unsorted(
                &page.data()[(self.page_offset + bytes)..],
                skip.tf_bits,
            );
            self.freq_decoder
                .output_mut()
                .iter_mut()
                .for_each(|v| *v += 1);
        }
        self.block_offset = 0;
        self.block_decoded = true;
    }

    fn update_page_cursor(&mut self) {
        self.page_offset += self.skip_blocks[self.cur_block - 1].block_size();

        if self.remain_doc_cnt >= COMPRESSION_BLOCK_SIZE as u32 {
            let current_block_size = self.skip_blocks[self.cur_block].block_size();
            if self.page_offset + current_block_size > bm25_page_size() {
                self.cur_page += 1;
                self.page_offset = 0;
                self.page_inner = None;
            }
        } else {
            let page = self.page_inner.get_or_insert_with(|| {
                page_read(self.index, self.virtual_reader.get_block_id(self.cur_page))
            });
            if page.data().len() == self.page_offset {
                self.cur_page += 1;
                self.page_offset = 0;
                self.page_inner = None;
            }
        }
    }
}
