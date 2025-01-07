use std::{mem::MaybeUninit, num::NonZeroU32};

use crate::{
    algorithm::{BlockDecode, BlockDecodeTrait},
    options::EncodeOption,
    page::{bm25_page_size, page_read, VirtualPageReader},
    segment::{field_norm::id_to_fieldnorm, posting::SkipBlockFlags},
    weight::Bm25Weight,
};

use super::{PostingTermInfo, PostingTermMetaData, SkipBlock, TERMINATED_DOC};

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

pub struct PostingCursor {
    index: pgrx::pg_sys::Relation,
    block_decode: BlockDecode,
    // block reader
    block_page_reader: VirtualPageReader,
    block_page_id: u32,
    page_offset: usize,
    // skip info reader
    skip_info_page_id: u32,
    skip_info_offset: usize,
    decode_offset: u32,
    cur_skip_info: SkipBlock,
    // helper state
    block_decoded: bool,
    remain_block_cnt: u32,
    // unfulled block
    unfulled_docid: Box<[u32]>,
    unfulled_freq: Box<[u32]>,
}

impl PostingCursor {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        term_info: PostingTermInfo,
        encode_option: EncodeOption,
    ) -> Self {
        let PostingTermInfo { meta_blkno } = term_info;

        let term_meta_guard = page_read(index, meta_blkno);
        let block_decode = BlockDecode::new(encode_option);
        let term_meta: &PostingTermMetaData = term_meta_guard.as_ref();
        let block_page_reader = VirtualPageReader::new(index, term_meta.block_data_blkno);
        let remain_block_cnt = term_meta.block_count;
        let unfulled_docid = term_meta.unfulled_docid[..term_meta.unfulled_doc_cnt as usize].into();
        let unfulled_freq = term_meta.unfulled_freq[..term_meta.unfulled_doc_cnt as usize].into();

        let mut this = Self {
            index,
            block_decode,
            block_page_reader,
            block_page_id: 0,
            page_offset: 0,
            skip_info_page_id: term_meta.skip_info_blkno,
            skip_info_offset: 0,
            decode_offset: 0,
            cur_skip_info: SkipBlock::default(),
            block_decoded: false,
            remain_block_cnt,
            unfulled_docid,
            unfulled_freq,
        };

        this.update_skip_info();
        this
    }

    pub fn next_block(&mut self) -> bool {
        debug_assert!(!self.completed());
        self.remain_block_cnt -= 1;
        self.block_decoded = false;
        if self.completed() {
            return false;
        }

        let skip = &self.cur_skip_info;
        self.decode_offset = skip.last_doc;
        self.page_offset += skip.size as usize;
        if skip.flag.contains(SkipBlockFlags::PAGE_CHANGED) || self.is_in_unfulled_block() {
            self.block_page_id += 1;
            self.page_offset = 0;
        }

        self.skip_info_offset += std::mem::size_of::<SkipBlock>();
        if self.skip_info_offset == bm25_page_size() {
            let page = page_read(self.index, self.skip_info_page_id);
            self.skip_info_page_id = page.opaque.next_blkno;
            self.skip_info_offset = 0;
        }
        self.update_skip_info();

        true
    }

    pub fn next_doc(&mut self) -> bool {
        debug_assert!(self.block_decoded);
        if self.is_in_unfulled_block() {
            self.page_offset += 1;
            debug_assert!(self.page_offset <= self.unfulled_doc_cnt() as usize);
            if self.page_offset == self.unfulled_doc_cnt() as usize {
                return false;
            }
            true
        } else {
            self.block_decode.next()
        }
    }

    pub fn next_with_auto_decode(&mut self) -> bool {
        if self.completed() {
            return false;
        }
        if self.next_doc() {
            return true;
        }
        if self.next_block() {
            self.decode_block();
            true
        } else {
            false
        }
    }

    pub fn shallow_seek(&mut self, docid: u32) -> bool {
        if self.completed() {
            return false;
        }
        while self.last_doc_in_block() < docid {
            if !self.next_block() {
                return false;
            }
        }
        true
    }

    pub fn seek(&mut self, docid: u32) -> u32 {
        if self.completed() {
            return TERMINATED_DOC;
        }
        if !self.shallow_seek(docid) {
            return TERMINATED_DOC;
        }
        if !self.block_decoded {
            self.decode_block();
        }

        if self.is_in_unfulled_block() {
            self.page_offset = self.unfulled_docid.partition_point(|&d| d < docid);
            debug_assert!(self.page_offset < self.unfulled_doc_cnt() as usize);
        } else {
            let incomplete = self.block_decode.seek(docid);
            debug_assert!(incomplete);
        }
        debug_assert!(self.docid() >= docid);
        self.docid()
    }

    pub fn decode_block(&mut self) {
        debug_assert!(!self.completed());
        if self.block_decoded {
            return;
        }
        self.block_decoded = true;
        if self.is_in_unfulled_block() {
            return;
        }

        let skip = &self.cur_skip_info;
        let page = page_read(
            self.index,
            self.block_page_reader.get_block_id(self.block_page_id),
        );
        self.block_decode.decode(
            &page.data()[self.page_offset..][..skip.size as usize],
            NonZeroU32::new(self.decode_offset),
            skip.doc_cnt,
        );
    }

    pub fn docid(&self) -> u32 {
        if self.completed() {
            return TERMINATED_DOC;
        }
        debug_assert!(self.block_decoded);
        if self.is_in_unfulled_block() {
            return self.unfulled_docid[self.page_offset];
        }
        debug_assert!(self.block_decode.docid() <= self.last_doc_in_block());
        self.block_decode.docid()
    }

    pub fn freq(&self) -> u32 {
        debug_assert!(!self.completed());
        debug_assert!(self.block_decoded);
        if self.is_in_unfulled_block() {
            return self.unfulled_freq[self.page_offset];
        }
        self.block_decode.freq()
    }

    pub fn block_max_score(&self, weight: &Bm25Weight) -> f32 {
        if self.completed() {
            return 0.0;
        }
        let len = id_to_fieldnorm(self.cur_skip_info.blockwand_fieldnorm_id);
        weight.score(len, self.cur_skip_info.blockwand_tf)
    }

    pub fn last_doc_in_block(&self) -> u32 {
        if self.completed() {
            return TERMINATED_DOC;
        }
        self.cur_skip_info.last_doc
    }

    pub fn completed(&self) -> bool {
        self.remain_block_cnt == 0
    }

    fn update_skip_info(&mut self) {
        let page = page_read(self.index, self.skip_info_page_id);
        let skip_info = *bytemuck::from_bytes(
            &page.data()[self.skip_info_offset..][..std::mem::size_of::<SkipBlock>()],
        );
        self.cur_skip_info = skip_info;
    }

    fn unfulled_doc_cnt(&self) -> u32 {
        self.unfulled_docid.len() as u32
    }

    fn is_in_unfulled_block(&self) -> bool {
        !self.unfulled_docid.is_empty() && self.remain_block_cnt == 1
    }
}
