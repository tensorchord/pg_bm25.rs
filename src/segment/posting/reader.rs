use std::{mem::MaybeUninit, num::NonZeroU32};

use crate::{
    algorithm::{BlockDecode, BlockDecodeTrait},
    options::EncodeOption,
    page::{page_read, PageReadGuard, VirtualPageReader},
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
    term_meta_guard: PageReadGuard,
    block_decode: BlockDecode,
    // ----
    block_page_reader: VirtualPageReader,
    block_page_id: u32,
    page_inner: Option<PageReadGuard>,
    page_offset: usize,
    // ----
    skip_info_reader: PageReadGuard,
    skip_info_offset: usize,
    decode_offset: u32,
    // ----
    block_decoded: bool,
    remain_block_cnt: u32,
    unfulled_doc_cnt: u32,
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
        let skip_info_reader = page_read(index, term_meta.skip_info_blkno);
        let remain_block_cnt = term_meta.block_count;
        let unfulled_doc_cnt = term_meta.unfulled_doc_cnt;

        Self {
            index,
            term_meta_guard,
            block_decode,
            block_page_reader,
            block_page_id: 0,
            page_inner: None,
            page_offset: 0,
            skip_info_reader,
            skip_info_offset: 0,
            decode_offset: 0,
            block_decoded: false,
            remain_block_cnt,
            unfulled_doc_cnt,
        }
    }

    pub fn next_block(&mut self) -> bool {
        debug_assert!(!self.completed());
        self.remain_block_cnt -= 1;
        self.block_decoded = false;
        if self.completed() {
            self.page_inner = None;
            return false;
        }

        let skip = self.skip_info();
        self.decode_offset = skip.last_doc;
        self.page_offset += self.block_decode.size(skip.auxiliary, skip.doc_cnt);
        if skip.flag.contains(SkipBlockFlags::PAGE_CHANGED) || self.is_in_unfulled_block() {
            self.block_page_id += 1;
            self.page_offset = 0;
            self.page_inner = None;
        }

        self.skip_info_offset += std::mem::size_of::<SkipBlock>();
        if self.skip_info_offset == self.skip_info_reader.data().len() {
            self.skip_info_offset = 0;
            let next_blkno = self.skip_info_reader.opaque.next_blkno;
            self.skip_info_reader = page_read(self.index, next_blkno);
        }

        true
    }

    pub fn next_doc(&mut self) -> bool {
        debug_assert!(self.block_decoded);
        if self.is_in_unfulled_block() {
            self.page_offset += 1;
            debug_assert!(self.page_offset <= self.unfulled_doc_cnt as usize);
            if self.page_offset == self.unfulled_doc_cnt as usize {
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
            let term_meta: &PostingTermMetaData = self.term_meta_guard.as_ref();
            self.page_offset = term_meta.unfulled_docid.partition_point(|&d| d < docid)
        } else {
            self.block_decode.seek(docid);
        }
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

        let skip = self.skip_info();
        let page = self.page_inner.get_or_insert_with(|| {
            page_read(
                self.index,
                self.block_page_reader.get_block_id(self.block_page_id),
            )
        });
        self.block_decode.decode(
            &page.data()[self.page_offset..],
            skip.auxiliary,
            NonZeroU32::new(self.decode_offset),
            skip.doc_cnt,
        );
    }

    pub fn docid(&self) -> u32 {
        if self.completed() {
            return TERMINATED_DOC;
        }
        if self.is_in_unfulled_block() {
            debug_assert!(self.page_offset < self.unfulled_doc_cnt as usize);
            let term_meta: &PostingTermMetaData = self.term_meta_guard.as_ref();
            return term_meta.unfulled_docid[self.page_offset];
        }
        self.block_decode.docid()
    }

    pub fn freq(&self) -> u32 {
        debug_assert!(!self.completed());
        debug_assert!(self.block_decoded);
        if self.is_in_unfulled_block() {
            debug_assert!(self.page_offset < self.unfulled_doc_cnt as usize);
            let term_meta: &PostingTermMetaData = self.term_meta_guard.as_ref();
            return term_meta.unfulled_freq[self.page_offset];
        }
        self.block_decode.freq()
    }

    pub fn block_max_score(&self, weight: &Bm25Weight) -> f32 {
        if self.completed() {
            return 0.0;
        }
        let skip = self.skip_info();
        let len = id_to_fieldnorm(skip.blockwand_fieldnorm_id);
        weight.score(len, skip.blockwand_tf)
    }

    pub fn last_doc_in_block(&self) -> u32 {
        let skip = self.skip_info();
        skip.last_doc
    }

    pub fn completed(&self) -> bool {
        self.remain_block_cnt == 0
    }

    fn skip_info(&self) -> SkipBlock {
        *bytemuck::from_bytes(
            &self.skip_info_reader.data()[self.skip_info_offset..]
                [..std::mem::size_of::<SkipBlock>()],
        )
    }

    fn is_in_unfulled_block(&self) -> bool {
        self.unfulled_doc_cnt > 0 && self.remain_block_cnt == 1
    }
}
