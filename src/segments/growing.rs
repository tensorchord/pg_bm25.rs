use std::num::NonZero;

use crate::{
    datatype::{Bm25VectorBorrowed, Bm25VectorHeader, Bm25VectorInput},
    page::{
        page_alloc, page_append_item, page_get_item, page_get_item_id, page_get_max_offset_number,
        page_read, page_write, PageFlags, PageReadGuard, METAPAGE_BLKNO,
    },
};

use super::meta::MetaPageData;

/// store bm25vector
#[derive(Debug, Clone, Copy)]
pub struct GrowingSegmentData {
    first_blkno: NonZero<pgrx::pg_sys::BlockNumber>,
}

pub struct GrowingSegmentReader {
    index: pgrx::pg_sys::Relation,
    blkno: pgrx::pg_sys::BlockNumber,
}

pub struct GrowingSegmentIterator {
    index: pgrx::pg_sys::Relation,
    blkno: pgrx::pg_sys::BlockNumber,
    page: Option<PageReadGuard>,
    offset: u16,
    count: u16,
}

impl GrowingSegmentReader {
    pub fn new(index: pgrx::pg_sys::Relation, meta: &MetaPageData) -> Option<Self> {
        let data = meta.growing_segment?;
        Some(Self {
            index,
            blkno: data.first_blkno.get(),
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self) -> GrowingSegmentIterator {
        let GrowingSegmentReader { index, blkno } = self;
        let page = page_read(index, blkno);
        let count = page_get_max_offset_number(&page);
        GrowingSegmentIterator {
            index,
            blkno,
            page: Some(page),
            offset: 1,
            count,
        }
    }
}

impl GrowingSegmentIterator {
    // It needs lifetime annotation for borrowed vector, so we don't use std::iter::Iterator
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Bm25VectorBorrowed<'_>> {
        if self.blkno == pgrx::pg_sys::InvalidBlockNumber {
            return None;
        }
        if self.offset > self.count {
            self.blkno = self.page().opaque.next_blkno;
            if self.blkno == pgrx::pg_sys::InvalidBlockNumber {
                self.page = None;
                return None;
            }
            self.page = Some(page_read(self.index, self.blkno));
            self.offset = 1;
            self.count = page_get_max_offset_number(self.page());
        }
        let offset = self.offset;
        self.offset += 1;
        let item_id = page_get_item_id(self.page(), offset);
        let item: &Bm25VectorHeader = page_get_item(self.page(), item_id);
        Some(item.borrow())
    }

    fn page(&self) -> &PageReadGuard {
        self.page.as_ref().unwrap()
    }
}

pub fn growing_segment_insert(index: pgrx::pg_sys::Relation, bm25vector: &Bm25VectorInput) {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(bm25vector.to_bytes());

    {
        let mut metapage = page_write(index, METAPAGE_BLKNO);
        let metapage: *mut MetaPageData = metapage.data_mut().as_mut_ptr().cast();
        let growing_segment = unsafe { &mut (*metapage).growing_segment };

        if growing_segment.is_none() {
            let mut page = page_alloc(index, PageFlags::GROWING, false);
            *growing_segment = Some(GrowingSegmentData {
                first_blkno: NonZero::new(page.blkno()).unwrap(),
            });
            let success = page_append_item(&mut page, &buf);
            assert!(success);
            return;
        }
    }

    let blkno = unsafe {
        pgrx::pg_sys::RelationGetNumberOfBlocksInFork(index, pgrx::pg_sys::ForkNumber::MAIN_FORKNUM)
    } - 1;
    let mut page = page_write(index, blkno);
    if !page_append_item(&mut page, &buf) {
        let mut new_page = page_alloc(index, PageFlags::GROWING, false);
        let success = page_append_item(&mut new_page, &buf);
        assert!(success);
        page.opaque.next_blkno = new_page.blkno();
    }
}
