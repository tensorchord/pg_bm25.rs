use std::num::NonZero;

use crate::{
    datatype::{Bm25VectorBorrowed, Bm25VectorHeader, Bm25VectorInput},
    guc::SEGMENT_GROWING_MAX_PAGE_SIZE,
    page::{
        page_alloc_with_fsm, page_append_item, page_get_item, page_get_item_id,
        page_get_max_offset_number, page_read, page_write, PageFlags, PageReadGuard,
    },
    segment::sealed::SealedSegmentWriter,
};

use super::{
    field_norm::FieldNormReader, meta::MetaPageData, posting::InvertedSerializer,
    sealed::SealedSegmentData,
};

/// store bm25vector
#[derive(Debug, Clone, Copy)]
pub struct GrowingSegmentData {
    pub first_blkno: NonZero<u32>,
    pub last_blkno: pgrx::pg_sys::BlockNumber,
    pub growing_full_page_count: u32,
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
    page_count: u32,
    max_page_count: u32,
}

impl GrowingSegmentReader {
    pub fn new(index: pgrx::pg_sys::Relation, data: &GrowingSegmentData) -> Self {
        Self {
            index,
            blkno: data.first_blkno.get(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self, max_page_count: u32) -> GrowingSegmentIterator {
        let GrowingSegmentReader { index, blkno } = self;
        let page = page_read(index, blkno);
        let count = page_get_max_offset_number(&page);
        GrowingSegmentIterator {
            index,
            blkno,
            page: Some(page),
            offset: 1,
            count,
            page_count: 0,
            max_page_count,
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
            self.page_count += 1;
            if self.page_count == self.max_page_count {
                self.blkno = pgrx::pg_sys::InvalidBlockNumber;
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

/// - if no growing segment, create one
/// - append to the last page
/// - if growing segment is full, seal it
///
/// return (first_blkno, growing_full_page_count) if growing segment is full
pub fn growing_segment_insert(
    index: pgrx::pg_sys::Relation,
    meta: &mut MetaPageData,
    bm25vector: &Bm25VectorInput,
) -> Option<u32> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(bm25vector.to_bytes());

    let Some(growing_segment) = &mut meta.growing_segment else {
        let mut page = page_alloc_with_fsm(index, PageFlags::GROWING, false);
        meta.growing_segment = Some(GrowingSegmentData {
            first_blkno: NonZero::new(page.blkno()).unwrap(),
            last_blkno: page.blkno(),
            growing_full_page_count: 0,
        });
        let success = page_append_item(&mut page, &buf);
        assert!(success);
        return None;
    };

    let mut page = page_write(index, growing_segment.last_blkno);
    if !page_append_item(&mut page, &buf) {
        let mut new_page = page_alloc_with_fsm(index, PageFlags::GROWING, false);
        let success = page_append_item(&mut new_page, &buf);
        assert!(success);
        page.opaque.next_blkno = new_page.blkno();
        growing_segment.last_blkno = new_page.blkno();
        growing_segment.growing_full_page_count += 1;
        if growing_segment.growing_full_page_count >= SEGMENT_GROWING_MAX_PAGE_SIZE.get() as u32 {
            return Some(growing_segment.growing_full_page_count);
        }
    }
    None
}

// return (sealed_segment_data, current_sealed_doc_id)
pub fn build_sealed_segment(
    index: pgrx::pg_sys::Relation,
    meta: &MetaPageData,
) -> (SealedSegmentData, u32) {
    let mut doc_id = meta.sealed_doc_id;
    let growing_segment = meta.growing_segment.unwrap();
    let mut sealed_writer = SealedSegmentWriter::new();
    {
        let growing_reader = GrowingSegmentReader::new(index, &growing_segment);
        let mut iter = growing_reader.into_iter(SEGMENT_GROWING_MAX_PAGE_SIZE.get() as u32);
        while let Some(vector) = iter.next() {
            sealed_writer.insert(doc_id, vector);
            doc_id += 1;
        }
        sealed_writer.finalize_insert();
    }

    let fieldnorm_reader = FieldNormReader::new(index, meta.field_norm_blkno);
    let mut serializer =
        InvertedSerializer::new(index, meta.doc_cnt, meta.avgdl(), fieldnorm_reader);
    sealed_writer.serialize(&mut serializer);
    let sealed_blkno = serializer.finalize();
    let sealed_data = SealedSegmentData {
        term_info_blkno: sealed_blkno,
    };

    (sealed_data, doc_id)
}
