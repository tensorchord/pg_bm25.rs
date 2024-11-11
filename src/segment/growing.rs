use std::num::NonZero;

use crate::{
    datatype::{Bm25VectorBorrowed, Bm25VectorHeader, Bm25VectorInput},
    guc::SEGMENT_GROWING_MAX_SIZE,
    page::{
        page_append_item, page_get_item, page_get_item_id, page_get_max_offset_number, page_read,
        page_write, PageData, PageFlags, PageReadGuard,
    },
    segment::{
        field_norm::{fieldnorm_to_id, FieldNormRead},
        page_alloc_from_free_list,
        sealed::SealedSegmentWriter,
    },
};

use super::{
    field_norm::FieldNormReader,
    free_segment,
    meta::{metapage_append_sealed_segment, MetaPageData},
    posting::InvertedSerializer,
    sealed::SealedSegmentData,
};

/// store bm25vector
#[derive(Debug, Clone, Copy)]
pub struct GrowingSegmentData {
    first_blkno: NonZero<pgrx::pg_sys::BlockNumber>,
    last_blkno: pgrx::pg_sys::BlockNumber,
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
    pub fn new(index: pgrx::pg_sys::Relation, data: &GrowingSegmentData) -> Self {
        Self {
            index,
            blkno: data.first_blkno.get(),
        }
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

pub fn free_growing_segment(
    index: pgrx::pg_sys::Relation,
    meta: &mut MetaPageData,
    growing_segment: GrowingSegmentData,
) {
    free_segment(index, meta, growing_segment.first_blkno.get());
}

/// - if no growing segment, create one
/// - if growing segment is full, seal it
/// - otherwise, append to the last page
pub fn growing_segment_insert(
    index: pgrx::pg_sys::Relation,
    metapage: &mut PageData,
    bm25vector: &Bm25VectorInput,
) {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(bm25vector.to_bytes());

    let meta: &mut MetaPageData = metapage.as_mut();

    let Some(growing_segment) = &meta.growing_segment else {
        let mut page = page_alloc_from_free_list(index, meta, PageFlags::GROWING, false);
        meta.growing_segment = Some(GrowingSegmentData {
            first_blkno: NonZero::new(page.blkno()).unwrap(),
            last_blkno: page.blkno(),
        });
        let success = page_append_item(&mut page, &buf);
        assert!(success);
        return;
    };

    if meta.current_doc_id - meta.sealed_doc_id >= SEGMENT_GROWING_MAX_SIZE.get() as u32 {
        let mut doc_id = meta.sealed_doc_id;
        let mut sealed_writer = SealedSegmentWriter::new();
        {
            let growing_reader = GrowingSegmentReader::new(index, growing_segment);
            let mut iter = growing_reader.into_iter();
            while let Some(vector) = iter.next() {
                sealed_writer.insert(doc_id, vector);
                doc_id += 1;
            }
            sealed_writer.insert(doc_id, bm25vector.borrow());
            sealed_writer.finalize_insert();
        }

        let growing_segment = *growing_segment;
        free_growing_segment(index, meta, growing_segment);

        struct FieldNormReaderTmp {
            uninsert_doc_id: u32,
            uninsert_field_norm: u8,
            reader: FieldNormReader,
        }

        impl FieldNormRead for FieldNormReaderTmp {
            fn read(&self, doc_id: u32) -> u8 {
                if doc_id == self.uninsert_doc_id {
                    return self.uninsert_field_norm;
                }
                self.reader.read(doc_id)
            }
        }

        let fieldnorm_reader = FieldNormReader::new(index, meta.field_norm_blkno);
        let fieldnorm_reader_tmp = FieldNormReaderTmp {
            uninsert_doc_id: doc_id,
            uninsert_field_norm: fieldnorm_to_id(bm25vector.borrow().doc_len()),
            reader: fieldnorm_reader,
        };
        let mut serializer =
            InvertedSerializer::new(index, meta.doc_cnt, meta.avgdl(), fieldnorm_reader_tmp);
        sealed_writer.serialize(meta, &mut serializer);
        let sealed_data = serializer.finalize(meta);

        meta.sealed_doc_id = meta.current_doc_id;
        meta.growing_segment = None;
        metapage_append_sealed_segment(
            metapage,
            SealedSegmentData {
                term_info_blkno: sealed_data,
            },
        );
        return;
    }

    let mut page = page_write(index, growing_segment.last_blkno);
    if !page_append_item(&mut page, &buf) {
        let mut new_page = page_alloc_from_free_list(index, meta, PageFlags::GROWING, false);
        let success = page_append_item(&mut new_page, &buf);
        assert!(success);
        page.opaque.next_blkno = new_page.blkno();
        meta.growing_segment.as_mut().unwrap().last_blkno = new_page.blkno();
    }
}
