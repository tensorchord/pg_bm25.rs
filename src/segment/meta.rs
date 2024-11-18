use std::alloc::Layout;

use super::{growing::GrowingSegmentData, sealed::SealedSegmentData};
use crate::page::{bm25_page_size, PageData};

pub const META_VERSION: u32 = 1;

pub struct MetaPageData {
    pub version: u32,
    pub doc_cnt: u32,
    pub doc_term_cnt: u64,
    pub sealed_doc_id: u32,
    pub current_doc_id: u32,
    pub field_norm_blkno: u32,
    pub payload_blkno: u32,
    pub term_stat_blkno: u32,
    pub delete_bitmap_blkno: u32,
    pub growing_segment: Option<GrowingSegmentData>,
    pub sealed_length: u32,
    pub sealed_segment: [SealedSegmentData; 0],
}

impl std::fmt::Debug for MetaPageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPageData")
            .field("version", &self.version)
            .field("doc_cnt", &self.doc_cnt)
            .field("doc_term_cnt", &self.doc_term_cnt)
            .field("sealed_doc_cnt", &self.sealed_doc_id)
            .field("current_doc_id", &self.current_doc_id)
            .field("field_norm_blkno", &self.field_norm_blkno)
            .field("payload_blkno", &self.payload_blkno)
            .field("term_info_blkno", &self.term_stat_blkno)
            .field("delete_bitmap_blkno", &self.delete_bitmap_blkno)
            .field("growing_segment", &self.growing_segment)
            .field("sealed_length", &self.sealed_length)
            .field("sealed_segment", &self.sealed_segment())
            .finish()
    }
}

impl MetaPageData {
    pub fn sealed_segment(&self) -> &[SealedSegmentData] {
        unsafe {
            std::slice::from_raw_parts(self.sealed_segment.as_ptr(), self.sealed_length as usize)
        }
    }

    pub fn avgdl(&self) -> f32 {
        self.doc_term_cnt as f32 / self.doc_cnt as f32
    }
}

pub fn metapage_append_sealed_segment(this: &mut PageData, sealed_segment: SealedSegmentData) {
    let meta: &mut MetaPageData = this.as_mut();
    let len = meta.sealed_length + 1;
    meta.sealed_length = len;
    unsafe {
        let sealed_ptr = meta.sealed_segment.as_mut_ptr();
        sealed_ptr.add(len as usize - 1).write(sealed_segment);
    }

    let mut layout = Layout::new::<MetaPageData>();
    let layout_sealed = Layout::array::<SealedSegmentData>(len as usize).unwrap();
    layout = layout.extend(layout_sealed).unwrap().0.pad_to_align();
    assert!(layout.size() <= bm25_page_size());

    this.header.pd_lower =
        layout.size() as u16 + std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() as u16;
}
