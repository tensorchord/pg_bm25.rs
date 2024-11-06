use std::alloc::Layout;

use super::{growing::GrowingSegmentData, sealed::SealedSegmentData};
use crate::page::{bm25_page_size, PageData};

pub const META_VERSION: u32 = 1;

pub struct MetaPageData {
    pub version: u32,
    pub doc_cnt: u32,
    pub doc_term_cnt: u64,
    pub field_norm_blkno: u32,
    pub payload_blkno: u32,
    pub term_info_blkno: u32,
    pub sealed_doc_cnt: u32,
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
            .field("field_norm_blkno", &self.field_norm_blkno)
            .field("payload_blkno", &self.payload_blkno)
            .field("term_info_blkno", &self.term_info_blkno)
            .field("sealed_doc_cnt", &self.sealed_doc_cnt)
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

pub fn metapage_update_sealed_segment(this: &mut PageData, sealed_segment: &[SealedSegmentData]) {
    let mut layout = Layout::new::<MetaPageData>();
    let layout_sealed = Layout::array::<SealedSegmentData>(sealed_segment.len()).unwrap();
    layout = layout.extend(layout_sealed).unwrap().0.pad_to_align();
    assert!(layout.size() <= bm25_page_size());

    let ptr = this.content.as_mut_ptr() as *mut MetaPageData;
    unsafe {
        (*ptr).sealed_length = sealed_segment.len() as u32;
        let sealed_ptr = (*ptr).sealed_segment.as_mut_ptr();
        std::ptr::copy_nonoverlapping(sealed_segment.as_ptr(), sealed_ptr, sealed_segment.len());
    }

    this.header.pd_lower =
        layout.size() as u16 + std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() as u16;
}
