pub const META_VERSION: u32 = 1;

#[repr(C, align(8))]
#[derive(Clone)]
pub struct MetaPageData {
    pub version: u32,
    pub doc_cnt: u32,
    pub avg_dl: f32,
    pub term_info_blkno: pgrx::pg_sys::BlockNumber,
    pub field_norms_blkno: pgrx::pg_sys::BlockNumber,
    pub payload_blkno: pgrx::pg_sys::BlockNumber,
}
