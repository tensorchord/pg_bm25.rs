#[repr(C, align(8))]
#[derive(Clone)]
pub struct MetaPageData {
    pub doc_cnt: u32,
    pub avg_dl: f32,
    pub term_dict_blkno: pgrx::pg_sys::BlockNumber,
    pub term_info_blkno: pgrx::pg_sys::BlockNumber,
    pub field_norms_blkno: pgrx::pg_sys::BlockNumber,
    pub payload_blkno: pgrx::pg_sys::BlockNumber,
}
