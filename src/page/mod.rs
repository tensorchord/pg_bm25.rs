mod builder;
mod reader;

pub use builder::PageBuilder;
pub use reader::{ContinousPageReader, PageReader};

pub const P_NEW: pgrx::pg_sys::BlockNumber = pgrx::pg_sys::InvalidBlockNumber;

pub const METAPAGE_BLKNO: pgrx::pg_sys::BlockNumber = 0;
pub const BM25_PAGE_ID: u16 = 0xFF88;

// page flags
// TODO: package page type into small bits
pub const BM25_META: u16 = 1 << 0;
pub const BM25_PAYLOAD: u16 = 1 << 1;
pub const BM25_FIELD_NORMS: u16 = 1 << 2;
pub const BM25_POSTINGS: u16 = 1 << 3;
pub const BM25_TERM_DICT: u16 = 1 << 4;
pub const BM25_TERM_INFO: u16 = 1 << 5;

#[repr(C, align(8))]
pub struct Bm25PageOpaqueData {
    pub next_blkno: pgrx::pg_sys::BlockNumber,
    page_flag: u16,
    bm25_page_id: u16, // for identification of bm25 index
}

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

pub unsafe fn init_page(page: pgrx::pg_sys::Page, flag: u16) {
    pgrx::pg_sys::PageInit(
        page,
        pgrx::pg_sys::BLCKSZ as _,
        std::mem::size_of::<Bm25PageOpaqueData>(),
    );
    let opaque = page_get_opaque(page);
    (*opaque).next_blkno = pgrx::pg_sys::InvalidBlockNumber;
    (*opaque).page_flag = flag;
    (*opaque).bm25_page_id = BM25_PAGE_ID;
}

unsafe fn page_get_opaque(page: pgrx::pg_sys::Page) -> *mut Bm25PageOpaqueData {
    assert!(!page.is_null());
    let pd_special = (*(page as pgrx::pg_sys::PageHeader)).pd_special;
    assert!(pd_special <= pgrx::pg_sys::BLCKSZ as _);
    assert!(pd_special >= std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() as _);
    page.add(pd_special as usize) as _
}

pub unsafe fn page_get_contents<T>(page: pgrx::pg_sys::Page) -> *mut T {
    page.add(pgrx::pg_sys::MAXALIGN(std::mem::size_of::<
        pgrx::pg_sys::PageHeaderData,
    >()))
    .cast()
}

pub const fn bm25_page_size() -> usize {
    unsafe {
        pgrx::pg_sys::BLCKSZ as usize
            - pgrx::pg_sys::MAXALIGN(std::mem::size_of::<pgrx::pg_sys::PageHeaderData>())
            - pgrx::pg_sys::MAXALIGN(std::mem::size_of::<Bm25PageOpaqueData>())
    }
}

const INVALID_SUB_TRANSACTION_ID: pgrx::pg_sys::SubTransactionId = 0;

pub unsafe fn relation_needs_wal(relation: pgrx::pg_sys::Relation) -> bool {
    ((*(*relation).rd_rel).relpersistence == pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8)
        && ((pgrx::pg_sys::wal_level >= pgrx::pg_sys::WalLevel::WAL_LEVEL_REPLICA as _)
            || (*relation).rd_createSubid == INVALID_SUB_TRANSACTION_ID
            || (*relation).rd_firstRelfilelocatorSubid == INVALID_SUB_TRANSACTION_ID)
}
