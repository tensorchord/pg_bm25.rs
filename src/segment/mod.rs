use meta::MetaPageData;

use crate::page::{page_alloc, page_write, PageData, PageFlags, PageWriteGuard};

pub mod builder;
pub mod delete;
pub mod field_norm;
pub mod growing;
pub mod meta;
pub mod payload;
pub mod posting;
pub mod sealed;
pub mod term_stat;

pub fn page_alloc_from_free_list(
    index: pgrx::pg_sys::Relation,
    meta: &mut MetaPageData,
    flag: PageFlags,
    skip_lock_rel: bool,
) -> PageWriteGuard {
    let blkno = meta.free_page_blkno;

    if blkno == pgrx::pg_sys::InvalidBlockNumber {
        page_alloc(index, flag, skip_lock_rel)
    } else {
        let mut page = page_write(index, blkno);
        meta.free_page_blkno = page.opaque.next_blkno;
        page.opaque.next_blkno = pgrx::pg_sys::InvalidBlockNumber;
        page.opaque.page_flag = flag;
        page
    }
}

pub fn free_segment(
    index: pgrx::pg_sys::Relation,
    meta: &mut MetaPageData,
    blkno: pgrx::pg_sys::BlockNumber,
) {
    let mut last_free_blkno = meta.free_page_blkno;
    let mut current_free_blkno = blkno;

    while current_free_blkno != pgrx::pg_sys::InvalidBlockNumber {
        let mut page = page_write(index, current_free_blkno);
        let next_blkno = page.opaque.next_blkno;
        PageData::init_mut(&mut page, PageFlags::FREE);
        page.opaque.next_blkno = last_free_blkno;
        last_free_blkno = current_free_blkno;
        current_free_blkno = next_blkno;
    }

    meta.free_page_blkno = last_free_blkno;
}
