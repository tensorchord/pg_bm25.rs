use pgrx::itemptr::u64_to_item_pointer;

use crate::{
    datatype::Bm25VectorHeader,
    page::{
        bm25_page_size, page_get_item, page_get_item_id, page_read, page_write, METAPAGE_BLKNO,
    },
    segment::{
        delete::DeleteBitmapReader,
        field_norm::{FieldNormRead, FieldNormReader},
        meta::MetaPageData,
        payload::PayloadReader,
        term_stat::TermStatReader,
    },
};

#[allow(unused_variables)]
#[pgrx::pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
    callback: pgrx::pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    let mut callback = {
        let callback = callback.unwrap();
        let mut item: pgrx::pg_sys::ItemPointerData = Default::default();
        move |p: u64| unsafe {
            u64_to_item_pointer(p, &mut item);
            callback(&mut item, callback_state)
        }
    };

    let index = (*info).index;
    let heap = (*info).heaprel;
    let mut metapage = page_write(index, METAPAGE_BLKNO);
    let meta: &mut MetaPageData = metapage.as_mut();
    let term_info_reader = TermStatReader::new(index, meta.term_stat_blkno);
    let payload_reader = PayloadReader::new(index, meta.payload_blkno);
    let field_norm_reader = FieldNormReader::new(index, meta.field_norm_blkno);
    let mut delete_bitmap_reader = DeleteBitmapReader::new(index, meta.delete_bitmap_blkno);

    for i in 0..meta.current_doc_id {
        if i % bm25_page_size() as u32 == 0 {
            pgrx::pg_sys::vacuum_delay_point();
        }
        if delete_bitmap_reader.is_delete(i) {
            continue;
        }
        let tid = payload_reader.read(i);
        if callback(tid) {
            delete_bitmap_reader.delete(i);
            meta.doc_cnt -= 1;
            meta.doc_term_cnt -= field_norm_reader.read(i) as u64;

            let (blkno, off) = pgrx::itemptr::u64_to_item_pointer_parts(tid);
            let page = page_read(heap, blkno);
            let item_id = page_get_item_id(&page, off);
            let item: &Bm25VectorHeader = page_get_item(&page, item_id);

            for term_id in item.borrow().indexes().iter() {
                term_info_reader.update(*term_id, |tf| {
                    *tf -= 1;
                });
            }
        }
    }

    std::ptr::null_mut()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    std::ptr::null_mut()
}
