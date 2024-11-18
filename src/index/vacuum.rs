use crate::{
    page::{bm25_page_size, page_read, page_write, METAPAGE_BLKNO},
    segment::{
        delete::DeleteBitmapReader,
        field_norm::{FieldNormRead, FieldNormReader},
        growing::GrowingSegmentReader,
        meta::MetaPageData,
        payload::PayloadReader,
        sealed::SealedSegmentReader,
        term_stat::TermStatReader,
    },
    token::vocab_len,
};

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
    callback: pgrx::pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    let mut callback = {
        let callback = callback.unwrap();
        let mut item: pgrx::pg_sys::ItemPointerData = Default::default();
        move |p: u64| unsafe {
            pgrx::itemptr::u64_to_item_pointer(p, &mut item);
            callback(&mut item, callback_state)
        }
    };

    let mut stats = stats;
    if stats.is_null() {
        stats = unsafe {
            pgrx::pg_sys::palloc0(size_of::<pgrx::pg_sys::IndexBulkDeleteResult>()).cast()
        };
    }
    let stats = stats.as_mut().unwrap();

    let index = (*info).index;
    let mut metapage = page_write(index, METAPAGE_BLKNO);
    let meta: &mut MetaPageData = metapage.as_mut();
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
            stats.tuples_removed += 1.0;
        } else {
            stats.num_index_tuples += 1.0;
        }
    }

    stats
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvacuumcleanup(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    if stats.is_null() {
        return std::ptr::null_mut();
    }

    (*stats).num_pages = pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
        (*info).index,
        pgrx::pg_sys::ForkNumber::MAIN_FORKNUM,
    );

    if (*stats).tuples_removed == 0.0 {
        return stats;
    }

    let index = (*info).index;
    let mut term_stats = (0..vocab_len()).map(|_| 0u32).collect::<Vec<_>>();

    let metapage = page_read(index, METAPAGE_BLKNO);
    let meta: &MetaPageData = metapage.as_ref();
    let delete_bitmap_reader = DeleteBitmapReader::new(index, meta.delete_bitmap_blkno);

    if let Some(growing) = meta.growing_segment.as_ref() {
        let reader = GrowingSegmentReader::new(index, growing);
        let mut doc_id = meta.sealed_doc_id;
        let mut iter = reader.into_iter(u32::MAX);
        while let Some(vector) = iter.next() {
            if !delete_bitmap_reader.is_delete(doc_id) {
                for &idx in vector.indexes() {
                    term_stats[idx as usize] += 1;
                }
            }
            doc_id += 1;
        }
    }

    for &sealed_data in meta.sealed_segment() {
        let sealed_reader = SealedSegmentReader::new(index, sealed_data);
        for i in 0..vocab_len() {
            let Some(mut posting) = sealed_reader.get_postings_docid_only(i) else {
                continue;
            };
            loop {
                posting.decode_block();
                loop {
                    let doc_id = posting.doc_id();
                    if !delete_bitmap_reader.is_delete(doc_id) {
                        term_stats[i as usize] += 1;
                    }
                    if !posting.advance_cur() {
                        break;
                    }
                }
                if !posting.advance_block() {
                    break;
                }
            }
        }
    }

    let mut metapage = metapage.upgrade(index);
    let meta: &mut MetaPageData = metapage.as_mut();
    let term_stat_reader = TermStatReader::new(index, meta.term_stat_blkno);
    for i in 0..vocab_len() {
        term_stat_reader.update(i, |tf| {
            *tf = term_stats[i as usize];
        });
    }

    stats
}
