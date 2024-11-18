use pgrx::{itemptr::item_pointer_to_u64, FromDatum};

use crate::{
    datatype::Bm25VectorInput,
    guc::SEGMENT_GROWING_MAX_PAGE_SIZE,
    page::{page_free, page_read, page_write, VirtualPageWriter, METAPAGE_BLKNO},
    segment::{
        delete::extend_delete_bit,
        field_norm::fieldnorm_to_id,
        meta::{metapage_append_sealed_segment, MetaPageData},
        term_stat::TermStatReader,
    },
};

/// Insert Progress:
/// 1. lock metapage
/// 2. update doc_cnt, doc_term_cnt
/// 3. insert into growing segment
///   - if no growing segment, create one
///   - if growing segment is full, seal it
///   - otherwise, append to the last page
/// 4. write payload, field_norm, term_stat
#[allow(clippy::too_many_arguments)]
#[pgrx::pg_guard]
pub unsafe extern "C" fn aminsert(
    index: pgrx::pg_sys::Relation,
    values: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pgrx::pg_sys::ItemPointer,
    _heap: pgrx::pg_sys::Relation,
    _check_unique: pgrx::pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pgrx::pg_sys::IndexInfo,
) -> bool {
    let Some(vector) = Bm25VectorInput::from_datum(*values, *is_null) else {
        return false;
    };

    let vector_borrow = vector.borrow();
    let doc_len = vector_borrow.doc_len();

    let mut metapage = page_write(index, METAPAGE_BLKNO);
    

    let meta: &mut MetaPageData = metapage.as_mut();
    let current_doc_id = meta.current_doc_id;
    meta.current_doc_id += 1;
    meta.doc_cnt += 1;
    meta.doc_term_cnt += doc_len as u64;

    let need_sealed = crate::segment::growing::growing_segment_insert(index, meta, &vector);

    let payload_blkno = meta.payload_blkno;
    let field_norm_blkno = meta.field_norm_blkno;
    let term_stat_blkno = meta.term_stat_blkno;
    let delete_bitmap_blkno = meta.delete_bitmap_blkno;

    let tid = item_pointer_to_u64(heap_tid.read());
    {
        let mut payload_writer = VirtualPageWriter::open(index, payload_blkno, false);
        payload_writer.write(&tid.to_le_bytes());
    }

    {
        let mut field_norm_writer = VirtualPageWriter::open(index, field_norm_blkno, false);
        field_norm_writer.write(&fieldnorm_to_id(doc_len).to_le_bytes());
    }

    {
        let term_info_reader = TermStatReader::new(index, term_stat_blkno);
        for term_id in vector_borrow.indexes().iter() {
            term_info_reader.update(*term_id, |tf| {
                *tf += 1;
            });
        }
    }

    extend_delete_bit(index, delete_bitmap_blkno, current_doc_id);

    if need_sealed {
        let metapage = metapage.degrade();
        let (sealed_data, sealed_doc_id) =
            crate::segment::growing::build_sealed_segment(index, metapage.as_ref());

        let max_growing_segment_page_size = SEGMENT_GROWING_MAX_PAGE_SIZE.get() as u32;
        let mut metapage = metapage.upgrade(index);
        let meta: &mut MetaPageData = metapage.as_mut();

        let growing_segment = meta.growing_segment.as_mut().unwrap();
        let mut current_free_blkno = growing_segment.first_blkno;
        let mut free_page_count = 0;
        while free_page_count < max_growing_segment_page_size {
            assert!(current_free_blkno != pgrx::pg_sys::InvalidBlockNumber);
            let page = page_read(index, current_free_blkno);
            let next_blkno = page.opaque.next_blkno;
            page_free(index, current_free_blkno);
            free_page_count += 1;
            current_free_blkno = next_blkno;
        }
        assert!(current_free_blkno != pgrx::pg_sys::InvalidBlockNumber);

        meta.sealed_doc_id = sealed_doc_id;
        growing_segment.first_blkno = current_free_blkno;
        growing_segment.growing_full_page_count -= free_page_count;
        metapage_append_sealed_segment(&mut metapage, sealed_data);
    }

    false
}
