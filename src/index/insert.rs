use pgrx::{itemptr::item_pointer_to_u64, FromDatum};

use crate::{
    datatype::Bm25VectorInput,
    page::{page_free, page_read, page_write, VirtualPageWriter, METAPAGE_BLKNO},
    segment::{
        delete::extend_delete_bit,
        field_norm::fieldnorm_to_id,
        growing::{GrowingSegmentData, GrowingSegmentReader},
        meta::MetaPageData,
        posting::{InvertedAppender, InvertedWriter},
        sealed::extend_sealed_term_id,
        term_stat::{extend_term_id, TermStatReader},
    },
};

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

    let growing_results = crate::segment::growing::growing_segment_insert(index, meta, &vector);

    let payload_blkno = meta.payload_blkno;
    let field_norm_blkno = meta.field_norm_blkno;
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
        let term_id_cnt = vector_borrow
            .indexes()
            .iter()
            .max()
            .map(|&x| x + 1)
            .unwrap_or(0);
        extend_term_id(index, meta, term_id_cnt);

        let term_info_reader = TermStatReader::new(index, meta);
        for term_id in vector_borrow.indexes().iter() {
            term_info_reader.update(*term_id, |tf| {
                *tf += 1;
            });
        }
    }

    extend_delete_bit(index, delete_bitmap_blkno, current_doc_id);

    let prev_growing_segment = *meta.growing_segment.as_ref().unwrap();
    let sealed_doc_id = meta.sealed_doc_id;
    drop(metapage);

    if let Some(block_count) = growing_results {
        let growing_reader = GrowingSegmentReader::new(index, &prev_growing_segment);
        let mut doc_id = sealed_doc_id;

        // check if any other process is sealing the segment
        if !pgrx::pg_sys::ConditionalLockPage(
            index,
            METAPAGE_BLKNO,
            pgrx::pg_sys::ExclusiveLock as _,
        ) {
            return false;
        }

        let mut writer = InvertedWriter::new();
        let mut iter = growing_reader.into_iter(block_count);
        while let Some(vector) = iter.next() {
            writer.insert(doc_id, vector);
            doc_id += 1;
        }
        writer.finalize();
        let term_id_cnt = writer.term_id_cnt();

        let mut metapage = page_write(index, METAPAGE_BLKNO);
        let meta: &mut MetaPageData = metapage.as_mut();
        extend_sealed_term_id(index, &mut meta.sealed_segment, term_id_cnt);
        let mut appender = InvertedAppender::new(index, meta);
        writer.serialize(&mut appender);

        meta.sealed_doc_id = doc_id;
        let growing_segment = meta.growing_segment.as_mut().unwrap();
        growing_segment.first_blkno = prev_growing_segment.last_blkno.try_into().unwrap();
        growing_segment.growing_full_page_count -= block_count;
        drop(metapage);

        pgrx::pg_sys::UnlockPage(index, METAPAGE_BLKNO, pgrx::pg_sys::ExclusiveLock as _);

        free_growing_segment(index, prev_growing_segment);
    }

    false
}

fn free_growing_segment(index: pgrx::pg_sys::Relation, segment: GrowingSegmentData) {
    let mut blkno = segment.first_blkno.get();
    for _ in 0..segment.growing_full_page_count {
        assert!(blkno != pgrx::pg_sys::InvalidBlockNumber);
        let next_blkno = page_read(index, blkno).opaque.next_blkno;
        page_free(index, blkno);
        blkno = next_blkno;
    }
}
