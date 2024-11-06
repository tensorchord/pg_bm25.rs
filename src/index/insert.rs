use pgrx::{itemptr::item_pointer_to_u64, FromDatum};

use crate::{
    datatype::Bm25VectorInput,
    field_norm::fieldnorm_to_id,
    page::{page_write, VirtualPageWriter, METAPAGE_BLKNO},
    segments::meta::MetaPageData,
    term_info::TermInfoReader,
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
    crate::segments::growing::growing_segment_insert(index, &vector);

    let mut metapage = page_write(index, METAPAGE_BLKNO);
    let meta: &mut MetaPageData = metapage.as_mut();
    let tid = item_pointer_to_u64(heap_tid.read());

    {
        let mut payload_writer = VirtualPageWriter::open(index, meta.payload_blkno, false);
        payload_writer.write(&tid.to_le_bytes());
    }

    let vector = vector.borrow();
    let doc_len = vector.doc_len();
    {
        let mut field_norm_writer = VirtualPageWriter::open(index, meta.field_norm_blkno, false);
        field_norm_writer.write(&fieldnorm_to_id(doc_len).to_le_bytes());
    }

    {
        let term_info_reader = TermInfoReader::new(index, meta.term_info_blkno);
        for (term_id, term_tf) in vector.indexes().iter().zip(vector.values()) {
            term_info_reader.update(*term_id, |tf| {
                *tf += term_tf;
            });
        }
    }

    meta.doc_cnt += 1;
    meta.doc_term_cnt += doc_len as u64;
    false
}
