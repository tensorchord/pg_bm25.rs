use std::ops::DerefMut;

use pgrx::{itemptr::item_pointer_to_u64, FromDatum, PgMemoryContexts};

use crate::{
    datatype::Bm25VectorInput,
    field_norm::FieldNormReader,
    page::{page_alloc, page_write, PageFlags, PageWriter, VirtualPageWriter, METAPAGE_BLKNO},
    postings::InvertedSerializer,
    segments::{
        meta::{metapage_update_sealed_segment, MetaPageData, META_VERSION},
        sealed::SealedSegmentWriter,
    },
};

struct BuildState {
    heap_tuples: usize,
    index_tuples: usize,
    index: pgrx::pg_sys::Relation,
    writer: SealedSegmentWriter,
    memctx: PgMemoryContexts,
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuild(
    heap: pgrx::pg_sys::Relation,
    index: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    {
        let meta_blkno = page_alloc(index, PageFlags::META, true);
        assert_eq!(meta_blkno.blkno(), METAPAGE_BLKNO);
    }

    let mut state = BuildState {
        heap_tuples: 0,
        index_tuples: 0,
        index,
        writer: SealedSegmentWriter::new(0),
        memctx: PgMemoryContexts::new("pg_bm25_index_build"),
    };

    pgrx::pg_sys::IndexBuildHeapScan(heap, index, index_info, Some(build_callback), &mut state);
    state.writer.finalize();
    write_down(&state);

    let mut result = unsafe { pgrx::PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc() };
    result.heap_tuples = state.heap_tuples as f64;
    result.index_tuples = state.index_tuples as f64;

    result.into_pg()
}

#[pgrx::pg_guard]
unsafe extern "C" fn build_callback(
    _index: pgrx::pg_sys::Relation,
    ctid: pgrx::pg_sys::ItemPointer,
    datum: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let state = &mut *(state.cast::<BuildState>());
    state.memctx.reset();
    state.memctx.switch_to(|_| {
        let Some(vector) = Bm25VectorInput::from_datum(*datum, *is_null) else {
            return;
        };
        let id = item_pointer_to_u64(unsafe { ctid.read() });
        state.writer.insert(id, vector.borrow());
        state.index_tuples += 1;
    });
    state.memctx.reset();

    state.heap_tuples += 1;
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(_index: pgrx::pg_sys::Relation) {
    pgrx::error!("Unlogged indexes are not supported.");
}

unsafe fn write_down(state: &BuildState) {
    // payload
    let mut page_builder = VirtualPageWriter::new(state.index, PageFlags::PAYLOAD, true);
    state.writer.write_payload(&mut page_builder);
    let payload_blkno = page_builder.finalize();

    // field norm
    let mut page_builder = VirtualPageWriter::new(state.index, PageFlags::FIELD_NORM, true);
    state.writer.write_field_norm(&mut page_builder);
    let field_norm_blkno = page_builder.finalize();

    // term info
    let mut term_info_writer = PageWriter::new(state.index, PageFlags::TERM_INFO, true);
    for count in state.writer.term_info() {
        term_info_writer.write(&count.to_le_bytes());
    }
    let term_info_blkno = term_info_writer.finalize();

    // postings
    let doc_cnt = state.writer.doc_cnt();
    let doc_term_cnt = state.writer.doc_term_cnt();
    let avgdl = doc_term_cnt as f32 / doc_cnt as f32;
    let fieldnorm_reader = FieldNormReader::new(state.index, field_norm_blkno);
    let inverted_sealizer = InvertedSerializer::new(state.index, doc_cnt, avgdl, fieldnorm_reader);
    let sealed_data = state.writer.write_postings(inverted_sealizer);

    {
        let mut meta_page = page_write(state.index, METAPAGE_BLKNO);
        let ptr = meta_page.content.as_mut_ptr() as *mut MetaPageData;

        unsafe {
            ptr.write(MetaPageData {
                version: META_VERSION,
                doc_cnt,
                doc_term_cnt,
                field_norm_blkno,
                payload_blkno,
                term_info_blkno,
                sealed_doc_cnt: doc_cnt,
                growing_segment: None,
                sealed_length: 0,
                sealed_segment: [],
            });
            meta_page.header.pd_lower += std::mem::size_of::<MetaPageData>() as u16;
        }
        metapage_update_sealed_segment(meta_page.deref_mut(), &[sealed_data]);
    }
}
