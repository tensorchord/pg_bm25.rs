use pgrx::{itemptr::item_pointer_to_u64, FromDatum, PgMemoryContexts};

use crate::{
    builder::IndexBuilder,
    datatype::Bm25VectorInput,
    page::{
        page_alloc, page_write, MetaPageData, PageBuilder, PageFlags, METAPAGE_BLKNO, META_VERSION,
    },
};

struct BuildState {
    heap_tuples: usize,
    index_tuples: usize,
    index: pgrx::pg_sys::Relation,
    builder: IndexBuilder,
    memctx: PgMemoryContexts,
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuild(
    heap: pgrx::pg_sys::Relation,
    index: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    let mut state = BuildState {
        heap_tuples: 0,
        index_tuples: 0,
        index,
        builder: IndexBuilder::new(),
        memctx: PgMemoryContexts::new("pg_bm25_index_build"),
    };

    pgrx::pg_sys::IndexBuildHeapScan(heap, index, index_info, Some(build_callback), &mut state);
    state.builder.finalize();
    init_metapage(&state);
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
        state.builder.insert(id, vector.as_ref());
        state.index_tuples += 1;
    });
    state.memctx.reset();

    state.heap_tuples += 1;
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(_index: pgrx::pg_sys::Relation) {
    pgrx::error!("Unlogged indexes are not supported.");
}

unsafe fn init_metapage(state: &BuildState) {
    let mut meta_page = page_alloc(state.index, PageFlags::META, false);
    assert_eq!(meta_page.blkno(), METAPAGE_BLKNO);
    meta_page
        .freespace_mut()
        .as_mut_ptr()
        .cast::<MetaPageData>()
        .write(MetaPageData {
            version: META_VERSION,
            doc_cnt: state.builder.doc_cnt(),
            avg_dl: state.builder.avg_dl(),
            term_info_blkno: pgrx::pg_sys::InvalidBlockNumber,
            field_norms_blkno: pgrx::pg_sys::InvalidBlockNumber,
            payload_blkno: pgrx::pg_sys::InvalidBlockNumber,
        });
    meta_page.header.pd_lower += std::mem::size_of::<MetaPageData>() as u16;
}

unsafe fn write_down(state: &BuildState) {
    // payload
    let mut page_builder = PageBuilder::new(state.index, PageFlags::PAYLOAD, true);
    state.builder.write_payload(&mut page_builder);
    let payload_blk = page_builder.finalize();

    // field norms
    let mut page_builder = PageBuilder::new(state.index, PageFlags::FIELD_NORMS, true);
    state.builder.write_field_norms(&mut page_builder);
    let field_norms_blk = page_builder.finalize();
    {
        // postings need field norms
        let mut meta_page = page_write(state.index, METAPAGE_BLKNO);
        let metadata = &mut *meta_page.data_mut().as_mut_ptr().cast::<MetaPageData>();
        metadata.payload_blkno = payload_blk;
        metadata.field_norms_blkno = field_norms_blk;
    }

    // postings
    let term_info_blk = state.builder.write_postings(state.index);
    {
        let mut meta_page = page_write(state.index, METAPAGE_BLKNO);
        let metadata = &mut *meta_page.data_mut().as_mut_ptr().cast::<MetaPageData>();
        metadata.term_info_blkno = term_info_blk;
    }
}
