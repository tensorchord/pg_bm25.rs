use pgrx::{FromDatum, PgMemoryContexts};

use crate::{
    builder::IndexBuilder,
    page::{
        init_page, page_get_contents, MetaPageData, PageBuilder, BM25_FIELD_NORMS, BM25_META,
        BM25_PAYLOAD, METAPAGE_BLKNO,
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
    write_down(&state).unwrap();
    if crate::page::relation_needs_wal(index) {
        pgrx::pg_sys::log_newpage_range(
            index,
            pgrx::pg_sys::ForkNumber::MAIN_FORKNUM,
            0,
            pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                index,
                pgrx::pg_sys::ForkNumber::MAIN_FORKNUM,
            ),
            true,
        );
    }

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
        let Some(docs) = <&[u8]>::from_datum(*datum, *is_null) else {
            return;
        };
        let pointer = pgrx::itemptr::item_pointer_to_u64(unsafe { ctid.read() });
        state.builder.insert(pointer, docs);
        state.index_tuples += 1;
    });
    state.memctx.reset();

    state.heap_tuples += 1;
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(_index: pgrx::pg_sys::Relation) {
    pgrx::error!("Unlogged indexes are not supported.");
}

// TODO: deal with xlog
unsafe fn init_metapage(state: &BuildState) {
    let meta_buffer = pgrx::pg_sys::ReadBuffer(state.index, pgrx::pg_sys::InvalidBlockNumber);
    pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
    assert!(pgrx::pg_sys::BufferGetBlockNumber(meta_buffer) == METAPAGE_BLKNO);

    let meta_page = pgrx::pg_sys::BufferGetPage(meta_buffer);
    init_page(meta_page, BM25_META);
    let meta_data: *mut MetaPageData = page_get_contents(meta_page);
    (*meta_data).doc_cnt = state.builder.doc_cnt();
    (*meta_data).avg_dl = state.builder.avg_dl();
    (*meta_data).term_dict_blkno = pgrx::pg_sys::InvalidBlockNumber;
    (*meta_data).term_info_blkno = pgrx::pg_sys::InvalidBlockNumber;
    (*meta_data).field_norms_blkno = pgrx::pg_sys::InvalidBlockNumber;
    (*meta_data).payload_blkno = pgrx::pg_sys::InvalidBlockNumber;

    (*(meta_page as pgrx::pg_sys::PageHeader)).pd_lower +=
        std::mem::size_of::<MetaPageData>() as u16;
    pgrx::pg_sys::MarkBufferDirty(meta_buffer);
    pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
}

unsafe fn write_down(state: &BuildState) -> anyhow::Result<()> {
    // payload
    let mut page_builder = PageBuilder::new(state.index, BM25_PAYLOAD, true);
    state.builder.write_payload(&mut page_builder)?;
    let payload_blk = page_builder.finalize();

    // field norms
    let mut page_builder = PageBuilder::new(state.index, BM25_FIELD_NORMS, true);
    state.builder.write_field_norms(&mut page_builder)?;
    let field_norms_blk = page_builder.finalize();

    // postings
    let [term_dict_blk, term_info_blk] = state.builder.write_postings(state.index)?;

    let meta_buffer = pgrx::pg_sys::ReadBuffer(state.index, METAPAGE_BLKNO);
    pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
    let meta_page = pgrx::pg_sys::BufferGetPage(meta_buffer);
    let meta_data: *mut MetaPageData = page_get_contents(meta_page);
    (*meta_data).term_dict_blkno = term_dict_blk;
    (*meta_data).term_info_blkno = term_info_blk;
    (*meta_data).field_norms_blkno = field_norms_blk;
    (*meta_data).payload_blkno = payload_blk;
    pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);

    Ok(())
}
