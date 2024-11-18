use std::ops::DerefMut;

use pgrx::{itemptr::item_pointer_to_u64, FromDatum, PgMemoryContexts};

use crate::{
    datatype::Bm25VectorInput,
    page::{
        page_alloc, page_alloc_init_forknum, page_write, PageFlags, PageWriter, PageWriterInitFork,
        VirtualPageWriter, METAPAGE_BLKNO,
    },
    segment::{
        builder::IndexBuilder,
        meta::{metapage_append_sealed_segment, MetaPageData, META_VERSION},
    },
    token::vocab_len,
};

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(index: pgrx::pg_sys::Relation) {
    let mut meta_page = page_alloc_init_forknum(index, PageFlags::META);
    assert_eq!(meta_page.blkno(), METAPAGE_BLKNO);
    let field_norm_blkno = VirtualPageWriter::init_fork(index, PageFlags::FIELD_NORM);
    let payload_blkno = VirtualPageWriter::init_fork(index, PageFlags::PAYLOAD);
    let delete_bitmap_blkno = VirtualPageWriter::init_fork(index, PageFlags::DELETE);

    let mut term_stat_writer = PageWriterInitFork::new(index, PageFlags::TERM_STATISTIC);
    for _ in 0..vocab_len() {
        term_stat_writer.write(&0u32.to_le_bytes());
    }
    let term_stat_blkno = term_stat_writer.finalize();

    let ptr = meta_page.content.as_mut_ptr() as *mut MetaPageData;
    unsafe {
        ptr.write(MetaPageData {
            version: META_VERSION,
            doc_cnt: 0,
            doc_term_cnt: 0,
            field_norm_blkno,
            payload_blkno,
            term_stat_blkno,
            delete_bitmap_blkno,
            current_doc_id: 0,
            sealed_doc_id: 0,
            growing_segment: None,
            sealed_length: 0,
            sealed_segment: [],
        });
        meta_page.header.pd_lower += std::mem::size_of::<MetaPageData>() as u16;
    }
}

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
    {
        let metapage = page_alloc(index, PageFlags::META, true);
        assert_eq!(metapage.blkno(), METAPAGE_BLKNO);
    }

    let mut state = BuildState {
        heap_tuples: 0,
        index_tuples: 0,
        index,
        builder: IndexBuilder::new(),
        memctx: PgMemoryContexts::new("pg_bm25_index_build"),
    };

    pgrx::pg_sys::IndexBuildHeapScan(heap, index, index_info, Some(build_callback), &mut state);
    state.builder.finalize_insert();
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
        state.builder.insert(id, vector.borrow());
        state.index_tuples += 1;
    });
    state.memctx.reset();

    state.heap_tuples += 1;
}

unsafe fn write_down(state: &BuildState) {
    let doc_cnt = state.builder.doc_cnt();
    let doc_term_cnt = state.builder.doc_term_cnt();
    let mut meta_page = page_write(state.index, METAPAGE_BLKNO);
    let ptr = meta_page.content.as_mut_ptr() as *mut MetaPageData;
    unsafe {
        ptr.write(MetaPageData {
            version: META_VERSION,
            doc_cnt,
            doc_term_cnt,
            field_norm_blkno: pgrx::pg_sys::InvalidBlockNumber,
            payload_blkno: pgrx::pg_sys::InvalidBlockNumber,
            term_stat_blkno: pgrx::pg_sys::InvalidBlockNumber,
            delete_bitmap_blkno: pgrx::pg_sys::InvalidBlockNumber,
            current_doc_id: doc_cnt,
            sealed_doc_id: doc_cnt,
            growing_segment: None,
            sealed_length: 0,
            sealed_segment: [],
        });
        meta_page.header.pd_lower += std::mem::size_of::<MetaPageData>() as u16;
    }
    let meta: &mut MetaPageData = meta_page.as_mut();

    // delete bitmap
    let mut delete_bitmap_writer = VirtualPageWriter::new(state.index, PageFlags::DELETE, true);
    for _ in 0..(doc_cnt.div_ceil(8)) {
        delete_bitmap_writer.write(&[0u8]);
    }
    let delete_bitmap_blkno = delete_bitmap_writer.finalize();

    // term info
    let mut term_stat_writer = PageWriter::new(state.index, PageFlags::TERM_STATISTIC, true);
    for count in state.builder.term_stat() {
        term_stat_writer.write(&count.to_le_bytes());
    }
    let term_stat_blkno = term_stat_writer.finalize();

    let (payload_blkno, field_norm_blkno, sealed_data) = state.builder.serialize(state.index);

    meta.field_norm_blkno = field_norm_blkno;
    meta.payload_blkno = payload_blkno;
    meta.term_stat_blkno = term_stat_blkno;
    meta.delete_bitmap_blkno = delete_bitmap_blkno;
    metapage_append_sealed_segment(meta_page.deref_mut(), sealed_data);
}
