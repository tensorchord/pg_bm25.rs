use std::ffi::CStr;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub static BM25_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static ENABLE_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);
pub static SEGMENT_GROWING_MAX_PAGE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1000);
pub static TOKENIZER_NAME: GucSetting<Option<&CStr>> =
    GucSetting::<Option<&CStr>>::new(Some(c"WORD"));

pub unsafe fn init() {
    GucRegistry::define_int_guc(
        "bm25_catalog.bm25_limit",
        "bm25 query limit",
        "The maximum number of documents to return in a search",
        &BM25_LIMIT,
        1,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "bm25_catalog.enable_index",
        "Whether to enable the bm25 index",
        "Whether to enable the bm25 index",
        &ENABLE_INDEX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "bm25_catalog.segment_growing_max_page_size",
        "bm25 growing segment max page size",
        "The maximum page count of the growing segment. When the size of the growing segment exceeds this value, the segment will be sealed into a read-only segment.",
        &SEGMENT_GROWING_MAX_PAGE_SIZE,
        1,
        1_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "bm25_catalog.tokenizer",
        "tokenizer name",
        "tokenizer name",
        &TOKENIZER_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
}
