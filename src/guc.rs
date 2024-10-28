use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub static BM25_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static ENABLE_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub unsafe fn init() {
    GucRegistry::define_int_guc(
        "bm25_catalog.bm25_limit",
        "bm25 query limit closure",
        "The maximum number of documents to return in a search",
        &BM25_LIMIT,
        1,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "bm25_catalog.enable_index",
        "bm25 index enable closure",
        "Whether to enable the bm25 index",
        &ENABLE_INDEX,
        GucContext::Userset,
        GucFlags::default(),
    );
}
