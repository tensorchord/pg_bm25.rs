#![feature(raw_ref_op)]

pub mod bm25query;
pub mod builder;
pub mod field_norm;
pub mod guc;
pub mod index;
pub mod page;
pub mod payload;
pub mod postings;
pub mod token;
pub mod utils;
pub mod weight;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("Target is not supported.");

#[cfg(not(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17")))]
compiler_error!("PostgreSQL version must be selected.");

#[pgrx::pg_guard]
unsafe extern "C" fn _PG_init() {
    index::init();
    guc::init();
}
