#![feature(raw_ref_op)]
#![feature(new_uninit)]
#![feature(doc_cfg)]
#![allow(clippy::len_without_is_empty)]

pub mod builder;
pub mod datatype;
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

const SCHEMA: &str = "bm25_catalog";

const SCHEMA_C_BYTES: [u8; SCHEMA.len() + 1] = {
    let mut bytes = [0u8; SCHEMA.len() + 1];
    let mut i = 0_usize;
    while i < SCHEMA.len() {
        bytes[i] = SCHEMA.as_bytes()[i];
        i += 1;
    }
    bytes
};

const SCHEMA_C_STR: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(&SCHEMA_C_BYTES) {
    Ok(x) => x,
    Err(_) => panic!("there are null characters in schema"),
};
