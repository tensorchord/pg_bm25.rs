pub mod bm25query;
pub mod bm25weight;
pub mod builder;
pub mod field_norm;
pub mod guc;
pub mod index;
pub mod page;
pub mod payload;
pub mod postings;
pub mod token;
pub mod utils;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("Target is not supported.");

#[cfg(not(any(
    feature = "pg12",
    feature = "pg13",
    feature = "pg14",
    feature = "pg15",
    feature = "pg16"
)))]
compiler_error!("PostgreSQL version must be selected.");

#[pgrx::pg_guard]
unsafe extern "C" fn _PG_init() {
    index::init();
    guc::init();
    set_panic_hook();
}

fn set_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            format!("Message: {}", s)
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            format!("Message: {}", s)
        } else {
            String::new()
        };
        let location = info
            .location()
            .map(|location| {
                format!(
                    "Location: {}:{}:{}.",
                    location.file(),
                    location.line(),
                    location.column()
                )
            })
            .unwrap_or_default();
        let backtrace = format!("Backtrace: {}", std::backtrace::Backtrace::force_capture());
        pgrx::info!("Panickied.\n{message};\n{location};\n{backtrace}");
    }));
}
