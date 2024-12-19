use serde::Deserialize;

use crate::{options::IndexingOption, utils::cells::PgCell};
use std::ffi::CStr;

static RELOPT_KIND_BM25: PgCell<pgrx::pg_sys::relopt_kind::Type> = unsafe { PgCell::new(0) };

#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct Reloption {
    vl_len_: i32,
    pub options: i32,
}

impl Reloption {
    pub const TAB: &'static [pgrx::pg_sys::relopt_parse_elt] = &[pgrx::pg_sys::relopt_parse_elt {
        optname: c"options".as_ptr(),
        opttype: pgrx::pg_sys::relopt_type::RELOPT_TYPE_STRING,
        offset: std::mem::offset_of!(Reloption, options) as i32,
    }];

    #[allow(unused)]
    pub unsafe fn options(&self) -> &CStr {
        unsafe {
            let ptr = std::ptr::addr_of!(*self)
                .cast::<std::ffi::c_char>()
                .offset(self.options as _);
            CStr::from_ptr(ptr)
        }
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pgrx::pg_sys::Datum,
    validate: bool,
) -> *mut pgrx::pg_sys::bytea {
    let rdopts = unsafe {
        pgrx::pg_sys::build_reloptions(
            reloptions,
            validate,
            RELOPT_KIND_BM25.get(),
            size_of::<Reloption>(),
            Reloption::TAB.as_ptr(),
            Reloption::TAB.len() as _,
        )
    };
    rdopts as *mut pgrx::pg_sys::bytea
}

pub fn init() {
    unsafe {
        RELOPT_KIND_BM25.set(pgrx::pg_sys::add_reloption_kind());
        pgrx::pg_sys::add_string_reloption(
            RELOPT_KIND_BM25.get(),
            c"options".as_ptr(),
            c"BM25 index options, represented as a TOML string.".as_ptr(),
            c"".as_ptr(),
            None,
            pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE,
        );
    }
}

unsafe fn convert_reloptions_to_options(
    reloptions: *const pgrx::pg_sys::varlena,
) -> IndexingOption {
    #[derive(Debug, Clone, Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    struct Parsed {
        #[serde(flatten)]
        option: IndexingOption,
    }
    let reloption = reloptions as *const Reloption;
    if reloption.is_null() || unsafe { (*reloption).options == 0 } {
        return Default::default();
    }
    let s = unsafe { (*reloption).options() }.to_string_lossy();
    match toml::from_str::<Parsed>(&s) {
        Ok(p) => p.option,
        Err(e) => pgrx::error!("failed to parse options: {}", e),
    }
}

pub unsafe fn get_options(index: pgrx::pg_sys::Relation) -> IndexingOption {
    convert_reloptions_to_options((*index).rd_options)
}
