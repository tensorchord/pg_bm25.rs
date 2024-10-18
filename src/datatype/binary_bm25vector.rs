use std::ffi::c_char;

use pgrx::IntoDatum;
use pgrx::{pg_sys::Oid, Internal};

use crate::datatype::bm25vector::Bm25VectorBorrowed;

use super::binary::Bytea;
use super::memory_bm25vector::Bm25VectorInput;
use super::memory_bm25vector::Bm25VectorOutput;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _bm25catalog_bm25vector_send(vector: Bm25VectorInput<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    let vector = vector.as_ref();
    unsafe {
        let mut buf = StringInfoData::default();
        let len = vector.len();
        let b_indexes = size_of::<u32>() * vector.len() as usize;
        let b_values = size_of::<u32>() * vector.len() as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&vector.doc_len()) as *const u32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.indexes().as_ptr() as _, b_indexes as _);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.values().as_ptr() as _, b_values as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _bm25catalog_bm25vector_recv(internal: Internal, _oid: Oid, _typmod: i32) -> Bm25VectorOutput {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const u32).read_unaligned() as usize;
        let doc_len = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const u32).read_unaligned();

        let b_all = 2 * size_of::<u32>() * len;
        let p_all = pgrx::pg_sys::pq_getmsgbytes(buf, b_all as _);
        let mut all_aligned = Vec::<u32>::with_capacity(2 * len);
        std::ptr::copy_nonoverlapping(p_all, all_aligned.as_mut_ptr().cast::<c_char>(), b_all);
        all_aligned.set_len(2 * len);

        if let Some(vector) =
            Bm25VectorBorrowed::new_checked(doc_len, &all_aligned[..len], &all_aligned[len..])
        {
            Bm25VectorOutput::new(vector)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
