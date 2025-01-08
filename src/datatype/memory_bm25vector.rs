use std::collections::BTreeMap;
use std::{alloc::Layout, ops::Deref, ptr::NonNull};

use pgrx::{
    pg_sys::{Datum, Oid},
    pgrx_sql_entity_graph::metadata::{
        ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
    },
    FromDatum, IntoDatum,
};

use super::bm25vector::Bm25VectorBorrowed;

#[repr(C, align(8))]
pub struct Bm25VectorHeader {
    varlena: u32,
    len: u32,
    doc_len: u32, // sum of all term frequencies
    reserved: u32,
    phantom: [u8; 0],
}

impl Bm25VectorHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: u32) -> Layout {
        let layout = Layout::new::<Bm25VectorHeader>();
        let layout1 = Layout::array::<u32>(len as usize).unwrap();
        let layout2 = Layout::array::<u32>(len as usize).unwrap();
        let layout = layout.extend(layout1).unwrap().0.pad_to_align();
        layout.extend(layout2).unwrap().0.pad_to_align()
    }
    fn indexes(&self) -> &[u32] {
        let ptr = self.phantom.as_ptr().cast();
        unsafe { std::slice::from_raw_parts(ptr, self.len as usize) }
    }
    fn values(&self) -> &[u32] {
        let len = self.len as usize;
        unsafe {
            let ptr = self.phantom.as_ptr().cast::<u32>().add(len);
            let offset = ptr.align_offset(8);
            let ptr = ptr.add(offset);
            std::slice::from_raw_parts(ptr, len)
        }
    }
    pub fn borrow(&self) -> Bm25VectorBorrowed {
        unsafe { Bm25VectorBorrowed::new_unchecked(self.doc_len, self.indexes(), self.values()) }
    }
    pub fn to_bytes(&self) -> &[u8] {
        let len = self.varlena as usize >> 2;
        let ptr = self as *const Bm25VectorHeader as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

pub enum Bm25VectorInput<'a> {
    Owned(Bm25VectorOutput),
    Borrowed(&'a Bm25VectorHeader),
}

impl Bm25VectorInput<'_> {
    unsafe fn new(p: NonNull<Bm25VectorHeader>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Bm25VectorInput::Owned(Bm25VectorOutput(q))
        } else {
            unsafe { Bm25VectorInput::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Bm25VectorInput<'_> {
    type Target = Bm25VectorHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            Bm25VectorInput::Owned(x) => x,
            Bm25VectorInput::Borrowed(x) => x,
        }
    }
}

pub struct Bm25VectorOutput(NonNull<Bm25VectorHeader>);

impl Bm25VectorOutput {
    pub fn new(vector: Bm25VectorBorrowed) -> Self {
        let len = vector.len();
        unsafe {
            let layout = Bm25VectorHeader::layout(len);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Bm25VectorHeader;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            (&raw mut (*ptr).varlena).write(Bm25VectorHeader::varlena(layout.size()));
            (&raw mut (*ptr).len).write(len);
            (&raw mut (*ptr).doc_len).write(vector.doc_len());
            (&raw mut (*ptr).reserved).write(0);
            let mut data_ptr = (*ptr).phantom.as_mut_ptr().cast::<u32>();
            std::ptr::copy_nonoverlapping(vector.indexes().as_ptr(), data_ptr, len as usize);
            data_ptr = data_ptr.add(len as usize);
            let offset = data_ptr.align_offset(8);
            std::ptr::write_bytes(data_ptr, 0, offset);
            data_ptr = data_ptr.add(offset);
            std::ptr::copy_nonoverlapping(vector.values().as_ptr(), data_ptr, len as usize);
            Bm25VectorOutput(NonNull::new(ptr).unwrap())
        }
    }

    pub fn from_ids(ids: &[u32]) -> Self {
        let mut map: BTreeMap<u32, u32> = BTreeMap::new();
        for term_id in ids {
            *map.entry(*term_id).or_insert(0) += 1;
        }
        let mut doc_len: u32 = 0;
        let mut indexes = Vec::with_capacity(map.len());
        let mut values = Vec::with_capacity(map.len());
        for (index, value) in map {
            indexes.push(index);
            values.push(value);
            doc_len = doc_len.checked_add(value).expect("overflow");
        }
        let vector = unsafe { Bm25VectorBorrowed::new_unchecked(doc_len, &indexes, &values) };
        Self::new(vector)
    }

    pub fn into_raw(self) -> *mut Bm25VectorHeader {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Bm25VectorOutput {
    type Target = Bm25VectorHeader;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl Drop for Bm25VectorOutput {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl FromDatum for Bm25VectorInput<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Bm25VectorHeader>()).unwrap();
            unsafe { Some(Bm25VectorInput::new(ptr)) }
        }
    }
}

impl IntoDatum for Bm25VectorOutput {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::regtypein("bm25vector")
    }
}

impl FromDatum for Bm25VectorOutput {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let p = NonNull::new(datum.cast_mut_ptr::<Bm25VectorHeader>())?;
            let q =
                unsafe { NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast())? };
            if p != q {
                Some(Bm25VectorOutput(q))
            } else {
                let vector = p.as_ref().borrow();
                Some(Bm25VectorOutput::new(vector))
            }
        }
    }
}

unsafe impl pgrx::datum::UnboxDatum for Bm25VectorOutput {
    type As<'src> = Bm25VectorOutput;
    #[inline]
    unsafe fn unbox<'src>(d: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let p = NonNull::new(d.sans_lifetime().cast_mut_ptr::<Bm25VectorHeader>()).unwrap();
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Bm25VectorOutput(q)
        } else {
            let vector = p.as_ref().borrow();
            Bm25VectorOutput::new(vector)
        }
    }
}

unsafe impl SqlTranslatable for Bm25VectorInput<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bm25vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bm25vector"))))
    }
}

unsafe impl SqlTranslatable for Bm25VectorOutput {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bm25vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bm25vector"))))
    }
}

unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for Bm25VectorInput<'fcx> {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

unsafe impl pgrx::callconv::BoxRet for Bm25VectorOutput {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.into_raw() as *mut ())) }
    }
}
