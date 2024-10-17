use std::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use pgrx::pg_sys::BufferManagerRelation;

const _: () = {
    assert!(std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() % 8 == 0);
    assert!(std::mem::size_of::<Bm25PageOpaqueData>() % 8 == 0);
    assert!(std::mem::size_of::<PageData>() == pgrx::pg_sys::BLCKSZ as usize);
};

pub const METAPAGE_BLKNO: pgrx::pg_sys::BlockNumber = 0;
pub const BM25_PAGE_ID: u16 = 0xFF88;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct PageFlags: u16 {
        const META = 1 << 0;
        const PAYLOAD = 1 << 1;
        const FIELD_NORMS = 1 << 2;
        const POSTINGS = 1 << 3;
        const TERM_DICT = 1 << 4;
        const TERM_INFO = 1 << 5;
    }
}

#[repr(C, align(8))]
pub struct Bm25PageOpaqueData {
    pub next_blkno: pgrx::pg_sys::BlockNumber,
    page_flag: PageFlags,
    bm25_page_id: u16, // for identification of bm25 index
}

#[repr(C, align(8))]
pub struct PageData {
    pub header: pgrx::pg_sys::PageHeaderData,
    pub content: [u8; bm25_page_size()],
    pub opaque: Bm25PageOpaqueData,
}

impl PageData {
    pub fn init_mut(this: &mut MaybeUninit<Self>, flag: PageFlags) -> &mut Self {
        unsafe {
            pgrx::pg_sys::PageInit(
                this.as_mut_ptr() as _,
                pgrx::pg_sys::BLCKSZ as _,
                std::mem::size_of::<Bm25PageOpaqueData>(),
            );
            (&raw mut (*this.as_mut_ptr()).opaque).write(Bm25PageOpaqueData {
                next_blkno: pgrx::pg_sys::InvalidBlockNumber,
                page_flag: flag,
                bm25_page_id: BM25_PAGE_ID,
            });
            MaybeUninit::assume_init_mut(this)
        }
    }

    pub fn data(&self) -> &[u8] {
        let pd_lower = self.header.pd_lower as usize;
        let lower_offset = pd_lower - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>();
        &self.content[..lower_offset]
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        let pd_lower = self.header.pd_lower as usize;
        let lower_offset = pd_lower - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>();
        &mut self.content[..lower_offset]
    }

    pub fn freespace_mut(&mut self) -> &mut [u8] {
        let pd_lower = self.header.pd_lower as usize;
        let lower_offset = pd_lower - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>();
        &mut self.content[lower_offset..]
    }
}

pub struct PageReadGuard {
    buf: i32,
    page: NonNull<PageData>,
}

impl PageReadGuard {
    pub fn blkno(&self) -> pgrx::pg_sys::BlockNumber {
        unsafe { pgrx::pg_sys::BufferGetBlockNumber(self.buf) }
    }
}

impl Deref for PageReadGuard {
    type Target = PageData;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::UnlockReleaseBuffer(self.buf);
        }
    }
}

pub fn page_read(
    relation: pgrx::pg_sys::Relation,
    blkno: pgrx::pg_sys::BlockNumber,
) -> PageReadGuard {
    assert!(blkno != pgrx::pg_sys::InvalidBlockNumber);
    unsafe {
        use pgrx::pg_sys::{
            BufferGetPage, LockBuffer, ReadBufferExtended, ReadBufferMode, BUFFER_LOCK_SHARE,
        };
        let buf = ReadBufferExtended(
            relation,
            0,
            blkno,
            ReadBufferMode::RBM_NORMAL,
            std::ptr::null_mut(),
        );
        LockBuffer(buf, BUFFER_LOCK_SHARE as _);
        let page = NonNull::new(BufferGetPage(buf).cast()).expect("failed to get page");
        PageReadGuard { buf, page }
    }
}

pub struct PageWriteGuard {
    buf: i32,
    page: NonNull<PageData>,
    state: *mut pgrx::pg_sys::GenericXLogState,
}

impl PageWriteGuard {
    pub fn blkno(&self) -> pgrx::pg_sys::BlockNumber {
        unsafe { pgrx::pg_sys::BufferGetBlockNumber(self.buf) }
    }
}

impl Deref for PageWriteGuard {
    type Target = PageData;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl DerefMut for PageWriteGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.page.as_mut() }
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        unsafe {
            if std::thread::panicking() {
                pgrx::pg_sys::GenericXLogAbort(self.state);
            } else {
                pgrx::pg_sys::GenericXLogFinish(self.state);
            }
            pgrx::pg_sys::MarkBufferDirty(self.buf);
            pgrx::pg_sys::UnlockReleaseBuffer(self.buf);
        }
    }
}

pub fn page_write(
    relation: pgrx::pg_sys::Relation,
    blkno: pgrx::pg_sys::BlockNumber,
) -> PageWriteGuard {
    assert!(blkno != pgrx::pg_sys::InvalidBlockNumber);
    unsafe {
        use pgrx::pg_sys::{
            ForkNumber, GenericXLogRegisterBuffer, GenericXLogStart, LockBuffer,
            ReadBufferExtended, ReadBufferMode, BUFFER_LOCK_EXCLUSIVE, GENERIC_XLOG_FULL_IMAGE,
        };
        let buf = ReadBufferExtended(
            relation,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::RBM_NORMAL,
            std::ptr::null_mut(),
        );
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE as _);
        let state = GenericXLogStart(relation);
        let page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE as _);
        let page = NonNull::new(page.cast()).expect("failed to get page");
        PageWriteGuard { buf, page, state }
    }
}

pub fn page_alloc(
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
) -> PageWriteGuard {
    unsafe {
        use pgrx::pg_sys::{
            ExtendBufferedFlags::{EB_LOCK_FIRST, EB_SKIP_EXTENSION_LOCK},
            ExtendBufferedRel, ForkNumber, GenericXLogRegisterBuffer, GenericXLogStart,
            GENERIC_XLOG_FULL_IMAGE,
        };
        let mut arg_flags = EB_LOCK_FIRST;
        if skip_lock_rel {
            arg_flags |= EB_SKIP_EXTENSION_LOCK;
        }
        let buf = ExtendBufferedRel(
            BufferManagerRelation {
                rel: relation,
                smgr: std::ptr::null_mut(),
                relpersistence: 0,
            },
            ForkNumber::MAIN_FORKNUM,
            std::ptr::null_mut(),
            arg_flags,
        );
        pgrx::info!(
            "page_alloc: blkno={}",
            pgrx::pg_sys::BufferGetBlockNumber(buf)
        );
        let state = GenericXLogStart(relation);
        let page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE as _);
        let mut page = NonNull::new(page.cast()).expect("failed to get page");
        PageData::init_mut(page.as_mut(), flag);
        PageWriteGuard {
            buf,
            page: page.cast(),
            state,
        }
    }
}

pub const fn bm25_page_size() -> usize {
    pgrx::pg_sys::BLCKSZ as usize
        - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>()
        - std::mem::size_of::<Bm25PageOpaqueData>()
}
