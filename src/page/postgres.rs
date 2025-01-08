use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

const _: () = {
    assert!(std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() % 8 == 0);
    assert!(std::mem::size_of::<Bm25PageOpaqueData>() % 8 == 0);
    assert!(std::mem::size_of::<PageData>() == pgrx::pg_sys::BLCKSZ as usize);
};

pub const P_NEW: pgrx::pg_sys::BlockNumber = pgrx::pg_sys::InvalidBlockNumber;
pub const METAPAGE_BLKNO: pgrx::pg_sys::BlockNumber = 0;
pub const BM25_PAGE_ID: u16 = 0xFF88;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy)]
    pub struct PageFlags: u16 {
        const META = 1 << 0;
        const PAYLOAD = 1 << 1;
        const FIELD_NORM = 1 << 2;
        const TERM_STATISTIC = 1 << 3;
        const TERM_INFO = 1 << 4;
        const TERM_META = 1 << 5;
        const SKIP_INFO = 1 << 6;
        const BLOCK_DATA = 1 << 7;
        const GROWING = 1 << 8;
        const DELETE = 1 << 9;
        const FREE = 1 << 15;
    }
}

pub const fn bm25_page_size() -> usize {
    pgrx::pg_sys::BLCKSZ as usize
        - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>()
        - std::mem::size_of::<Bm25PageOpaqueData>()
}

#[repr(C, align(8))]
pub struct Bm25PageOpaqueData {
    pub next_blkno: pgrx::pg_sys::BlockNumber,
    pub page_flag: PageFlags,
    bm25_page_id: u16, // for identification of bm25 index
}

#[repr(C, align(8))]
pub struct PageData {
    pub header: pgrx::pg_sys::PageHeaderData,
    pub content: [u8; bm25_page_size()],
    pub opaque: Bm25PageOpaqueData,
}

impl PageData {
    pub fn init_mut(&mut self, flag: PageFlags) {
        unsafe {
            pgrx::pg_sys::PageInit(
                self as *mut _ as _,
                pgrx::pg_sys::BLCKSZ as _,
                std::mem::size_of::<Bm25PageOpaqueData>(),
            );
            (&raw mut self.opaque).write(Bm25PageOpaqueData {
                next_blkno: pgrx::pg_sys::InvalidBlockNumber,
                page_flag: flag,
                bm25_page_id: BM25_PAGE_ID,
            });
        };
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

impl<T> AsRef<T> for PageData {
    fn as_ref(&self) -> &T {
        const {
            assert!(std::mem::size_of::<T>() <= bm25_page_size());
        }
        unsafe { &*(self.content.as_ptr() as *const T) }
    }
}

impl<T> AsMut<T> for PageData {
    fn as_mut(&mut self) -> &mut T {
        const {
            assert!(std::mem::size_of::<T>() <= bm25_page_size());
        }
        unsafe { &mut *(self.content.as_mut_ptr() as *mut T) }
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

    // not guaranteed to be atomic
    pub fn upgrade(self, relation: pgrx::pg_sys::Relation) -> PageWriteGuard {
        unsafe {
            use pgrx::pg_sys::{
                GenericXLogRegisterBuffer, GenericXLogStart, LockBuffer, BUFFER_LOCK_EXCLUSIVE,
                BUFFER_LOCK_UNLOCK, GENERIC_XLOG_FULL_IMAGE,
            };
            let buf = self.buf;
            std::mem::forget(self);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK as _);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE as _);
            let state = GenericXLogStart(relation);
            let page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE as _);
            let page = NonNull::new(page.cast()).expect("failed to get page");
            PageWriteGuard { buf, page, state }
        }
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
            BufferGetPage, ForkNumber, LockBuffer, ReadBufferExtended, ReadBufferMode,
            BUFFER_LOCK_SHARE,
        };
        let buf = ReadBufferExtended(
            relation,
            ForkNumber::MAIN_FORKNUM,
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

    // not guaranteed to be atomic
    pub fn degrade(self) -> PageReadGuard {
        unsafe {
            use pgrx::pg_sys::{BufferGetPage, LockBuffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
            let buf = self.buf;
            let state = self.state;
            std::mem::forget(self);
            pgrx::pg_sys::GenericXLogFinish(state);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK as _);
            LockBuffer(buf, BUFFER_LOCK_SHARE as _);
            let page = NonNull::new(BufferGetPage(buf).cast()).expect("failed to get page");
            PageReadGuard { buf, page }
        }
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
            ReadBufferExtended, ReadBufferMode, BUFFER_LOCK_EXCLUSIVE,
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
        let page = GenericXLogRegisterBuffer(state, buf, 0);
        let page = NonNull::new(page.cast()).expect("failed to get page");
        PageWriteGuard { buf, page, state }
    }
}

#[cfg(any(feature = "pg16", feature = "pg17"))]
pub fn page_alloc(
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
) -> PageWriteGuard {
    unsafe {
        use pgrx::pg_sys::{
            BufferManagerRelation,
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

#[cfg(any(feature = "pg14", feature = "pg15"))]
pub fn page_alloc(
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
) -> PageWriteGuard {
    unsafe {
        use pgrx::pg_sys::{
            ExclusiveLock, GenericXLogRegisterBuffer, GenericXLogStart, LockBuffer,
            LockRelationForExtension, ReadBuffer, UnlockRelationForExtension,
            BUFFER_LOCK_EXCLUSIVE, GENERIC_XLOG_FULL_IMAGE,
        };
        if !skip_lock_rel {
            LockRelationForExtension(relation, ExclusiveLock as _);
        }
        let buf = ReadBuffer(relation, P_NEW);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE as _);
        if !skip_lock_rel {
            UnlockRelationForExtension(relation, ExclusiveLock as _);
        }
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

#[cfg(any(feature = "pg16", feature = "pg17"))]
pub fn page_alloc_init_forknum(
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
) -> PageWriteGuard {
    unsafe {
        use pgrx::pg_sys::{
            BufferManagerRelation,
            ExtendBufferedFlags::{EB_LOCK_FIRST, EB_SKIP_EXTENSION_LOCK},
            ExtendBufferedRel, ForkNumber, GenericXLogRegisterBuffer, GenericXLogStart,
            GENERIC_XLOG_FULL_IMAGE,
        };
        let arg_flags = EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK;
        let buf = ExtendBufferedRel(
            BufferManagerRelation {
                rel: relation,
                smgr: std::ptr::null_mut(),
                relpersistence: 0,
            },
            ForkNumber::INIT_FORKNUM,
            std::ptr::null_mut(),
            arg_flags,
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

#[cfg(any(feature = "pg14", feature = "pg15"))]
pub fn page_alloc_init_forknum(
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
) -> PageWriteGuard {
    unsafe {
        use pgrx::pg_sys::{
            ForkNumber, GenericXLogRegisterBuffer, GenericXLogStart, LockBuffer,
            ReadBufferExtended, ReadBufferMode, BUFFER_LOCK_EXCLUSIVE, GENERIC_XLOG_FULL_IMAGE,
        };
        let buf = ReadBufferExtended(
            relation,
            ForkNumber::INIT_FORKNUM,
            P_NEW,
            ReadBufferMode::RBM_NORMAL,
            std::ptr::null_mut(),
        );
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE as _);
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

pub fn page_alloc_with_fsm(
    index: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
) -> PageWriteGuard {
    let blkno = unsafe { pgrx::pg_sys::GetFreeIndexPage(index) };

    if blkno == pgrx::pg_sys::InvalidBlockNumber {
        page_alloc(index, flag, skip_lock_rel)
    } else {
        let mut page = page_write(index, blkno);
        PageData::init_mut(&mut page, flag);
        page
    }
}

pub fn page_free(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) {
    unsafe {
        pgrx::pg_sys::RecordFreeIndexPage(index, blkno);
    }
}

pub fn page_get_max_offset_number(page: &PageData) -> u16 {
    assert!(page.header.pd_lower >= std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() as u16);
    (page.header.pd_lower - std::mem::size_of::<pgrx::pg_sys::PageHeaderData>() as u16)
        / std::mem::size_of::<pgrx::pg_sys::ItemIdData>() as u16
}

pub fn page_get_item_id(
    page: &PageData,
    offset_number: pgrx::pg_sys::OffsetNumber,
) -> pgrx::pg_sys::ItemIdData {
    unsafe {
        page.header
            .pd_linp
            .as_ptr()
            .add(offset_number as usize - 1)
            .read()
    }
}

pub fn page_get_item<T>(page: &PageData, item_id: pgrx::pg_sys::ItemIdData) -> &T {
    unsafe {
        let offset = item_id.lp_off();
        let ptr = (page as *const PageData)
            .cast::<u8>()
            .add(offset as usize)
            .cast::<T>();
        assert!(ptr.is_aligned());
        &*ptr
    }
}

pub fn page_append_item(page: &mut PageData, item: &[u8]) -> bool {
    let offset_number = unsafe {
        pgrx::pg_sys::PageAddItemExtended(
            page as *mut _ as _,
            item.as_ptr() as *const _ as _,
            item.len(),
            pgrx::pg_sys::InvalidOffsetNumber,
            0,
        )
    };
    offset_number != pgrx::pg_sys::InvalidOffsetNumber
}
