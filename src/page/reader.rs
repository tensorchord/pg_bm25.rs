use aligned_vec::AVec;

use super::{bm25_page_size, page_get_contents, page_get_opaque};

pub struct ContinousPageReader<T> {
    index: pgrx::pg_sys::Relation,
    start_blkno: pgrx::pg_sys::BlockNumber,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Copy> ContinousPageReader<T> {
    pub fn new(index: pgrx::pg_sys::Relation, start_blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self {
            index,
            start_blkno,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn read(&self, idx: u32) -> T {
        let blkno_offset = idx / Self::page_count() as u32;
        let blkno = self.start_blkno + blkno_offset as pgrx::pg_sys::BlockNumber;
        let offset = (idx % Self::page_count() as u32) as usize;
        unsafe {
            let buffer = pgrx::pg_sys::ReadBuffer(self.index, blkno);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
            let page = pgrx::pg_sys::BufferGetPage(buffer);
            let page_start = page_get_contents::<T>(page);
            let data = page_start.add(offset).read();
            pgrx::pg_sys::UnlockReleaseBuffer(buffer);
            data
        }
    }

    const fn page_count() -> usize {
        assert!(std::mem::align_of::<T>() <= 8);
        bm25_page_size() / std::mem::size_of::<T>()
    }
}

pub struct PageReader {
    index: pgrx::pg_sys::Relation,
    blkno: pgrx::pg_sys::BlockNumber,
    inner: Option<PageReaderInner>,
    finished: bool,
}

struct PageReaderInner {
    buffer: pgrx::pg_sys::Buffer,
    next_blkno: pgrx::pg_sys::BlockNumber,
    data: &'static [u8], // manual control lifetime
}

impl PageReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self {
            index,
            blkno,
            inner: None,
            finished: false,
        }
    }

    fn load_block(&mut self) {
        let buffer = unsafe { pgrx::pg_sys::ReadBuffer(self.index, self.blkno) };
        unsafe {
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
            let page = pgrx::pg_sys::BufferGetPage(buffer);
            let pd_lower = (*(page as pgrx::pg_sys::PageHeader)).pd_lower;
            let next_blkno = (*page_get_opaque(page)).next_blkno;
            let data = page_get_contents::<u8>(page);
            let slice = std::slice::from_raw_parts(
                data,
                pd_lower as usize
                    - pgrx::pg_sys::MAXALIGN(std::mem::size_of::<pgrx::pg_sys::PageHeaderData>()),
            );
            self.inner = Some(PageReaderInner {
                buffer,
                next_blkno,
                data: slice,
            });
        }
    }

    fn unload_block(&mut self) {
        if let Some(inner) = self.inner.take() {
            unsafe {
                pgrx::pg_sys::UnlockReleaseBuffer(inner.buffer);
                if inner.next_blkno == pgrx::pg_sys::InvalidBlockNumber {
                    self.finished = true;
                } else {
                    self.blkno = inner.next_blkno;
                }
            }
        }
    }

    pub fn read_to_end_aligned<A: aligned_vec::Alignment>(
        &mut self,
        buf: &mut AVec<u8, A>,
    ) -> std::io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.inner.is_none() {
            self.load_block();
        }

        let mut read_len = 0;
        loop {
            let data = self.inner.as_ref().unwrap().data;
            buf.extend_from_slice(data);
            read_len += data.len();
            self.unload_block();
            if self.finished {
                break;
            } else {
                self.load_block();
            }
        }

        Ok(read_len)
    }
}

impl Drop for PageReader {
    fn drop(&mut self) {
        self.unload_block();
    }
}

impl std::io::Read for PageReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.inner.is_none() {
            self.load_block();
        }
        let data = &mut self.inner.as_mut().unwrap().data;
        let to_read = std::cmp::min(buf.len(), data.len());
        buf[..to_read].copy_from_slice(&data[..to_read]);
        *data = &data[to_read..];
        if data.is_empty() {
            self.unload_block();
        }
        Ok(to_read)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.inner.is_none() {
            self.load_block();
        }

        let mut read_len = 0;
        loop {
            let data = self.inner.as_ref().unwrap().data;
            buf.extend_from_slice(data);
            read_len += data.len();
            self.unload_block();
            if self.finished {
                break;
            } else {
                self.load_block();
            }
        }

        Ok(read_len)
    }
}
