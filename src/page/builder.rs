use std::backtrace::Backtrace;

use super::P_NEW;

pub struct PageBuilder {
    rel: pgrx::pg_sys::Relation,
    flag: u16,
    check_continuous: bool,
    first_blkno: pgrx::pg_sys::BlockNumber,
    buf: pgrx::pg_sys::Buffer,
    page: pgrx::pg_sys::Page,
}

impl PageBuilder {
    pub fn new(rel: pgrx::pg_sys::Relation, flag: u16, check_continuous: bool) -> Self {
        unsafe {
            let buf = pgrx::pg_sys::ReadBuffer(rel, P_NEW);
            pgrx::pg_sys::LockBuffer(buf, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            let first_blkno = pgrx::pg_sys::BufferGetBlockNumber(buf);
            let page = pgrx::pg_sys::BufferGetPage(buf);
            super::init_page(page, flag);
            Self {
                rel,
                flag,
                check_continuous,
                first_blkno,
                buf,
                page,
            }
        }
    }

    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        unsafe {
            pgrx::pg_sys::MarkBufferDirty(self.buf);
            pgrx::pg_sys::UnlockReleaseBuffer(self.buf);
        }
        let res = self.first_blkno;
        std::mem::forget(self);
        res
    }

    fn flush_page(&mut self) {
        let len = self.page_space().len();
        if len == 0 {
            unsafe {
                let buf = pgrx::pg_sys::ReadBuffer(self.rel, P_NEW);
                pgrx::pg_sys::LockBuffer(buf, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                let blkno = pgrx::pg_sys::BufferGetBlockNumber(buf);
                if self.check_continuous {
                    assert_eq!(blkno, pgrx::pg_sys::BufferGetBlockNumber(self.buf) + 1);
                }
                (*super::page_get_opaque(self.page)).next_blkno = blkno;

                pgrx::pg_sys::MarkBufferDirty(self.buf);
                pgrx::pg_sys::UnlockReleaseBuffer(self.buf);

                let page = pgrx::pg_sys::BufferGetPage(buf);
                super::init_page(page, self.flag);

                self.buf = buf;
                self.page = page;
            }
        }
    }

    fn offset(&mut self) -> &mut u16 {
        unsafe { &mut (*(self.page as pgrx::pg_sys::PageHeader)).pd_lower }
    }

    fn page_space(&mut self) -> &mut [u8] {
        unsafe {
            let pd_lower = (*(self.page as pgrx::pg_sys::PageHeader)).pd_lower;
            let pd_upper = (*(self.page as pgrx::pg_sys::PageHeader)).pd_upper;
            std::slice::from_raw_parts_mut(
                self.page.add(pd_lower as usize).cast(),
                (pd_upper - pd_lower) as usize,
            )
        }
    }
}

impl Drop for PageBuilder {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::MarkBufferDirty(self.buf);
            pgrx::pg_sys::UnlockReleaseBuffer(self.buf);
            pgrx::warning!(
                "PageBuilder dropped without finalizing, Backtrace: \n{}",
                Backtrace::force_capture()
            );
        }
    }
}

impl std::io::Write for PageBuilder {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_all(buf).map(|_| buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, mut data: &[u8]) -> std::io::Result<()> {
        while !data.is_empty() {
            let space = self.page_space();
            let len = space.len().min(data.len());
            space[..len].copy_from_slice(&data[..len]);
            *self.offset() += len as u16;
            self.flush_page();
            data = &data[len..];
        }
        Ok(())
    }
}
