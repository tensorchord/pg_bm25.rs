use std::ops::DerefMut;

use super::{page_alloc, PageFlags, PageWriteGuard};

/// Utility to build pages like a file
pub struct PageBuilder {
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
    first_blkno: pgrx::pg_sys::BlockNumber,
    page: Option<PageWriteGuard>,
}

impl PageBuilder {
    pub fn new(relation: pgrx::pg_sys::Relation, flag: PageFlags, skip_lock_rel: bool) -> Self {
        Self {
            relation,
            flag,
            skip_lock_rel,
            first_blkno: pgrx::pg_sys::InvalidBlockNumber,
            page: None,
        }
    }

    /// finalize the page and return the first block number
    /// if it hasn't been written yet, it will return InvalidBlockNumber
    pub fn finalize(mut self) -> pgrx::pg_sys::BlockNumber {
        self.page.take();
        self.first_blkno
    }

    fn change_page(&mut self) {
        let mut old_page = self.page.take().unwrap();
        let new_page = page_alloc(self.relation, self.flag, self.skip_lock_rel);
        old_page.opaque.next_blkno = new_page.blkno();
        self.page = Some(new_page);
    }

    fn offset(&mut self) -> &mut u16 {
        let page = self.page.as_mut().unwrap().deref_mut();
        &mut page.header.pd_lower
    }

    fn freespace_mut(&mut self) -> &mut [u8] {
        if self.page.is_none() {
            let page = page_alloc(self.relation, self.flag, self.skip_lock_rel);
            self.first_blkno = page.blkno();
            self.page = Some(page);
        }
        self.page.as_mut().unwrap().deref_mut().freespace_mut()
    }
}

impl Drop for PageBuilder {
    fn drop(&mut self) {
        if self.page.is_some() {
            pgrx::warning!("PageBuilder dropped without finalizing");
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
            let space = self.freespace_mut();
            let space_len = space.len();
            let len = space_len.min(data.len());
            space[..len].copy_from_slice(&data[..len]);
            *self.offset() += len as u16;
            if len == space_len {
                self.change_page();
            }
            data = &data[len..];
        }
        Ok(())
    }
}
