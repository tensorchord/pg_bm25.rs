use std::ops::DerefMut;

use super::{page_alloc_init_forknum, page_alloc_with_fsm, page_write, PageFlags, PageWriteGuard};

pub struct PageWriterInitFork {
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    first_blkno: pgrx::pg_sys::BlockNumber,
    page: Option<PageWriteGuard>,
}

impl PageWriterInitFork {
    pub fn new(relation: pgrx::pg_sys::Relation, flag: PageFlags) -> Self {
        Self {
            relation,
            flag,
            first_blkno: pgrx::pg_sys::InvalidBlockNumber,
            page: None,
        }
    }

    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.first_blkno
    }

    fn change_page(&mut self) {
        let mut old_page = self.page.take().unwrap();
        let new_page = page_alloc_init_forknum(self.relation, self.flag);
        assert!(
            old_page.blkno() + 1 == new_page.blkno(),
            "old: {}, new: {}",
            old_page.blkno(),
            new_page.blkno()
        );
        old_page.opaque.next_blkno = new_page.blkno();
        self.page = Some(new_page);
    }

    fn offset(&mut self) -> &mut u16 {
        let page = self.page.as_mut().unwrap().deref_mut();
        &mut page.header.pd_lower
    }

    fn freespace_mut(&mut self) -> &mut [u8] {
        if self.page.is_none() {
            let page = page_alloc_init_forknum(self.relation, self.flag);
            self.first_blkno = page.blkno();
            self.page = Some(page);
        }
        self.page.as_mut().unwrap().deref_mut().freespace_mut()
    }

    pub fn write(&mut self, mut data: &[u8]) {
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
    }
}

pub struct PageWriter {
    relation: pgrx::pg_sys::Relation,
    flag: PageFlags,
    skip_lock_rel: bool,
    first_blkno: pgrx::pg_sys::BlockNumber,
    page: Option<PageWriteGuard>,
}

impl PageWriter {
    pub fn new(relation: pgrx::pg_sys::Relation, flag: PageFlags, skip_lock_rel: bool) -> Self {
        Self {
            relation,
            flag,
            skip_lock_rel,
            first_blkno: pgrx::pg_sys::InvalidBlockNumber,
            page: None,
        }
    }

    pub fn open(
        relation: pgrx::pg_sys::Relation,
        last_blkno: pgrx::pg_sys::BlockNumber,
        skip_lock_rel: bool,
    ) -> Self {
        let page = page_write(relation, last_blkno);
        Self {
            relation,
            flag: page.opaque.page_flag,
            skip_lock_rel,
            first_blkno: pgrx::pg_sys::InvalidBlockNumber,
            page: Some(page),
        }
    }
}

impl PageWriter {
    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.first_blkno
    }

    pub fn blkno(&self) -> pgrx::pg_sys::BlockNumber {
        self.page.as_ref().unwrap().blkno()
    }

    fn change_page(&mut self) {
        let mut old_page = self.page.take().unwrap();
        let new_page = page_alloc_with_fsm(self.relation, self.flag, self.skip_lock_rel);
        assert!(
            old_page.blkno() + 1 == new_page.blkno(),
            "old: {}, new: {}",
            old_page.blkno(),
            new_page.blkno()
        );
        old_page.opaque.next_blkno = new_page.blkno();
        self.page = Some(new_page);
    }

    fn offset(&mut self) -> &mut u16 {
        let page = self.page.as_mut().unwrap().deref_mut();
        &mut page.header.pd_lower
    }

    fn freespace_mut(&mut self) -> &mut [u8] {
        if self.page.is_none() {
            let page = page_alloc_with_fsm(self.relation, self.flag, self.skip_lock_rel);
            self.first_blkno = page.blkno();
            self.page = Some(page);
        }
        self.page.as_mut().unwrap().deref_mut().freespace_mut()
    }

    pub fn write(&mut self, mut data: &[u8]) {
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
    }
}
