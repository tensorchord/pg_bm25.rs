use std::ops::DerefMut;

use crate::{
    page::{bm25_page_size, page_alloc_init_forknum},
    segment::{meta::MetaPageData, page_alloc_from_free_list},
};

use super::{PageFlags, PageWriteGuard};

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

pub struct PageWriter<'a> {
    relation: pgrx::pg_sys::Relation,
    meta: &'a mut MetaPageData,
    flag: PageFlags,
    skip_lock_rel: bool,
    first_blkno: pgrx::pg_sys::BlockNumber,
    page: Option<PageWriteGuard>,
}

impl<'a> PageWriter<'a> {
    pub fn new(
        relation: pgrx::pg_sys::Relation,
        meta: &'a mut MetaPageData,
        flag: PageFlags,
        skip_lock_rel: bool,
    ) -> Self {
        Self {
            relation,
            meta,
            flag,
            skip_lock_rel,
            first_blkno: pgrx::pg_sys::InvalidBlockNumber,
            page: None,
        }
    }
}

impl<'a> PageWriter<'a> {
    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.first_blkno
    }

    fn change_page(&mut self) {
        let mut old_page = self.page.take().unwrap();
        let new_page =
            page_alloc_from_free_list(self.relation, self.meta, self.flag, self.skip_lock_rel);
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
            let page =
                page_alloc_from_free_list(self.relation, self.meta, self.flag, self.skip_lock_rel);
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

    // it will make sure the data is on the same page
    pub fn write_no_cross(&mut self, data: &[u8]) {
        assert!(data.len() <= bm25_page_size());
        let mut space = self.freespace_mut();
        if space.len() < data.len() {
            self.change_page();
            space = self.freespace_mut();
        }
        space[..data.len()].copy_from_slice(data);
        let space_len = space.len();
        *self.offset() += data.len() as u16;
        if data.len() == space_len {
            self.change_page();
        }
    }
}
