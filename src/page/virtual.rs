use crate::{
    page::page_read,
    segment::{meta::MetaPageData, page_alloc_from_free_list},
};

use super::{
    bm25_page_size, page_alloc_init_forknum, page_write, PageFlags, PageReadGuard, PageWriteGuard,
};

const DIRECT_COUNT: usize = bm25_page_size() / 4;
const INDIRECT1_COUNT: usize = DIRECT_COUNT * DIRECT_COUNT;
const INDIRECT2_COUNT: usize = INDIRECT1_COUNT * DIRECT_COUNT;

pub struct VirtualPageReader {
    relation: pgrx::pg_sys::Relation,
    direct_inode: PageReadGuard,
}

impl VirtualPageReader {
    pub fn new(relation: pgrx::pg_sys::Relation, blkno: u32) -> Self {
        Self {
            relation,
            direct_inode: page_read(relation, blkno),
        }
    }

    pub fn read_at(&self, offset: u32, buf: &mut [u8]) {
        let virtual_id = offset / bm25_page_size() as u32;
        let page_offset = offset % bm25_page_size() as u32;
        assert!(page_offset + buf.len() as u32 <= bm25_page_size() as u32);
        let block_id = self.get_block_id(virtual_id);
        let block = page_read(self.relation, block_id);
        let data = &block.data()[page_offset as usize..][..buf.len()];
        buf.copy_from_slice(data);
    }

    pub fn update_at(&self, offset: u32, len: u32, f: impl FnOnce(&mut [u8])) {
        let virtual_id = offset / bm25_page_size() as u32;
        let page_offset = offset % bm25_page_size() as u32;
        assert!(page_offset + len <= bm25_page_size() as u32);
        let block_id = self.get_block_id(virtual_id);
        let mut block = page_write(self.relation, block_id);
        let data = &mut block.data_mut()[page_offset as usize..][..len as usize];
        f(data);
    }

    pub fn get_block_id(&self, virtual_id: u32) -> u32 {
        let mut virtual_id = virtual_id as usize;
        if virtual_id < DIRECT_COUNT {
            let slice = &self.direct_inode.content[virtual_id * 4..][..4];
            return u32::from_le_bytes(slice.try_into().unwrap());
        }

        virtual_id -= DIRECT_COUNT;
        let indirect1_inode = page_read(self.relation, self.direct_inode.opaque.next_blkno);
        if virtual_id < INDIRECT1_COUNT {
            let indirect1_id = virtual_id / DIRECT_COUNT;
            let indirect1_offset = virtual_id % DIRECT_COUNT;
            let slice = &indirect1_inode.content[indirect1_id * 4..][..4];
            let blkno = u32::from_le_bytes(slice.try_into().unwrap());
            let indirect = page_read(self.relation, blkno);
            let slice = &indirect.content[indirect1_offset * 4..][..4];
            return u32::from_le_bytes(slice.try_into().unwrap());
        }

        virtual_id -= INDIRECT1_COUNT;
        assert!(virtual_id < INDIRECT2_COUNT);
        let indirect2_inode = page_read(self.relation, indirect1_inode.opaque.next_blkno);
        let indirect2_id = virtual_id / INDIRECT1_COUNT;
        let indirect2_offset = virtual_id % INDIRECT1_COUNT;
        let indirect1_id = indirect2_offset / DIRECT_COUNT;
        let indirect1_offset = indirect2_offset % DIRECT_COUNT;
        let slice = &indirect2_inode.content[indirect2_id * 4..][..4];
        let blkno = u32::from_le_bytes(slice.try_into().unwrap());
        let indirect1 = page_read(self.relation, blkno);
        let slice = &indirect1.content[indirect1_id * 4..][..4];
        let blkno = u32::from_le_bytes(slice.try_into().unwrap());
        let indirect = page_read(self.relation, blkno);
        let slice = &indirect.content[indirect1_offset * 4..][..4];
        u32::from_le_bytes(slice.try_into().unwrap())
    }
}

enum VirtualPageWriterState {
    Direct([PageWriteGuard; 2]),
    Indirect1([PageWriteGuard; 3]),
    Indirect2([PageWriteGuard; 4]),
}

pub struct VirtualPageWriter<'a> {
    relation: pgrx::pg_sys::Relation,
    meta: &'a mut MetaPageData,
    flag: PageFlags,
    skip_lock_rel: bool,
    first_blkno: pgrx::pg_sys::BlockNumber,
    state: VirtualPageWriterState,
}

impl<'a> VirtualPageWriter<'a> {
    pub fn init_fork(relation: pgrx::pg_sys::Relation, flag: PageFlags) -> u32 {
        let mut direct_inode = page_alloc_init_forknum(relation, flag);
        let data_page = page_alloc_init_forknum(relation, flag);
        let first_blkno = direct_inode.blkno();
        direct_inode.freespace_mut()[..4].copy_from_slice(&data_page.blkno().to_le_bytes());
        direct_inode.header.pd_lower += 4;
        first_blkno
    }

    pub fn new(
        relation: pgrx::pg_sys::Relation,
        meta: &'a mut MetaPageData,
        flag: PageFlags,
        skip_lock_rel: bool,
    ) -> Self {
        let mut direct_inode = page_alloc_from_free_list(relation, meta, flag, skip_lock_rel);
        let data_page = page_alloc_from_free_list(relation, meta, flag, skip_lock_rel);
        let first_blkno = direct_inode.blkno();
        direct_inode.freespace_mut()[..4].copy_from_slice(&data_page.blkno().to_le_bytes());
        direct_inode.header.pd_lower += 4;

        Self {
            relation,
            meta,
            flag,
            skip_lock_rel,
            first_blkno,
            state: VirtualPageWriterState::Direct([data_page, direct_inode]),
        }
    }

    pub fn open(
        relation: pgrx::pg_sys::Relation,
        meta: &'a mut MetaPageData,
        first_blkno: u32,
        skip_lock_rel: bool,
    ) -> Self {
        let direct_inode = page_read(relation, first_blkno);
        let flag = direct_inode.opaque.page_flag;
        let indirect1_blkno = direct_inode.opaque.next_blkno;
        drop(direct_inode);
        if indirect1_blkno == pgrx::pg_sys::InvalidBlockNumber {
            let direct_inode = page_write(relation, first_blkno);
            let inode_data = direct_inode.data();
            let data_page_id =
                u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
            let data_page = page_write(relation, data_page_id);
            return Self {
                relation,
                meta,
                flag,
                skip_lock_rel,
                first_blkno,
                state: VirtualPageWriterState::Direct([data_page, direct_inode]),
            };
        }

        let indirect1_inode = page_read(relation, indirect1_blkno);
        let indirect2_blkno = indirect1_inode.opaque.next_blkno;
        drop(indirect1_inode);
        if indirect2_blkno == pgrx::pg_sys::InvalidBlockNumber {
            let indirect1_inode = page_write(relation, indirect1_blkno);
            let inode_data = indirect1_inode.data();
            let indirect1_page_id =
                u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
            let indirect1_page = page_write(relation, indirect1_page_id);
            let inode_data = indirect1_page.data();
            let data_page_id =
                u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
            let data_page = page_write(relation, data_page_id);
            return Self {
                relation,
                meta,
                flag,
                skip_lock_rel,
                first_blkno,
                state: VirtualPageWriterState::Indirect1([
                    data_page,
                    indirect1_page,
                    indirect1_inode,
                ]),
            };
        }

        let indirect2_inode = page_write(relation, indirect2_blkno);
        let inode_data = indirect2_inode.data();
        let indirect2_page_id =
            u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
        let indirect2_page = page_write(relation, indirect2_page_id);
        let inode_data = indirect2_page.data();
        let indirect1_page_id =
            u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
        let indirect1_page = page_write(relation, indirect1_page_id);
        let inode_data = indirect1_page.data();
        let data_page_id =
            u32::from_le_bytes((&inode_data[inode_data.len() - 4..]).try_into().unwrap());
        let data_page = page_write(relation, data_page_id);
        Self {
            relation,
            meta,
            flag,
            skip_lock_rel,
            first_blkno,
            state: VirtualPageWriterState::Indirect2([
                data_page,
                indirect1_page,
                indirect2_page,
                indirect2_inode,
            ]),
        }
    }

    pub fn finalize(self) -> u32 {
        self.first_blkno
    }

    pub fn write(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let mut space = self.freespace_mut();
            if space.is_empty() {
                self.new_page();
                space = self.freespace_mut();
            }
            let space_len = space.len();
            let len = space_len.min(data.len());
            space[..len].copy_from_slice(&data[..len]);
            *self.offset() += len as u16;
            data = &data[len..];
        }
    }

    // it will make sure the data is on the same page
    pub fn write_no_cross(&mut self, data: &[u8]) {
        assert!(data.len() <= bm25_page_size());
        let mut space = self.freespace_mut();
        if space.len() < data.len() {
            self.new_page();
            space = self.freespace_mut();
        }
        space[..data.len()].copy_from_slice(data);
        *self.offset() += data.len() as u16;
    }

    fn offset(&mut self) -> &mut u16 {
        &mut self.data_page().header.pd_lower
    }

    fn freespace_mut(&mut self) -> &mut [u8] {
        match &mut self.state {
            VirtualPageWriterState::Direct([page, _]) => page.freespace_mut(),
            VirtualPageWriterState::Indirect1([page, _, _]) => page.freespace_mut(),
            VirtualPageWriterState::Indirect2([page, _, _, _]) => page.freespace_mut(),
        }
    }

    fn new_page(&mut self) {
        match &mut self.state {
            VirtualPageWriterState::Direct([old_data_page, direct_inode]) => {
                let data_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                old_data_page.opaque.next_blkno = data_page.blkno();
                let inode_space = direct_inode.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&data_page.blkno().to_le_bytes());
                    direct_inode.header.pd_lower += 4;
                    *old_data_page = data_page;
                    return;
                }

                let mut indirect1_inode = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                direct_inode.opaque.next_blkno = indirect1_inode.blkno();
                let mut indirect1_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                indirect1_inode.freespace_mut()[..4]
                    .copy_from_slice(&data_page.blkno().to_le_bytes());
                indirect1_inode.header.pd_lower += 4;
                indirect1_page.freespace_mut()[..4]
                    .copy_from_slice(&data_page.blkno().to_le_bytes());
                indirect1_page.header.pd_lower += 4;
                self.state =
                    VirtualPageWriterState::Indirect1([data_page, indirect1_page, indirect1_inode]);
            }
            VirtualPageWriterState::Indirect1([old_data_page, indirect1_page, indirect1_inode]) => {
                let data_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                old_data_page.opaque.next_blkno = data_page.blkno();
                let inode_space = indirect1_page.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&data_page.blkno().to_le_bytes());
                    indirect1_inode.header.pd_lower += 4;
                    *old_data_page = data_page;
                    return;
                }

                let mut new_indirect1_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                new_indirect1_page.freespace_mut()[..4]
                    .copy_from_slice(&data_page.blkno().to_le_bytes());
                new_indirect1_page.header.pd_lower += 4;
                let inode_space = indirect1_inode.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&new_indirect1_page.blkno().to_le_bytes());
                    indirect1_inode.header.pd_lower += 4;
                    *old_data_page = data_page;
                    *indirect1_page = new_indirect1_page;
                    return;
                }

                let mut indirect2_inode = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                indirect1_inode.opaque.next_blkno = indirect2_inode.blkno();
                let mut indirect2_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                indirect2_inode.freespace_mut()[..4]
                    .copy_from_slice(&indirect2_page.blkno().to_le_bytes());
                indirect2_inode.header.pd_lower += 4;
                indirect2_page.freespace_mut()[..4]
                    .copy_from_slice(&new_indirect1_page.blkno().to_le_bytes());
                indirect2_page.header.pd_lower += 4;
                self.state = VirtualPageWriterState::Indirect2([
                    data_page,
                    new_indirect1_page,
                    indirect2_page,
                    indirect2_inode,
                ]);
            }
            VirtualPageWriterState::Indirect2(
                [old_data_page, indirect1_page, indirect2_page, indirect2_inode],
            ) => {
                let data_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                old_data_page.opaque.next_blkno = data_page.blkno();
                let inode_space = indirect1_page.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&data_page.blkno().to_le_bytes());
                    indirect1_page.header.pd_lower += 4;
                    *old_data_page = data_page;
                    return;
                }

                let mut new_indirect1_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                new_indirect1_page.freespace_mut()[..4]
                    .copy_from_slice(&data_page.blkno().to_le_bytes());
                new_indirect1_page.header.pd_lower += 4;
                let inode_space = indirect2_page.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&new_indirect1_page.blkno().to_le_bytes());
                    indirect2_page.header.pd_lower += 4;
                    *old_data_page = data_page;
                    *indirect1_page = new_indirect1_page;
                    return;
                }

                let mut new_indirect2_page = page_alloc_from_free_list(
                    self.relation,
                    self.meta,
                    self.flag,
                    self.skip_lock_rel,
                );
                new_indirect2_page.freespace_mut()[..4]
                    .copy_from_slice(&new_indirect1_page.blkno().to_le_bytes());
                new_indirect2_page.header.pd_lower += 4;
                let inode_space = indirect2_inode.freespace_mut();
                if inode_space.len() >= 4 {
                    inode_space[..4].copy_from_slice(&new_indirect2_page.blkno().to_le_bytes());
                    indirect2_inode.header.pd_lower += 4;
                    *old_data_page = data_page;
                    *indirect1_page = new_indirect1_page;
                    *indirect2_page = new_indirect2_page;
                    return;
                }

                panic!("VirtualPageWriter: too many pages");
            }
        }
    }

    fn data_page(&mut self) -> &mut PageWriteGuard {
        match &mut self.state {
            VirtualPageWriterState::Direct(pages) => &mut pages[0],
            VirtualPageWriterState::Indirect1(pages) => &mut pages[0],
            VirtualPageWriterState::Indirect2(pages) => &mut pages[0],
        }
    }
}
