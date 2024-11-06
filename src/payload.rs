use crate::page::VirtualPageReader;

pub struct PayloadWriter {
    pub buffer: Vec<u64>,
}

impl PayloadWriter {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn insert(&mut self, id: u64) {
        self.buffer.push(id);
    }

    pub fn data(&self) -> &[u8] {
        bytemuck::cast_slice(&self.buffer)
    }
}

pub struct PayloadReader(VirtualPageReader);

impl PayloadReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(VirtualPageReader::new(index, blkno))
    }

    pub fn read(&self, doc_id: u32) -> u64 {
        let mut buf = [0u8; 8];
        self.0.read_at(doc_id * 8, &mut buf);
        u64::from_le_bytes(buf)
    }
}
