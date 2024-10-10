use crate::page::ContinuousPageReader;

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

pub struct PayloadReader(ContinuousPageReader<u64>);

impl PayloadReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(ContinuousPageReader::new(index, blkno))
    }

    pub fn read(&self, doc_id: u32) -> u64 {
        self.0.read(doc_id)
    }
}
