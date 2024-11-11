/// Term statistic segment is a **global** segment that stores the statistic for each terms.
/// Currently, it stores total count of documents which contains the term.
/// Used to calculate the inverse document frequency.
use crate::page::ContinuousPageReader;

pub struct TermStatReader(ContinuousPageReader<u32>);

impl TermStatReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(ContinuousPageReader::new(index, blkno))
    }

    pub fn read(&self, term_id: u32) -> u32 {
        self.0.read(term_id)
    }

    pub fn update(&self, term_id: u32, f: impl FnOnce(&mut u32)) {
        self.0.update(term_id, f)
    }
}
