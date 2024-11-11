/// Payload segment is a global segment that stores the ctid of the documents.
/// doc_id -> ctid mapping
use crate::{
    page::{VirtualPageReader, VirtualPageWriter},
    segment::meta::MetaPageData,
};

pub struct DeleteBitmapReader(VirtualPageReader);

impl DeleteBitmapReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(VirtualPageReader::new(index, blkno))
    }

    pub fn is_delete(&self, doc_id: u32) -> bool {
        let mut buf = [0u8; 1];
        self.0.read_at(doc_id / 8, &mut buf);
        buf[0] & (1 << (doc_id % 8)) != 0
    }

    pub fn delete(&mut self, doc_id: u32) {
        self.0.update_at(doc_id / 8, 1, |byte| {
            byte[0] |= 1 << (doc_id % 8);
        });
    }
}

pub fn extend_delete_bit(
    index: pgrx::pg_sys::Relation,
    meta: &mut MetaPageData,
    blkno: pgrx::pg_sys::BlockNumber,
    doc_id: u32,
) {
    if doc_id % 8 == 0 {
        let mut writer = VirtualPageWriter::open(index, meta, blkno, true);
        writer.write(&[0]);
    }
}
