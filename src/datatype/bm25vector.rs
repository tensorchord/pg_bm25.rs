#[derive(Debug, Clone, Copy)]
pub struct Bm25VectorBorrowed<'a> {
    doc_len: u32,
    indexes: &'a [u32],
    values: &'a [u32],
}

impl<'a> Bm25VectorBorrowed<'a> {
    pub fn new_checked(doc_len: u32, indexes: &'a [u32], values: &'a [u32]) -> Option<Self> {
        if indexes.len() != values.len() {
            return None;
        }
        if indexes.len() > u32::MAX as usize {
            return None;
        }
        for i in 1..indexes.len() {
            if indexes[i] <= indexes[i - 1] {
                return None;
            }
        }
        if values.iter().map(|&v| v as usize).sum::<usize>() != doc_len as usize {
            return None;
        }
        Some(unsafe { Self::new_unchecked(doc_len, indexes, values) })
    }

    pub unsafe fn new_unchecked(doc_len: u32, indexes: &'a [u32], values: &'a [u32]) -> Self {
        Self {
            doc_len,
            indexes,
            values,
        }
    }

    pub fn len(&self) -> u32 {
        self.indexes.len() as u32
    }

    pub fn doc_len(&self) -> u32 {
        self.doc_len
    }

    pub fn indexes(&self) -> &[u32] {
        self.indexes
    }

    pub fn values(&self) -> &[u32] {
        self.values
    }
}
