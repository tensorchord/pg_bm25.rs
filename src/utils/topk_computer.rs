

// Store 2*k elements in a buffer, and truncate to k elements when the buffer is full.
// Using variant of median selection from quicksort.
pub struct TopKComputer {
    buffer: Box<[(f32, u32)]>,
    len: usize,
    k: usize,
    threshold: f32,
}

impl TopKComputer {
    pub fn new(k: usize) -> Self {
        assert!(k > 0);
        Self {
            buffer: vec![(0.0, 0); k * 2].into_boxed_slice(),
            len: 0,
            k,
            threshold: f32::MIN,
        }
    }

    pub fn push(&mut self, score: f32, id: u32) {
        if score < self.threshold {
            return;
        }
        if self.buffer.len() == self.len {
            let median = self.truncate_top_k();
            self.threshold = median;
        }
        self.buffer[self.len] = (score, id);
        self.len += 1;
    }

    pub fn threshold(&self) -> f32 {
        self.threshold
    }

    // Return top-k elements in ascending order.
    pub fn to_sorted_slice(&mut self) -> &[(f32, u32)] {
        if self.len > self.k {
            self.truncate_top_k();
        }
        self.buffer[..self.len].sort_by(|a, b| a.0.total_cmp(&b.0));
        &self.buffer[..self.len]
    }

    fn truncate_top_k(&mut self) -> f32 {
        let (_, median, _) = self
            .buffer
            .select_nth_unstable_by(self.k, |a, b| a.0.total_cmp(&b.0).reverse());
        self.len = self.k;
        median.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use super::*;

    #[derive(PartialEq)]
    struct Cmp(f32, u32);
    impl std::fmt::Debug for Cmp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "({}, {})", self.0, self.1)
        }
    }
    impl Eq for Cmp {}
    impl PartialOrd for Cmp {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Cmp {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.0.total_cmp(&other.0).reverse()
        }
    }

    #[test]
    fn test_topk_computer() {
        let mut topk = TopKComputer::new(20);
        let mut reference = BinaryHeap::new();

        for _ in 0..100000 {
            let score = rand::random::<f32>();
            let id = rand::random::<u32>();
            topk.push(score, id);
            reference.push(Cmp(score, id));
            if reference.len() > 20 {
                reference.pop();
            }
        }

        let topk = topk.to_sorted_slice();
        let mut reference = reference.into_sorted_vec();
        reference.reverse();

        assert_eq!(topk.len(), reference.len());
        for (a, b) in topk.iter().zip(reference.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1);
        }
    }   
}
