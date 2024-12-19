// Antonio Mallia, Giuseppe Ottaviano, Elia Porciani, Nicola Tonellotto, and Rossano Venturini.
// 2017. Faster BlockMax WAND with Variable-sized Blocks. In Proc. SIGIR

// reference: https://github.com/pisa-engine/pisa/blob/v0.10.0/include/pisa/score_opt_partition.hpp

use std::collections::VecDeque;

use super::BlockPartitionTrait;

pub struct VariableBlockPartition {
    lambda: f32,
    eps1: f32,
    eps2: f32,
    scores: Vec<f32>,
    min_cost: Vec<f32>,
    path: Vec<u32>,
    score_window: Vec<ScoreWindow>,
    partitions: Vec<u32>,
    max_doc: Vec<u32>,
}

impl VariableBlockPartition {
    pub fn new(lambda: f32, eps1: f32, eps2: f32) -> Self {
        Self {
            lambda,
            eps1,
            eps2,
            scores: Vec::new(),
            min_cost: Vec::new(),
            path: Vec::new(),
            score_window: Vec::new(),
            partitions: Vec::new(),
            max_doc: Vec::new(),
        }
    }
}
impl BlockPartitionTrait for VariableBlockPartition {
    fn partitions(&self) -> &[u32] {
        &self.partitions
    }

    fn max_doc(&self) -> &[u32] {
        &self.max_doc
    }

    fn add_doc(&mut self, score: f32) {
        self.scores.push(score);
    }

    fn reset(&mut self) {
        self.scores.clear();
        self.min_cost.clear();
        self.path.clear();
        self.score_window.clear();
        self.partitions.clear();
        self.max_doc.clear();
    }

    fn make_partitions(&mut self) {
        let doc_cnt = self.scores.len();
        let max_score = self.scores.iter().cloned().fold(f32::MIN, f32::max);
        let sum_score: f32 = self.scores.iter().sum();
        let max_block_cost = doc_cnt as f32 * max_score - sum_score;
        self.min_cost.resize(doc_cnt + 1, max_block_cost);
        self.min_cost[0] = 0.;

        let mut cost_bound = self.lambda;
        while self.eps1 == 0. || cost_bound < self.lambda / self.eps1 {
            self.score_window.push(Default::default());
            if cost_bound >= max_block_cost {
                break;
            }
            cost_bound *= 1. + self.eps2;
        }

        self.path.resize(doc_cnt + 1, 0);
        for i in 0..doc_cnt {
            let i = i as u32;
            let mut last_end = i + 1;
            for window in self.score_window.iter_mut() {
                assert!(window.start == i);
                while window.end < last_end {
                    window.advance_end(&self.scores);
                }

                loop {
                    let window_cost = window.cost(self.lambda);
                    if self.min_cost[i as usize] + window_cost < self.min_cost[window.end as usize]
                    {
                        self.min_cost[window.end as usize] =
                            self.min_cost[i as usize] + window_cost;
                        self.path[window.end as usize] = window.start;
                    }
                    last_end = window.end;
                    if window.end == doc_cnt as u32 {
                        break;
                    }
                    if window_cost >= window.cost_upper_bound {
                        break;
                    }
                    window.advance_end(&self.scores);
                }

                window.advance_start(&self.scores);
            }
        }

        let mut pos = doc_cnt as u32;
        while pos != 0 {
            self.partitions.push(pos - 1);
            pos = self.path[pos as usize];
        }
        self.partitions.reverse();
        for (&start, &end) in
            (std::iter::once(&0).chain(self.partitions.iter())).zip(self.partitions.iter())
        {
            let max_doc: u32 = self.scores[start as usize..(end + 1) as usize]
                .iter()
                .cloned()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
                .unwrap()
                .0
                .try_into()
                .unwrap();
            self.max_doc.push(max_doc + start);
        }
    }
}

#[derive(Default)]
struct ScoreWindow {
    start: u32,
    end: u32,
    cost_upper_bound: f32,
    sum: f32,
    max_queue: VecDeque<f32>,
}

impl ScoreWindow {
    fn advance_start(&mut self, scores: &[f32]) {
        let score = scores[self.start as usize];
        self.sum -= score;
        if self.max_queue.front() == Some(&score) {
            self.max_queue.pop_front();
        }
        self.start += 1;
    }

    fn advance_end(&mut self, scores: &[f32]) {
        let score = scores[self.end as usize];
        self.sum += score;
        while !self.max_queue.is_empty() && self.max_queue.back().unwrap() < &score {
            self.max_queue.pop_back();
        }
        self.max_queue.push_back(score);
        self.end += 1;
    }

    fn cost(&self, fixed_cost: f32) -> f32 {
        (self.end - self.start) as f32 * self.max_queue.front().unwrap() - self.sum + fixed_cost
    }
}
