#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub(crate) count: usize,
    pub(crate) min_ms: f64,
    pub(crate) mean_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p90_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) max_ms: f64,
}

impl LatencySummary {
    pub(crate) fn from_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        let mut sorted = samples.to_vec();
        sorted.sort_by(f64::total_cmp);
        let sum: f64 = sorted.iter().sum();
        Self {
            count: sorted.len(),
            min_ms: sorted[0],
            mean_ms: sum / sorted.len() as f64,
            p50_ms: percentile_nearest_rank(&sorted, 0.50),
            p90_ms: percentile_nearest_rank(&sorted, 0.90),
            p95_ms: percentile_nearest_rank(&sorted, 0.95),
            p99_ms: percentile_nearest_rank(&sorted, 0.99),
            max_ms: sorted[sorted.len() - 1],
        }
    }
}

fn percentile_nearest_rank(sorted: &[f64], percentile: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::LatencySummary;

    #[test]
    fn summarizes_empty_samples() {
        let summary = LatencySummary::from_samples(&[]);

        assert_eq!(summary.count, 0);
        assert_eq!(summary.p99_ms, 0.0);
    }

    #[test]
    fn summarizes_with_nearest_rank_percentiles() {
        let summary = LatencySummary::from_samples(&[4.0, 1.0, 3.0, 2.0]);

        assert_eq!(summary.count, 4);
        assert_eq!(summary.min_ms, 1.0);
        assert_eq!(summary.p50_ms, 2.0);
        assert_eq!(summary.p90_ms, 4.0);
        assert_eq!(summary.max_ms, 4.0);
    }
}
