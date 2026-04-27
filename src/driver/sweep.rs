#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::driver::paced::PacedTaskRunReport;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ThroughputSweepPolicy {
    pub(crate) start_ops_per_s: f64,
    pub(crate) step_multiplier: f64,
    pub(crate) max_ops_per_s: f64,
    pub(crate) max_points: usize,
    pub(crate) saturation_achieved_ratio: f64,
    pub(crate) stop_on_failure: bool,
}

impl ThroughputSweepPolicy {
    pub(crate) fn paper_default(start_ops_per_s: Option<f64>) -> Self {
        Self {
            start_ops_per_s: start_ops_per_s.unwrap_or(100.0),
            step_multiplier: 2.0,
            max_ops_per_s: 1_000_000.0,
            max_points: 12,
            saturation_achieved_ratio: 0.5,
            stop_on_failure: false,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.start_ops_per_s.is_finite() || self.start_ops_per_s <= 0.0 {
            return Err(
                "throughput sweep start_ops_per_s must be a finite positive number".to_string(),
            );
        }
        if !self.step_multiplier.is_finite() || self.step_multiplier <= 1.0 {
            return Err(
                "throughput sweep step_multiplier must be a finite number greater than 1"
                    .to_string(),
            );
        }
        if !self.max_ops_per_s.is_finite() || self.max_ops_per_s <= 0.0 {
            return Err(
                "throughput sweep max_ops_per_s must be a finite positive number".to_string(),
            );
        }
        if self.max_points == 0 {
            return Err("throughput sweep max_points must be greater than zero".to_string());
        }
        if !self.saturation_achieved_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.saturation_achieved_ratio)
            || self.saturation_achieved_ratio == 0.0
        {
            return Err(
                "throughput sweep saturation_achieved_ratio must be in the range (0, 1]"
                    .to_string(),
            );
        }
        Ok(())
    }

    pub(crate) fn decide_after(
        &self,
        completed_points: usize,
        report: &PacedTaskRunReport,
    ) -> SweepDecision {
        self.decide_after_metrics(
            completed_points,
            report.target_ops_per_s,
            report.successful_ops_per_s,
            report.failed_tasks,
        )
    }

    pub(crate) fn decide_after_metrics(
        &self,
        completed_points: usize,
        target_ops_per_s: f64,
        successful_ops_per_s: f64,
        failed_tasks: usize,
    ) -> SweepDecision {
        if self.stop_on_failure && failed_tasks > 0 {
            return SweepDecision::Stop {
                reason: SweepStopReason::FailedTasks,
            };
        }
        if target_ops_per_s <= 0.0
            || successful_ops_per_s / target_ops_per_s < self.saturation_achieved_ratio
        {
            return SweepDecision::Stop {
                reason: SweepStopReason::Saturated,
            };
        }
        if completed_points >= self.max_points {
            return SweepDecision::Stop {
                reason: SweepStopReason::MaxPoints,
            };
        }

        let next_ops_per_s = target_ops_per_s * self.step_multiplier;
        if next_ops_per_s > self.max_ops_per_s {
            return SweepDecision::Stop {
                reason: SweepStopReason::MaxOpsPerS,
            };
        }

        SweepDecision::Continue { next_ops_per_s }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SweepStopReason {
    Saturated,
    FailedTasks,
    MaxPoints,
    MaxOpsPerS,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SweepDecision {
    Continue { next_ops_per_s: f64 },
    Stop { reason: SweepStopReason },
}

#[cfg(test)]
mod tests {
    use crate::driver::latency::LatencySummary;
    use crate::driver::paced::PacedTaskRunReport;

    use super::{SweepDecision, SweepStopReason, ThroughputSweepPolicy};

    fn report(target: f64, achieved: f64, failed_tasks: usize) -> PacedTaskRunReport {
        report_with_success(target, achieved, achieved, failed_tasks)
    }

    fn report_with_success(
        target: f64,
        achieved: f64,
        successful: f64,
        failed_tasks: usize,
    ) -> PacedTaskRunReport {
        PacedTaskRunReport {
            target_ops_per_s: target,
            achieved_ops_per_s: achieved,
            successful_ops_per_s: successful,
            total_tasks: 10,
            completed_tasks: 10,
            failed_tasks,
            wall_time_ms: 10.0,
            offered_latency: LatencySummary::default(),
            service_latency: LatencySummary::default(),
            schedule_lag: LatencySummary::default(),
            samples: Vec::new(),
            failures: Vec::new(),
        }
    }

    #[test]
    fn continues_with_multiplier_before_saturation() {
        let policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        let decision = policy.decide_after(1, &report(100.0, 95.0, 0));

        assert_eq!(
            decision,
            SweepDecision::Continue {
                next_ops_per_s: 200.0
            }
        );
    }

    #[test]
    fn stops_when_achieved_ratio_is_low() {
        let policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        let decision = policy.decide_after(1, &report(100.0, 40.0, 0));

        assert_eq!(
            decision,
            SweepDecision::Stop {
                reason: SweepStopReason::Saturated
            }
        );
    }

    #[test]
    fn stops_when_successful_ratio_is_low_even_if_achieved_is_high() {
        let policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        let decision = policy.decide_after(1, &report_with_success(100.0, 90.0, 40.0, 2));

        assert_eq!(
            decision,
            SweepDecision::Stop {
                reason: SweepStopReason::Saturated
            }
        );
    }

    #[test]
    fn continues_above_half_target() {
        let policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        let decision = policy.decide_after(1, &report(100.0, 80.0, 0));

        assert_eq!(
            decision,
            SweepDecision::Continue {
                next_ops_per_s: 200.0
            }
        );
    }

    #[test]
    fn continues_on_failures_by_default() {
        let policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        let decision = policy.decide_after(1, &report(100.0, 100.0, 1));

        assert_eq!(
            decision,
            SweepDecision::Continue {
                next_ops_per_s: 200.0
            }
        );
    }

    #[test]
    fn stops_on_failures_when_enabled() {
        let mut policy = ThroughputSweepPolicy::paper_default(Some(100.0));
        policy.stop_on_failure = true;
        let decision = policy.decide_after(1, &report(100.0, 100.0, 1));

        assert_eq!(
            decision,
            SweepDecision::Stop {
                reason: SweepStopReason::FailedTasks
            }
        );
    }
}
