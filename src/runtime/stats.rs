use std::time::Duration;

use deno_core::stats::{RuntimeActivity, RuntimeActivitySnapshot};

/// Identifies the JavaScript entry point that was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCallKind {
    EvalSync,
    EvalAsync,
    EvalModuleSync,
    EvalModuleAsync,
    CallFunctionAsync,
    CallFunctionSync,
}

impl RuntimeCallKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeCallKind::EvalSync => "eval_sync",
            RuntimeCallKind::EvalAsync => "eval_async",
            RuntimeCallKind::EvalModuleSync => "eval_module_sync",
            RuntimeCallKind::EvalModuleAsync => "eval_module_async",
            RuntimeCallKind::CallFunctionAsync => "call_function_async",
            RuntimeCallKind::CallFunctionSync => "call_function_sync",
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RuntimeExecutionCounters {
    pub eval_sync_count: u64,
    pub eval_async_count: u64,
    pub eval_module_sync_count: u64,
    pub eval_module_async_count: u64,
    pub call_function_async_count: u64,
    pub call_function_sync_count: u64,
}

impl RuntimeExecutionCounters {
    pub fn increment(&mut self, kind: RuntimeCallKind) {
        match kind {
            RuntimeCallKind::EvalSync => {
                self.eval_sync_count = self.eval_sync_count.saturating_add(1)
            }
            RuntimeCallKind::EvalAsync => {
                self.eval_async_count = self.eval_async_count.saturating_add(1)
            }
            RuntimeCallKind::EvalModuleSync => {
                self.eval_module_sync_count = self.eval_module_sync_count.saturating_add(1)
            }
            RuntimeCallKind::EvalModuleAsync => {
                self.eval_module_async_count = self.eval_module_async_count.saturating_add(1)
            }
            RuntimeCallKind::CallFunctionAsync => {
                self.call_function_async_count = self.call_function_async_count.saturating_add(1)
            }
            RuntimeCallKind::CallFunctionSync => {
                self.call_function_sync_count = self.call_function_sync_count.saturating_add(1)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionSnapshot {
    pub total_execution: Duration,
    pub last_execution: Option<Duration>,
    pub last_call_kind: Option<RuntimeCallKind>,
    pub counters: RuntimeExecutionCounters,
}

#[derive(Debug, Default)]
pub struct RuntimeStatsState {
    total_execution: Duration,
    last_execution: Option<Duration>,
    last_call_kind: Option<RuntimeCallKind>,
    counters: RuntimeExecutionCounters,
}

impl RuntimeStatsState {
    pub fn record(&mut self, kind: RuntimeCallKind, elapsed: Duration) {
        self.total_execution = self.total_execution.saturating_add(elapsed);
        self.last_execution = Some(elapsed);
        self.last_call_kind = Some(kind);
        self.counters.increment(kind);
    }

    pub fn snapshot(&self) -> ExecutionSnapshot {
        ExecutionSnapshot {
            total_execution: self.total_execution,
            last_execution: self.last_execution,
            last_call_kind: self.last_call_kind,
            counters: self.counters.clone(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct HeapSnapshot {
    pub heap_total_bytes: u64,
    pub heap_used_bytes: u64,
    pub external_memory_bytes: u64,
    pub physical_total_bytes: u64,
}

#[derive(Debug, Default, Clone)]
pub struct ActivitySummary {
    pub active_async_ops: u64,
    pub open_resources: u64,
    pub active_timers: u64,
    pub active_intervals: u64,
}

impl ActivitySummary {
    pub fn from_snapshot(snapshot: RuntimeActivitySnapshot) -> Self {
        let mut summary = ActivitySummary::default();
        for activity in snapshot.active {
            match activity {
                RuntimeActivity::AsyncOp(_, _, _) => {
                    summary.active_async_ops = summary.active_async_ops.saturating_add(1);
                }
                RuntimeActivity::Resource(_, _, _) => {
                    summary.open_resources = summary.open_resources.saturating_add(1);
                }
                RuntimeActivity::Timer(_, _) => {
                    summary.active_timers = summary.active_timers.saturating_add(1);
                }
                RuntimeActivity::Interval(_, _) => {
                    summary.active_intervals = summary.active_intervals.saturating_add(1);
                }
            }
        }
        summary
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeStatsSnapshot {
    pub heap_total_bytes: u64,
    pub heap_used_bytes: u64,
    pub external_memory_bytes: u64,
    pub physical_total_bytes: u64,
    pub total_execution_time_ms: u64,
    pub last_execution_time_ms: u64,
    pub last_execution_kind: Option<RuntimeCallKind>,
    pub eval_sync_count: u64,
    pub eval_async_count: u64,
    pub eval_module_sync_count: u64,
    pub eval_module_async_count: u64,
    pub call_function_async_count: u64,
    pub call_function_sync_count: u64,
    pub active_async_ops: u64,
    pub open_resources: u64,
    pub active_timers: u64,
    pub active_intervals: u64,
}

impl RuntimeStatsSnapshot {
    pub fn new(
        heap: HeapSnapshot,
        execution: ExecutionSnapshot,
        activity: ActivitySummary,
    ) -> Self {
        let total_execution_time_ms = duration_to_u64_ms(execution.total_execution);
        let last_execution_time_ms = execution
            .last_execution
            .map(duration_to_u64_ms)
            .unwrap_or(0);

        RuntimeStatsSnapshot {
            heap_total_bytes: heap.heap_total_bytes,
            heap_used_bytes: heap.heap_used_bytes,
            external_memory_bytes: heap.external_memory_bytes,
            physical_total_bytes: heap.physical_total_bytes,
            total_execution_time_ms,
            last_execution_time_ms,
            last_execution_kind: execution.last_call_kind,
            eval_sync_count: execution.counters.eval_sync_count,
            eval_async_count: execution.counters.eval_async_count,
            eval_module_sync_count: execution.counters.eval_module_sync_count,
            eval_module_async_count: execution.counters.eval_module_async_count,
            call_function_async_count: execution.counters.call_function_async_count,
            call_function_sync_count: execution.counters.call_function_sync_count,
            active_async_ops: activity.active_async_ops,
            open_resources: activity.open_resources,
            active_timers: activity.active_timers,
            active_intervals: activity.active_intervals,
        }
    }
}

/// Convert a duration to milliseconds by flooring sub-millisecond values to zero.
fn duration_to_u64_ms(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_stats_state_record_and_snapshot() {
        let mut state = RuntimeStatsState::default();

        state.record(RuntimeCallKind::EvalSync, Duration::from_millis(10));
        state.record(RuntimeCallKind::EvalAsync, Duration::from_millis(25));

        let snapshot = state.snapshot();
        assert_eq!(snapshot.total_execution, Duration::from_millis(35));
        assert_eq!(snapshot.last_call_kind, Some(RuntimeCallKind::EvalAsync));
        assert_eq!(snapshot.last_execution, Some(Duration::from_millis(25)));
        assert_eq!(snapshot.counters.eval_sync_count, 1);
        assert_eq!(snapshot.counters.eval_async_count, 1);
    }
}
