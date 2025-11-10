//! Runtime statistics and inspector endpoint bindings.
use crate::runtime::inspector::InspectorMetadata;
use crate::runtime::stats::{RuntimeCallKind, RuntimeStatsSnapshot};
use pyo3::prelude::*;

#[pyclass(module = "_jsrun")]
pub struct RuntimeStats {
    #[pyo3(get)]
    heap_total_bytes: u64,
    #[pyo3(get)]
    heap_used_bytes: u64,
    #[pyo3(get)]
    external_memory_bytes: u64,
    #[pyo3(get)]
    physical_total_bytes: u64,
    #[pyo3(get)]
    total_execution_time_ms: u64,
    #[pyo3(get)]
    last_execution_time_ms: u64,
    #[pyo3(get)]
    last_execution_kind: Option<String>,
    #[pyo3(get)]
    eval_sync_count: u64,
    #[pyo3(get)]
    eval_async_count: u64,
    #[pyo3(get)]
    eval_module_sync_count: u64,
    #[pyo3(get)]
    eval_module_async_count: u64,
    #[pyo3(get)]
    call_function_async_count: u64,
    #[pyo3(get)]
    call_function_sync_count: u64,
    #[pyo3(get)]
    active_async_ops: u64,
    #[pyo3(get)]
    open_resources: u64,
    #[pyo3(get)]
    active_timers: u64,
    #[pyo3(get)]
    active_intervals: u64,
    #[pyo3(get)]
    active_js_streams: u64,
    #[pyo3(get)]
    active_py_streams: u64,
    #[pyo3(get)]
    total_js_streams: u64,
    #[pyo3(get)]
    total_py_streams: u64,
    #[pyo3(get)]
    bytes_streamed_js_to_py: u64,
    #[pyo3(get)]
    bytes_streamed_py_to_js: u64,
}

impl RuntimeStats {
    pub fn from_snapshot(snapshot: RuntimeStatsSnapshot) -> Self {
        RuntimeStats {
            heap_total_bytes: snapshot.heap_total_bytes,
            heap_used_bytes: snapshot.heap_used_bytes,
            external_memory_bytes: snapshot.external_memory_bytes,
            physical_total_bytes: snapshot.physical_total_bytes,
            total_execution_time_ms: snapshot.total_execution_time_ms,
            last_execution_time_ms: snapshot.last_execution_time_ms,
            last_execution_kind: snapshot
                .last_execution_kind
                .map(|kind: RuntimeCallKind| kind.as_str().to_string()),
            eval_sync_count: snapshot.eval_sync_count,
            eval_async_count: snapshot.eval_async_count,
            eval_module_sync_count: snapshot.eval_module_sync_count,
            eval_module_async_count: snapshot.eval_module_async_count,
            call_function_async_count: snapshot.call_function_async_count,
            call_function_sync_count: snapshot.call_function_sync_count,
            active_async_ops: snapshot.active_async_ops,
            open_resources: snapshot.open_resources,
            active_timers: snapshot.active_timers,
            active_intervals: snapshot.active_intervals,
            active_js_streams: snapshot.active_js_streams,
            active_py_streams: snapshot.active_py_streams,
            total_js_streams: snapshot.total_js_streams,
            total_py_streams: snapshot.total_py_streams,
            bytes_streamed_js_to_py: snapshot.bytes_streamed_js_to_py,
            bytes_streamed_py_to_js: snapshot.bytes_streamed_py_to_js,
        }
    }
}

#[pymethods]
impl RuntimeStats {
    fn __repr__(&self) -> String {
        format!(
            "RuntimeStats(heap_used_bytes={}, total_execution_time_ms={}, last_execution_kind={}, active_async_ops={}, open_resources={}, eval_sync_count={}, call_function_async_count={}, call_function_sync_count={}, active_js_streams={}, active_py_streams={})",
            self.heap_used_bytes,
            self.total_execution_time_ms,
            self.last_execution_kind
                .as_deref()
                .unwrap_or("None"),
            self.active_async_ops,
            self.open_resources,
            self.eval_sync_count,
            self.call_function_async_count,
            self.call_function_sync_count,
            self.active_js_streams,
            self.active_py_streams
        )
    }
}

#[pyclass(module = "jsrun")]
#[derive(Clone)]
pub struct InspectorEndpoints {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    websocket_url: String,
    #[pyo3(get)]
    devtools_frontend_url: String,
    #[pyo3(get)]
    title: String,
    #[pyo3(get)]
    description: String,
    #[pyo3(get)]
    target_url: String,
    #[pyo3(get)]
    favicon_url: String,
    #[pyo3(get)]
    host: String,
}

impl From<InspectorMetadata> for InspectorEndpoints {
    fn from(meta: InspectorMetadata) -> Self {
        Self {
            id: meta.id,
            websocket_url: meta.websocket_url,
            devtools_frontend_url: meta.devtools_frontend_url,
            title: meta.title,
            description: meta.description,
            target_url: meta.target_url,
            favicon_url: meta.favicon_url,
            host: meta.host,
        }
    }
}

#[pymethods]
impl InspectorEndpoints {
    fn __repr__(&self) -> String {
        format!(
            "InspectorEndpoints(id={}, websocket_url={}, devtools_frontend_url={})",
            self.id, self.websocket_url, self.devtools_frontend_url
        )
    }
}
