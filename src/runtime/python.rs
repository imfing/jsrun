//! Python bindings exposing the runtime to Python callers.

use super::config::RuntimeConfig;
use super::conversion::{js_value_to_python, python_to_js_value};
use super::error::{JsExceptionDetails, RuntimeError, RuntimeResult};
use super::handle::RuntimeHandle;
use super::inspector::InspectorMetadata;
use super::js_value::{JSValue, SerializationLimits};
use super::ops::PythonOpMode;
use super::runner::{self, FunctionCallResult};
use super::snapshot::{SnapshotBuilder, SnapshotBuilderConfig};
use super::stats::{RuntimeCallKind, RuntimeStatsSnapshot};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::BoundObject;
use pyo3_async_runtimes::{tokio as pyo3_tokio, TaskLocals};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;

#[pyclass(unsendable, weakref)]
pub struct Runtime {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
}

static JS_UNDEFINED_SINGLETON: OnceLock<Py<JsUndefined>> = OnceLock::new();

create_exception!(crate::runtime::python, JavaScriptError, PyException);
create_exception!(crate::runtime::python, RuntimeTerminated, PyRuntimeError);

#[pyclass(name = "SnapshotBuilder", module = "jsrun", unsendable)]
pub struct SnapshotBuilderPy {
    builder: std::cell::RefCell<Option<SnapshotBuilder>>,
}

#[pyfunction]
pub fn _debug_active_runtime_threads() -> usize {
    runner::active_runtime_threads()
}

fn set_optional_attr(py: Python<'_>, value: &Bound<'_, PyAny>, name: &str, attr: Option<String>) {
    match attr {
        Some(val) => {
            let _ = value.setattr(name, val);
        }
        None => {
            let _ = value.setattr(name, py.None());
        }
    }
}

fn build_js_exception(py: Python<'_>, details: JsExceptionDetails, context: Option<&str>) -> PyErr {
    let summary = match context {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}: {}", details.summary()),
        _ => details.summary(),
    };
    let py_err = PyErr::new::<JavaScriptError, _>(summary);
    let value = py_err.value(py);

    set_optional_attr(py, value, "name", details.name.clone());
    set_optional_attr(py, value, "message", details.message.clone());
    set_optional_attr(py, value, "stack", details.stack.clone());

    let frames_list = PyList::empty(py);
    for frame in &details.frames {
        let frame_dict = PyDict::new(py);
        if let Some(function_name) = &frame.function_name {
            let _ = frame_dict.set_item("function_name", function_name);
        }
        if let Some(file_name) = &frame.file_name {
            let _ = frame_dict.set_item("file_name", file_name);
        }
        if let Some(line_number) = frame.line_number {
            let _ = frame_dict.set_item("line_number", line_number);
        }
        if let Some(column_number) = frame.column_number {
            let _ = frame_dict.set_item("column_number", column_number);
        }
        let _ = frames_list.append(frame_dict);
    }
    let _ = value.setattr("frames", frames_list);

    py_err
}

fn runtime_error_to_py_with(py: Python<'_>, err: RuntimeError, context: Option<&str>) -> PyErr {
    match err {
        RuntimeError::JavaScript(details) => build_js_exception(py, details, context),
        RuntimeError::Timeout { context: msg } => {
            let message = match context {
                Some(prefix) => format!("{prefix}: {msg}"),
                None => msg,
            };
            PyRuntimeError::new_err(message)
        }
        RuntimeError::Internal { context: msg } => {
            let message = match context {
                Some(prefix) => format!("{prefix}: {msg}"),
                None => msg,
            };
            PyRuntimeError::new_err(message)
        }
        RuntimeError::Terminated => {
            let message = match context {
                Some(prefix) if !prefix.is_empty() => format!("{prefix}: Runtime terminated"),
                _ => "Runtime terminated".to_string(),
            };
            PyErr::new::<RuntimeTerminated, _>(message)
        }
    }
}

pub(crate) fn runtime_error_to_py(err: RuntimeError) -> PyErr {
    Python::attach(|py| runtime_error_to_py_with(py, err, None))
}

pub(crate) fn runtime_error_with_context(context: &str, err: RuntimeError) -> PyErr {
    Python::attach(|py| runtime_error_to_py_with(py, err, Some(context)))
}

/// Normalize a Python timeout value to milliseconds.
///
/// Accepts:
/// - `None` -> `None`
/// - `float` or `int` (seconds) -> `Some(u64)` milliseconds
/// - `datetime.timedelta` -> `Some(u64)` milliseconds
///
/// Rejects:
/// - Negative values
/// - Zero values
/// - Invalid types
///
/// Returns milliseconds as `Option<u64>` for internal use.
fn validate_timeout_seconds(seconds: f64) -> PyResult<()> {
    if !seconds.is_finite() {
        return Err(PyValueError::new_err("Timeout must be finite"));
    }
    if seconds < 0.0 {
        return Err(PyValueError::new_err("Timeout cannot be negative"));
    }
    if seconds == 0.0 {
        return Err(PyValueError::new_err("Timeout cannot be zero"));
    }
    if seconds > u64::MAX as f64 {
        return Err(PyValueError::new_err("Timeout is too large"));
    }
    Ok(())
}

fn normalize_timeout_to_ms<'py>(timeout: Option<&Bound<'py, PyAny>>) -> PyResult<Option<u64>> {
    let Some(timeout_value) = timeout else {
        return Ok(None);
    };

    let duration = if let Ok(seconds) = timeout_value.extract::<f64>() {
        validate_timeout_seconds(seconds)?;
        Duration::from_secs_f64(seconds)
    } else if let Ok(seconds) = timeout_value.extract::<u64>() {
        if seconds == 0 {
            return Err(PyValueError::new_err("Timeout cannot be zero"));
        }
        Duration::from_secs(seconds)
    } else if let Ok(seconds) = timeout_value.extract::<i64>() {
        if seconds < 0 {
            return Err(PyValueError::new_err("Timeout cannot be negative"));
        }
        if seconds == 0 {
            return Err(PyValueError::new_err("Timeout cannot be zero"));
        }
        Duration::from_secs(seconds as u64)
    } else {
        // Try to extract as timedelta
        let py = timeout_value.py();
        let timedelta = py.import("datetime")?.getattr("timedelta")?;
        if timeout_value.is_instance(&timedelta)? {
            let total_seconds: f64 = timeout_value.getattr("total_seconds")?.call0()?.extract()?;
            validate_timeout_seconds(total_seconds)?;
            Duration::from_secs_f64(total_seconds)
        } else {
            return Err(PyValueError::new_err(
                "Timeout must be a number (seconds), datetime.timedelta, or None",
            ));
        }
    };

    // Convert to milliseconds, handling overflow
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        Ok(Some(u64::MAX))
    } else {
        Ok(Some(millis as u64))
    }
}

/// Return true if the supplied Python future has been cancelled.
fn python_future_cancelled(future: &Bound<'_, PyAny>) -> PyResult<bool> {
    future
        .getattr(pyo3::intern!(future.py(), "cancelled"))?
        .call0()?
        .is_truthy()
}

#[pymethods]
impl SnapshotBuilderPy {
    #[new]
    #[pyo3(signature = (
        bootstrap = None,
        enable_console = Some(true),
    ))]
    fn new(bootstrap: Option<String>, enable_console: Option<bool>) -> PyResult<Self> {
        let config = SnapshotBuilderConfig {
            bootstrap_script: bootstrap,
            enable_console,
        };
        let builder = SnapshotBuilder::new(config).map_err(runtime_error_to_py)?;
        Ok(Self {
            builder: std::cell::RefCell::new(Some(builder)),
        })
    }

    fn execute_script(&self, name: &str, source: &str) -> PyResult<()> {
        let mut guard = self.builder.borrow_mut();
        let builder = guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Snapshot already built"))?;
        builder
            .execute_script(name, source)
            .map_err(runtime_error_to_py)?;
        Ok(())
    }

    fn build(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let builder = self
            .builder
            .borrow_mut()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Snapshot already built"))?;
        let bytes = builder.build().map_err(runtime_error_to_py)?;
        Ok(PyBytes::new(py, &bytes).into())
    }
}

#[pyclass]
/// Callback object that relays Python-side cancellation back to the Rust task.
struct JsAsyncCancelCallback {
    cancel_tx: Option<oneshot::Sender<()>>,
}

#[pymethods]
impl JsAsyncCancelCallback {
    /// Forward cancellation notifications from Python to the waiting Rust future.
    fn __call__(&mut self, future: &Bound<PyAny>) -> PyResult<()> {
        if python_future_cancelled(future)? {
            if let Some(tx) = self.cancel_tx.take() {
                let _ = tx.send(());
            }
        }
        Ok(())
    }
}

/// Result pending delivery back to Python once we hop onto the event loop.
enum JsAsyncOutcome {
    Value(JSValue, RuntimeHandle),
    Error(RuntimeError),
}

#[pyclass]
/// Helper that runs on the Python event loop to complete the awaiting future.
struct JsAsyncResultSetter {
    future: Py<PyAny>,
    outcome: Option<JsAsyncOutcome>,
    error_context: String,
}

impl JsAsyncResultSetter {
    /// Store the pending outcome and metadata until the loop thread executes the setter.
    fn new(future: Py<PyAny>, outcome: JsAsyncOutcome, error_context: String) -> Self {
        Self {
            future,
            outcome: Some(outcome),
            error_context,
        }
    }
}

#[pymethods]
impl JsAsyncResultSetter {
    /// Execute the deferred conversion and resolve the Python `asyncio.Future`.
    fn __call__(&mut self, py: Python<'_>) -> PyResult<()> {
        let future = self.future.bind(py);

        if future
            .getattr(pyo3::intern!(py, "done"))?
            .call0()?
            .is_truthy()?
        {
            return Ok(());
        }

        if python_future_cancelled(future)? {
            return Ok(());
        }

        let outcome = self
            .outcome
            .take()
            .expect("JsAsyncResultSetter invoked more than once");

        match outcome {
            JsAsyncOutcome::Value(value, handle) => {
                let py_value = js_value_to_python(py, &value, Some(&handle))?;
                future.call_method1(pyo3::intern!(py, "set_result"), (py_value.into_bound(py),))?;
            }
            JsAsyncOutcome::Error(err) => {
                let exception = runtime_error_with_context(&self.error_context, err);
                let exception_value = exception.into_value(py);
                future.call_method1(pyo3::intern!(py, "set_exception"), (exception_value,))?;
            }
        }

        Ok(())
    }
}

/// Queue the conversion closure onto Python's event loop for execution.
fn schedule_js_future_result(
    py: Python<'_>,
    locals: &TaskLocals,
    future: &Py<PyAny>,
    result: RuntimeResult<JSValue>,
    handle: RuntimeHandle,
    error_context: &str,
) -> PyResult<()> {
    let event_loop = locals.event_loop(py);
    let context = locals.context(py);

    let outcome = match result {
        Ok(value) => JsAsyncOutcome::Value(value, handle),
        Err(err) => JsAsyncOutcome::Error(err),
    };

    let setter = Py::new(
        py,
        JsAsyncResultSetter::new(future.clone_ref(py), outcome, error_context.to_string()),
    )?;
    let setter_bound = setter.into_bound(py);
    let kwargs = PyDict::new(py);
    kwargs.set_item(pyo3::intern!(py, "context"), context)?;

    event_loop.call_method(
        pyo3::intern!(py, "call_soon_threadsafe"),
        (setter_bound,),
        Some(&kwargs),
    )?;
    Ok(())
}

/// Immediately propagate a PyErr to the awaiting future if scheduling cannot be completed.
fn set_future_exception_immediate(py: Python<'_>, future: &Py<PyAny>, err: PyErr) -> PyResult<()> {
    let future_bound = future.clone_ref(py).into_bound(py);
    if future_bound
        .getattr(pyo3::intern!(py, "done"))?
        .call0()?
        .is_truthy()?
    {
        err.restore(py);
        return Ok(());
    }

    let exception_value = err.into_value(py);
    future_bound.call_method1(pyo3::intern!(py, "set_exception"), (exception_value,))?;
    Ok(())
}

/// Convert a Tokio future returning `JSValue` into a Python awaitable resolved on the loop thread.
fn bridge_js_future<'py, Fut>(
    py: Python<'py>,
    locals: TaskLocals,
    future: Fut,
    handle: RuntimeHandle,
    error_context: &'static str,
) -> PyResult<Bound<'py, PyAny>>
where
    Fut: Future<Output = RuntimeResult<JSValue>> + Send + 'static,
{
    let event_loop = locals.event_loop(py);
    let python_future = event_loop.call_method0(pyo3::intern!(py, "create_future"))?;

    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let cancel_callback = Py::new(
        py,
        JsAsyncCancelCallback {
            cancel_tx: Some(cancel_tx),
        },
    )?;
    python_future.call_method1(pyo3::intern!(py, "add_done_callback"), (cancel_callback,))?;

    let py_future_obj: Py<PyAny> = python_future.unbind();
    let ret_future = py_future_obj.clone_ref(py).into_bound(py);

    let locals_for_scope = locals.clone();
    let locals_for_schedule = locals.clone();
    let py_future_for_schedule = py_future_obj.clone_ref(py);
    let handle_for_schedule = handle.clone();
    let error_context_owned = error_context.to_string();

    pyo3_tokio::get_runtime().spawn(async move {
        let scoped_future = pyo3_tokio::scope(locals_for_scope.clone(), future);
        tokio::pin!(scoped_future);

        let outcome = tokio::select! {
            res = &mut scoped_future => Some(res),
            _ = &mut cancel_rx => None,
        };

        if let Some(result) = outcome {
            Python::attach(|py| {
                if let Err(err) = schedule_js_future_result(
                    py,
                    &locals_for_schedule,
                    &py_future_for_schedule,
                    result,
                    handle_for_schedule.clone(),
                    &error_context_owned,
                ) {
                    if let Err(set_err) =
                        set_future_exception_immediate(py, &py_future_for_schedule, err)
                    {
                        log::error!(
                            "Failed to propagate async error to Python future: {}",
                            set_err
                        );
                    }
                }
            });
        }
    });

    Ok(ret_future)
}

#[pyclass(module = "_jsrun")]
pub struct JsUndefined;

#[pymethods]
impl JsUndefined {
    #[new]
    fn __new__() -> PyResult<Self> {
        Err(PyRuntimeError::new_err(
            "JsUndefined is a singleton; use jsrun.undefined",
        ))
    }

    fn __repr__(&self) -> &'static str {
        "JsUndefined"
    }

    fn __str__(&self) -> &'static str {
        "undefined"
    }

    fn __bool__(&self) -> bool {
        false
    }
}

pub(crate) fn get_js_undefined(py: Python<'_>) -> PyResult<Py<JsUndefined>> {
    if let Some(existing) = JS_UNDEFINED_SINGLETON.get() {
        Ok(existing.clone_ref(py))
    } else {
        let value = Py::new(py, JsUndefined)?;
        let stored = value.clone_ref(py);
        let _ = JS_UNDEFINED_SINGLETON.set(stored);
        Ok(value)
    }
}

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
    fn from_snapshot(snapshot: RuntimeStatsSnapshot) -> Self {
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

impl Runtime {
    fn init_with_config(config: RuntimeConfig) -> PyResult<Self> {
        let handle = RuntimeHandle::spawn(config)
            .map_err(|err| runtime_error_with_context("Failed to spawn runtime", err))?;
        Ok(Self {
            handle: std::cell::RefCell::new(Some(handle)),
        })
    }

    fn detect_async(py: Python<'_>, handler: &Py<PyAny>) -> PyResult<bool> {
        let inspect = py.import("inspect")?;
        let mut is_async: bool = inspect
            .call_method1("iscoroutinefunction", (handler.bind(py),))?
            .extract()?;
        if !is_async {
            let handler_bound = handler.bind(py);
            if handler_bound.hasattr("__call__")? {
                let call_attr = handler_bound.getattr("__call__")?;
                is_async = inspect
                    .call_method1("iscoroutinefunction", (call_attr,))?
                    .extract()?;
            }
        }
        Ok(is_async)
    }

    fn checked_mode(py: Python<'_>, mode: &str, handler: &Py<PyAny>) -> PyResult<PythonOpMode> {
        let is_async = Self::detect_async(py, handler)?;
        match mode {
            "sync" if is_async => Err(PyRuntimeError::new_err(
                "Handler is async but mode='sync'; use mode='async'",
            )),
            "async" if !is_async => Err(PyRuntimeError::new_err(
                "Handler is sync but mode='async'; use mode='sync'",
            )),
            "sync" => Ok(PythonOpMode::Sync),
            "async" => Ok(PythonOpMode::Async),
            other => Err(PyRuntimeError::new_err(format!(
                "Invalid mode '{}', expected 'sync' or 'async'",
                other
            ))),
        }
    }
}

fn js_value_to_js_expression(value: &JSValue) -> PyResult<String> {
    match value {
        JSValue::Undefined => Ok("undefined".to_string()),
        JSValue::Null => Ok("null".to_string()),
        JSValue::Bool(b) => Ok(b.to_string()),
        JSValue::Int(i) => Ok(i.to_string()),
        JSValue::BigInt(bigint) => Ok(format!("BigInt(\"{}\")", bigint.to_str_radix(10))),
        JSValue::Float(f) => {
            if f.is_nan() {
                Ok("Number.NaN".to_string())
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    Ok("Number.POSITIVE_INFINITY".to_string())
                } else {
                    Ok("Number.NEGATIVE_INFINITY".to_string())
                }
            } else {
                Ok(f.to_string())
            }
        }
        JSValue::String(s) => serde_json::to_string(s).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to serialize string literal: {e}"))
        }),
        JSValue::Bytes(bytes) => serde_json::to_string(bytes)
            .map(|data| format!("new Uint8Array({data})"))
            .map_err(|e| {
                PyRuntimeError::new_err(format!("Failed to serialize bytes literal: {e}"))
            }),
        JSValue::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(js_value_to_js_expression(item)?);
            }
            Ok(format!("[{}]", parts.join(", ")))
        }
        JSValue::Set(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(js_value_to_js_expression(item)?);
            }
            Ok(format!("new Set([{}])", parts.join(", ")))
        }
        JSValue::Object(map) => {
            let mut parts = Vec::with_capacity(map.len());
            for (key, val) in map.iter() {
                let key_literal = serde_json::to_string(key).map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to serialize object key '{key}': {e}"))
                })?;
                let expr = js_value_to_js_expression(val)?;
                parts.push(format!("{key_literal}: {expr}"));
            }
            Ok(format!("{{{}}}", parts.join(", ")))
        }
        JSValue::Date(epoch_ms) => Ok(format!("new Date({epoch_ms})")),
        JSValue::Function { .. } => Err(PyRuntimeError::new_err(
            "Cannot serialize function values; use callables directly",
        )),
        JSValue::JsStream { id } => Ok(format!("/* JsStream({id}) */ undefined")),
        JSValue::PyStream { id } => Ok(format!("/* PyStream({id}) */ undefined")),
    }
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (config = None))]
    fn py_new(config: Option<&RuntimeConfig>) -> PyResult<Self> {
        let runtime_config = match config {
            Some(config_py) => config_py.clone(),
            None => RuntimeConfig::default(),
        };
        Self::init_with_config(runtime_config)
    }

    #[pyo3(signature = (code, /, *, timeout=None))]
    fn eval_async<'py>(
        &self,
        py: Python<'py>,
        code: String,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        let timeout_ms = normalize_timeout_to_ms(timeout)?;

        let task_locals = pyo3_tokio::get_current_locals(py)?;
        let handle_for_conversion = handle.clone();
        let eval_task_locals = Some(task_locals.clone());

        let future = async move { handle.eval_async(&code, timeout_ms, eval_task_locals).await };

        bridge_js_future(
            py,
            task_locals,
            future,
            handle_for_conversion,
            "Evaluation failed",
        )
    }

    fn eval(&self, py: Python<'_>, code: &str) -> PyResult<Py<PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();
        let code_owned = code.to_owned();
        let js_value = py
            .detach(|| handle.eval_sync(&code_owned))
            .map_err(|e| runtime_error_with_context("Evaluation failed", e))?;
        js_value_to_python(py, &js_value, Some(&handle))
    }

    fn is_closed(&self) -> bool {
        self.handle
            .borrow()
            .as_ref()
            .map(|handle| handle.is_shutdown())
            .unwrap_or(true)
    }

    fn get_stats(&self, py: Python<'_>) -> PyResult<RuntimeStats> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();
        let snapshot = py
            .detach(|| handle.get_stats())
            .map_err(|e| runtime_error_with_context("Failed to obtain runtime stats", e))?;
        Ok(RuntimeStats::from_snapshot(snapshot))
    }

    fn inspector_endpoints(&self) -> PyResult<Option<InspectorEndpoints>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        Ok(handle.inspector_metadata().map(InspectorEndpoints::from))
    }

    fn _debug_tracked_function_count(&self) -> PyResult<usize> {
        let handle = self.handle.borrow();
        Ok(handle
            .as_ref()
            .map(|handle| handle.tracked_function_count())
            .unwrap_or(0))
    }

    fn close(&self) -> PyResult<()> {
        let mut handle = self.handle.borrow_mut();
        if let Some(mut runtime) = handle.take() {
            for stream_id in runtime.drain_tracked_js_stream_ids() {
                if runtime.is_shutdown() {
                    break;
                }
                if let Err(err) = runtime.stream_release(stream_id) {
                    log::debug!(
                        "Runtime.close failed to release stream id {}: {}",
                        stream_id,
                        err
                    );
                }
            }
            for stream_id in runtime.drain_tracked_py_stream_ids() {
                runtime.cancel_py_stream_async(stream_id);
            }
            for fn_id in runtime.drain_tracked_function_ids() {
                if runtime.is_shutdown() {
                    break;
                }
                if let Err(err) = runtime.release_function(fn_id) {
                    log::debug!(
                        "Runtime.close failed to release function id {}: {}",
                        fn_id,
                        err
                    );
                }
            }
            runtime
                .close()
                .map_err(|e| runtime_error_with_context("Shutdown failed", e))?;
        }
        Ok(())
    }

    fn terminate(&self) -> PyResult<()> {
        let handle = self.handle.borrow().as_ref().cloned();

        if let Some(handle) = handle {
            handle
                .terminate()
                .map_err(|e| runtime_error_with_context("Termination failed", e))
        } else {
            Ok(())
        }
    }

    #[pyo3(signature = (name, handler, /, *, mode="sync"))]
    fn register_op(
        &self,
        py: Python<'_>,
        name: String,
        handler: Py<PyAny>,
        mode: &str,
    ) -> PyResult<u32> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        let handler_clone = handler.clone_ref(py);
        let mode_enum = Self::checked_mode(py, mode, &handler_clone)?;

        handle
            .register_op(name, mode_enum, handler_clone)
            .map_err(|e| runtime_error_with_context("Op registration failed", e))
    }

    #[pyo3(signature = (name, handler))]
    fn bind_function(&self, py: Python<'_>, name: String, handler: Py<PyAny>) -> PyResult<()> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        let handler_clone = handler.clone_ref(py);
        let is_async = Self::detect_async(py, &handler_clone)?;
        let mode_enum = if is_async {
            PythonOpMode::Async
        } else {
            PythonOpMode::Sync
        };

        let op_id = handle
            .register_op(name.clone(), mode_enum, handler_clone)
            .map_err(|e| runtime_error_with_context("Op registration failed", e))?;

        let bridge_name = match mode_enum {
            PythonOpMode::Sync => "__host_op_sync__",
            PythonOpMode::Async => "__host_op_async__",
        };

        let script = format!(
            "globalThis.{name} = (...args) => {bridge}({op_id}, ...args); void 0;",
            name = name,
            bridge = bridge_name,
            op_id = op_id
        );

        // Execute the binding script; ignore the return value ("undefined").
        let _ = self.eval(py, script.as_str())?;
        Ok(())
    }

    #[pyo3(signature = (iterable))]
    fn stream_from_async_iterable(
        &self,
        py: Python<'_>,
        iterable: Py<PyAny>,
    ) -> PyResult<Py<PyStreamSource>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        PyStreamSource::new(py, handle, iterable)
    }

    #[pyo3(signature = (name, obj))]
    fn bind_object(&self, py: Python<'_>, name: String, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        use pyo3::types::PyDict;

        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();
        let serialization_limits = handle.serialization_limits();

        let obj_bound = obj.clone();
        let dict = obj_bound
            .cast::<PyDict>()
            .map_err(|_| PyRuntimeError::new_err("bind_object expects a dict with string keys"))?;

        let mut assignments: Vec<String> = Vec::with_capacity(dict.len());

        for (key, value) in dict.iter() {
            let key_str: String = key.extract()?;
            let key_literal = serde_json::to_string(&key_str).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Failed to serialize property name '{key_str}': {e}"
                ))
            })?;

            if value.is_callable() {
                let handler_py = value.unbind();
                let is_async = Self::detect_async(py, &handler_py)?;
                let mode_enum = if is_async {
                    PythonOpMode::Async
                } else {
                    PythonOpMode::Sync
                };
                let op_name = format!("{name}.{key_str}");
                let op_id = handle
                    .register_op(op_name, mode_enum, handler_py)
                    .map_err(|e| runtime_error_with_context("Op registration failed", e))?;

                let bridge_name = match mode_enum {
                    PythonOpMode::Sync => "__host_op_sync__",
                    PythonOpMode::Async => "__host_op_async__",
                };

                assignments.push(format!(
                    "target[{key_literal}] = (...args) => {bridge_name}({op_id}, ...args);"
                ));
            } else {
                let js_value = python_to_js_value(value, &serialization_limits)?;
                let literal = js_value_to_js_expression(&js_value)?;
                assignments.push(format!("target[{key_literal}] = {literal};"));
            }
        }

        let name_literal = serde_json::to_string(&name).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to serialize object name '{name}': {e}"))
        })?;

        let mut script = String::from("(() => {\n");
        script.push_str("  const globalName = ");
        script.push_str(&name_literal);
        script.push_str(
            ";\n  const target = globalThis[globalName] ?? (globalThis[globalName] = {});\n",
        );
        for assignment in assignments {
            script.push_str("  ");
            script.push_str(&assignment);
            script.push('\n');
        }
        script.push_str("  return void 0;\n})();");

        let _ = self.eval(py, script.as_str())?;
        Ok(())
    }

    fn set_module_resolver(&self, _py: Python<'_>, resolver: Py<PyAny>) -> PyResult<()> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        handle
            .set_module_resolver(resolver)
            .map_err(|e| runtime_error_with_context("Failed to set module resolver", e))?;
        Ok(())
    }

    fn set_module_loader(&self, _py: Python<'_>, loader: Py<PyAny>) -> PyResult<()> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        handle
            .set_module_loader(loader)
            .map_err(|e| runtime_error_with_context("Failed to set module loader", e))?;
        Ok(())
    }

    fn add_static_module(&self, _py: Python<'_>, name: String, source: String) -> PyResult<()> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        handle
            .add_static_module(name, source)
            .map_err(|e| runtime_error_with_context("Failed to add static module", e))?;
        Ok(())
    }

    fn eval_module(&self, py: Python<'_>, specifier: &str) -> PyResult<Py<PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();
        let specifier_owned = specifier.to_owned();
        let js_value = py
            .detach(|| handle.eval_module_sync(&specifier_owned))
            .map_err(|e| runtime_error_with_context("Module evaluation failed", e))?;
        js_value_to_python(py, &js_value, Some(&handle))
    }

    #[pyo3(signature = (specifier, /, *, timeout=None))]
    fn eval_module_async<'py>(
        &self,
        py: Python<'py>,
        specifier: String,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        let timeout_ms = normalize_timeout_to_ms(timeout)?;

        let task_locals = pyo3_tokio::get_current_locals(py)?;
        let handle_for_conversion = handle.clone();
        let module_task_locals = Some(task_locals.clone());

        let future = async move {
            handle
                .eval_module_async(&specifier, timeout_ms, module_task_locals)
                .await
        };

        bridge_js_future(
            py,
            task_locals,
            future,
            handle_for_conversion,
            "Module evaluation failed",
        )
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// Python proxy for a JavaScript function.
///
/// This class represents a JavaScript function that can be called from Python.
/// Functions are awaitable by default (async-first design).
#[pyclass(unsendable, weakref)] // allow Python weak references for finalizers
pub struct JsFunction {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
    fn_id: u32,
    closed: std::cell::Cell<bool>,
    serialization_limits: SerializationLimits,
}

impl JsFunction {
    pub fn new(
        py: Python<'_>,
        handle: RuntimeHandle,
        fn_id: u32,
        serialization_limits: SerializationLimits,
    ) -> PyResult<Py<Self>> {
        handle.track_function_id(fn_id);
        let finalizer_handle = handle.clone();
        let instance = Self {
            handle: std::cell::RefCell::new(Some(handle)),
            fn_id,
            closed: std::cell::Cell::new(false),
            serialization_limits,
        };
        let py_obj = Py::new(py, instance)?;
        Self::attach_finalizer(py, &py_obj, finalizer_handle, fn_id)?;
        Ok(py_obj)
    }

    /// Get the function ID for transfer back to JavaScript.
    ///
    /// Validates that the function is not closed and the runtime is still alive.
    /// This prevents cryptic "Function ID not found" errors from the runtime thread.
    pub(crate) fn function_id_for_transfer(&self) -> PyResult<u32> {
        // Check if function has been closed
        if self.closed.get() {
            return Err(PyRuntimeError::new_err("Function has been closed"));
        }

        // Check if runtime handle is still alive
        let handle = self.handle.borrow();
        if handle.is_none() {
            return Err(PyRuntimeError::new_err("Runtime has been shut down"));
        }

        // Additional check: verify runtime is not shutdown
        if let Some(h) = handle.as_ref() {
            if h.is_shutdown() {
                return Err(PyRuntimeError::new_err("Runtime has been shut down"));
            }
        }

        Ok(self.fn_id)
    }

    fn attach_finalizer(
        py: Python<'_>,
        py_obj: &Py<Self>,
        handle: RuntimeHandle,
        fn_id: u32,
    ) -> PyResult<()> {
        let weakref = py.import("weakref")?;
        let finalize = weakref.getattr(pyo3::intern!(py, "finalize"))?;
        let finalizer = Py::new(py, JsFunctionFinalizer::new(handle, fn_id))?;
        finalize.call1((py_obj.clone_ref(py), finalizer))?;
        Ok(())
    }

    fn convert_python_args(
        &self,
        args: &Bound<'_, pyo3::types::PyTuple>,
    ) -> PyResult<Vec<JSValue>> {
        let mut js_args = Vec::with_capacity(args.len());
        for arg in args.iter() {
            js_args.push(super::conversion::python_to_js_value(
                arg,
                &self.serialization_limits,
            )?);
        }
        Ok(js_args)
    }
}

#[pymethods]
impl JsFunction {
    /// Call the JavaScript function with the given arguments.
    ///
    /// Returns an awaitable that resolves to the function result.
    ///
    /// Args:
    ///     *args: Arguments to pass to the JavaScript function
    ///     timeout: Optional timeout (seconds as float/int, or datetime.timedelta)
    ///
    /// Returns:
    ///     An awaitable that resolves to the function's return value
    #[pyo3(signature = (*args, timeout=None))]
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, pyo3::types::PyTuple>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.closed.get() {
            return Err(PyRuntimeError::new_err("Function has been closed"));
        }

        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been shut down"))?
            .clone();

        let fn_id = self.fn_id;

        let js_args = self.convert_python_args(args)?;
        let timeout_ms = normalize_timeout_to_ms(timeout)?;

        let call_result = handle
            .call_function_sync(fn_id, js_args, timeout_ms)
            .map_err(|e| runtime_error_with_context("Function call failed", e))?;

        match call_result {
            FunctionCallResult::Immediate(value) => {
                let py_obj = js_value_to_python(py, &value, Some(&handle))?;
                Ok(py_obj.into_bound(py))
            }
            FunctionCallResult::Pending { call_id } => {
                let task_locals = pyo3_tokio::get_current_locals(py)?;
                let call_task_locals = Some(task_locals.clone());
                let handle_for_conversion = handle.clone();
                let future =
                    async move { handle.resume_function_call(call_id, call_task_locals).await };

                bridge_js_future(
                    py,
                    task_locals,
                    future,
                    handle_for_conversion,
                    "Function call failed",
                )
            }
        }
    }

    /// Explicit async invocation that always returns an awaitable.
    #[pyo3(signature = (*args, timeout=None))]
    fn call_async<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, pyo3::types::PyTuple>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.closed.get() {
            return Err(PyRuntimeError::new_err("Function has been closed"));
        }

        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been shut down"))?
            .clone();

        let fn_id = self.fn_id;
        let js_args = self.convert_python_args(args)?;
        let timeout_ms = normalize_timeout_to_ms(timeout)?;

        let task_locals = pyo3_tokio::get_current_locals(py)?;
        let call_task_locals = Some(task_locals.clone());
        let handle_for_conversion = handle.clone();

        let future = async move {
            handle
                .call_function_async(fn_id, js_args, timeout_ms, call_task_locals)
                .await
        };

        bridge_js_future(
            py,
            task_locals,
            future,
            handle_for_conversion,
            "Function call failed",
        )
    }

    /// Close the function handle and release resources.
    ///
    /// After calling close(), the function can no longer be invoked.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.closed.get() {
            // Already closed, return immediately
            return pyo3_tokio::future_into_py(py, async { Ok(()) });
        }

        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been shut down"))?
            .clone();
        self.handle.borrow_mut().take();

        let fn_id = self.fn_id;
        self.closed.set(true);

        let untrack_handle = handle.clone();
        let future = async move {
            let result = handle
                .release_function_async(fn_id)
                .await
                .map_err(|e| runtime_error_with_context("Failed to release function", e));
            untrack_handle.untrack_function_id(fn_id);
            result
        };

        let py_future = pyo3_tokio::future_into_py(py, future)?;
        Ok(py_future.into_bound())
    }

    /// String representation of the function.
    fn __repr__(&self) -> String {
        if self.closed.get() {
            "<JsFunction (closed)>".to_string()
        } else {
            format!("<JsFunction id={}>", self.fn_id)
        }
    }
}

#[pyclass(module = "_jsrun", name = "_JsFunctionFinalizer", unsendable)]
pub(crate) struct JsFunctionFinalizer {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
    fn_id: u32,
}

impl JsFunctionFinalizer {
    fn new(handle: RuntimeHandle, fn_id: u32) -> Self {
        Self {
            handle: std::cell::RefCell::new(Some(handle)),
            fn_id,
        }
    }
}

#[pymethods]
impl JsFunctionFinalizer {
    fn __call__(&self) {
        let mut handle = self.handle.borrow_mut();
        if let Some(runtime_handle) = handle.take() {
            if !runtime_handle.is_function_tracked(self.fn_id) {
                return;
            }
            if runtime_handle.is_shutdown() {
                runtime_handle.untrack_function_id(self.fn_id);
                return;
            }
            if let Err(err) = runtime_handle.release_function(self.fn_id) {
                log::debug!(
                    "JsFunction finalizer failed to release function id {}: {}",
                    self.fn_id,
                    err
                );
            }
            runtime_handle.untrack_function_id(self.fn_id);
        }
    }
}

struct StreamSharedState {
    handle: Mutex<Option<RuntimeHandle>>,
    stream_id: u32,
    closed: AtomicBool,
}

impl StreamSharedState {
    fn new(handle: RuntimeHandle, stream_id: u32) -> Self {
        Self {
            handle: Mutex::new(Some(handle)),
            stream_id,
            closed: AtomicBool::new(false),
        }
    }

    fn stream_id(&self) -> u32 {
        self.stream_id
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn mark_remote_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.handle.lock().unwrap().take();
    }

    fn cancel(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(handle) = self.handle.lock().unwrap().take() {
            if let Err(err) = handle.stream_cancel(self.stream_id) {
                log::debug!(
                    "JsStream cancel failed for stream id {}: {}",
                    self.stream_id,
                    err
                );
            }
        }
    }
}

#[pyclass(unsendable, weakref)]
pub struct JsStream {
    state: Arc<StreamSharedState>,
}

impl JsStream {
    pub fn new(py: Python<'_>, handle: RuntimeHandle, stream_id: u32) -> PyResult<Py<Self>> {
        handle.track_js_stream_id(stream_id);
        let state = Arc::new(StreamSharedState::new(handle.clone(), stream_id));
        let instance = Self {
            state: state.clone(),
        };
        let py_obj = Py::new(py, instance)?;
        Self::attach_finalizer(py, &py_obj, state)?;
        Ok(py_obj)
    }

    fn attach_finalizer(
        py: Python<'_>,
        py_obj: &Py<Self>,
        state: Arc<StreamSharedState>,
    ) -> PyResult<()> {
        let weakref = py.import("weakref")?;
        let finalize = weakref.getattr(pyo3::intern!(py, "finalize"))?;
        let finalizer = Py::new(py, JsStreamFinalizer::new(state))?;
        finalize.call1((py_obj.clone_ref(py), finalizer))?;
        Ok(())
    }

    fn cancel_with_runtime(&self) {
        self.state.cancel();
    }
}

#[pymethods]
impl JsStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if slf.state.is_closed() {
            return Err(PyErr::new::<PyStopAsyncIteration, _>("Stream closed"));
        }

        let handle = slf
            .state
            .handle
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been shut down"))?
            .clone();
        let stream_id = slf.state.stream_id();
        let state = slf.state.clone();
        let handle_for_conversion = handle.clone();
        let future = async move {
            match handle.stream_read(stream_id).await {
                Ok(mut chunk) => {
                    if chunk.done {
                        state.mark_remote_closed();
                        Err(PyErr::new::<PyStopAsyncIteration, _>(""))
                    } else {
                        let chunk_value = chunk.value.take();
                        let py_value = Python::attach(|py| match chunk_value {
                            Some(value) => {
                                js_value_to_python(py, &value, Some(&handle_for_conversion))
                            }
                            None => Ok(py.None()),
                        })?;
                        Ok(py_value)
                    }
                }
                Err(err) => Err(runtime_error_to_py(err)),
            }
        };

        pyo3_tokio::future_into_py(py, future)
    }

    fn close(&self) -> PyResult<()> {
        self.cancel_with_runtime();
        Ok(())
    }

    fn __repr__(&self) -> String {
        let stream_id = self.state.stream_id();
        if self.state.is_closed() {
            "<JsStream (closed)>".to_string()
        } else {
            format!("<JsStream id={}>", stream_id)
        }
    }
}

#[pyclass(module = "_jsrun", name = "_JsStreamFinalizer", unsendable)]
pub(crate) struct JsStreamFinalizer {
    state: Arc<StreamSharedState>,
}

impl JsStreamFinalizer {
    fn new(state: Arc<StreamSharedState>) -> Self {
        Self { state }
    }
}

#[pymethods]
impl JsStreamFinalizer {
    fn __call__(&self) {
        self.state.cancel();
    }
}

#[pyclass(module = "_jsrun", unsendable, weakref)]
pub struct PyStreamSource {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
    stream_id: u32,
    closed: std::cell::Cell<bool>,
}

impl PyStreamSource {
    pub fn new(py: Python<'_>, handle: RuntimeHandle, iterable: Py<PyAny>) -> PyResult<Py<Self>> {
        let task_locals = pyo3_tokio::get_current_locals(py)?;
        let stream_id = handle
            .register_py_stream(iterable, task_locals)
            .map_err(|e| runtime_error_with_context("Stream registration failed", e))?;
        let finalizer_handle = handle.clone();
        let instance = Self {
            handle: std::cell::RefCell::new(Some(handle)),
            stream_id,
            closed: std::cell::Cell::new(false),
        };
        let py_obj = Py::new(py, instance)?;
        Self::attach_finalizer(py, &py_obj, finalizer_handle, stream_id)?;
        Ok(py_obj)
    }

    fn attach_finalizer(
        py: Python<'_>,
        py_obj: &Py<Self>,
        handle: RuntimeHandle,
        stream_id: u32,
    ) -> PyResult<()> {
        let weakref = py.import("weakref")?;
        let finalize = weakref.getattr(pyo3::intern!(py, "finalize"))?;
        let finalizer = Py::new(py, PyStreamFinalizer::new(handle, stream_id))?;
        finalize.call1((py_obj.clone_ref(py), finalizer))?;
        Ok(())
    }

    pub(crate) fn stream_id_for_transfer(&self) -> PyResult<u32> {
        if self.closed.get() {
            return Err(PyRuntimeError::new_err("Stream has been closed"));
        }
        if self.handle.borrow().is_none() {
            return Err(PyRuntimeError::new_err("Runtime has been shut down"));
        }
        Ok(self.stream_id)
    }

    fn mark_closed(&self) {
        if self.closed.replace(true) {
            return;
        }
        if let Some(handle) = self.handle.borrow_mut().take() {
            handle.cancel_py_stream_async(self.stream_id);
        }
    }
}

#[pymethods]
impl PyStreamSource {
    #[pyo3(name = "close")]
    fn close_py(&self) {
        self.mark_closed();
    }

    fn __repr__(&self) -> String {
        if self.closed.get() {
            "<PyStreamSource (closed)>".to_string()
        } else {
            format!("<PyStreamSource id={}>", self.stream_id)
        }
    }
}

#[pyclass(module = "_jsrun", name = "_PyStreamFinalizer", unsendable)]
pub(crate) struct PyStreamFinalizer {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
    stream_id: u32,
}

impl PyStreamFinalizer {
    fn new(handle: RuntimeHandle, stream_id: u32) -> Self {
        Self {
            handle: std::cell::RefCell::new(Some(handle)),
            stream_id,
        }
    }
}

#[pymethods]
impl PyStreamFinalizer {
    fn __call__(&self) {
        let mut handle = self.handle.borrow_mut();
        if let Some(runtime_handle) = handle.take() {
            runtime_handle.cancel_py_stream_async(self.stream_id);
        }
    }
}
