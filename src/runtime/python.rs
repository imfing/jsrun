//! Python bindings exposing the runtime to Python callers.

use super::config::RuntimeConfig;
use super::conversion::{js_value_to_python, python_to_js_value};
use super::error::{JsExceptionDetails, RuntimeError};
use super::handle::RuntimeHandle;
use super::js_value::JSValue;
use super::ops::PythonOpMode;
use super::stats::{RuntimeCallKind, RuntimeStatsSnapshot};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio;
use std::sync::OnceLock;

#[pyclass(unsendable)]
pub struct Runtime {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
}

static JS_UNDEFINED_SINGLETON: OnceLock<Py<JsUndefined>> = OnceLock::new();

create_exception!(crate::runtime::python, JavaScriptError, PyException);
create_exception!(crate::runtime::python, RuntimeTerminated, PyRuntimeError);

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
    active_async_ops: u64,
    #[pyo3(get)]
    open_resources: u64,
    #[pyo3(get)]
    active_timers: u64,
    #[pyo3(get)]
    active_intervals: u64,
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
            active_async_ops: snapshot.active_async_ops,
            open_resources: snapshot.open_resources,
            active_timers: snapshot.active_timers,
            active_intervals: snapshot.active_intervals,
        }
    }
}

#[pymethods]
impl RuntimeStats {
    fn __repr__(&self) -> String {
        format!(
            "RuntimeStats(heap_used_bytes={}, total_execution_time_ms={}, last_execution_kind={}, active_async_ops={}, open_resources={}, eval_sync_count={}, call_function_async_count={})",
            self.heap_used_bytes,
            self.total_execution_time_ms,
            self.last_execution_kind
                .as_deref()
                .unwrap_or("None"),
            self.active_async_ops,
            self.open_resources,
            self.eval_sync_count,
            self.call_function_async_count
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

    #[pyo3(signature = (code, /, *, timeout_ms=None))]
    fn eval_async<'py>(
        &self,
        py: Python<'py>,
        code: String,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        // Capture task_locals from the current async context
        let task_locals = tokio::get_current_locals(py).ok();

        // Convert the JSValue result to Python after the async operation completes
        let handle_for_conversion = handle.clone();
        let future = async move {
            let js_result = handle
                .eval_async(&code, timeout_ms, task_locals)
                .await
                .map_err(|e| runtime_error_with_context("Evaluation failed", e))?;

            // Convert JSValue to Python in the current Python context
            Python::attach(|py| js_value_to_python(py, &js_result, Some(&handle_for_conversion)))
        };

        pyo3_async_runtimes::tokio::future_into_py(py, future)
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

    #[pyo3(signature = (name, obj))]
    fn bind_object(&self, py: Python<'_>, name: String, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        use pyo3::types::PyDict;

        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

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
                let js_value = python_to_js_value(value)?;
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

    #[pyo3(signature = (specifier, /, *, timeout_ms=None))]
    fn eval_module_async<'py>(
        &self,
        py: Python<'py>,
        specifier: String,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been closed"))?
            .clone();

        // Capture task_locals from the current async context
        let task_locals = tokio::get_current_locals(py).ok();

        // Convert the JSValue result to Python after the async operation completes
        let handle_for_conversion = handle.clone();
        let future = async move {
            let js_result = handle
                .eval_module_async(&specifier, timeout_ms, task_locals)
                .await
                .map_err(|e| runtime_error_with_context("Module evaluation failed", e))?;

            // Convert JSValue to Python in the current Python context
            Python::attach(|py| js_value_to_python(py, &js_result, Some(&handle_for_conversion)))
        };

        pyo3_async_runtimes::tokio::future_into_py(py, future)
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
}

impl JsFunction {
    pub fn new(py: Python<'_>, handle: RuntimeHandle, fn_id: u32) -> PyResult<Py<Self>> {
        handle.track_function_id(fn_id);
        let finalizer_handle = handle.clone();
        let instance = Self {
            handle: std::cell::RefCell::new(Some(handle)),
            fn_id,
            closed: std::cell::Cell::new(false),
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
}

#[pymethods]
impl JsFunction {
    /// Call the JavaScript function with the given arguments.
    ///
    /// Returns an awaitable that resolves to the function result.
    ///
    /// Args:
    ///     *args: Arguments to pass to the JavaScript function
    ///     timeout_ms: Optional timeout in milliseconds
    ///
    /// Returns:
    ///     An awaitable that resolves to the function's return value
    #[pyo3(signature = (*args, timeout_ms=None))]
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, pyo3::types::PyTuple>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Check if closed
        if self.closed.get() {
            return Err(PyRuntimeError::new_err("Function has been closed"));
        }

        // Get handle
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Runtime has been shut down"))?
            .clone();

        let fn_id = self.fn_id;

        // Convert Python args to Vec<JSValue>
        use super::conversion::python_to_js_value;
        let mut js_args = Vec::with_capacity(args.len());
        for arg in args.iter() {
            js_args.push(python_to_js_value(arg)?);
        }

        // Capture task_locals
        let task_locals = tokio::get_current_locals(py).ok();

        // Call the function asynchronously
        let handle_for_conversion = handle.clone();
        let future = async move {
            let js_result = handle
                .call_function_async(fn_id, js_args, timeout_ms, task_locals)
                .await
                .map_err(|e| runtime_error_with_context("Function call failed", e))?;

            // Convert JSValue result to Python
            Python::attach(|py| js_value_to_python(py, &js_result, Some(&handle_for_conversion)))
        };

        pyo3_async_runtimes::tokio::future_into_py(py, future)
    }

    /// Close the function handle and release resources.
    ///
    /// After calling close(), the function can no longer be invoked.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.closed.get() {
            // Already closed, return immediately
            return pyo3_async_runtimes::tokio::future_into_py(py, async { Ok(()) });
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

        pyo3_async_runtimes::tokio::future_into_py(py, future)
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
