//! Python bindings exposing the runtime to Python callers.

use super::config::RuntimeConfig;
use super::conversion::{js_value_to_python, python_to_js_value};
use super::handle::RuntimeHandle;
use super::js_value::JSValue;
use super::ops::PythonOpMode;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio;

#[pyclass(unsendable)]
pub struct Runtime {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
}

impl Runtime {
    fn init_with_config(config: RuntimeConfig) -> PyResult<Self> {
        let handle = RuntimeHandle::spawn(config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to spawn runtime: {}", e)))?;
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
        JSValue::Null => Ok("null".to_string()),
        JSValue::Bool(b) => Ok(b.to_string()),
        JSValue::Int(i) => Ok(i.to_string()),
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
        JSValue::Array(items) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(js_value_to_js_expression(item)?);
            }
            Ok(format!("[{}]", parts.join(", ")))
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
                .map_err(|e| PyRuntimeError::new_err(format!("Evaluation failed: {}", e)))?;

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
            .map_err(|e| PyRuntimeError::new_err(format!("Evaluation failed: {}", e)))?;
        js_value_to_python(py, &js_value, Some(&handle))
    }

    fn is_closed(&self) -> bool {
        self.handle
            .borrow()
            .as_ref()
            .map(|handle| handle.is_shutdown())
            .unwrap_or(true)
    }

    fn close(&self) -> PyResult<()> {
        let mut handle = self.handle.borrow_mut();
        if let Some(mut runtime) = handle.take() {
            runtime
                .close()
                .map_err(|e| PyRuntimeError::new_err(format!("Shutdown failed: {}", e)))?;
        }
        Ok(())
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
            .map_err(|e| PyRuntimeError::new_err(format!("Op registration failed: {}", e)))
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
            .map_err(|e| PyRuntimeError::new_err(format!("Op registration failed: {}", e)))?;

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
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Op registration failed: {}", e))
                    })?;

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

        handle.set_module_resolver(resolver).map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to set module resolver: {}", e))
        })?;
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
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to set module loader: {}", e)))?;
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
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to add static module: {}", e)))?;
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
            .map_err(|e| PyRuntimeError::new_err(format!("Module evaluation failed: {}", e)))?;
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
                .map_err(|e| PyRuntimeError::new_err(format!("Module evaluation failed: {}", e)))?;

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
#[pyclass(unsendable)]
pub struct JsFunction {
    handle: std::cell::RefCell<Option<RuntimeHandle>>,
    fn_id: u32,
    closed: std::cell::Cell<bool>,
}

impl JsFunction {
    pub fn new(handle: RuntimeHandle, fn_id: u32) -> PyResult<Self> {
        Ok(Self {
            handle: std::cell::RefCell::new(Some(handle)),
            fn_id,
            closed: std::cell::Cell::new(false),
        })
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
                .map_err(|e| PyRuntimeError::new_err(format!("Function call failed: {}", e)))?;

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

        let fn_id = self.fn_id;
        self.closed.set(true);

        let future = async move {
            handle
                .release_function_async(fn_id)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to release function: {}", e)))
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

    /// Destructor that warns if the function wasn't closed.
    fn __del__(&self) {
        if !self.closed.get() {
            // Note: Can't do async cleanup in __del__, user must call close()
            Python::attach(|py| {
                if let Ok(warnings) = py.import("warnings") {
                    let message = format!(
                        "JsFunction id={} not closed before drop. Call .close() explicitly.",
                        self.fn_id
                    );
                    let _ = warnings.call_method1("warn", (message, "ResourceWarning"));
                }
            });
        }
    }
}
