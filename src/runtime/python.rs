//! Python bindings exposing the runtime to Python callers.

use super::config::RuntimeConfig;
use super::conversion::js_value_to_python;
use super::handle::RuntimeHandle;
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
        let future = async move {
            let js_result = handle
                .eval_async(&code, timeout_ms, task_locals)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Evaluation failed: {}", e)))?;

            // Convert JSValue to Python in the current Python context
            Python::attach(|py| js_value_to_python(py, &js_result))
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
        js_value_to_python(py, &js_value)
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
