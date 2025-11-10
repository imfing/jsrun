//! Python bindings entry point that ties together the submodules.
use crate::runtime::runner;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use std::sync::OnceLock;

/// Async bridge helpers shared across the bindings.
mod bridge;
/// JsRuntime error translation helpers.
pub(crate) mod error;
/// Primary runtime bindings exposed to Python (`Runtime`, streams, functions).
pub(crate) mod runtime;
/// Snapshot builder PyO3 wrapper and helpers.
pub(crate) mod snapshot;
/// Runtime statistics and inspector endpoints exposure.
pub(crate) mod stats;
/// Utility helpers (e.g., timeout normalization).
pub(crate) mod utils;

pub(crate) use error::runtime_error_to_py;
#[allow(unused_imports)]
pub(crate) use error::runtime_error_with_context;
pub use runtime::{JsFunction, JsStream, PyStreamSource, Runtime};
pub(crate) use runtime::{JsFunctionFinalizer, JsStreamFinalizer, PyStreamFinalizer};
pub use snapshot::SnapshotBuilderPy;
pub use stats::{InspectorEndpoints, RuntimeStats};

create_exception!(crate::runtime::python, JavaScriptError, PyException);
create_exception!(crate::runtime::python, RuntimeTerminated, PyRuntimeError);

#[pyfunction]
pub fn _debug_active_runtime_threads() -> usize {
    runner::active_runtime_threads()
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

static JS_UNDEFINED_SINGLETON: OnceLock<Py<JsUndefined>> = OnceLock::new();

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
