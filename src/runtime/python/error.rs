//! Helper utilities for translating runtime errors into Python exceptions.
use crate::runtime::error::{JsExceptionDetails, RuntimeError};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::{JavaScriptError, RuntimeTerminated};

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

/// Build a PyErr from a runtime error, optionally tagging a context string.
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

/// Expose the crate-local variant used across the bindings.
pub(crate) fn runtime_error_to_py(err: RuntimeError) -> PyErr {
    Python::attach(|py| runtime_error_to_py_with(py, err, None))
}

/// Include context when converting runtime failures to Python exceptions.
pub(crate) fn runtime_error_with_context(context: &str, err: RuntimeError) -> PyErr {
    Python::attach(|py| runtime_error_to_py_with(py, err, Some(context)))
}
