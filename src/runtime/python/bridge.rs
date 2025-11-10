//! Async bridge between Tokio futures on the runtime thread and Python asyncio futures.
use crate::runtime::conversion::js_value_to_python;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::handle::RuntimeHandle;
use crate::runtime::js_value::JSValue;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use pyo3_async_runtimes::{tokio as pyo3_tokio, TaskLocals};
use std::future::Future;
use tokio::sync::oneshot;

use super::error::runtime_error_with_context;

/// Return true if the supplied Python future has been cancelled.
fn python_future_cancelled(future: &Bound<'_, PyAny>) -> PyResult<bool> {
    future
        .getattr(pyo3::intern!(future.py(), "cancelled"))?
        .call0()?
        .is_truthy()
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
pub(crate) fn bridge_js_future<'py, Fut>(
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
