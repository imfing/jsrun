//! Python op registry and deno_core integration.
//!
//! This module exposes two ops (`op_jsrun_call_python_sync` and
//! `op_jsrun_call_python_async`) that bridge JavaScript calls into Python
//! handlers. Python handlers are registered dynamically at runtime and
//! identified by an integer op id.

use crate::runtime::conversion::{js_value_to_python, python_to_js_value};
use crate::runtime::js_value::JSValue;
use deno_core::ascii_str;
use deno_core::op2;
use deno_core::Extension;
use deno_core::ExtensionFileSource;
use deno_core::OpState;
use deno_error::JsErrorBox;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_async_runtimes::TaskLocals;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Execution mode for Python handlers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PythonOpMode {
    Sync,
    Async,
}

/// Metadata for a registered Python op.
pub struct PythonOpEntry {
    pub id: u32,
    pub name: String,
    pub mode: PythonOpMode,
    pub handler: Py<PyAny>,
}

/// Global asyncio task locals for all async ops in this runtime.
#[derive(Clone)]
pub struct GlobalTaskLocals(pub Option<TaskLocals>);

impl Clone for PythonOpEntry {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            id: self.id,
            name: self.name.clone(),
            mode: self.mode,
            handler: self.handler.clone_ref(py),
        })
    }
}

#[derive(Default)]
struct PythonOpRegistryInner {
    next_id: AtomicU32,
    handlers: Mutex<HashMap<u32, PythonOpEntry>>,
}

/// Thread-safe registry of Python operations.
#[derive(Clone, Default)]
pub struct PythonOpRegistry {
    inner: Arc<PythonOpRegistryInner>,
}

impl PythonOpRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, name: String, mode: PythonOpMode, handler: Py<PyAny>) -> u32 {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = PythonOpEntry {
            id,
            name,
            mode,
            handler,
        };
        let mut handlers = self.inner.handlers.lock().unwrap();
        handlers.insert(id, entry);
        id
    }

    pub fn get(&self, id: u32) -> Option<PythonOpEntry> {
        let handlers = self.inner.handlers.lock().unwrap();
        handlers.get(&id).cloned()
    }
}

fn lookup_entry(
    op_state: &mut OpState,
    op_id: u32,
) -> Result<(PythonOpRegistry, PythonOpEntry), JsErrorBox> {
    let registry = op_state
        .try_borrow::<PythonOpRegistry>()
        .ok_or_else(|| JsErrorBox::type_error("Python op registry is missing"))?
        .clone();

    let entry = registry
        .get(op_id)
        .ok_or_else(|| JsErrorBox::type_error(format!("Unknown Python op id {}", op_id)))?;

    Ok((registry, entry))
}

fn map_pyerr(err: PyErr) -> JsErrorBox {
    JsErrorBox::type_error(err.to_string())
}

#[op2]
#[serde]
fn op_jsrun_call_python_sync(
    state: &mut OpState,
    #[smi] op_id: u32,
    #[serde] args: Vec<JSValue>,
) -> Result<JSValue, JsErrorBox> {
    let (_registry, entry) = lookup_entry(state, op_id)?;
    if entry.mode != PythonOpMode::Sync {
        return Err(JsErrorBox::type_error(format!(
            "Op {} is not synchronous",
            entry.name
        )));
    }

    Python::attach(|py| -> Result<JSValue, JsErrorBox> {
        let py_args = args
            .iter()
            .map(|arg| js_value_to_python(py, arg).map_err(map_pyerr))
            .collect::<Result<Vec<_>, _>>()?;
        let py_args_tuple = PyTuple::new(py, py_args).map_err(map_pyerr)?;
        let result = entry
            .handler
            .call(py, py_args_tuple, None)
            .map_err(map_pyerr)?;
        python_to_js_value(result.into_bound(py)).map_err(map_pyerr)
    })
}

#[op2(async)]
#[serde]
fn op_jsrun_call_python_async(
    state: &mut OpState,
    #[smi] op_id: u32,
    #[serde] args: Vec<JSValue>,
) -> Result<impl std::future::Future<Output = Result<JSValue, JsErrorBox>>, JsErrorBox> {
    let (_registry, entry) = lookup_entry(state, op_id)?;
    if entry.mode != PythonOpMode::Async {
        return Err(JsErrorBox::type_error(format!(
            "Op {} is not asynchronous",
            entry.name
        )));
    }

    // Get global task locals from OpState
    let global_locals = state
        .try_borrow::<GlobalTaskLocals>()
        .ok_or_else(|| JsErrorBox::type_error("GlobalTaskLocals not found in OpState"))?
        .clone();

    let concurrent_future = Python::attach(|py| -> Result<Py<PyAny>, JsErrorBox> {
        let py_args = args
            .iter()
            .map(|arg| js_value_to_python(py, arg).map_err(map_pyerr))
            .collect::<Result<Vec<_>, _>>()?;
        let py_args_tuple = PyTuple::new(py, py_args).map_err(map_pyerr)?;
        let awaitable = entry
            .handler
            .call(py, py_args_tuple, None)
            .map_err(map_pyerr)?;
        let coroutine = awaitable.into_bound(py);
        let locals = global_locals.0.as_ref().ok_or_else(|| {
            JsErrorBox::type_error(
                "Async op requires asyncio context. Call eval_async() first to establish context.",
            )
        })?;
        let event_loop = locals.event_loop(py);
        let is_running = event_loop
            .call_method0(pyo3::intern!(py, "is_running"))
            .map_err(map_pyerr)?
            .extract::<bool>()
            .map_err(map_pyerr)?;
        if !is_running {
            return Err(JsErrorBox::type_error(
                "Python event loop is not running for async op",
            ));
        }
        let asyncio = py.import("asyncio").map_err(map_pyerr)?;
        let future = asyncio
            .call_method1(
                pyo3::intern!(py, "run_coroutine_threadsafe"),
                (coroutine, event_loop),
            )
            .map_err(map_pyerr)?;
        Ok(future.unbind())
    })?;

    Ok(async move {
        let result = tokio::task::spawn_blocking(move || {
            Python::attach(|py| -> Result<Py<PyAny>, JsErrorBox> {
                let fut = concurrent_future.bind(py);
                fut.call_method0(pyo3::intern!(py, "result"))
                    .map(|value| value.into())
                    .map_err(map_pyerr)
            })
        })
        .await
        .map_err(|err| JsErrorBox::type_error(err.to_string()))??;

        Python::attach(|py| python_to_js_value(result.into_bound(py)).map_err(map_pyerr))
    })
}

/// Build the `deno_core::Extension` that wires the Python op registry into the runtime.
pub fn python_extension(registry: PythonOpRegistry) -> Extension {
    let bridge_code = ascii_str!(
        r#"(function (globalThis) {
  const { ops } = Deno.core;
  globalThis.__jsrunCallSync = function (opId, ...args) {
    return ops.op_jsrun_call_python_sync(opId, args);
  };
  globalThis.__jsrunCallAsync = function (opId, ...args) {
    return ops.op_jsrun_call_python_async(opId, args);
  };
  globalThis.__host_op_sync__ = globalThis.__jsrunCallSync;
  globalThis.__host_op_async__ = function (opId, ...args) {
    return globalThis.__jsrunCallAsync(opId, ...args);
  };
})(globalThis);"#
    );

    let registry_for_state = registry.clone();

    Extension {
        name: "jsrun_python",
        ops: std::borrow::Cow::Owned(vec![
            op_jsrun_call_python_sync(),
            op_jsrun_call_python_async(),
        ]),
        js_files: std::borrow::Cow::Owned(vec![ExtensionFileSource::new(
            "ext:jsrun/python_bridge.js",
            bridge_code,
        )]),
        op_state_fn: Some(Box::new(move |state| {
            state.put::<PythonOpRegistry>(registry_for_state.clone());
            state.put::<GlobalTaskLocals>(GlobalTaskLocals(None));
        })),
        ..Default::default()
    }
}
