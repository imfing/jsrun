//! Python op registry and deno_core integration.
//!
//! This module exposes two ops (`op_jsrun_call_python_sync` and
//! `op_jsrun_call_python_async`) that bridge JavaScript calls into Python
//! handlers. Python handlers are registered dynamically at runtime and
//! identified by an integer op id.

use crate::runtime::conversion::{js_value_to_python, python_to_js_value};
use crate::runtime::js_value::{JSValue, SerializationLimits};
use crate::runtime::stream::PyStreamRegistry;
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

    let serialization_limits = *state
        .try_borrow::<SerializationLimits>()
        .ok_or_else(|| JsErrorBox::type_error("Serialization limits not configured"))?;

    Python::attach(|py| -> Result<JSValue, JsErrorBox> {
        let py_args = args
            .iter()
            .map(|arg| js_value_to_python(py, arg, None).map_err(map_pyerr))
            .collect::<Result<Vec<_>, _>>()?;
        let py_args_tuple = PyTuple::new(py, py_args).map_err(map_pyerr)?;
        let result = entry
            .handler
            .call(py, py_args_tuple, None)
            .map_err(map_pyerr)?;
        python_to_js_value(result.into_bound(py), &serialization_limits).map_err(map_pyerr)
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

    let serialization_limits = *state
        .try_borrow::<SerializationLimits>()
        .ok_or_else(|| JsErrorBox::type_error("Serialization limits not configured"))?;

    let coroutine = Python::attach(|py| -> Result<Py<PyAny>, JsErrorBox> {
        let py_args = args
            .iter()
            .map(|arg| js_value_to_python(py, arg, None).map_err(map_pyerr))
            .collect::<Result<Vec<_>, _>>()?;
        let py_args_tuple = PyTuple::new(py, py_args).map_err(map_pyerr)?;
        let awaitable = entry
            .handler
            .call(py, py_args_tuple, None)
            .map_err(map_pyerr)?;

        // Validate that we have task locals with a running event loop
        let _locals = global_locals.0.as_ref().ok_or_else(|| {
            JsErrorBox::type_error(
                "Async op requires asyncio context. Call eval_async() first to establish context.",
            )
        })?;

        Ok(awaitable)
    })?;

    // Use into_future_with_locals to properly await the Python coroutine
    // This allows the Rust future to be suspended and resumed, enabling re-entrancy
    let task_locals = global_locals
        .0
        .ok_or_else(|| JsErrorBox::type_error("TaskLocals not available for async op"))?;

    Ok(async move {
        // Convert the coroutine future using into_future_with_locals
        // This must be done with GIL acquired
        let future = Python::attach(|py| {
            let bound_coroutine = coroutine.bind(py).clone();
            pyo3_async_runtimes::into_future_with_locals(&task_locals, bound_coroutine)
        })
        .map_err(|err| JsErrorBox::type_error(format!("Python coroutine error: {}", err)))?;

        let result = future
            .await
            .map_err(|err| JsErrorBox::type_error(format!("Python coroutine failed: {}", err)))?;

        Python::attach(|py| {
            python_to_js_value(result.into_bound(py), &serialization_limits).map_err(map_pyerr)
        })
    })
}

#[op2(async)]
#[serde]
fn op_jsrun_stream_pull_py(
    state: &mut OpState,
    #[smi] stream_id: u32,
) -> Result<impl std::future::Future<Output = Result<JSValue, JsErrorBox>>, JsErrorBox> {
    let registry = state
        .try_borrow::<PyStreamRegistry>()
        .ok_or_else(|| JsErrorBox::type_error("PyStreamRegistry is missing"))?
        .clone();

    Ok(async move {
        registry
            .pull_next(stream_id)
            .await
            .map(|chunk| chunk.to_js_value())
            .map_err(|err| JsErrorBox::type_error(err.to_string()))
    })
}

#[op2(async)]
fn op_jsrun_stream_cancel_py(
    state: &mut OpState,
    #[smi] stream_id: u32,
) -> Result<impl std::future::Future<Output = Result<(), JsErrorBox>>, JsErrorBox> {
    let registry = state
        .try_borrow::<PyStreamRegistry>()
        .ok_or_else(|| JsErrorBox::type_error("PyStreamRegistry is missing"))?
        .clone();

    Ok(async move {
        registry
            .cancel(stream_id)
            .await
            .map_err(|err| JsErrorBox::type_error(err.to_string()))
    })
}

/// Build the `deno_core::Extension` that wires the Python op registry into the runtime.
pub fn python_extension(registry: PythonOpRegistry) -> Extension {
    let bridge_code = ascii_str!(
        r#"(function (globalThis) {
  const { ops } = Deno.core;

  // Delete Deno global after caching ops
  delete globalThis.Deno;

  function prepare(value) {
    if (value === undefined || value === null) {
      return value;
    }
    if (ArrayBuffer.isView(value)) {
      return value;
    }
    if (Array.isArray(value)) {
      return value.map(prepare);
    }
    if (value instanceof Date) {
      return { __jsrun_type: "Date", epoch_ms: value.valueOf() };
    }
    if (value instanceof Set) {
      return {
        __jsrun_type: "Set",
        values: Array.from(value, (entry) => prepare(entry)),
      };
    }
    if (typeof value === "bigint") {
      return { __jsrun_type: "BigInt", value: value.toString() };
    }
    if (typeof value === "object") {
      const result = {};
      for (const [key, val] of Object.entries(value)) {
        result[key] = prepare(val);
      }
      return result;
    }
    return value;
  }

  function reviveStreamChunk(entry) {
    if (!entry || entry.__jsrun_type !== "StreamChunk") {
      return entry;
    }
    return {
      done: Boolean(entry.done),
      value: entry.value === undefined ? undefined : revive(entry.value),
    };
  }

  function revive(value) {
    if (value && typeof value === "object") {
      if (ArrayBuffer.isView(value)) {
        return value;
      }
      if (Array.isArray(value)) {
        return value.map(revive);
      }
      const tag = value.__jsrun_type;
      switch (tag) {
        case "Undefined":
          return undefined;
        case "Date":
          return new Date(value.epoch_ms);
        case "Set": {
          const set = new Set();
          if (Array.isArray(value.values)) {
            for (const entry of value.values) {
              set.add(revive(entry));
            }
          }
          return set;
        }
        case "BigInt":
          return BigInt(value.value);
        case "PyStream":
          if (typeof globalThis.__jsrun_from_py_stream === "function") {
            return globalThis.__jsrun_from_py_stream(value.id);
          }
          return value;
        default: {
          const result = {};
          for (const [key, val] of Object.entries(value)) {
            result[key] = revive(val);
          }
          return result;
        }
      }
    }
    return value;
  }

  globalThis.__jsrunCallSync = function (opId, ...args) {
    const prepared = args.map(prepare);
    return revive(ops.op_jsrun_call_python_sync(opId, prepared));
  };
  globalThis.__jsrunCallAsync = function (opId, ...args) {
    const prepared = args.map(prepare);
    return ops.op_jsrun_call_python_async(opId, prepared).then(revive);
  };
  globalThis.__host_op_sync__ = globalThis.__jsrunCallSync;
  globalThis.__host_op_async__ = function (opId, ...args) {
    return globalThis.__jsrunCallAsync(opId, ...args);
  };
  globalThis.__jsrun_bind_object = function (globalName, assignments) {
    if (typeof globalName !== "string" || !Array.isArray(assignments)) {
      return;
    }
    const target = globalThis[globalName] ?? (globalThis[globalName] = {});
    for (const entry of assignments) {
      if (!entry || typeof entry !== "object" || typeof entry.key !== "string") {
        continue;
      }
      if (entry.kind === "op") {
        const bridge =
          entry.mode === "async"
            ? globalThis.__host_op_async__
            : globalThis.__host_op_sync__;
        if (typeof bridge !== "function" || typeof entry.op_id !== "number") {
          continue;
        }
        target[entry.key] = (...args) => bridge(entry.op_id, ...args);
      } else if (entry.kind === "value") {
        target[entry.key] = entry.value;
      }
    }
  };

  if (typeof globalThis.ReadableStream !== "function") {
    // Note: This minimal polyfill does not implement backpressure or BYOB readers.
    class JsrunReadableStream {
      constructor(underlying = {}) {
        this._queue = [];
        this._closed = false;
        this._errored = false;
        this._error = undefined;
        this._pulling = false;
        this._underlying = underlying;
        this._controller = {
          enqueue: (value) => {
            if (this._closed) {
              return;
            }
            this._queue.push(value);
          },
          close: () => {
            this._closed = true;
          },
          error: (reason) => {
            this._errored = true;
            this._error =
              reason instanceof Error
                ? reason
                : new Error(String(reason ?? "ReadableStream error"));
            this._closed = true;
          },
        };
        if (typeof underlying.start === "function") {
          underlying.start(this._controller);
        }
      }

      async _maybePull() {
        if (this._closed || this._pulling) {
          return;
        }
        if (typeof this._underlying.pull === "function") {
          this._pulling = true;
          try {
            await this._underlying.pull(this._controller);
          } finally {
            this._pulling = false;
          }
        }
      }

      getReader() {
        const stream = this;
        return {
          async read() {
            if (stream._queue.length === 0 && !stream._closed) {
              await stream._maybePull();
            }
            if (stream._queue.length > 0) {
              const value = stream._queue.shift();
              return { done: false, value };
            }
            if (stream._errored) {
              throw stream._error || new Error("ReadableStream error");
            }
            return { done: true, value: undefined };
          },
          async cancel(reason) {
            stream._closed = true;
            if (typeof stream._underlying.cancel === "function") {
              await stream._underlying.cancel(reason);
            }
          },
        };
      }
    }
    globalThis.ReadableStream = JsrunReadableStream;
  }

  globalThis.__jsrun_from_py_stream = function (id) {
    return new ReadableStream({
      async pull(controller) {
        const chunk = reviveStreamChunk(await ops.op_jsrun_stream_pull_py(id));
        if (chunk.done) {
          controller.close();
          return;
        }
        controller.enqueue(chunk.value);
      },
      cancel(reason) {
        return ops.op_jsrun_stream_cancel_py(id);
      },
    });
  };
})(globalThis);"#
    );

    let registry_for_state = registry.clone();

    Extension {
        name: "jsrun_python",
        ops: std::borrow::Cow::Owned(vec![
            op_jsrun_call_python_sync(),
            op_jsrun_call_python_async(),
            op_jsrun_stream_pull_py(),
            op_jsrun_stream_cancel_py(),
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
