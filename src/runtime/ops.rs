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

/// Execution mode for Python op handlers.
///
/// Determines whether the Python callable is synchronous or asynchronous.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PythonOpMode {
    /// Synchronous Python function (returns value directly).
    Sync,
    /// Asynchronous Python function (returns awaitable/coroutine).
    Async,
}

/// Metadata for a registered Python op.
///
/// Stores the handler callable and its execution mode for op invocations from JavaScript.
pub struct PythonOpEntry {
    /// Unique op identifier.
    pub id: u32,
    /// Human-readable op name.
    pub name: String,
    /// Sync or async execution mode.
    pub mode: PythonOpMode,
    /// Python callable (function or bound method).
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
///
/// Manages dynamic registration and lookup of Python callables that can be invoked
/// from JavaScript via `op_jsrun_call_python_sync` and `op_jsrun_call_python_async`.
#[derive(Clone, Default)]
pub struct PythonOpRegistry {
    inner: Arc<PythonOpRegistryInner>,
}

impl PythonOpRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a Python callable as an op.
    ///
    /// Returns a unique op ID that can be used to invoke the handler from JavaScript.
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

/// Deno op for synchronously calling a Python handler from JavaScript.
///
/// Looks up the registered Python op by ID, serializes JS arguments to Python,
/// invokes the handler, and returns the result as a JSValue.
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

/// Deno op for asynchronously calling a Python handler from JavaScript.
///
/// Returns a future that polls the Python coroutine using the asyncio event loop
/// from the TaskLocals stored in OpState. This enables Python async functions to
/// be awaited from JavaScript.
#[op2]
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
    // This allows the Rust future to be suspended and resumed, enabling re-entrance
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

#[op2]
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

#[op2]
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
  const { createTimer, cancelTimer } = Deno.core;

  // Delete Deno global after caching ops
  delete globalThis.Deno;

  // Web-style timers backed by deno_core's native timer machinery.
  // createTimer returns an opaque timer object; keep a registry so the
  // public API hands out web-compatible numeric ids.
  if (typeof globalThis.setTimeout !== "function") {
    const toDelay = (delay) => {
      const ms = Number(delay);
      return Number.isFinite(ms) && ms > 0 ? ms : 0;
    };
    const toTask = (callback, args) => {
      if (typeof callback !== "function") {
        throw new TypeError("Timer callback must be a function");
      }
      return args.length === 0 ? callback : () => callback(...args);
    };
    let nextTimerId = 1;
    const activeTimers = new Map();
    const schedule = (callback, delay, args, repeat) => {
      const task = toTask(callback, args);
      const id = nextTimerId++;
      const timer = createTimer(
        repeat
          ? task
          : () => {
              activeTimers.delete(id);
              task();
            },
        toDelay(delay),
        undefined,
        repeat,
        true,
        false,
      );
      activeTimers.set(id, timer);
      return id;
    };
    const cancel = (id) => {
      const timer = activeTimers.get(Number(id));
      if (timer !== undefined) {
        activeTimers.delete(Number(id));
        cancelTimer(timer);
      }
    };
    globalThis.setTimeout = function setTimeout(callback, delay = 0, ...args) {
      return schedule(callback, delay, args, false);
    };
    globalThis.setInterval = function setInterval(callback, delay = 0, ...args) {
      return schedule(callback, delay, args, true);
    };
    globalThis.clearTimeout = function clearTimeout(id) {
      cancel(id);
    };
    globalThis.clearInterval = function clearInterval(id) {
      cancel(id);
    };
  }

  // Temporal values cross the boundary as tagged plain objects (their data
  // lives in internal slots, so generic object conversion yields {}).
  // Calendar-dependent fields are normalized to ISO; Durations with
  // years/months/weeks are calendar-relative and stay unconverted.
  function temporalPrepare(value) {
    if (typeof Temporal === "undefined" || value === null || typeof value !== "object") {
      return undefined;
    }
    try {
      if (value instanceof Temporal.Instant) {
        return { __jsrun_type: "Temporal", kind: "Instant", epoch_ns: value.epochNanoseconds.toString() };
      }
      if (value instanceof Temporal.ZonedDateTime) {
        return {
          __jsrun_type: "Temporal",
          kind: "ZonedDateTime",
          epoch_ns: value.epochNanoseconds.toString(),
          time_zone: value.timeZoneId,
        };
      }
      if (value instanceof Temporal.PlainDate) {
        const d = value.withCalendar("iso8601");
        return { __jsrun_type: "Temporal", kind: "PlainDate", year: d.year, month: d.month, day: d.day };
      }
      if (value instanceof Temporal.PlainDateTime) {
        const d = value.withCalendar("iso8601");
        return {
          __jsrun_type: "Temporal",
          kind: "PlainDateTime",
          year: d.year, month: d.month, day: d.day,
          hour: d.hour, minute: d.minute, second: d.second,
          nanosecond: d.millisecond * 1e6 + d.microsecond * 1e3 + d.nanosecond,
        };
      }
      if (value instanceof Temporal.PlainTime) {
        return {
          __jsrun_type: "Temporal",
          kind: "PlainTime",
          hour: value.hour, minute: value.minute, second: value.second,
          nanosecond: value.millisecond * 1e6 + value.microsecond * 1e3 + value.nanosecond,
        };
      }
      if (value instanceof Temporal.Duration) {
        if (value.years !== 0 || value.months !== 0 || value.weeks !== 0) {
          return undefined;
        }
        const seconds =
          ((BigInt(value.days) * 24n + BigInt(value.hours)) * 60n + BigInt(value.minutes)) * 60n +
          BigInt(value.seconds);
        const total =
          seconds * 1000000000n +
          BigInt(value.milliseconds) * 1000000n +
          BigInt(value.microseconds) * 1000n +
          BigInt(value.nanoseconds);
        return { __jsrun_type: "Temporal", kind: "Duration", ns: total.toString() };
      }
    } catch (_) {
      // fall through to generic conversion
    }
    return undefined;
  }

  function temporalRevive(entry) {
    if (typeof Temporal === "undefined") {
      return entry;
    }
    switch (entry.kind) {
      case "Instant":
        return Temporal.Instant.fromEpochNanoseconds(BigInt(entry.epoch_ns));
      case "ZonedDateTime":
        return new Temporal.ZonedDateTime(BigInt(entry.epoch_ns), entry.time_zone);
      case "PlainDate":
        return new Temporal.PlainDate(entry.year, entry.month, entry.day);
      case "PlainTime": {
        const ns = Number(entry.nanosecond);
        return new Temporal.PlainTime(
          entry.hour, entry.minute, entry.second,
          Math.floor(ns / 1e6), Math.floor(ns / 1e3) % 1000, ns % 1000,
        );
      }
      case "PlainDateTime": {
        const ns = Number(entry.nanosecond);
        return new Temporal.PlainDateTime(
          entry.year, entry.month, entry.day,
          entry.hour, entry.minute, entry.second,
          Math.floor(ns / 1e6), Math.floor(ns / 1e3) % 1000, ns % 1000,
        );
      }
      case "Duration": {
        const total = BigInt(entry.ns);
        const sign = total < 0n ? -1n : 1n;
        const abs = total * sign;
        return new Temporal.Duration(
          0, 0, 0, 0, 0, 0,
          Number((abs / 1000000000n) * sign), 0, 0,
          Number((abs % 1000000000n) * sign),
        );
      }
    }
    return entry;
  }
  globalThis.__jsrun_temporal_prepare = temporalPrepare;
  globalThis.__jsrun_temporal_revive = temporalRevive;

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
    const temporal = temporalPrepare(value);
    if (temporal !== undefined) {
      return temporal;
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
        case "Temporal":
          return temporalRevive(value);
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
