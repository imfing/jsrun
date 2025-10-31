//! Runtime thread backed by `deno_core::JsRuntime`.
//!
//! This module hosts the JavaScript engine on a dedicated OS thread with a
//! single-threaded Tokio runtime. Commands from Python are forwarded through
//! [`RuntimeCommand`] and executed sequentially on that thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::js_value::{JSValue, LimitTracker, MAX_JS_BYTES, MAX_JS_DEPTH};
use crate::runtime::ops::{python_extension, PythonOpMode, PythonOpRegistry};
use deno_core::error::CoreError;
use deno_core::{v8, JsRuntime, PollEventLoopOptions, RuntimeOptions};
use indexmap::IndexMap;
use pyo3::prelude::Py;
use pyo3::PyAny;
use pyo3_async_runtimes::TaskLocals;
use std::collections::HashSet;
use std::sync::mpsc::Receiver as StdReceiver;
use std::sync::mpsc::Sender as StdSender;
use std::sync::mpsc::Sender;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type InitSignalChannel = (
    StdSender<Result<(), String>>,
    StdReceiver<Result<(), String>>,
);

/// Commands sent to the runtime thread.
pub enum RuntimeCommand {
    Eval {
        code: String,
        responder: Sender<Result<JSValue, String>>,
    },
    EvalAsync {
        code: String,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<Result<JSValue, String>>,
    },
    RegisterPythonOp {
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
        responder: Sender<Result<u32, String>>,
    },
    Shutdown {
        responder: Sender<()>,
    },
}

pub fn spawn_runtime_thread(
    config: RuntimeConfig,
) -> Result<mpsc::UnboundedSender<RuntimeCommand>, String> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RuntimeCommand>();
    let (init_tx, init_rx): InitSignalChannel = std::sync::mpsc::channel();

    std::thread::Builder::new()
        .name("jsrun-deno-runtime".to_string())
        .spawn(move || {
            let tokio_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");

            let mut core = match RuntimeCore::new(config) {
                Ok(core) => {
                    let _ = init_tx.send(Ok(()));
                    core
                }
                Err(err) => {
                    let _ = init_tx.send(Err(err));
                    return;
                }
            };

            tokio_rt.block_on(async move {
                core.run(cmd_rx).await;
            });
        })
        .map_err(|e| format!("Failed to spawn runtime thread: {}", e))?;

    match init_rx.recv() {
        Ok(Ok(())) => Ok(cmd_tx),
        Ok(Err(err)) => Err(err),
        Err(_) => Err("Runtime thread initialization failed".to_string()),
    }
}

struct RuntimeCore {
    js_runtime: JsRuntime,
    registry: PythonOpRegistry,
    task_locals: Option<TaskLocals>,
    execution_timeout: Option<Duration>,
}

impl RuntimeCore {
    fn new(config: RuntimeConfig) -> Result<Self, String> {
        let registry = PythonOpRegistry::new();
        let extension = python_extension(registry.clone());

        let RuntimeConfig {
            max_heap_size,
            initial_heap_size,
            execution_timeout,
            bootstrap_script,
        } = config;

        if initial_heap_size.is_some() && max_heap_size.is_none() {
            return Err("initial_heap_size requires max_heap_size to be set as well".to_string());
        }

        if let (Some(initial), Some(max)) = (initial_heap_size, max_heap_size) {
            if initial > max {
                return Err(format!(
                    "initial_heap_size ({}) cannot exceed max_heap_size ({})",
                    initial, max
                ));
            }
        }

        let create_params = match (max_heap_size, initial_heap_size) {
            (Some(max), initial) => {
                let initial_bytes = initial.unwrap_or(0);
                Some(v8::CreateParams::default().heap_limits(initial_bytes, max))
            }
            (None, _) => None,
        };

        let mut js_runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![extension],
            create_params,
            ..Default::default()
        });

        if let Some(script) = bootstrap_script {
            js_runtime
                .execute_script("<bootstrap>", script)
                .map_err(|err| err.to_string())?;
        }

        Ok(Self {
            js_runtime,
            registry,
            task_locals: None,
            execution_timeout,
        })
    }

    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<RuntimeCommand>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                RuntimeCommand::Eval { code, responder } => {
                    let result = self.eval_sync(&code);
                    let _ = responder.send(result);
                }
                RuntimeCommand::EvalAsync {
                    code,
                    timeout_ms,
                    task_locals,
                    responder,
                } => {
                    // Update task_locals if provided
                    if let Some(ref locals) = task_locals {
                        self.task_locals = Some(locals.clone());
                        // Update the OpState with the new task_locals
                        self.js_runtime
                            .op_state()
                            .borrow_mut()
                            .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                    }
                    let result = self.eval_async(code, timeout_ms).await;
                    let _ = responder.send(result);
                }
                RuntimeCommand::RegisterPythonOp {
                    name,
                    mode,
                    handler,
                    responder,
                } => {
                    let result = self.register_python_op(name, mode, handler);
                    let _ = responder.send(result);
                }
                RuntimeCommand::Shutdown { responder } => {
                    let _ = responder.send(());
                    break;
                }
            }
        }
    }

    fn register_python_op(
        &self,
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
    ) -> Result<u32, String> {
        Ok(self.registry.register(name, mode, handler))
    }

    fn eval_sync(&mut self, code: &str) -> Result<JSValue, String> {
        let global_value = self
            .js_runtime
            .execute_script("<eval>", code.to_string())
            .map_err(|err| err.to_string())?;

        let scope = &mut self.js_runtime.handle_scope();
        let local = deno_core::v8::Local::new(scope, global_value);
        value_to_js_value(scope, local)
    }

    async fn eval_async(
        &mut self,
        code: String,
        timeout_ms: Option<u64>,
    ) -> Result<JSValue, String> {
        let timeout_ms = timeout_ms.or_else(|| {
            self.execution_timeout.map(|duration| {
                let millis = duration.as_millis();
                if millis > u128::from(u64::MAX) {
                    u64::MAX
                } else {
                    millis as u64
                }
            })
        });

        let global_value = self
            .js_runtime
            .execute_script("<eval_async>", code.clone())
            .map_err(|err| err.to_string())?;

        let resolve_future = self.js_runtime.resolve(global_value);
        let poll_options = PollEventLoopOptions::default();

        let resolved = if let Some(ms) = timeout_ms {
            tokio::time::timeout(
                Duration::from_millis(ms),
                self.js_runtime
                    .with_event_loop_promise(resolve_future, poll_options),
            )
            .await
            .map_err(|_| format!("Evaluation timed out after {}ms", ms))?
            .map_err(to_string)?
        } else {
            self.js_runtime
                .with_event_loop_promise(resolve_future, poll_options)
                .await
                .map_err(to_string)?
        };

        let scope = &mut self.js_runtime.handle_scope();
        let local = deno_core::v8::Local::new(scope, resolved);
        value_to_js_value(scope, local)
    }
}

fn to_string(error: CoreError) -> String {
    error.to_string()
}

/// Convert a V8 value to JSValue with circular reference detection and limits enforced
fn value_to_js_value<'s>(
    scope: &mut deno_core::v8::HandleScope<'s>,
    value: deno_core::v8::Local<'s, deno_core::v8::Value>,
) -> Result<JSValue, String> {
    let mut seen = HashSet::new();
    let mut tracker = LimitTracker::new(MAX_JS_DEPTH, MAX_JS_BYTES);
    value_to_js_value_internal(scope, value, &mut seen, &mut tracker)
}

/// Internal recursive converter with cycle detection
fn value_to_js_value_internal<'s>(
    scope: &mut deno_core::v8::HandleScope<'s>,
    value: deno_core::v8::Local<'s, deno_core::v8::Value>,
    seen: &mut HashSet<i32>,
    tracker: &mut LimitTracker,
) -> Result<JSValue, String> {
    tracker.enter()?;

    let result = if value.is_null() || value.is_undefined() {
        tracker.add_bytes(4)?; // "null" or "undefined"
        Ok(JSValue::Null)
    } else if value.is_boolean() {
        tracker.add_bytes(5)?; // "false" (worst case)
        Ok(JSValue::Bool(value.boolean_value(scope)))
    } else if value.is_number() {
        // Handle special numeric values (NaN, ±Infinity)
        let num_obj = value
            .to_number(scope)
            .ok_or_else(|| "Failed to convert value to number".to_string())?;
        let num_val = num_obj.value();
        if num_val.is_nan() || num_val.is_infinite() {
            tracker.add_bytes(24)?;
            Ok(JSValue::Float(num_val))
        } else if num_val.fract() == 0.0 && num_val.is_finite() {
            let as_int = num_val as i64;
            if as_int as f64 == num_val {
                tracker.add_bytes(20)?;
                Ok(JSValue::Int(as_int))
            } else {
                tracker.add_bytes(24)?;
                Ok(JSValue::Float(num_val))
            }
        } else {
            tracker.add_bytes(24)?;
            Ok(JSValue::Float(num_val))
        }
    } else if value.is_string() {
        let string = value
            .to_string(scope)
            .ok_or_else(|| "Failed to convert string".to_string())?;
        let rust_str = string.to_rust_string_lossy(scope);
        tracker.add_bytes(rust_str.len())?;
        Ok(JSValue::String(rust_str))
    } else if value.is_big_int() {
        // Try to convert BigInt to i64, otherwise error
        let bigint = deno_core::v8::Local::<deno_core::v8::BigInt>::try_from(value)
            .map_err(|_| "Failed to cast to BigInt".to_string())?;
        let (val, lossless) = bigint.i64_value();
        if lossless {
            tracker.add_bytes(20)?;
            Ok(JSValue::Int(val))
        } else {
            Err("BigInt value too large to represent as i64".to_string())
        }
    } else if value.is_function() || value.is_symbol() {
        Err("Cannot serialize V8 function or symbol".to_string())
    } else if value.is_array() {
        // Check for circular reference using identity hash
        let obj = deno_core::v8::Local::<deno_core::v8::Object>::try_from(value)
            .map_err(|_| "Failed to cast array to object".to_string())?;
        let hash = obj.get_identity_hash().get();

        if !seen.insert(hash) {
            return Err("Cannot serialize circular reference".to_string());
        }

        let array = deno_core::v8::Local::<deno_core::v8::Array>::try_from(value)
            .map_err(|_| "Failed to cast to array".to_string())?;
        let len = array.length() as usize;

        let mut items = Vec::with_capacity(len);
        for i in 0..len {
            let idx = i as u32;
            let item = array
                .get_index(scope, idx)
                .ok_or_else(|| format!("Failed to get array index {}", i))?;
            items.push(value_to_js_value_internal(scope, item, seen, tracker)?);
        }

        seen.remove(&hash);
        Ok(JSValue::Array(items))
    } else if value.is_object() {
        // Check for circular reference using identity hash
        let obj = deno_core::v8::Local::<deno_core::v8::Object>::try_from(value)
            .map_err(|_| "Failed to cast to object".to_string())?;
        let hash = obj.get_identity_hash().get();

        if !seen.insert(hash) {
            return Err("Cannot serialize circular reference".to_string());
        }

        // Get property names
        let prop_names = obj
            .get_own_property_names(scope, deno_core::v8::GetPropertyNamesArgs::default())
            .ok_or_else(|| "Failed to get property names".to_string())?;

        let mut map = IndexMap::new();
        for i in 0..prop_names.length() {
            let key = prop_names
                .get_index(scope, i)
                .ok_or_else(|| "Failed to get property name".to_string())?;
            let key_str = key
                .to_string(scope)
                .ok_or_else(|| "Failed to convert key to string".to_string())?
                .to_rust_string_lossy(scope);

            let val = obj
                .get(scope, key)
                .ok_or_else(|| format!("Failed to get property '{}'", key_str))?;

            tracker.add_bytes(key_str.len())?;
            map.insert(
                key_str,
                value_to_js_value_internal(scope, val, seen, tracker)?,
            );
        }

        seen.remove(&hash);
        Ok(JSValue::Object(map))
    } else {
        // Fallback: convert to string
        let string = value
            .to_string(scope)
            .ok_or_else(|| "Failed to convert value to string".to_string())?;
        let rust_str = string.to_rust_string_lossy(scope);
        tracker.add_bytes(rust_str.len())?;
        Ok(JSValue::String(rust_str))
    };

    tracker.exit();
    result
}
