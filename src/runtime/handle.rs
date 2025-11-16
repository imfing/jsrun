//! Python-facing handle for interacting with the runtime thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::inspector::{InspectorConnectionState, InspectorMetadata};
use crate::runtime::js_value::{JSValue, SerializationLimits};
use crate::runtime::ops::PythonOpMode;
use crate::runtime::runner::{
    spawn_runtime_thread, FunctionCallResult, RuntimeCommand, TerminationController,
};
use crate::runtime::stats::RuntimeStatsSnapshot;
use crate::runtime::stream::{PyStreamRegistry, StreamChunk};
use pyo3::prelude::Py;
use pyo3::PyAny;
use pyo3_async_runtimes::{tokio as pyo3_tokio, TaskLocals};
use std::collections::HashSet;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::oneshot;

/// Thread-safe handle for communicating with a JavaScript runtime thread.
///
/// Each `RuntimeHandle` owns a channel to a dedicated runtime thread running a V8 isolate
/// and Tokio event loop. The handle can be cloned to share access across threads, and commands
/// are sent via an unbounded async channel.
///
/// The handle does NOT automatically shut down on drop - callers must explicitly call
/// [`RuntimeHandle::close`] or [`RuntimeHandle::terminate`] to clean up resources.
#[derive(Clone)]
pub struct RuntimeHandle {
    /// Command channel to the runtime thread (None after shutdown).
    tx: Option<async_mpsc::UnboundedSender<RuntimeCommand>>,
    /// Shutdown state shared across clones.
    shutdown: Arc<Mutex<bool>>,
    /// Controller for V8 execution termination.
    termination: TerminationController,
    /// Tracked JavaScript function handles (for cleanup).
    tracked_functions: Arc<Mutex<HashSet<u32>>>,
    /// Tracked JavaScript stream IDs (for cleanup).
    tracked_js_streams: Arc<Mutex<HashSet<u32>>>,
    /// Tracked Python stream IDs (for cleanup).
    tracked_py_streams: Arc<Mutex<HashSet<u32>>>,
    /// Inspector metadata (address, endpoints) if inspector is enabled.
    inspector_metadata: Arc<Mutex<Option<InspectorMetadata>>>,
    /// Inspector connection state for DevTools protocol.
    inspector_connection: Option<InspectorConnectionState>,
    /// Serialization limits for Python<->JS value transfers.
    serialization_limits: SerializationLimits,
    /// Registry for Python async iterables exposed as JS streams.
    py_stream_registry: PyStreamRegistry,
}

/// Represents a property assignment when binding Python objects into the JS global namespace.
#[derive(Debug)]
pub(crate) enum BoundObjectProperty {
    Value {
        key: String,
        value: JSValue,
    },
    Op {
        key: String,
        op_id: u32,
        mode: PythonOpMode,
    },
}

impl RuntimeHandle {
    /// Spawn a new runtime thread with the given configuration.
    ///
    /// This starts a dedicated OS thread running a V8 isolate and Tokio event loop.
    /// Returns a handle for sending commands to the runtime.
    ///
    /// # Errors
    /// Returns an error if the runtime thread fails to start or initialize.
    pub fn spawn(config: RuntimeConfig) -> RuntimeResult<Self> {
        let serialization_limits = config.serialization_limits();
        let (tx, termination, inspector_info, py_stream_registry) = spawn_runtime_thread(config)?;
        let (metadata, connection) = inspector_info
            .map(|(meta, state)| (Some(meta), Some(state)))
            .unwrap_or((None, None));
        let tracked_functions = Arc::new(Mutex::new(HashSet::new()));
        let tracked_js_streams = Arc::new(Mutex::new(HashSet::new()));
        let tracked_py_streams = Arc::new(Mutex::new(HashSet::new()));
        {
            let tracked = Arc::downgrade(&tracked_py_streams);
            py_stream_registry.add_release_listener(move |stream_id| {
                if let Some(set) = tracked.upgrade() {
                    let mut guard = set.lock().unwrap();
                    guard.remove(&stream_id);
                }
            });
        }
        Ok(Self {
            tx: Some(tx),
            shutdown: Arc::new(Mutex::new(false)),
            termination,
            tracked_functions,
            tracked_js_streams,
            tracked_py_streams,
            inspector_metadata: Arc::new(Mutex::new(metadata)),
            inspector_connection: connection,
            serialization_limits,
            py_stream_registry,
        })
    }

    /// Get a reference to the command sender, checking shutdown state first.
    ///
    /// # Errors
    /// Returns an error if runtime is terminated or shut down.
    fn sender(&self) -> RuntimeResult<&async_mpsc::UnboundedSender<RuntimeCommand>> {
        if self.termination.is_requested() || self.termination.is_terminated() {
            return Err(self.termination.terminated_error());
        }
        if *self.shutdown.lock().unwrap() {
            return Err(RuntimeError::internal("Runtime has been shut down"));
        }
        self.tx
            .as_ref()
            .ok_or_else(|| RuntimeError::internal("Runtime has been shut down"))
    }

    /// Evaluate JavaScript code synchronously and return the result.
    ///
    /// Blocks the calling thread until evaluation completes. The code runs in the
    /// global scope and can access previously defined variables/functions.
    ///
    /// # Errors
    /// Returns an error if the code throws an exception or the runtime is shut down.
    pub fn eval_sync(&self, code: &str) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::Eval {
                code: code.to_string(),
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send eval command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive eval result"))?
    }

    /// Evaluate JavaScript code asynchronously with optional timeout.
    ///
    /// If the code returns a promise, waits for it to resolve while polling the event loop.
    /// The `task_locals` parameter provides the asyncio context for Python ops.
    ///
    /// # Errors
    /// Returns an error if the code throws, times out, or the runtime is shut down.
    pub async fn eval_async(
        &self,
        code: &str,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
    ) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::EvalAsync {
                code: code.to_string(),
                timeout_ms,
                task_locals,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send eval_async command"))?;

        result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive async eval result"))?
    }

    /// Register a Python callable as an op callable from JavaScript.
    ///
    /// The `mode` specifies whether the handler is sync or async. Returns an op ID
    /// that can be used to bind the op to JavaScript.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down or registration fails.
    pub fn register_op(
        &self,
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
    ) -> RuntimeResult<u32> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::RegisterPythonOp {
                name,
                mode,
                handler,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send register_op command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive op registration result"))?
    }

    /// Set a custom Python resolver for module specifier resolution.
    ///
    /// The resolver receives `(specifier, referrer)` and returns a resolved URL or None.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down or the command fails.
    pub fn set_module_resolver(&self, handler: Py<PyAny>) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::SetModuleResolver {
                handler,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send set_module_resolver command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive set_module_resolver result"))?
    }

    /// Set a custom Python loader for fetching module source code.
    ///
    /// The loader receives a resolved specifier and returns the module source.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down or the command fails.
    pub fn set_module_loader(&self, handler: Py<PyAny>) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::SetModuleLoader {
                handler,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send set_module_loader command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive set_module_loader result"))?
    }

    /// Register a static ES module with pre-defined source code.
    ///
    /// Static modules can be imported without a custom loader.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down or the command fails.
    pub fn add_static_module(&self, name: String, source: String) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::AddStaticModule {
                name,
                source,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send add_static_module command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive add_static_module result"))?
    }

    /// Bind a Python object to the JavaScript global namespace.
    ///
    /// Creates a JavaScript object with the given name and properties (values and ops).
    /// Internal API used by the Python bindings.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down or the command fails.
    pub(crate) fn bind_object(
        &self,
        name: String,
        properties: Vec<BoundObjectProperty>,
    ) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::BindObject {
                name,
                properties,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send bind_object command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive bind_object result"))?
    }

    /// Evaluate an ES module synchronously and return its namespace object.
    ///
    /// Loads and evaluates the module, blocking until completion.
    ///
    /// # Errors
    /// Returns an error if the module fails to load/evaluate or the runtime is shut down.
    pub fn eval_module_sync(&self, specifier: &str) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::EvalModule {
                specifier: specifier.to_string(),
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send eval_module command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive eval_module result"))?
    }

    /// Evaluate an ES module asynchronously with optional timeout.
    ///
    /// Loads and evaluates the module, waiting for top-level await if present.
    ///
    /// # Errors
    /// Returns an error if the module fails, times out, or the runtime is shut down.
    pub async fn eval_module_async(
        &self,
        specifier: &str,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
    ) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::EvalModuleAsync {
                specifier: specifier.to_string(),
                timeout_ms,
                task_locals,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send eval_module_async command"))?;

        result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive async eval_module result"))?
    }

    /// Call a JavaScript function synchronously with optional timeout.
    ///
    /// If the function returns a promise, returns `FunctionCallResult::Pending` with a call ID
    /// that can be used to resume polling. Otherwise, returns the immediate result.
    ///
    /// # Errors
    /// Returns an error if the function throws or the runtime is shut down.
    pub fn call_function_sync(
        &self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<FunctionCallResult> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::CallFunctionSync {
                fn_id,
                args,
                timeout_ms,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send call_function_sync command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive function call result"))?
    }

    /// Call a JavaScript function asynchronously with optional timeout.
    ///
    /// Waits for the function to complete (including promise resolution) before returning.
    ///
    /// # Errors
    /// Returns an error if the function throws, times out, or the runtime is shut down.
    pub async fn call_function_async(
        &self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
    ) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::CallFunctionAsync {
                fn_id,
                args,
                timeout_ms,
                task_locals,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send call_function command"))?;

        result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive function call result"))?
    }

    /// Resume polling a pending function call by its call ID.
    ///
    /// Used to continue waiting for a promise returned by `call_function_sync`.
    ///
    /// # Errors
    /// Returns an error if the call ID is invalid or the runtime is shut down.
    pub async fn resume_function_call(
        &self,
        call_id: u64,
        task_locals: Option<TaskLocals>,
    ) -> RuntimeResult<JSValue> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::ResumeFunctionCall {
                call_id,
                task_locals,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send resume_function_call command"))?;

        result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive resumed function call result"))?
    }

    /// Release a function handle so the underlying V8 global can be dropped.
    pub fn release_function(&self, fn_id: u32) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::ReleaseFunction {
                fn_id,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send release_function command"))?;

        result_rx
            .blocking_recv()
            .map_err(|_| RuntimeError::internal("Failed to receive release result"))?
    }

    /// Release a function handle asynchronously.
    ///
    /// Async variant of `release_function` for use in async contexts.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down.
    pub async fn release_function_async(&self, fn_id: u32) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::ReleaseFunction {
                fn_id,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send release_function command"))?;

        result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive release result"))?
    }

    /// Read the next chunk from a JavaScript ReadableStream.
    ///
    /// Returns a chunk with `done=true` when the stream ends.
    ///
    /// # Errors
    /// Returns an error if the stream ID is invalid or reading fails.
    pub async fn stream_read(&self, stream_id: u32) -> RuntimeResult<StreamChunk> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::StreamRead {
                stream_id,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send stream_read command"))?;

        let chunk_value = result_rx
            .await
            .map_err(|_| RuntimeError::internal("Failed to receive stream chunk"))??;
        let chunk = StreamChunk::from_js_value(chunk_value)?;
        if chunk.done {
            self.untrack_js_stream_id(stream_id);
        }
        Ok(chunk)
    }

    /// Release a JavaScript stream handle.
    ///
    /// Drops the V8 global handle and reader, allowing garbage collection.
    ///
    /// # Errors
    /// Returns an error if the stream ID is invalid or the runtime is shut down.
    pub fn stream_release(&self, stream_id: u32) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::StreamRelease {
                stream_id,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send stream_release command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive stream_release result"))??;
        self.untrack_js_stream_id(stream_id);
        Ok(())
    }

    /// Cancel a JavaScript stream.
    ///
    /// Calls the stream's cancel method and releases the handle.
    ///
    /// # Errors
    /// Returns an error if the stream ID is invalid or the runtime is shut down.
    pub fn stream_cancel(&self, stream_id: u32) -> RuntimeResult<()> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::StreamCancel {
                stream_id,
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send stream_cancel command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive stream_cancel result"))??;
        self.untrack_js_stream_id(stream_id);
        Ok(())
    }

    /// Register a Python async iterable as a stream.
    ///
    /// Returns a stream ID that can be used to create a JavaScript ReadableStream.
    ///
    /// # Errors
    /// Returns an error if registration fails.
    pub fn register_py_stream(
        &self,
        iterable: Py<PyAny>,
        task_locals: TaskLocals,
    ) -> RuntimeResult<u32> {
        let stream_id = self
            .py_stream_registry
            .register_iterable(iterable, task_locals)?;
        self.track_py_stream_id(stream_id);
        Ok(stream_id)
    }

    /// Cancel a Python stream asynchronously.
    ///
    /// Spawns a task to cancel the stream without blocking.
    pub fn cancel_py_stream_async(&self, stream_id: u32) {
        let registry = self.py_stream_registry.clone();
        pyo3_tokio::get_runtime().spawn(async move {
            if let Err(err) = registry.cancel(stream_id).await {
                log::debug!("PyStream cancellation for id {} failed: {}", stream_id, err);
            }
        });
        self.untrack_py_stream_id(stream_id);
    }

    /// Release a Python stream handle.
    ///
    /// Removes the stream from the registry and untracks it.
    pub fn release_py_stream(&self, stream_id: u32) {
        self.py_stream_registry.release(stream_id);
        self.untrack_py_stream_id(stream_id);
    }

    /// Get current runtime statistics snapshot.
    ///
    /// Returns metrics about heap usage, call counts, and stream activity.
    ///
    /// # Errors
    /// Returns an error if the runtime is shut down.
    pub fn get_stats(&self) -> RuntimeResult<RuntimeStatsSnapshot> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::GetStats {
                responder: result_tx,
            })
            .map_err(|_| RuntimeError::internal("Failed to send get_stats command"))?;

        result_rx
            .recv()
            .map_err(|_| RuntimeError::internal("Failed to receive stats result"))?
    }

    /// Get the inspector connection state if inspector is enabled.
    pub fn inspector_connection(&self) -> Option<InspectorConnectionState> {
        self.inspector_connection.clone()
    }

    /// Check if the runtime has been shut down or terminated.
    pub fn is_shutdown(&self) -> bool {
        self.termination.is_requested()
            || self.termination.is_terminated()
            || *self.shutdown.lock().unwrap()
    }

    /// Forcefully terminate the runtime by canceling V8 execution.
    ///
    /// This triggers V8's execution termination mechanism, which interrupts any running
    /// JavaScript code. The runtime thread will shut down after processing the termination.
    ///
    /// # Errors
    /// Returns an error if sending the termination command fails.
    pub fn terminate(&self) -> RuntimeResult<()> {
        if self.termination.is_terminated() {
            return Ok(());
        }

        let tx = match self.tx.as_ref() {
            Some(sender) => sender.clone(),
            None => {
                *self.shutdown.lock().unwrap() = true;
                return Ok(());
            }
        };

        self.termination.ensure_reason("Terminated by host request");
        let first_request = self.termination.request();
        if !first_request {
            while !self.termination.is_terminated() {
                thread::sleep(Duration::from_millis(1));
            }
            return Ok(());
        }

        let (result_tx, result_rx) = mpsc::channel();

        tx.send(RuntimeCommand::Terminate {
            responder: result_tx,
        })
        .map_err(|_| RuntimeError::internal("Failed to send terminate command"))?;

        self.termination.terminate_execution();

        match result_rx.recv() {
            Ok(result) => {
                if result.is_ok() {
                    *self.shutdown.lock().unwrap() = true;
                }
                result
            }
            Err(_) => Err(RuntimeError::internal(
                "Failed to receive terminate confirmation",
            )),
        }
    }

    /// Gracefully shut down the runtime thread.
    ///
    /// Sends a shutdown command and waits for the thread to exit cleanly.
    /// This consumes the command channel and marks the handle as shut down.
    ///
    /// # Errors
    /// Returns an error if the shutdown command fails to send or confirm.
    pub fn close(&mut self) -> RuntimeResult<()> {
        let mut shutdown_guard = self.shutdown.lock().unwrap();
        log::debug!(
            "RuntimeHandle::close invoked (shutdown={}, termination_requested={}, terminated={})",
            *shutdown_guard,
            self.termination.is_requested(),
            self.termination.is_terminated()
        );
        if *shutdown_guard {
            return Ok(());
        }

        if self.termination.is_requested() || self.termination.is_terminated() {
            self.tx.take();
            *shutdown_guard = true;
            return Ok(());
        }

        if let Some(tx) = self.tx.take() {
            let (result_tx, result_rx) = mpsc::channel();
            if tx
                .send(RuntimeCommand::Shutdown {
                    responder: result_tx,
                })
                .is_err()
            {
                return Err(RuntimeError::internal("Failed to send shutdown command"));
            }

            match result_rx.recv() {
                Ok(_) => {
                    *shutdown_guard = true;
                    log::debug!("RuntimeHandle::close completed shutdown");
                }
                Err(_) => {
                    log::warn!("RuntimeHandle::close failed to confirm runtime shutdown");
                    return Err(RuntimeError::internal("Failed to confirm runtime shutdown"));
                }
            }
        }

        log::debug!("RuntimeHandle::close exit (shutdown={})", *shutdown_guard);
        Ok(())
    }

    /// Track a function ID for cleanup on handle drop.
    pub fn track_function_id(&self, fn_id: u32) {
        let mut set = self.tracked_functions.lock().unwrap();
        set.insert(fn_id);
    }

    /// Untrack a function ID.
    pub fn untrack_function_id(&self, fn_id: u32) {
        let mut set = self.tracked_functions.lock().unwrap();
        set.remove(&fn_id);
    }

    /// Drain all tracked function IDs for cleanup.
    pub fn drain_tracked_function_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_functions.lock().unwrap();
        set.drain().collect()
    }

    /// Track a JavaScript stream ID for cleanup on handle drop.
    pub fn track_js_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.insert(stream_id);
    }

    /// Untrack a JavaScript stream ID.
    pub fn untrack_js_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.remove(&stream_id);
    }

    /// Drain all tracked JavaScript stream IDs for cleanup.
    pub fn drain_tracked_js_stream_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.drain().collect()
    }

    /// Track a Python stream ID for cleanup on handle drop.
    pub fn track_py_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.insert(stream_id);
    }

    /// Untrack a Python stream ID.
    pub fn untrack_py_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.remove(&stream_id);
    }

    /// Drain all tracked Python stream IDs for cleanup.
    pub fn drain_tracked_py_stream_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.drain().collect()
    }

    /// Check if a function ID is currently tracked.
    pub fn is_function_tracked(&self, fn_id: u32) -> bool {
        let set = self.tracked_functions.lock().unwrap();
        set.contains(&fn_id)
    }

    /// Get the count of tracked function handles.
    pub fn tracked_function_count(&self) -> usize {
        let set = self.tracked_functions.lock().unwrap();
        set.len()
    }

    /// Get inspector metadata (endpoints, display name) if inspector is enabled.
    pub fn inspector_metadata(&self) -> Option<InspectorMetadata> {
        self.inspector_metadata.lock().unwrap().clone()
    }

    /// Get the serialization limits for this runtime.
    pub fn serialization_limits(&self) -> SerializationLimits {
        self.serialization_limits
    }
}
