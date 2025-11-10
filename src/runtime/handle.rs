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

#[derive(Clone)]
pub struct RuntimeHandle {
    tx: Option<async_mpsc::UnboundedSender<RuntimeCommand>>,
    shutdown: Arc<Mutex<bool>>,
    termination: TerminationController,
    tracked_functions: Arc<Mutex<HashSet<u32>>>,
    tracked_js_streams: Arc<Mutex<HashSet<u32>>>,
    tracked_py_streams: Arc<Mutex<HashSet<u32>>>,
    inspector_metadata: Arc<Mutex<Option<InspectorMetadata>>>,
    inspector_connection: Option<InspectorConnectionState>,
    serialization_limits: SerializationLimits,
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

    fn sender(&self) -> RuntimeResult<&async_mpsc::UnboundedSender<RuntimeCommand>> {
        if self.termination.is_requested() || self.termination.is_terminated() {
            return Err(RuntimeError::terminated());
        }
        if *self.shutdown.lock().unwrap() {
            return Err(RuntimeError::internal("Runtime has been shut down"));
        }
        self.tx
            .as_ref()
            .ok_or_else(|| RuntimeError::internal("Runtime has been shut down"))
    }

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

    // Call a JavaScript function asynchronously.
    /// Invoke a previously registered JavaScript function on the runtime thread.
    ///
    /// The `fn_id` must originate from the same runtime; arguments are transferred as
    /// `JSValue`s and executed with the runtime's async event loop.
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

    pub fn cancel_py_stream_async(&self, stream_id: u32) {
        let registry = self.py_stream_registry.clone();
        pyo3_tokio::get_runtime().spawn(async move {
            if let Err(err) = registry.cancel(stream_id).await {
                log::debug!("PyStream cancellation for id {} failed: {}", stream_id, err);
            }
        });
        self.untrack_py_stream_id(stream_id);
    }

    pub fn release_py_stream(&self, stream_id: u32) {
        self.py_stream_registry.release(stream_id);
        self.untrack_py_stream_id(stream_id);
    }

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

    pub fn inspector_connection(&self) -> Option<InspectorConnectionState> {
        self.inspector_connection.clone()
    }

    pub fn is_shutdown(&self) -> bool {
        self.termination.is_requested()
            || self.termination.is_terminated()
            || *self.shutdown.lock().unwrap()
    }

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

    pub fn close(&mut self) -> RuntimeResult<()> {
        let mut shutdown_guard = self.shutdown.lock().unwrap();
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
                }
                Err(_) => {
                    return Err(RuntimeError::internal("Failed to confirm runtime shutdown"));
                }
            }
        }

        Ok(())
    }

    pub fn track_function_id(&self, fn_id: u32) {
        let mut set = self.tracked_functions.lock().unwrap();
        set.insert(fn_id);
    }

    pub fn untrack_function_id(&self, fn_id: u32) {
        let mut set = self.tracked_functions.lock().unwrap();
        set.remove(&fn_id);
    }

    pub fn drain_tracked_function_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_functions.lock().unwrap();
        set.drain().collect()
    }

    pub fn track_js_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.insert(stream_id);
    }

    pub fn untrack_js_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.remove(&stream_id);
    }

    pub fn drain_tracked_js_stream_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_js_streams.lock().unwrap();
        set.drain().collect()
    }

    pub fn track_py_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.insert(stream_id);
    }

    pub fn untrack_py_stream_id(&self, stream_id: u32) {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.remove(&stream_id);
    }

    pub fn drain_tracked_py_stream_ids(&self) -> Vec<u32> {
        let mut set = self.tracked_py_streams.lock().unwrap();
        set.drain().collect()
    }

    pub fn is_function_tracked(&self, fn_id: u32) -> bool {
        let set = self.tracked_functions.lock().unwrap();
        set.contains(&fn_id)
    }

    pub fn tracked_function_count(&self) -> usize {
        let set = self.tracked_functions.lock().unwrap();
        set.len()
    }

    pub fn inspector_metadata(&self) -> Option<InspectorMetadata> {
        self.inspector_metadata.lock().unwrap().clone()
    }

    pub fn serialization_limits(&self) -> SerializationLimits {
        self.serialization_limits
    }
}
