//! Python-facing handle for interacting with the runtime thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::js_value::JSValue;
use crate::runtime::ops::PythonOpMode;
use crate::runtime::runner::{spawn_runtime_thread, RuntimeCommand};
use crate::runtime::stats::RuntimeStatsSnapshot;
use pyo3::prelude::Py;
use pyo3::PyAny;
use pyo3_async_runtimes::TaskLocals;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc as async_mpsc;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct RuntimeHandle {
    tx: Option<async_mpsc::UnboundedSender<RuntimeCommand>>,
    shutdown: Arc<Mutex<bool>>,
}

impl RuntimeHandle {
    pub fn spawn(config: RuntimeConfig) -> RuntimeResult<Self> {
        let tx = spawn_runtime_thread(config)?;
        Ok(Self {
            tx: Some(tx),
            shutdown: Arc::new(Mutex::new(false)),
        })
    }

    fn sender(&self) -> RuntimeResult<&async_mpsc::UnboundedSender<RuntimeCommand>> {
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

    /// Release a function handle so the underlying V8 global can be dropped.
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

    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().unwrap()
    }

    pub fn close(&mut self) -> RuntimeResult<()> {
        let mut shutdown_guard = self.shutdown.lock().unwrap();
        if *shutdown_guard {
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
}
