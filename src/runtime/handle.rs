//! Python-facing handle for interacting with the runtime thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::js_value::JSValue;
use crate::runtime::ops::PythonOpMode;
use crate::runtime::runner::{spawn_runtime_thread, RuntimeCommand, TerminationController};
use crate::runtime::stats::RuntimeStatsSnapshot;
use pyo3::prelude::Py;
use pyo3::PyAny;
use pyo3_async_runtimes::TaskLocals;
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
}

impl RuntimeHandle {
    pub fn spawn(config: RuntimeConfig) -> RuntimeResult<Self> {
        let (tx, termination) = spawn_runtime_thread(config)?;
        Ok(Self {
            tx: Some(tx),
            shutdown: Arc::new(Mutex::new(false)),
            termination,
            tracked_functions: Arc::new(Mutex::new(HashSet::new())),
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

    pub fn is_function_tracked(&self, fn_id: u32) -> bool {
        let set = self.tracked_functions.lock().unwrap();
        set.contains(&fn_id)
    }

    pub fn tracked_function_count(&self) -> usize {
        let set = self.tracked_functions.lock().unwrap();
        set.len()
    }
}
