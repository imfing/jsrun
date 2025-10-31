//! Python-facing handle for interacting with the runtime thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::js_value::JSValue;
use crate::runtime::ops::PythonOpMode;
use crate::runtime::runner::{spawn_runtime_thread, RuntimeCommand};
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
    pub fn spawn(config: RuntimeConfig) -> Result<Self, String> {
        let tx = spawn_runtime_thread(config)?;
        Ok(Self {
            tx: Some(tx),
            shutdown: Arc::new(Mutex::new(false)),
        })
    }

    fn sender(&self) -> Result<&async_mpsc::UnboundedSender<RuntimeCommand>, String> {
        if *self.shutdown.lock().unwrap() {
            return Err("Runtime has been shut down".to_string());
        }
        self.tx
            .as_ref()
            .ok_or_else(|| "Runtime has been shut down".to_string())
    }

    pub fn eval_sync(&self, code: &str) -> Result<JSValue, String> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::Eval {
                code: code.to_string(),
                responder: result_tx,
            })
            .map_err(|_| "Failed to send eval command".to_string())?;

        result_rx
            .recv()
            .map_err(|_| "Failed to receive eval result".to_string())?
    }

    pub async fn eval_async(
        &self,
        code: &str,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
    ) -> Result<JSValue, String> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = oneshot::channel();

        sender
            .send(RuntimeCommand::EvalAsync {
                code: code.to_string(),
                timeout_ms,
                task_locals,
                responder: result_tx,
            })
            .map_err(|_| "Failed to send eval_async command".to_string())?;

        result_rx
            .await
            .map_err(|_| "Failed to receive async eval result".to_string())?
    }

    pub fn register_op(
        &self,
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
    ) -> Result<u32, String> {
        let sender = self.sender()?.clone();
        let (result_tx, result_rx) = mpsc::channel();

        sender
            .send(RuntimeCommand::RegisterPythonOp {
                name,
                mode,
                handler,
                responder: result_tx,
            })
            .map_err(|_| "Failed to send register_op command".to_string())?;

        result_rx
            .recv()
            .map_err(|_| "Failed to receive op registration result".to_string())?
    }

    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().unwrap()
    }

    pub fn close(&mut self) -> Result<(), String> {
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
                return Err("Failed to send shutdown command".to_string());
            }

            match result_rx.recv() {
                Ok(_) => {
                    *shutdown_guard = true;
                }
                Err(_) => {
                    return Err("Failed to confirm runtime shutdown".to_string());
                }
            }
        }

        Ok(())
    }
}
