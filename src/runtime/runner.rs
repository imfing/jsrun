//! Runtime thread backed by `deno_core::JsRuntime`.
//!
//! This module hosts the JavaScript engine on a dedicated OS thread with a
//! single-threaded Tokio runtime. Commands from Python are forwarded through
//! [`RuntimeCommand`] and executed sequentially on that thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::error::{JsExceptionDetails, RuntimeError, RuntimeResult};
use crate::runtime::js_value::{JSValue, LimitTracker, MAX_JS_BYTES, MAX_JS_DEPTH};
use crate::runtime::loader::PythonModuleLoader;
use crate::runtime::ops::{python_extension, PythonOpMode, PythonOpRegistry};
use crate::runtime::stats::{
    ActivitySummary, HeapSnapshot, RuntimeCallKind, RuntimeStatsSnapshot, RuntimeStatsState,
};
use deno_core::error::{CoreError, JsError};
use deno_core::stats::{RuntimeActivityStatsFactory, RuntimeActivityStatsFilter};
use deno_core::{v8, JsRuntime, PollEventLoopOptions, RuntimeOptions};
use indexmap::IndexMap;
use num_bigint::{BigInt, Sign};
use pyo3::prelude::Py;
use pyo3::PyAny;
use pyo3_async_runtimes::TaskLocals;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::Receiver as StdReceiver;
use std::sync::mpsc::Sender as StdSender;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type InitSignalChannel = (
    StdSender<RuntimeResult<TerminationController>>,
    StdReceiver<RuntimeResult<TerminationController>>,
);

/// Stored function with optional receiver for 'this' binding
struct StoredFunction {
    function: v8::Global<v8::Function>,
    receiver: Option<v8::Global<v8::Value>>,
}

const TERMINATION_STATUS_RUNNING: u8 = 0;
const TERMINATION_STATUS_REQUESTED: u8 = 1;
const TERMINATION_STATUS_TERMINATED: u8 = 2;

#[derive(Clone)]
pub struct TerminationController {
    inner: Arc<TerminationState>,
}

struct TerminationState {
    status: AtomicU8,
    isolate_handle: v8::IsolateHandle,
}

struct SyncWatchdog {
    handle: thread::JoinHandle<()>,
    fired: Arc<AtomicBool>,
    cancel_flag: Arc<AtomicBool>,
    duration: Duration,
}

impl TerminationController {
    fn new(isolate_handle: v8::IsolateHandle) -> Self {
        Self {
            inner: Arc::new(TerminationState {
                status: AtomicU8::new(TERMINATION_STATUS_RUNNING),
                isolate_handle,
            }),
        }
    }

    pub fn request(&self) -> bool {
        self.inner
            .status
            .compare_exchange(
                TERMINATION_STATUS_RUNNING,
                TERMINATION_STATUS_REQUESTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn terminate_execution(&self) {
        self.inner.isolate_handle.terminate_execution();
    }

    pub fn is_requested(&self) -> bool {
        matches!(
            self.inner.status.load(Ordering::SeqCst),
            TERMINATION_STATUS_REQUESTED | TERMINATION_STATUS_TERMINATED
        )
    }

    pub fn is_terminated(&self) -> bool {
        self.inner.status.load(Ordering::SeqCst) == TERMINATION_STATUS_TERMINATED
    }

    fn mark_terminated(&self) -> bool {
        self.inner
            .status
            .swap(TERMINATION_STATUS_TERMINATED, Ordering::SeqCst)
            != TERMINATION_STATUS_TERMINATED
    }
}

/// Commands sent to the runtime thread.
pub enum RuntimeCommand {
    Eval {
        code: String,
        responder: Sender<RuntimeResult<JSValue>>,
    },
    EvalAsync {
        code: String,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    EvalModule {
        specifier: String,
        responder: Sender<RuntimeResult<JSValue>>,
    },
    EvalModuleAsync {
        specifier: String,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    RegisterPythonOp {
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
        responder: Sender<RuntimeResult<u32>>,
    },
    SetModuleResolver {
        handler: Py<PyAny>,
        responder: Sender<RuntimeResult<()>>,
    },
    SetModuleLoader {
        handler: Py<PyAny>,
        responder: Sender<RuntimeResult<()>>,
    },
    AddStaticModule {
        name: String,
        source: String,
        responder: Sender<RuntimeResult<()>>,
    },
    CallFunctionAsync {
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    ReleaseFunction {
        fn_id: u32,
        responder: oneshot::Sender<RuntimeResult<()>>,
    },
    GetStats {
        responder: Sender<RuntimeResult<RuntimeStatsSnapshot>>,
    },
    Terminate {
        responder: Sender<RuntimeResult<()>>,
    },
    Shutdown {
        responder: Sender<()>,
    },
}

pub fn spawn_runtime_thread(
    config: RuntimeConfig,
) -> RuntimeResult<(mpsc::UnboundedSender<RuntimeCommand>, TerminationController)> {
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
                    let termination = core.termination_controller();
                    let _ = init_tx.send(Ok(termination));
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
        .map_err(|e| RuntimeError::internal(format!("Failed to spawn runtime thread: {}", e)))?;

    match init_rx.recv() {
        Ok(Ok(termination)) => Ok((cmd_tx, termination)),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(RuntimeError::internal(
            "Runtime thread initialization failed",
        )),
    }
}

struct RuntimeCore {
    js_runtime: JsRuntime,
    registry: PythonOpRegistry,
    module_loader: Rc<PythonModuleLoader>,
    task_locals: Option<TaskLocals>,
    execution_timeout: Option<Duration>,
    fn_registry: Rc<RefCell<HashMap<u32, StoredFunction>>>,
    next_fn_id: Rc<RefCell<u32>>,
    stats_state: RuntimeStatsState,
    termination: TerminationController,
    terminated: bool,
}

impl RuntimeCore {
    fn new(config: RuntimeConfig) -> RuntimeResult<Self> {
        let registry = PythonOpRegistry::new();
        let extension = python_extension(registry.clone());
        let module_loader = Rc::new(PythonModuleLoader::new());

        let RuntimeConfig {
            max_heap_size,
            initial_heap_size,
            execution_timeout,
            bootstrap_script,
        } = config;

        if initial_heap_size.is_some() && max_heap_size.is_none() {
            return Err(RuntimeError::internal(
                "initial_heap_size requires max_heap_size to be set as well",
            ));
        }

        if let (Some(initial), Some(max)) = (initial_heap_size, max_heap_size) {
            if initial > max {
                return Err(RuntimeError::internal(format!(
                    "initial_heap_size ({}) cannot exceed max_heap_size ({})",
                    initial, max
                )));
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
            module_loader: Some(module_loader.clone()),
            ..Default::default()
        });

        if let Some(script) = bootstrap_script {
            js_runtime
                .execute_script("<bootstrap>", script)
                .map_err(|err| RuntimeError::javascript(JsExceptionDetails::from_js_error(*err)))?;
        }

        let termination = {
            let isolate = js_runtime.v8_isolate();
            let handle = isolate.thread_safe_handle();
            TerminationController::new(handle)
        };

        Ok(Self {
            js_runtime,
            registry,
            module_loader,
            task_locals: None,
            execution_timeout,
            fn_registry: Rc::new(RefCell::new(HashMap::new())),
            next_fn_id: Rc::new(RefCell::new(0)),
            stats_state: RuntimeStatsState::default(),
            termination,
            terminated: false,
        })
    }

    fn termination_controller(&self) -> TerminationController {
        self.termination.clone()
    }

    async fn run(&mut self, mut rx: mpsc::UnboundedReceiver<RuntimeCommand>) {
        while let Some(cmd) = rx.recv().await {
            let mut break_loop = false;
            match cmd {
                RuntimeCommand::Eval { code, responder } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    let watchdog = match self.start_sync_watchdog() {
                        Ok(watchdog) => watchdog,
                        Err(err) => {
                            let _ = responder.send(Err(err));
                            continue;
                        }
                    };
                    let result = self.eval_sync(&code);
                    let result = self.apply_watchdog_result(result, watchdog, "Sync evaluation");
                    let _ = responder.send(result);
                }
                RuntimeCommand::EvalAsync {
                    code,
                    timeout_ms,
                    task_locals,
                    responder,
                } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    if let Some(ref locals) = task_locals {
                        self.task_locals = Some(locals.clone());
                        self.module_loader.set_task_locals(locals.clone());
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
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    let result = self.register_python_op(name, mode, handler);
                    let _ = responder.send(result);
                }
                RuntimeCommand::SetModuleResolver { handler, responder } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    self.module_loader.set_resolver(handler);
                    if let Some(ref locals) = self.task_locals {
                        self.module_loader.set_task_locals(locals.clone());
                    }
                    let _ = responder.send(Ok(()));
                }
                RuntimeCommand::SetModuleLoader { handler, responder } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    self.module_loader.set_loader(handler);
                    if let Some(ref locals) = self.task_locals {
                        self.module_loader.set_task_locals(locals.clone());
                    }
                    let _ = responder.send(Ok(()));
                }
                RuntimeCommand::AddStaticModule {
                    name,
                    source,
                    responder,
                } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    self.module_loader.add_static_module(name, source);
                    let _ = responder.send(Ok(()));
                }
                RuntimeCommand::EvalModule {
                    specifier,
                    responder,
                } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    let watchdog = match self.start_sync_watchdog() {
                        Ok(watchdog) => watchdog,
                        Err(err) => {
                            let _ = responder.send(Err(err));
                            continue;
                        }
                    };
                    let result = self.eval_module_sync(&specifier);
                    let result =
                        self.apply_watchdog_result(result, watchdog, "Sync module evaluation");
                    let _ = responder.send(result);
                }
                RuntimeCommand::EvalModuleAsync {
                    specifier,
                    timeout_ms,
                    task_locals,
                    responder,
                } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    if let Some(ref locals) = task_locals {
                        self.task_locals = Some(locals.clone());
                        self.module_loader.set_task_locals(locals.clone());
                        self.js_runtime
                            .op_state()
                            .borrow_mut()
                            .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                    }
                    let result = self.eval_module_async(specifier, timeout_ms).await;
                    let _ = responder.send(result);
                }
                RuntimeCommand::CallFunctionAsync {
                    fn_id,
                    args,
                    timeout_ms,
                    task_locals,
                    responder,
                } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    if let Some(ref locals) = task_locals {
                        self.task_locals = Some(locals.clone());
                        self.module_loader.set_task_locals(locals.clone());
                        self.js_runtime
                            .op_state()
                            .borrow_mut()
                            .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                    }
                    let result = self.call_function_async(fn_id, args, timeout_ms).await;
                    let _ = responder.send(result);
                }
                RuntimeCommand::ReleaseFunction { fn_id, responder } => {
                    if self.should_reject_new_work() {
                        let _ = responder.send(Err(RuntimeError::terminated()));
                        continue;
                    }
                    let result = self.release_function(fn_id);
                    let _ = responder.send(result);
                }
                RuntimeCommand::GetStats { responder } => {
                    let result = self.collect_stats();
                    let _ = responder.send(result);
                }
                RuntimeCommand::Terminate { responder } => {
                    let result = self.finalize_termination();
                    let _ = responder.send(result);
                    break_loop = true;
                }
                RuntimeCommand::Shutdown { responder } => {
                    let leaked_count = self.fn_registry.borrow().len();
                    if leaked_count > 0 {
                        eprintln!(
                            "[jsrun] Warning: {} function handles not released before shutdown",
                            leaked_count
                        );
                    }
                    self.fn_registry.borrow_mut().clear();
                    let _ = responder.send(());
                    break_loop = true;
                }
            }
            if break_loop {
                rx.close();
                break;
            }
        }
    }

    fn should_reject_new_work(&self) -> bool {
        self.terminated || self.termination.is_requested()
    }

    fn start_sync_watchdog(&self) -> RuntimeResult<Option<SyncWatchdog>> {
        match self.execution_timeout {
            None => Ok(None),
            Some(duration) => {
                let fired = Arc::new(AtomicBool::new(false));
                let fired_for_thread = fired.clone();
                let cancel_flag = Arc::new(AtomicBool::new(false));
                let cancel_for_thread = cancel_flag.clone();
                let termination = self.termination.clone();
                let handle = thread::Builder::new()
                    .name("jsrun-sync-watchdog".to_string())
                    .spawn(move || {
                        let deadline = Instant::now() + duration;
                        loop {
                            if cancel_for_thread.load(Ordering::Acquire) {
                                return;
                            }

                            let now = Instant::now();
                            if now >= deadline {
                                fired_for_thread.store(true, Ordering::Release);
                                termination.terminate_execution();
                                return;
                            }

                            let remaining = deadline.saturating_duration_since(now);
                            let sleep_dur = remaining.min(Duration::from_millis(10));
                            thread::sleep(sleep_dur);
                        }
                    })
                    .map_err(|e| {
                        RuntimeError::internal(format!("Failed to spawn watchdog thread: {}", e))
                    })?;
                Ok(Some(SyncWatchdog {
                    handle,
                    fired,
                    cancel_flag,
                    duration,
                }))
            }
        }
    }

    fn resolve_sync_watchdog(&mut self, watchdog: SyncWatchdog) -> RuntimeResult<(bool, Duration)> {
        watchdog.cancel_flag.store(true, Ordering::Release);
        if watchdog.handle.join().is_err() {
            return Err(RuntimeError::internal("Watchdog thread panicked"));
        }
        let fired = watchdog.fired.load(Ordering::Acquire);
        if fired {
            let isolate = self.js_runtime.v8_isolate();
            let _ = isolate.cancel_terminate_execution();
        }
        Ok((fired, watchdog.duration))
    }

    fn apply_watchdog_result(
        &mut self,
        result: RuntimeResult<JSValue>,
        watchdog: Option<SyncWatchdog>,
        context: &str,
    ) -> RuntimeResult<JSValue> {
        if let Some(watchdog) = watchdog {
            let (fired, duration) = self.resolve_sync_watchdog(watchdog)?;
            if fired {
                let message = format!("{context} timed out after {}ms", duration.as_millis());
                return match result {
                    Err(err) if Self::runtime_error_indicates_termination(&err) => {
                        Err(RuntimeError::timeout(message))
                    }
                    Err(err) => Err(err),
                    Ok(_) => Err(RuntimeError::timeout(message)),
                };
            }
        }
        result
    }

    fn finalize_termination(&mut self) -> RuntimeResult<()> {
        if self.terminated {
            return Ok(());
        }

        let isolate = self.js_runtime.v8_isolate();
        // ignore return value; false indicates no termination was pending.
        let _ = isolate.cancel_terminate_execution();

        self.fn_registry.borrow_mut().clear();
        self.termination.mark_terminated();
        self.terminated = true;
        Ok(())
    }

    fn translate_js_error(&mut self, err: JsError) -> RuntimeError {
        let details = JsExceptionDetails::from_js_error(err);
        if self.should_reject_new_work() && Self::js_error_indicates_termination(&details) {
            let _ = self.finalize_termination();
            RuntimeError::terminated()
        } else {
            RuntimeError::javascript(details)
        }
    }

    fn translate_core_error(&mut self, err: CoreError) -> RuntimeError {
        let runtime_error = RuntimeError::from(err);
        if self.should_reject_new_work()
            && Self::runtime_error_indicates_termination(&runtime_error)
        {
            let _ = self.finalize_termination();
            RuntimeError::terminated()
        } else {
            runtime_error
        }
    }

    fn runtime_error_indicates_termination(err: &RuntimeError) -> bool {
        match err {
            RuntimeError::JavaScript(details) => Self::js_error_indicates_termination(details),
            RuntimeError::Timeout { context } | RuntimeError::Internal { context } => {
                context.contains("execution terminated")
            }
            RuntimeError::Terminated => true,
        }
    }

    fn js_error_indicates_termination(details: &JsExceptionDetails) -> bool {
        let needle = "execution terminated";
        details
            .message
            .as_deref()
            .map(|msg| msg.contains(needle))
            .unwrap_or(false)
            || details.summary().contains(needle)
    }

    fn register_python_op(
        &self,
        name: String,
        mode: PythonOpMode,
        handler: Py<PyAny>,
    ) -> RuntimeResult<u32> {
        Ok(self.registry.register(name, mode, handler))
    }

    /// Measure the duration of a synchronous entry point, including error paths.
    fn with_timing<T, F>(&mut self, kind: RuntimeCallKind, f: F) -> RuntimeResult<T>
    where
        F: FnOnce(&mut Self) -> RuntimeResult<T>,
    {
        let start = Instant::now();
        let result = f(self);
        let elapsed = start.elapsed();
        self.stats_state.record(kind, elapsed);
        result
    }

    fn eval_sync(&mut self, code: &str) -> RuntimeResult<JSValue> {
        self.with_timing(RuntimeCallKind::EvalSync, |this| {
            let global_value = this
                .js_runtime
                .execute_script("<eval>", code.to_string())
                .map_err(|err| this.translate_js_error(*err))?;

            let scope = &mut this.js_runtime.handle_scope();
            let local = deno_core::v8::Local::new(scope, global_value);
            Self::value_to_js_value(&this.fn_registry, &this.next_fn_id, scope, local)
        })
    }

    async fn eval_async(
        &mut self,
        code: String,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
        let start = Instant::now();
        let result = self.eval_async_inner(code, timeout_ms).await;
        let elapsed = start.elapsed();
        self.stats_state.record(RuntimeCallKind::EvalAsync, elapsed);
        result
    }

    async fn eval_async_inner(
        &mut self,
        code: String,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
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
            .map_err(|err| self.translate_js_error(*err))?;

        let resolve_future = self.js_runtime.resolve(global_value);
        let poll_options = PollEventLoopOptions::default();

        let resolved = if let Some(ms) = timeout_ms {
            tokio::time::timeout(
                Duration::from_millis(ms),
                self.js_runtime
                    .with_event_loop_promise(resolve_future, poll_options),
            )
            .await
            .map_err(|_| RuntimeError::timeout(format!("Evaluation timed out after {}ms", ms)))?
            .map_err(|err| self.translate_core_error(err))?
        } else {
            self.js_runtime
                .with_event_loop_promise(resolve_future, poll_options)
                .await
                .map_err(|err| self.translate_core_error(err))?
        };

        let scope = &mut self.js_runtime.handle_scope();
        let local = deno_core::v8::Local::new(scope, resolved);
        Self::value_to_js_value(&self.fn_registry, &self.next_fn_id, scope, local)
    }

    fn eval_module_sync(&mut self, specifier: &str) -> RuntimeResult<JSValue> {
        self.with_timing(RuntimeCallKind::EvalModuleSync, |this| {
            // Try to parse as absolute URL first, if it fails, resolve it as a bare specifier
            let module_specifier = if specifier.contains(':') || specifier.starts_with('/') {
                // Already a URL or absolute path
                deno_core::ModuleSpecifier::parse(specifier).map_err(|e| {
                    RuntimeError::internal(format!(
                        "Invalid module specifier '{}': {}",
                        specifier, e
                    ))
                })?
            } else {
                // Bare specifier - resolve relative to a synthetic base
                let base = deno_core::ModuleSpecifier::parse("jsrun://runtime/").map_err(|e| {
                    RuntimeError::internal(format!("Failed to create base URL: {}", e))
                })?;
                base.join(specifier).map_err(|e| {
                    RuntimeError::internal(format!(
                        "Failed to resolve module specifier '{}': {}",
                        specifier, e
                    ))
                })?
            };

            // Load the module
            let module_id =
                futures::executor::block_on(this.js_runtime.load_main_es_module(&module_specifier))
                    .map_err(|e| {
                        RuntimeError::internal(format!(
                            "Failed to load module '{}': {}",
                            specifier, e
                        ))
                    })?;

            // Evaluate the module
            let receiver = this.js_runtime.mod_evaluate(module_id);

            // Poll the runtime until the module evaluation completes
            let poll_options = PollEventLoopOptions::default();
            futures::executor::block_on(this.js_runtime.run_event_loop(poll_options))
                .map_err(|err| this.translate_core_error(err))?;

            // Wait for the evaluation result - receiver returns Result<(), CoreError>
            let eval_result = futures::executor::block_on(receiver);

            // Check if evaluation succeeded
            if let Err(err) = eval_result {
                return Err(this.translate_core_error(err));
            }

            // Get the module namespace - must call get_module_namespace before handle_scope
            let module_namespace =
                this.js_runtime
                    .get_module_namespace(module_id)
                    .map_err(|e| {
                        RuntimeError::internal(format!("Failed to get module namespace: {}", e))
                    })?;
            let scope = &mut this.js_runtime.handle_scope();
            let local = deno_core::v8::Local::new(scope, module_namespace);
            Self::value_to_js_value(&this.fn_registry, &this.next_fn_id, scope, local.into())
        })
    }

    async fn eval_module_async(
        &mut self,
        specifier: String,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
        let start = Instant::now();
        let result = self.eval_module_async_inner(specifier, timeout_ms).await;
        let elapsed = start.elapsed();
        self.stats_state
            .record(RuntimeCallKind::EvalModuleAsync, elapsed);
        result
    }

    async fn eval_module_async_inner(
        &mut self,
        specifier: String,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
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

        // Try to parse as absolute URL first, if it fails, resolve it as a bare specifier
        let module_specifier = if specifier.contains(':') || specifier.starts_with('/') {
            // Already a URL or absolute path
            deno_core::ModuleSpecifier::parse(&specifier).map_err(|e| {
                RuntimeError::internal(format!("Invalid module specifier '{}': {}", specifier, e))
            })?
        } else {
            // Bare specifier - resolve relative to a synthetic base
            let base = deno_core::ModuleSpecifier::parse("jsrun://runtime/")
                .map_err(|e| RuntimeError::internal(format!("Failed to create base URL: {}", e)))?;
            base.join(&specifier).map_err(|e| {
                RuntimeError::internal(format!(
                    "Failed to resolve module specifier '{}': {}",
                    specifier, e
                ))
            })?
        };

        // Load the module
        let module_id = self
            .js_runtime
            .load_main_es_module(&module_specifier)
            .await
            .map_err(|e| {
                RuntimeError::internal(format!("Failed to load module '{}': {}", specifier, e))
            })?;

        // Evaluate the module
        let receiver = self.js_runtime.mod_evaluate(module_id);

        // Poll the runtime until the module evaluation completes
        let poll_options = PollEventLoopOptions::default();

        if let Some(ms) = timeout_ms {
            tokio::time::timeout(
                Duration::from_millis(ms),
                self.js_runtime.run_event_loop(poll_options),
            )
            .await
            .map_err(|_| {
                RuntimeError::timeout(format!("Module evaluation timed out after {}ms", ms))
            })?
            .map_err(|err| self.translate_core_error(err))?
        } else {
            self.js_runtime
                .run_event_loop(poll_options)
                .await
                .map_err(|err| self.translate_core_error(err))?
        };

        // Wait for the evaluation result - receiver returns Result<(), CoreError>
        let eval_result = receiver.await;

        // Check if evaluation succeeded
        if let Err(err) = eval_result {
            return Err(self.translate_core_error(err));
        }

        // Get the module namespace - must call get_module_namespace before handle_scope
        let module_namespace = self
            .js_runtime
            .get_module_namespace(module_id)
            .map_err(|e| {
                RuntimeError::internal(format!("Failed to get module namespace: {}", e))
            })?;
        let scope = &mut self.js_runtime.handle_scope();
        let local = deno_core::v8::Local::new(scope, module_namespace);
        Self::value_to_js_value(&self.fn_registry, &self.next_fn_id, scope, local.into())
    }

    /// Execute a stored JavaScript function by id with the provided arguments.
    async fn call_function_async(
        &mut self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
        let start = Instant::now();
        let result = self
            .call_function_async_inner(fn_id, args, timeout_ms)
            .await;
        let elapsed = start.elapsed();
        self.stats_state
            .record(RuntimeCallKind::CallFunctionAsync, elapsed);
        result
    }

    async fn call_function_async_inner(
        &mut self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<JSValue> {
        // Determine timeout
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

        // Look up function in registry and call it
        let result_global: Result<
            Result<deno_core::v8::Global<deno_core::v8::Value>, JsError>,
            RuntimeError,
        > = {
            let scope = &mut self.js_runtime.handle_scope();
            let mut try_catch = v8::TryCatch::new(scope);

            let (func, receiver) = {
                let registry = self.fn_registry.borrow();
                let stored = registry.get(&fn_id).ok_or_else(|| {
                    RuntimeError::internal(format!("Function ID {} not found", fn_id))
                })?;
                let func = deno_core::v8::Local::new(&mut try_catch, &stored.function);
                let receiver = stored
                    .receiver
                    .as_ref()
                    .map(|r| deno_core::v8::Local::new(&mut try_catch, r));
                (func, receiver)
            };

            // Convert arguments from JSValue to v8::Value
            let mut v8_args = Vec::with_capacity(args.len());
            for arg in &args {
                let v8_val = Self::js_value_to_v8(&self.fn_registry, &mut try_catch, arg)?;
                v8_args.push(v8_val);
            }

            let call_receiver = receiver.unwrap_or_else(|| {
                try_catch
                    .get_current_context()
                    .global(&mut try_catch)
                    .into()
            });

            // Call the function
            match func.call(&mut try_catch, call_receiver, &v8_args) {
                Some(value) => Ok(Ok(deno_core::v8::Global::new(&mut try_catch, value))),
                None => match try_catch.exception() {
                    Some(exception) => {
                        let js_error = JsError::from_v8_exception(&mut try_catch, exception);
                        Ok(Err(*js_error))
                    }
                    None => Err(RuntimeError::internal("Function call failed")),
                },
            }
            // scope and try_catch dropped here
        };

        let result_global = match result_global {
            Ok(Ok(global)) => global,
            Ok(Err(js_error)) => return Err(self.translate_js_error(js_error)),
            Err(err) => return Err(err),
        };

        // Resolve promises (reuse eval_async pattern)
        let resolve_future = self.js_runtime.resolve(result_global);
        let poll_options = PollEventLoopOptions::default();

        let resolved = if let Some(ms) = timeout_ms {
            tokio::time::timeout(
                Duration::from_millis(ms),
                self.js_runtime
                    .with_event_loop_promise(resolve_future, poll_options),
            )
            .await
            .map_err(|_| RuntimeError::timeout(format!("Function call timed out after {}ms", ms)))?
            .map_err(|err| self.translate_core_error(err))?
        } else {
            self.js_runtime
                .with_event_loop_promise(resolve_future, poll_options)
                .await
                .map_err(|err| self.translate_core_error(err))?
        };

        // Convert back to JSValue
        let scope = &mut self.js_runtime.handle_scope();
        let local = deno_core::v8::Local::new(scope, resolved);
        Self::value_to_js_value(&self.fn_registry, &self.next_fn_id, scope, local)
    }

    /// Remove a function from the registry, freeing its V8 global handle.
    fn release_function(&mut self, fn_id: u32) -> RuntimeResult<()> {
        let mut registry = self.fn_registry.borrow_mut();
        if registry.remove(&fn_id).is_none() {
            log::debug!("Attempted to release unknown function id {}", fn_id);
        }
        Ok(())
    }

    fn collect_stats(&mut self) -> RuntimeResult<RuntimeStatsSnapshot> {
        let heap = self.snapshot_memory_usage();
        let execution = self.stats_state.snapshot();
        let activity = self.snapshot_activity();
        Ok(RuntimeStatsSnapshot::new(heap, execution, activity))
    }

    /// Snapshot V8 heap statistics. `get_heap_statistics` is safe here because it only reads isolate state.
    fn snapshot_memory_usage(&mut self) -> HeapSnapshot {
        let stats = self.js_runtime.v8_isolate().get_heap_statistics();
        HeapSnapshot {
            heap_total_bytes: stats.total_heap_size() as u64,
            heap_used_bytes: stats.used_heap_size() as u64,
            external_memory_bytes: stats.external_memory() as u64,
            physical_total_bytes: stats.total_physical_size() as u64,
        }
    }

    fn snapshot_activity(&self) -> ActivitySummary {
        let factory: RuntimeActivityStatsFactory = self.js_runtime.runtime_activity_stats_factory();
        let filter = RuntimeActivityStatsFilter::all();
        let snapshot = factory.capture(&filter).dump();
        ActivitySummary::from_snapshot(snapshot)
    }

    /// Convert a V8 value to JSValue with circular reference detection and limits enforced.
    fn value_to_js_value<'s>(
        fn_registry: &Rc<RefCell<HashMap<u32, StoredFunction>>>,
        next_fn_id: &Rc<RefCell<u32>>,
        scope: &mut deno_core::v8::HandleScope<'s>,
        value: deno_core::v8::Local<'s, deno_core::v8::Value>,
    ) -> RuntimeResult<JSValue> {
        let mut seen = HashSet::new();
        let mut tracker = LimitTracker::new(MAX_JS_DEPTH, MAX_JS_BYTES);
        Self::value_to_js_value_internal(
            fn_registry,
            next_fn_id,
            scope,
            value,
            &mut seen,
            &mut tracker,
            None,
        )
    }

    fn js_value_to_v8<'s>(
        registry: &Rc<RefCell<HashMap<u32, StoredFunction>>>,
        scope: &mut v8::HandleScope<'s>,
        value: &JSValue,
    ) -> RuntimeResult<v8::Local<'s, v8::Value>> {
        match value {
            JSValue::Undefined => Ok(v8::undefined(scope).into()),
            JSValue::Null => Ok(v8::null(scope).into()),
            JSValue::Bool(b) => Ok(v8::Boolean::new(scope, *b).into()),
            JSValue::Int(i) => Ok(v8::Number::new(scope, *i as f64).into()),
            JSValue::BigInt(bigint) => {
                let (sign, bytes) = bigint.to_bytes_le();
                let mut words = Vec::with_capacity(bytes.len().div_ceil(8));
                for chunk in bytes.chunks(8) {
                    let mut buf = [0u8; 8];
                    buf[..chunk.len()].copy_from_slice(chunk);
                    words.push(u64::from_le_bytes(buf));
                }
                let sign_bit = matches!(sign, Sign::Minus);
                let v8_bigint = v8::BigInt::new_from_words(scope, sign_bit, &words)
                    .ok_or_else(|| RuntimeError::internal("Failed to create BigInt"))?;
                Ok(v8_bigint.into())
            }
            JSValue::Float(f) => Ok(v8::Number::new(scope, *f).into()),
            JSValue::String(s) => {
                let v8_str = v8::String::new(scope, s)
                    .ok_or_else(|| RuntimeError::internal("Failed to allocate string"))?;
                Ok(v8_str.into())
            }
            JSValue::Bytes(bytes) => {
                let backing = v8::ArrayBuffer::new_backing_store_from_vec(bytes.clone());
                let shared = backing.make_shared();
                let buffer = v8::ArrayBuffer::with_backing_store(scope, &shared);
                let len = bytes.len();
                let typed = v8::Uint8Array::new(scope, buffer, 0, len)
                    .ok_or_else(|| RuntimeError::internal("Failed to create Uint8Array"))?;
                Ok(typed.into())
            }
            JSValue::Array(items) => {
                let array = v8::Array::new(scope, items.len() as i32);
                for (index, item) in items.iter().enumerate() {
                    let v8_value = Self::js_value_to_v8(registry, scope, item)?;
                    array
                        .set_index(scope, index as u32, v8_value)
                        .ok_or_else(|| RuntimeError::internal("Failed to set array element"))?;
                }
                Ok(array.into())
            }
            JSValue::Set(values) => {
                let set = v8::Set::new(scope);
                for value in values {
                    let v8_value = Self::js_value_to_v8(registry, scope, value)?;
                    set.add(scope, v8_value);
                }
                Ok(set.into())
            }
            JSValue::Object(map) => {
                let object = v8::Object::new(scope);
                for (key, val) in map.iter() {
                    let key_str = v8::String::new(scope, key).ok_or_else(|| {
                        RuntimeError::internal(format!("Failed to allocate key '{key}'"))
                    })?;
                    let v8_value = Self::js_value_to_v8(registry, scope, val)?;
                    object.set(scope, key_str.into(), v8_value).ok_or_else(|| {
                        RuntimeError::internal(format!("Failed to set property '{key}'"))
                    })?;
                }
                Ok(object.into())
            }
            JSValue::Date(epoch_ms) => {
                let date = v8::Date::new(scope, *epoch_ms as f64)
                    .ok_or_else(|| RuntimeError::internal("Failed to create Date"))?;
                Ok(date.into())
            }
            JSValue::Function { id } => {
                let registry_ref = registry.borrow();
                let stored = registry_ref.get(id).ok_or_else(|| {
                    RuntimeError::internal(format!("Function ID {} not found in args", id))
                })?;
                Ok(v8::Local::new(scope, &stored.function).into())
            }
        }
    }

    /// Internal recursive converter with cycle detection and optional receiver capture.
    fn value_to_js_value_internal<'s>(
        fn_registry: &Rc<RefCell<HashMap<u32, StoredFunction>>>,
        next_fn_id: &Rc<RefCell<u32>>,
        scope: &mut deno_core::v8::HandleScope<'s>,
        value: deno_core::v8::Local<'s, deno_core::v8::Value>,
        seen: &mut HashSet<i32>,
        tracker: &mut LimitTracker,
        receiver: Option<deno_core::v8::Global<deno_core::v8::Value>>,
    ) -> RuntimeResult<JSValue> {
        tracker.enter()?;

        let result = if value.is_undefined() {
            tracker.add_bytes(0)?;
            Ok(JSValue::Undefined)
        } else if value.is_null() {
            tracker.add_bytes(4)?;
            Ok(JSValue::Null)
        } else if value.is_boolean() {
            tracker.add_bytes(5)?; // "false" (worst case)
            Ok(JSValue::Bool(value.boolean_value(scope)))
        } else if value.is_number() {
            // Handle special numeric values (NaN, ±Infinity)
            let num_obj = value
                .to_number(scope)
                .ok_or_else(|| RuntimeError::internal("Failed to convert value to number"))?;
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
        } else if value.is_big_int() {
            let bigint = deno_core::v8::Local::<deno_core::v8::BigInt>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to BigInt"))?;
            let (int_value, lossless) = bigint.i64_value();
            if lossless {
                tracker.add_bytes(20)?;
                Ok(JSValue::Int(int_value))
            } else {
                let string = bigint
                    .to_string(scope)
                    .ok_or_else(|| RuntimeError::internal("Failed to stringify BigInt"))?
                    .to_rust_string_lossy(scope);
                let parsed = BigInt::parse_bytes(string.as_bytes(), 10)
                    .ok_or_else(|| RuntimeError::internal("Failed to parse BigInt literal"))?;
                tracker.add_bytes(string.len())?;
                Ok(JSValue::BigInt(parsed))
            }
        } else if value.is_string() {
            let string = value
                .to_string(scope)
                .ok_or_else(|| RuntimeError::internal("Failed to convert string"))?;
            let rust_str = string.to_rust_string_lossy(scope);
            tracker.add_bytes(rust_str.len())?;
            Ok(JSValue::String(rust_str))
        } else if value.is_function() {
            // Register function and return proxy ID
            let func = deno_core::v8::Local::<deno_core::v8::Function>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to function"))?;

            // Create a Global handle to keep the function alive
            let fn_handle = deno_core::v8::Global::new(scope, func);

            // Register in the function registry
            let mut registry = fn_registry.borrow_mut();
            let mut next_id_val = next_fn_id.borrow_mut();

            let fn_id = *next_id_val;
            *next_id_val += 1;

            registry.insert(
                fn_id,
                StoredFunction {
                    function: fn_handle,
                    receiver, // Capture receiver for 'this' binding
                },
            );

            tracker.add_bytes(8)?; // ID size
            Ok(JSValue::Function { id: fn_id })
        } else if value.is_symbol() {
            Err(RuntimeError::internal("Cannot serialize V8 symbol"))
        } else if value.is_uint8_array() {
            let typed_array = deno_core::v8::Local::<deno_core::v8::Uint8Array>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Uint8Array"))?;
            let length = typed_array.byte_length();
            tracker.add_bytes(length)?;
            let mut buffer = vec![0u8; length];
            let view: deno_core::v8::Local<deno_core::v8::ArrayBufferView> = typed_array.into();
            view.copy_contents(&mut buffer);
            Ok(JSValue::Bytes(buffer))
        } else if value.is_array_buffer() {
            let array_buffer = deno_core::v8::Local::<deno_core::v8::ArrayBuffer>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to ArrayBuffer"))?;
            let length = array_buffer.byte_length();
            tracker.add_bytes(length)?;
            let mut buffer = vec![0u8; length];
            if length > 0 {
                if let Some(data_ptr) = array_buffer.data() {
                    unsafe {
                        ptr::copy_nonoverlapping(
                            data_ptr.as_ptr() as *const u8,
                            buffer.as_mut_ptr(),
                            length,
                        );
                    }
                }
            }
            Ok(JSValue::Bytes(buffer))
        } else if value.is_array() {
            // Check for circular reference using identity hash
            let obj = deno_core::v8::Local::<deno_core::v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast array to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            let array = deno_core::v8::Local::<deno_core::v8::Array>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to array"))?;
            let len = array.length() as usize;

            let mut items = Vec::with_capacity(len);
            for i in 0..len {
                let idx = i as u32;
                let item = array.get_index(scope, idx).ok_or_else(|| {
                    RuntimeError::internal(format!("Failed to get array index {}", i))
                })?;
                items.push(Self::value_to_js_value_internal(
                    fn_registry,
                    next_fn_id,
                    scope,
                    item,
                    seen,
                    tracker,
                    None,
                )?);
            }

            seen.remove(&hash);
            Ok(JSValue::Array(items))
        } else if value.is_set() {
            let obj = deno_core::v8::Local::<deno_core::v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast set to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            let set = deno_core::v8::Local::<deno_core::v8::Set>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Set"))?;
            let entries = set.as_array(scope);
            let len = entries.length() as usize;

            tracker.add_bytes(24)?;
            tracker.add_bytes(len.saturating_mul(std::mem::size_of::<usize>()))?;

            let mut values = Vec::with_capacity(len);
            for index in 0..len {
                let element = entries
                    .get_index(scope, index as u32)
                    .ok_or_else(|| RuntimeError::internal("Failed to get Set entry"))?;
                values.push(Self::value_to_js_value_internal(
                    fn_registry,
                    next_fn_id,
                    scope,
                    element,
                    seen,
                    tracker,
                    None,
                )?);
            }

            seen.remove(&hash);
            Ok(JSValue::Set(values))
        } else if value.is_date() {
            let date = deno_core::v8::Local::<deno_core::v8::Date>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Date"))?;
            let epoch_ms = date.value_of();
            if !epoch_ms.is_finite() || epoch_ms < i64::MIN as f64 || epoch_ms > i64::MAX as f64 {
                return Err(RuntimeError::internal("Date value out of range"));
            }
            tracker.add_bytes(16)?;
            Ok(JSValue::Date(epoch_ms.round() as i64))
        } else if value.is_object() {
            // Check for circular reference using identity hash
            let obj = deno_core::v8::Local::<deno_core::v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            // Get property names
            let prop_names = obj
                .get_own_property_names(scope, deno_core::v8::GetPropertyNamesArgs::default())
                .ok_or_else(|| RuntimeError::internal("Failed to get property names"))?;

            let mut map = IndexMap::new();
            for i in 0..prop_names.length() {
                let key = prop_names
                    .get_index(scope, i)
                    .ok_or_else(|| RuntimeError::internal("Failed to get property name"))?;
                let key_str = key
                    .to_string(scope)
                    .ok_or_else(|| RuntimeError::internal("Failed to convert key to string"))?
                    .to_rust_string_lossy(scope);

                let val = obj.get(scope, key).ok_or_else(|| {
                    RuntimeError::internal(format!("Failed to get property '{}'", key_str))
                })?;

                // If the value is a function, capture the object as the receiver for 'this' binding
                let receiver_for_val = if val.is_function() {
                    let obj_as_value: deno_core::v8::Local<deno_core::v8::Value> = obj.into();
                    Some(deno_core::v8::Global::new(scope, obj_as_value))
                } else {
                    None
                };

                tracker.add_bytes(key_str.len())?;
                map.insert(
                    key_str,
                    Self::value_to_js_value_internal(
                        fn_registry,
                        next_fn_id,
                        scope,
                        val,
                        seen,
                        tracker,
                        receiver_for_val,
                    )?,
                );
            }

            seen.remove(&hash);
            Ok(JSValue::Object(map))
        } else {
            // Fallback: convert to string
            let string = value
                .to_string(scope)
                .ok_or_else(|| RuntimeError::internal("Failed to convert value to string"))?;
            let rust_str = string.to_rust_string_lossy(scope);
            tracker.add_bytes(rust_str.len())?;
            Ok(JSValue::String(rust_str))
        };

        tracker.exit();
        result
    }
}
