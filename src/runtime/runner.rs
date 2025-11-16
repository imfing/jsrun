//! Runtime thread backed by `deno_core::JsRuntime`.
//!
//! This module hosts the JavaScript engine on a dedicated OS thread with a
//! single-threaded Tokio runtime. Commands from Python are forwarded through
//! [`RuntimeCommand`] and executed sequentially on that thread.

use crate::runtime::config::RuntimeConfig;
use crate::runtime::error::{JsExceptionDetails, RuntimeError, RuntimeResult};
use crate::runtime::handle::BoundObjectProperty;
use crate::runtime::inspector::{
    InspectorConnectionState, InspectorMetadata, InspectorRegistration,
    InspectorRegistrationParams, InspectorServer,
};
use crate::runtime::js_value::{JSValue, LimitTracker, SerializationLimits};
use crate::runtime::loader::PythonModuleLoader;
use crate::runtime::ops::{python_extension, PythonOpMode, PythonOpRegistry};
use crate::runtime::stats::{
    ActivitySummary, HeapSnapshot, RuntimeCallKind, RuntimeStatsSnapshot, RuntimeStatsState,
};
use crate::runtime::stream::{JsStreamRegistry, PyStreamRegistry, StreamChunk};
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
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::Receiver as StdReceiver;
use std::sync::mpsc::Sender as StdSender;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type RuntimeInitResult = RuntimeResult<(
    TerminationController,
    Option<(InspectorMetadata, InspectorConnectionState)>,
    PyStreamRegistry,
)>;
type InitSignalChannel = (StdSender<RuntimeInitResult>, StdReceiver<RuntimeInitResult>);
type SpawnRuntimeResult = (
    mpsc::UnboundedSender<RuntimeCommand>,
    TerminationController,
    Option<(InspectorMetadata, InspectorConnectionState)>,
    PyStreamRegistry,
);

static ACTIVE_RUNTIME_THREADS: AtomicUsize = AtomicUsize::new(0);

struct RuntimeThreadGuard;

impl RuntimeThreadGuard {
    fn new() -> Self {
        ACTIVE_RUNTIME_THREADS.fetch_add(1, Ordering::SeqCst);
        Self
    }
}

impl Drop for RuntimeThreadGuard {
    fn drop(&mut self) {
        ACTIVE_RUNTIME_THREADS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Stored function with optional receiver for 'this' binding.
///
/// Holds a V8 global handle to a JavaScript function and an optional receiver
/// object for method calls.
struct StoredFunction {
    /// Global handle to the JavaScript function.
    function: v8::Global<v8::Function>,
    /// Optional receiver object for method invocation ('this' binding).
    receiver: Option<v8::Global<v8::Value>>,
}

/// Pending JavaScript promise produced by a synchronous call.
///
/// Tracks a promise returned from a function call that hasn't resolved yet,
/// along with timeout information.
struct PendingFunctionCall {
    /// Global handle to the pending promise.
    promise: v8::Global<v8::Promise>,
    /// Time when the call started (for tracking elapsed time).
    start_time: Instant,
    /// Absolute deadline for timeout (if specified).
    deadline: Option<Instant>,
    /// Timeout duration in milliseconds (if specified).
    timeout_ms: Option<u64>,
}

/// Outcome of attempting to call a JS function synchronously.
///
/// A function call can either complete immediately (non-promise return value)
/// or require asynchronous polling (promise return value).
pub enum FunctionCallResult {
    /// Function returned a non-promise value immediately.
    Immediate(JSValue),
    /// Function returned a promise; call ID can be used to resume.
    Pending { call_id: u64 },
}

const TERMINATION_STATUS_RUNNING: u8 = 0;
const TERMINATION_STATUS_REQUESTED: u8 = 1;
const TERMINATION_STATUS_TERMINATED: u8 = 2;
/// Minimum amount of memory we temporarily add to the heap limit when V8 tells us
/// it's out of space so that it can unwind and propagate an exception instead of
/// killing the process.
const NEAR_HEAP_LIMIT_MIN_HEADROOM_BYTES: usize = 1024 * 1024; // 1 MiB

/// Thread-safe controller for V8 isolate termination.
///
/// Provides a clone-able handle to request and track termination of a V8 isolate.
/// Uses atomic operations to coordinate termination state across threads.
#[derive(Clone)]
pub struct TerminationController {
    inner: Arc<TerminationState>,
}

/// Internal state for termination tracking.
struct TerminationState {
    /// Atomic status: 0=running, 1=requested, 2=terminated.
    status: AtomicU8,
    /// V8 isolate handle for triggering execution termination.
    isolate_handle: v8::IsolateHandle,
    /// Optional reason describing why termination was requested.
    reason: Mutex<Option<String>>,
}

struct SyncWatchdog {
    handle: thread::JoinHandle<()>,
    fired: Arc<AtomicBool>,
    cancel_flag: Arc<AtomicBool>,
    duration: Duration,
}

impl SyncWatchdog {
    fn spawn(
        duration: Duration,
        termination: TerminationController,
        reason: impl Into<String>,
    ) -> RuntimeResult<Self> {
        let fired = Arc::new(AtomicBool::new(false));
        let fired_for_thread = fired.clone();
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let cancel_for_thread = cancel_flag.clone();
        let reason = reason.into();

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
                        termination.ensure_reason(reason.clone());
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

        Ok(Self {
            handle,
            fired,
            cancel_flag,
            duration,
        })
    }
}

enum SnapshotSource {
    Owned(OwnedSnapshot),
}

impl SnapshotSource {
    fn from_vec(bytes: Vec<u8>) -> Self {
        SnapshotSource::Owned(OwnedSnapshot::new(bytes))
    }

    fn as_static(&mut self) -> &'static [u8] {
        match self {
            SnapshotSource::Owned(owned) => owned.as_static(),
        }
    }
}

struct OwnedSnapshot {
    data: Option<Box<[u8]>>,
    leaked_ptr: Option<NonNull<[u8]>>,
}

impl OwnedSnapshot {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            data: Some(bytes.into_boxed_slice()),
            leaked_ptr: None,
        }
    }

    fn as_static(&mut self) -> &'static [u8] {
        if let Some(ptr) = self.leaked_ptr {
            // SAFETY: pointer remains valid until Drop reconstructs the box.
            return unsafe { ptr.as_ref() };
        }

        let boxed = self
            .data
            .take()
            .expect("OwnedSnapshot bytes already leaked");
        let leaked: &'static mut [u8] = Box::leak(boxed);
        self.leaked_ptr = Some(NonNull::from(&mut *leaked));
        leaked
    }
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        if let Some(ptr) = self.leaked_ptr.take() {
            // SAFETY: pointer came from Box::leak and has not been reclaimed yet.
            unsafe {
                let _ = Box::from_raw(ptr.as_ptr());
            }
        }
    }
}

impl TerminationController {
    fn new(isolate_handle: v8::IsolateHandle) -> Self {
        Self {
            inner: Arc::new(TerminationState {
                status: AtomicU8::new(TERMINATION_STATUS_RUNNING),
                isolate_handle,
                reason: Mutex::new(None),
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

    pub fn ensure_reason(&self, reason: impl Into<String>) {
        let mut guard = self.inner.reason.lock().unwrap();
        if guard.is_none() {
            *guard = Some(reason.into());
        }
    }

    pub fn reason(&self) -> Option<String> {
        self.inner.reason.lock().unwrap().clone()
    }

    pub fn terminated_error(&self) -> RuntimeError {
        match self.reason() {
            Some(reason) => RuntimeError::terminated_with(reason),
            None => RuntimeError::terminated(),
        }
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

/// Commands sent from the handle to the runtime thread.
///
/// Each command includes a responder channel for returning results or errors.
/// Commands are processed sequentially on the runtime thread.
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
    BindObject {
        name: String,
        properties: Vec<BoundObjectProperty>,
        responder: Sender<RuntimeResult<()>>,
    },
    CallFunctionSync {
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
        responder: Sender<RuntimeResult<FunctionCallResult>>,
    },
    CallFunctionAsync {
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    ResumeFunctionCall {
        call_id: u64,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    ReleaseFunction {
        fn_id: u32,
        responder: oneshot::Sender<RuntimeResult<()>>,
    },
    StreamRead {
        stream_id: u32,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    },
    StreamRelease {
        stream_id: u32,
        responder: Sender<RuntimeResult<()>>,
    },
    StreamCancel {
        stream_id: u32,
        responder: Sender<RuntimeResult<()>>,
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

/// Dispatcher that multiplexes command processing with async job execution.
///
/// Runs the main loop on the runtime thread, polling the V8 event loop, processing
/// incoming commands, and driving active async jobs to completion.
struct RuntimeDispatcher {
    /// Core runtime state (V8 isolate, module loader, op registry, etc).
    core: RuntimeCoreState,
    /// Channel for receiving commands from the handle.
    cmd_rx: mpsc::UnboundedReceiver<RuntimeCommand>,
    /// Queue of jobs waiting to execute.
    pending_jobs: std::collections::VecDeque<Box<dyn RuntimeJob>>,
    /// Currently executing job (if any).
    active_job: Option<Box<dyn RuntimeJob>>,
}

impl RuntimeDispatcher {
    fn new(core: RuntimeCoreState, cmd_rx: mpsc::UnboundedReceiver<RuntimeCommand>) -> Self {
        Self {
            core,
            cmd_rx,
            pending_jobs: std::collections::VecDeque::new(),
            active_job: None,
        }
    }

    async fn run(&mut self) {
        loop {
            // 1. SYNCHRONOUSLY drive the JavaScript event loop
            // This advances all promises, timers, and async ops one tick
            // Non-blocking - returns immediately even if work is pending
            let noop_waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&noop_waker);
            let poll_opts = PollEventLoopOptions {
                wait_for_inspector: false,
                pump_v8_message_loop: true,
            };

            // Check for event loop errors
            // Note: Termination errors (from timeout/abort) are expected and will be handled
            // by the job's own poll() method. Only fail the job on unexpected fatal errors.
            match self.core.js_runtime.poll_event_loop(&mut cx, poll_opts) {
                std::task::Poll::Ready(Err(err)) => {
                    let runtime_err = self.core.translate_core_error(err);

                    // Check if this is a termination-related error (expected during timeout/abort)
                    if RuntimeCoreState::runtime_error_indicates_termination(&runtime_err) {
                        // Termination error - let the job handle it via its own timeout check
                        // Do nothing here, just continue to job polling
                    } else {
                        // Unexpected fatal error - fail the active job immediately
                        let runtime_err_debug = format!("{runtime_err:?}");
                        if let Some(completed_job) = self.active_job.take() {
                            tracing::error!("Unexpected event loop error: {runtime_err_debug}");
                            let elapsed = completed_job.start_time().elapsed();
                            self.core.stats_state.record(completed_job.kind(), elapsed);
                            completed_job.finish(&mut self.core, Err(runtime_err));
                            self.core.clear_task_locals();
                            if let Some(next_job) = self.pending_jobs.pop_front() {
                                self.active_job = Some(next_job);
                            }
                        } else {
                            tracing::error!(
                                "JavaScript event loop failed without an active job: {runtime_err_debug}"
                            );
                        }
                        continue;
                    }
                }
                std::task::Poll::Ready(Ok(())) | std::task::Poll::Pending => {
                    // Normal - event loop completed or has pending work
                }
            }

            // 2. SYNCHRONOUSLY check if the active job is complete
            if let Some(job) = &mut self.active_job {
                match job.poll(&mut self.core) {
                    std::task::Poll::Ready(result) => {
                        // Job completed - record stats and send result
                        let completed_job = self.active_job.take().unwrap();
                        let elapsed = completed_job.start_time().elapsed();
                        self.core.stats_state.record(completed_job.kind(), elapsed);
                        completed_job.finish(&mut self.core, result);

                        // Clear task locals to prevent stale event loop references
                        self.core.clear_task_locals();

                        // Start the next pending job if any
                        if let Some(next_job) = self.pending_jobs.pop_front() {
                            self.active_job = Some(next_job);
                        }
                    }
                    std::task::Poll::Pending => {
                        // Job still running - continue
                    }
                }
            }

            // 3. ASYNCHRONOUSLY wait for new commands or yield to allow other tasks to run
            let should_exit = tokio::select! {
                biased; // Prefer new commands to yielding

                // New command from Python
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd),
                        None => self.handle_channel_closed(),
                    }
                }

                // No new commands - yield to allow tokio to schedule other tasks
                _ = tokio::task::yield_now() => {
                    false
                }
            };

            if should_exit {
                break;
            }
        }
    }

    /// Handle a command - returns true if dispatcher should exit
    fn handle_command(&mut self, cmd: RuntimeCommand) -> bool {
        match cmd {
            RuntimeCommand::Eval { code, responder } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else if let Err(err) = self.core.ensure_inspector_ready() {
                    Err(err)
                } else {
                    match self
                        .core
                        .start_sync_watchdog("Synchronous evaluation timed out")
                    {
                        Ok(watchdog) => {
                            let result = self.core.eval_sync(&code);
                            self.core
                                .apply_watchdog_result(result, watchdog, "Sync evaluation")
                        }
                        Err(err) => Err(err),
                    }
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::EvalAsync {
                code,
                timeout_ms,
                task_locals,
                responder,
            } => {
                if self.core.should_reject_new_work() {
                    let _ = responder.send(Err(self.core.terminated_error()));
                    return false;
                }
                if let Err(err) = self.core.ensure_inspector_ready() {
                    let _ = responder.send(Err(err));
                    return false;
                }

                // Determine effective timeout and spawn watchdog if needed
                let effective_timeout = self.core.effective_timeout_ms(timeout_ms);
                let watchdog = match self
                    .core
                    .start_timeout_watchdog(effective_timeout, "Asynchronous evaluation timed out")
                {
                    Ok(watchdog) => watchdog,
                    Err(err) => {
                        let _ = responder.send(Err(err));
                        return false;
                    }
                };

                // Create the job
                let job = EvalAsyncJob::new(
                    code.clone(),
                    effective_timeout,
                    task_locals,
                    responder,
                    watchdog,
                );

                // Queue or activate the job
                if self.active_job.is_none() {
                    self.active_job = Some(Box::new(job));
                } else {
                    // Another job is active - queue this one
                    self.pending_jobs.push_back(Box::new(job));
                }
                false
            }
            RuntimeCommand::RegisterPythonOp {
                name,
                mode,
                handler,
                responder,
            } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.register_python_op(name, mode, handler)
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::SetModuleResolver { handler, responder } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.module_loader.set_resolver(handler);
                    if let Some(ref locals) = self.core.task_locals {
                        self.core.module_loader.set_task_locals(locals.clone());
                    }
                    Ok(())
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::SetModuleLoader { handler, responder } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.module_loader.set_loader(handler);
                    if let Some(ref locals) = self.core.task_locals {
                        self.core.module_loader.set_task_locals(locals.clone());
                    }
                    Ok(())
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::AddStaticModule {
                name,
                source,
                responder,
            } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.module_loader.add_static_module(name, source);
                    Ok(())
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::BindObject {
                name,
                properties,
                responder,
            } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.bind_object(name, properties)
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::EvalModule {
                specifier,
                responder,
            } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else if let Err(err) = self.core.ensure_inspector_ready() {
                    Err(err)
                } else {
                    match self
                        .core
                        .start_sync_watchdog("Synchronous module evaluation timed out")
                    {
                        Ok(watchdog) => {
                            let result = self.core.eval_module_sync(&specifier);
                            self.core.apply_watchdog_result(
                                result,
                                watchdog,
                                "Sync module evaluation",
                            )
                        }
                        Err(err) => Err(err),
                    }
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::EvalModuleAsync {
                specifier,
                timeout_ms,
                task_locals,
                responder,
            } => {
                if self.core.should_reject_new_work() {
                    let _ = responder.send(Err(self.core.terminated_error()));
                    return false;
                }
                if let Err(err) = self.core.ensure_inspector_ready() {
                    let _ = responder.send(Err(err));
                    return false;
                }

                // Determine effective timeout and spawn watchdog if needed
                let effective_timeout = self.core.effective_timeout_ms(timeout_ms);
                let watchdog = match self.core.start_timeout_watchdog(
                    effective_timeout,
                    "Asynchronous module evaluation timed out",
                ) {
                    Ok(watchdog) => watchdog,
                    Err(err) => {
                        let _ = responder.send(Err(err));
                        return false;
                    }
                };

                // Create the job
                let job = EvalModuleAsyncJob::new(
                    specifier,
                    effective_timeout,
                    task_locals,
                    responder,
                    watchdog,
                );

                // Queue or activate the job
                if self.active_job.is_none() {
                    self.active_job = Some(Box::new(job));
                } else {
                    self.pending_jobs.push_back(Box::new(job));
                }
                false
            }
            RuntimeCommand::CallFunctionSync {
                fn_id,
                args,
                timeout_ms,
                responder,
            } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else if let Err(err) = self.core.ensure_inspector_ready() {
                    Err(err)
                } else {
                    match self
                        .core
                        .start_sync_watchdog("Synchronous function call timed out")
                    {
                        Ok(watchdog) => {
                            let result = self.core.call_function_sync(fn_id, args, timeout_ms);
                            self.core
                                .apply_watchdog_result(result, watchdog, "Sync function call")
                        }
                        Err(err) => Err(err),
                    }
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::CallFunctionAsync {
                fn_id,
                args,
                timeout_ms,
                task_locals,
                responder,
            } => {
                if self.core.should_reject_new_work() {
                    let _ = responder.send(Err(self.core.terminated_error()));
                    return false;
                }
                if let Err(err) = self.core.ensure_inspector_ready() {
                    let _ = responder.send(Err(err));
                    return false;
                }

                // Create the job
                let job = CallFunctionAsyncJob::new(
                    fn_id,
                    args,
                    timeout_ms,
                    task_locals,
                    responder,
                    &self.core,
                );

                // Queue or activate the job
                if self.active_job.is_none() {
                    self.active_job = Some(Box::new(job));
                } else {
                    self.pending_jobs.push_back(Box::new(job));
                }
                false
            }
            RuntimeCommand::ResumeFunctionCall {
                call_id,
                task_locals,
                responder,
            } => {
                if self.core.should_reject_new_work() {
                    let _ = responder.send(Err(self.core.terminated_error()));
                    return false;
                }
                if let Err(err) = self.core.ensure_inspector_ready() {
                    let _ = responder.send(Err(err));
                    return false;
                }

                let pending = match self.core.take_pending_call(call_id) {
                    Ok(pending) => pending,
                    Err(err) => {
                        let _ = responder.send(Err(err));
                        return false;
                    }
                };

                let job = ResumeFunctionCallJob::new(pending, task_locals, responder);

                if self.active_job.is_none() {
                    self.active_job = Some(Box::new(job));
                } else {
                    self.pending_jobs.push_back(Box::new(job));
                }
                false
            }
            RuntimeCommand::ReleaseFunction { fn_id, responder } => {
                let result = if self.core.should_reject_new_work() {
                    Err(self.core.terminated_error())
                } else {
                    self.core.release_function(fn_id)
                };
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::StreamRead {
                stream_id,
                responder,
            } => {
                if self.core.should_reject_new_work() {
                    let _ = responder.send(Err(self.core.terminated_error()));
                } else {
                    let job = StreamReadJob::new(stream_id, responder);
                    if self.active_job.is_none() {
                        self.active_job = Some(Box::new(job));
                    } else {
                        self.pending_jobs.push_back(Box::new(job));
                    }
                }
                false
            }
            RuntimeCommand::StreamRelease {
                stream_id,
                responder,
            } => {
                let result = self.core.release_js_stream(stream_id);
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::StreamCancel {
                stream_id,
                responder,
            } => {
                let result = self.core.cancel_js_stream(stream_id);
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::GetStats { responder } => {
                let result = self.core.collect_stats();
                let _ = responder.send(result);
                false
            }
            RuntimeCommand::Terminate { responder } => {
                let termination_error = self.core.terminated_error();
                // Cancel active job if exists
                if let Some(job) = self.active_job.take() {
                    tracing::debug!("Terminating active job on interrupt");
                    job.finish(&mut self.core, Err(termination_error.clone()));
                }

                // Cancel all pending jobs
                let pending_count = self.pending_jobs.len();
                if pending_count > 0 {
                    tracing::debug!(count = pending_count, "Cancelling pending jobs");
                }
                while let Some(job) = self.pending_jobs.pop_front() {
                    job.finish(&mut self.core, Err(termination_error.clone()));
                }

                // Clear task locals to prevent stale event loop references
                self.core.clear_task_locals();

                let result = self.core.finalize_termination();
                let _ = responder.send(result);
                self.cmd_rx.close();
                true // Exit the loop
            }
            RuntimeCommand::Shutdown { responder } => {
                let leaked_count = self.core.fn_registry.borrow().len();
                if leaked_count > 0 {
                    tracing::warn!(
                        leaked_count,
                        "Function handles not released before shutdown"
                    );
                }
                self.core.fn_registry.borrow_mut().clear();

                // Clear task locals on shutdown
                self.core.clear_task_locals();

                let _ = responder.send(());
                self.cmd_rx.close();
                true // Exit the loop
            }
        }
    }

    /// Handle the command channel closing without an explicit shutdown request.
    fn handle_channel_closed(&mut self) -> bool {
        tracing::warn!("Command channel closed without explicit shutdown - cleaning up");
        self.core
            .termination
            .ensure_reason("Command channel closed unexpectedly");
        let termination_error = self.core.terminated_error();
        if let Some(job) = self.active_job.take() {
            tracing::debug!("Dropping active job after command channel closed");
            job.finish(&mut self.core, Err(termination_error.clone()));
        }

        if !self.pending_jobs.is_empty() {
            tracing::debug!(
                count = self.pending_jobs.len(),
                "Cancelling pending jobs after command channel closed"
            );
        }
        while let Some(job) = self.pending_jobs.pop_front() {
            job.finish(&mut self.core, Err(termination_error.clone()));
        }

        self.core.clear_task_locals();
        if let Err(err) = self.core.finalize_termination() {
            tracing::warn!(
                "Failed to finalize termination after command channel closed: {}",
                err
            );
        }
        true
    }
}

/// Trait for async runtime jobs that can be polled without holding long-term borrows.
/// Jobs are state machines that advance one step at a time.
trait RuntimeJob {
    /// Returns the kind of runtime call for stats tracking
    fn kind(&self) -> RuntimeCallKind;

    /// Poll the job for one tick. Returns Poll::Ready when complete.
    /// The job can borrow core mutably but must release it before returning.
    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>>;

    /// Finalize the job with a result, allowing cleanup before responding.
    fn finish(self: Box<Self>, core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>);

    /// Get the start time for stats tracking
    fn start_time(&self) -> Instant;
}

/// State machine for async JavaScript evaluation
struct EvalAsyncJob {
    code: String,
    timeout_ms: Option<u64>,
    task_locals: Option<TaskLocals>,
    responder: oneshot::Sender<RuntimeResult<JSValue>>,
    start_time: Instant,
    deadline: Option<Instant>,
    state: EvalAsyncJobState,
    watchdog: Option<SyncWatchdog>,
}

enum EvalAsyncJobState {
    /// Initial state - need to execute script and get promise
    Init,
    /// Waiting for promise to resolve (dispatcher drives event loop)
    Waiting {
        /// The promise being resolved - dispatcher drives it via poll_event_loop
        promise: v8::Global<v8::Promise>,
    },
    /// Job completed
    Done,
}

impl EvalAsyncJob {
    fn new(
        code: String,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
        watchdog: Option<SyncWatchdog>,
    ) -> Self {
        let start_time = Instant::now();

        let deadline = timeout_ms.map(|ms| start_time + Duration::from_millis(ms));

        Self {
            code,
            timeout_ms,
            task_locals,
            responder,
            start_time,
            deadline,
            state: EvalAsyncJobState::Init,
            watchdog,
        }
    }
}

impl RuntimeJob for EvalAsyncJob {
    fn kind(&self) -> RuntimeCallKind {
        RuntimeCallKind::EvalAsync
    }

    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>> {
        use std::task::Poll;

        // Check timeout first
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                core.termination.ensure_reason(format!(
                    "Asynchronous evaluation timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ));
                core.termination.terminate_execution();
                return Poll::Ready(Err(RuntimeError::timeout(format!(
                    "Evaluation timed out after {}ms (promise still pending)",
                    self.timeout_ms.unwrap_or(0)
                ))));
            }
        }

        match &mut self.state {
            EvalAsyncJobState::Init => {
                // Set up task locals
                if let Some(ref locals) = self.task_locals {
                    core.task_locals = Some(locals.clone());
                    core.module_loader.set_task_locals(locals.clone());
                    core.js_runtime
                        .op_state()
                        .borrow_mut()
                        .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                }

                // Execute the script
                let global_value = match core
                    .js_runtime
                    .execute_script("<eval_async>", self.code.clone())
                {
                    Ok(val) => val,
                    Err(err) => return Poll::Ready(Err(core.translate_js_error(*err))),
                };

                // Resolve the value to a promise
                // The resolve() call wraps the value in a promise if it isn't already one
                deno_core::scope!(scope, core.js_runtime);
                let local_value = v8::Local::new(scope, global_value);

                // Check if it's already a promise
                let promise = if local_value.is_promise() {
                    // Already a promise - use it directly
                    v8::Local::<v8::Promise>::try_from(local_value)
                        .map_err(|_| RuntimeError::internal("Failed to cast to Promise"))?
                } else {
                    // Not a promise - wrap in a resolved promise
                    let resolver = v8::PromiseResolver::new(scope).ok_or_else(|| {
                        RuntimeError::internal("Failed to create PromiseResolver")
                    })?;
                    resolver.resolve(scope, local_value);
                    resolver.get_promise(scope)
                };

                // Store the promise as a Global handle
                let promise_global = v8::Global::new(scope, promise);

                // Transition to waiting state
                self.state = EvalAsyncJobState::Waiting {
                    promise: promise_global,
                };

                // Return pending - dispatcher will drive the event loop
                Poll::Pending
            }
            EvalAsyncJobState::Waiting { promise } => {
                // Check the promise state (dispatcher has been driving the event loop)
                let promise_state = {
                    deno_core::scope!(scope, core.js_runtime);
                    let promise_local: v8::Local<v8::Promise> = v8::Local::new(scope, &*promise);
                    promise_local.state()
                };

                match promise_state {
                    v8::PromiseState::Pending => {
                        // Still pending - dispatcher will continue driving event loop
                        Poll::Pending
                    }
                    v8::PromiseState::Fulfilled => {
                        // Promise resolved successfully
                        let fn_registry = core.fn_registry.clone();
                        let next_fn_id = core.next_fn_id.clone();
                        let limits = core.serialization_limits;
                        let stream_registry = core.js_stream_registry.clone();
                        deno_core::scope!(scope, core.js_runtime);
                        let promise_local: v8::Local<v8::Promise> =
                            v8::Local::new(scope, &*promise);
                        let result_value = promise_local.result(scope);
                        let result = RuntimeCoreState::value_to_js_value(
                            &fn_registry,
                            &next_fn_id,
                            scope,
                            result_value,
                            limits,
                            stream_registry,
                        );
                        self.state = EvalAsyncJobState::Done;
                        Poll::Ready(result)
                    }
                    v8::PromiseState::Rejected => {
                        // Promise was rejected - extract error while scope is active
                        let js_error = {
                            deno_core::scope!(scope, core.js_runtime);
                            let promise_local: v8::Local<v8::Promise> =
                                v8::Local::new(scope, &*promise);
                            let exception = promise_local.result(scope);
                            *JsError::from_v8_exception(scope, exception)
                        };
                        // Scope dropped, now we can borrow core again
                        let error = core.translate_js_error(js_error);
                        self.state = EvalAsyncJobState::Done;
                        Poll::Ready(Err(error))
                    }
                }
            }
            EvalAsyncJobState::Done => {
                Poll::Ready(Err(RuntimeError::internal("Job already completed")))
            }
        }
    }

    fn finish(mut self: Box<Self>, core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>) {
        let result = core.apply_watchdog_result(result, self.watchdog.take(), "Async evaluation");
        let _ = self.responder.send(result);
    }

    fn start_time(&self) -> Instant {
        self.start_time
    }
}

struct StreamReadJob {
    stream_id: u32,
    responder: oneshot::Sender<RuntimeResult<JSValue>>,
    state: StreamReadJobState,
    start_time: Instant,
}

enum StreamReadJobState {
    Init,
    Waiting { promise: v8::Global<v8::Promise> },
    Done,
}

impl StreamReadJob {
    fn new(stream_id: u32, responder: oneshot::Sender<RuntimeResult<JSValue>>) -> Self {
        Self {
            stream_id,
            responder,
            state: StreamReadJobState::Init,
            start_time: Instant::now(),
        }
    }
}

impl RuntimeJob for StreamReadJob {
    fn kind(&self) -> RuntimeCallKind {
        RuntimeCallKind::EvalAsync
    }

    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>> {
        use std::task::Poll;

        match &mut self.state {
            StreamReadJobState::Init => {
                let promise = {
                    deno_core::scope!(scope, core.js_runtime);
                    core.js_stream_registry.start_read(scope, self.stream_id)
                };

                match promise {
                    Ok(promise) => {
                        self.state = StreamReadJobState::Waiting { promise };
                        Poll::Pending
                    }
                    Err(err) => {
                        self.state = StreamReadJobState::Done;
                        Poll::Ready(Err(err))
                    }
                }
            }
            StreamReadJobState::Waiting { promise } => {
                let promise_state = {
                    deno_core::scope!(scope, core.js_runtime);
                    let promise_local: v8::Local<v8::Promise> = v8::Local::new(scope, &*promise);
                    promise_local.state()
                };

                match promise_state {
                    v8::PromiseState::Pending => Poll::Pending,
                    v8::PromiseState::Fulfilled => {
                        let chunk_js_value = {
                            let stream_registry = core.js_stream_registry.clone();
                            deno_core::scope!(scope, core.js_runtime);
                            let promise_local: v8::Local<v8::Promise> =
                                v8::Local::new(scope, &*promise);
                            let chunk_value = promise_local.result(scope);
                            let limits = core.serialization_limits;
                            RuntimeCoreState::value_to_js_value(
                                &core.fn_registry,
                                &core.next_fn_id,
                                scope,
                                chunk_value,
                                limits,
                                stream_registry,
                            )
                        }?;

                        let chunk = StreamChunk::from_js_value(chunk_js_value)?;
                        core.js_stream_registry
                            .update_stats_after_chunk(self.stream_id, &chunk);
                        if chunk.done {
                            core.js_stream_registry.release(self.stream_id);
                        }
                        self.state = StreamReadJobState::Done;
                        Poll::Ready(Ok(chunk.to_js_value()))
                    }
                    v8::PromiseState::Rejected => {
                        let js_error = {
                            deno_core::scope!(scope, core.js_runtime);
                            let promise_local: v8::Local<v8::Promise> =
                                v8::Local::new(scope, &*promise);
                            let exception = promise_local.result(scope);
                            *JsError::from_v8_exception(scope, exception)
                        };
                        self.state = StreamReadJobState::Done;
                        Poll::Ready(Err(core.translate_js_error(js_error)))
                    }
                }
            }
            StreamReadJobState::Done => {
                Poll::Ready(Err(RuntimeError::internal("Stream read already completed")))
            }
        }
    }

    fn finish(self: Box<Self>, _core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>) {
        let _ = self.responder.send(result);
    }

    fn start_time(&self) -> Instant {
        self.start_time
    }
}

/// State machine for async module evaluation
struct EvalModuleAsyncJob {
    specifier: String,
    timeout_ms: Option<u64>,
    task_locals: Option<TaskLocals>,
    responder: oneshot::Sender<RuntimeResult<JSValue>>,
    start_time: Instant,
    deadline: Option<Instant>,
    state: EvalModuleAsyncJobState,
    watchdog: Option<SyncWatchdog>,
}

enum EvalModuleAsyncJobState {
    /// Initial state - need to load module and start evaluation
    Init,
    /// Module loaded, evaluation in progress (polling receiver)
    Evaluating {
        module_id: deno_core::ModuleId,
        receiver: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), CoreError>>>>,
    },
    /// Evaluation complete, ready to extract namespace
    WaitingNamespace { module_id: deno_core::ModuleId },
    /// Done
    Done,
}

impl EvalModuleAsyncJob {
    fn new(
        specifier: String,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
        watchdog: Option<SyncWatchdog>,
    ) -> Self {
        let start_time = Instant::now();

        let deadline = timeout_ms.map(|ms| start_time + Duration::from_millis(ms));

        Self {
            specifier,
            timeout_ms,
            task_locals,
            responder,
            start_time,
            deadline,
            state: EvalModuleAsyncJobState::Init,
            watchdog,
        }
    }
}

impl RuntimeJob for EvalModuleAsyncJob {
    fn kind(&self) -> RuntimeCallKind {
        RuntimeCallKind::EvalModuleAsync
    }

    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>> {
        use std::task::Poll;

        // Check timeout
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                core.termination.ensure_reason(format!(
                    "Asynchronous module evaluation timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ));
                core.termination.terminate_execution();
                return Poll::Ready(Err(RuntimeError::timeout(format!(
                    "Module evaluation timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ))));
            }
        }

        match &mut self.state {
            EvalModuleAsyncJobState::Init => {
                // Set up task locals
                if let Some(ref locals) = self.task_locals {
                    core.task_locals = Some(locals.clone());
                    core.module_loader.set_task_locals(locals.clone());
                    core.js_runtime
                        .op_state()
                        .borrow_mut()
                        .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                }

                // Parse module specifier
                let module_specifier =
                    if self.specifier.contains(':') || self.specifier.starts_with('/') {
                        deno_core::ModuleSpecifier::parse(&self.specifier).map_err(|e| {
                            RuntimeError::internal(format!(
                                "Invalid module specifier '{}': {}",
                                self.specifier, e
                            ))
                        })?
                    } else {
                        let base =
                            deno_core::ModuleSpecifier::parse("jsrun://runtime/").map_err(|e| {
                                RuntimeError::internal(format!("Failed to create base URL: {}", e))
                            })?;
                        base.join(&self.specifier).map_err(|e| {
                            RuntimeError::internal(format!(
                                "Failed to resolve module specifier '{}': {}",
                                self.specifier, e
                            ))
                        })?
                    };

                // Load module synchronously (module loading is inherently blocking in deno_core)
                // This is consistent with eval_module_sync and doesn't prevent re-entrance
                // because the actual async work (promise resolution) happens in the Evaluating state
                let module_id = futures::executor::block_on(
                    core.js_runtime.load_main_es_module(&module_specifier),
                )
                .map_err(|e| {
                    RuntimeError::internal(format!(
                        "Failed to load module '{}': {}",
                        self.specifier, e
                    ))
                })?;

                // Start evaluation - this returns a future that we'll poll
                let receiver = Box::pin(core.js_runtime.mod_evaluate(module_id));

                self.state = EvalModuleAsyncJobState::Evaluating {
                    module_id,
                    receiver,
                };
                Poll::Pending
            }
            EvalModuleAsyncJobState::Evaluating {
                module_id,
                receiver,
            } => {
                // The dispatcher is driving poll_event_loop which will progress the module evaluation
                // We need to poll the receiver to see if it's done
                let noop_waker = futures::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&noop_waker);

                match receiver.as_mut().poll(&mut cx) {
                    Poll::Ready(result) => {
                        // Evaluation complete - check result
                        if let Err(err) = result {
                            self.state = EvalModuleAsyncJobState::Done;
                            return Poll::Ready(Err(core.translate_core_error(err)));
                        }

                        // Success - transition to namespace extraction
                        self.state = EvalModuleAsyncJobState::WaitingNamespace {
                            module_id: *module_id,
                        };
                        Poll::Pending
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            EvalModuleAsyncJobState::WaitingNamespace { module_id } => {
                // Extract module namespace
                let fn_registry = core.fn_registry.clone();
                let next_fn_id = core.next_fn_id.clone();
                let limits = core.serialization_limits;
                let module_namespace =
                    core.js_runtime
                        .get_module_namespace(*module_id)
                        .map_err(|e| {
                            RuntimeError::internal(format!("Failed to get module namespace: {}", e))
                        })?;

                let stream_registry = core.js_stream_registry.clone();
                deno_core::scope!(scope, core.js_runtime);
                let local = v8::Local::new(scope, module_namespace);
                let value: v8::Local<'_, v8::Value> = local.into();
                let result = RuntimeCoreState::value_to_js_value(
                    &fn_registry,
                    &next_fn_id,
                    scope,
                    value,
                    limits,
                    stream_registry,
                );

                self.state = EvalModuleAsyncJobState::Done;
                Poll::Ready(result)
            }
            EvalModuleAsyncJobState::Done => {
                Poll::Ready(Err(RuntimeError::internal("Job already completed")))
            }
        }
    }

    fn finish(mut self: Box<Self>, core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>) {
        let result =
            core.apply_watchdog_result(result, self.watchdog.take(), "Async module evaluation");
        let _ = self.responder.send(result);
    }

    fn start_time(&self) -> Instant {
        self.start_time
    }
}

/// State machine for async function calls
struct CallFunctionAsyncJob {
    fn_id: u32,
    args: Vec<JSValue>,
    timeout_ms: Option<u64>,
    task_locals: Option<TaskLocals>,
    responder: oneshot::Sender<RuntimeResult<JSValue>>,
    start_time: Instant,
    deadline: Option<Instant>,
    state: CallFunctionAsyncJobState,
}

enum CallFunctionAsyncJobState {
    Init,
    Waiting { promise: v8::Global<v8::Promise> },
    Done,
}

impl CallFunctionAsyncJob {
    fn new(
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
        core: &RuntimeCoreState,
    ) -> Self {
        let start_time = Instant::now();

        let effective_timeout = timeout_ms.or_else(|| {
            core.execution_timeout.map(|d| {
                let millis = d.as_millis();
                if millis > u128::from(u64::MAX) {
                    u64::MAX
                } else {
                    millis as u64
                }
            })
        });

        let deadline = effective_timeout.map(|ms| start_time + Duration::from_millis(ms));

        Self {
            fn_id,
            args,
            timeout_ms: effective_timeout,
            task_locals,
            responder,
            start_time,
            deadline,
            state: CallFunctionAsyncJobState::Init,
        }
    }
}

impl RuntimeJob for CallFunctionAsyncJob {
    fn kind(&self) -> RuntimeCallKind {
        RuntimeCallKind::CallFunctionAsync
    }

    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>> {
        use std::task::Poll;

        // Check timeout
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                core.termination.ensure_reason(format!(
                    "Asynchronous function call timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ));
                core.termination.terminate_execution();
                return Poll::Ready(Err(RuntimeError::timeout(format!(
                    "Function call timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ))));
            }
        }

        match &mut self.state {
            CallFunctionAsyncJobState::Init => {
                // Set up task locals
                if let Some(ref locals) = self.task_locals {
                    core.task_locals = Some(locals.clone());
                    core.module_loader.set_task_locals(locals.clone());
                    core.js_runtime
                        .op_state()
                        .borrow_mut()
                        .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                }

                // Look up function, call it, and convert result to promise - all in one scope
                // Check for missing function first (before entering scope)
                if !core.fn_registry.borrow().contains_key(&self.fn_id) {
                    return Poll::Ready(Err(RuntimeError::internal(format!(
                        "Function ID {} not found",
                        self.fn_id
                    ))));
                }

                let promise_result: Result<Result<v8::Global<v8::Promise>, JsError>, RuntimeError> =
                    (|| {
                        deno_core::scope!(scope, core.js_runtime);
                        v8::tc_scope!(let try_catch, scope);

                        // Get function and receiver from registry
                        let (func, receiver) = {
                            let registry = core.fn_registry.borrow();
                            let stored = registry.get(&self.fn_id).unwrap(); // Safe: checked above
                            let func = v8::Local::new(try_catch, &stored.function);
                            let receiver = stored
                                .receiver
                                .as_ref()
                                .map(|r| v8::Local::new(try_catch, r));
                            (func, receiver)
                        };

                        // Convert arguments
                        let mut v8_args = Vec::with_capacity(self.args.len());
                        for arg in &self.args {
                            let v8_val = RuntimeCoreState::js_value_to_v8(
                                &core.fn_registry,
                                try_catch,
                                arg,
                            )?;
                            v8_args.push(v8_val);
                        }

                        let call_receiver = receiver.unwrap_or_else(|| {
                            try_catch.get_current_context().global(try_catch).into()
                        });

                        // Call the function and convert result to promise
                        match func.call(try_catch, call_receiver, &v8_args) {
                            Some(result_value) => {
                                // Check if result is a promise and wrap if needed
                                let promise = if result_value.is_promise() {
                                    v8::Local::<v8::Promise>::try_from(result_value).map_err(
                                        |_| RuntimeError::internal("Failed to cast to Promise"),
                                    )?
                                } else {
                                    // Not a promise - wrap in resolved promise
                                    let resolver =
                                        v8::PromiseResolver::new(try_catch).ok_or_else(|| {
                                            RuntimeError::internal(
                                                "Failed to create PromiseResolver",
                                            )
                                        })?;
                                    resolver.resolve(try_catch, result_value);
                                    resolver.get_promise(try_catch)
                                };
                                Ok(Ok(v8::Global::new(try_catch, promise)))
                            }
                            None => match try_catch.exception() {
                                Some(exception) => {
                                    let js_error = JsError::from_v8_exception(try_catch, exception);
                                    Ok(Err(*js_error))
                                }
                                None => Err(RuntimeError::internal(
                                    "Function call failed with no exception",
                                )),
                            },
                        }
                    })();

                // Handle the result outside the scope
                let promise_global = match promise_result {
                    Ok(Ok(p)) => p,
                    Ok(Err(js_error)) => {
                        return Poll::Ready(Err(core.translate_js_error(js_error)));
                    }
                    Err(err) => {
                        return Poll::Ready(Err(err));
                    }
                };

                self.state = CallFunctionAsyncJobState::Waiting {
                    promise: promise_global,
                };
                Poll::Pending
            }
            CallFunctionAsyncJobState::Waiting { promise } => {
                // Check promise state
                let promise_state = {
                    deno_core::scope!(scope, core.js_runtime);
                    let promise_local: v8::Local<v8::Promise> = v8::Local::new(scope, &*promise);
                    promise_local.state()
                };

                match promise_state {
                    v8::PromiseState::Pending => Poll::Pending,
                    v8::PromiseState::Fulfilled => {
                        let fn_registry = core.fn_registry.clone();
                        let next_fn_id = core.next_fn_id.clone();
                        let limits = core.serialization_limits;
                        let stream_registry = core.js_stream_registry.clone();
                        deno_core::scope!(scope, core.js_runtime);
                        let promise_local: v8::Local<v8::Promise> =
                            v8::Local::new(scope, &*promise);
                        let result_value = promise_local.result(scope);
                        let result = RuntimeCoreState::value_to_js_value(
                            &fn_registry,
                            &next_fn_id,
                            scope,
                            result_value,
                            limits,
                            stream_registry,
                        );
                        self.state = CallFunctionAsyncJobState::Done;
                        Poll::Ready(result)
                    }
                    v8::PromiseState::Rejected => {
                        let js_error = {
                            deno_core::scope!(scope, core.js_runtime);
                            let promise_local: v8::Local<v8::Promise> =
                                v8::Local::new(scope, &*promise);
                            let exception = promise_local.result(scope);
                            *JsError::from_v8_exception(scope, exception)
                        };
                        let error = core.translate_js_error(js_error);
                        self.state = CallFunctionAsyncJobState::Done;
                        Poll::Ready(Err(error))
                    }
                }
            }
            CallFunctionAsyncJobState::Done => {
                Poll::Ready(Err(RuntimeError::internal("Job already completed")))
            }
        }
    }

    fn finish(self: Box<Self>, _core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>) {
        let _ = self.responder.send(result);
    }

    fn start_time(&self) -> Instant {
        self.start_time
    }
}

/// Job that resumes a previously-started JS function by awaiting its stored promise.
struct ResumeFunctionCallJob {
    promise: v8::Global<v8::Promise>,
    task_locals: Option<TaskLocals>,
    responder: oneshot::Sender<RuntimeResult<JSValue>>,
    start_time: Instant,
    deadline: Option<Instant>,
    timeout_ms: Option<u64>,
    state: ResumeFunctionCallJobState,
}

enum ResumeFunctionCallJobState {
    Init,
    Waiting,
    Done,
}

impl ResumeFunctionCallJob {
    fn new(
        pending: PendingFunctionCall,
        task_locals: Option<TaskLocals>,
        responder: oneshot::Sender<RuntimeResult<JSValue>>,
    ) -> Self {
        Self {
            promise: pending.promise,
            task_locals,
            responder,
            start_time: pending.start_time,
            deadline: pending.deadline,
            timeout_ms: pending.timeout_ms,
            state: ResumeFunctionCallJobState::Init,
        }
    }
}

impl RuntimeJob for ResumeFunctionCallJob {
    fn kind(&self) -> RuntimeCallKind {
        RuntimeCallKind::CallFunctionAsync
    }

    fn poll(&mut self, core: &mut RuntimeCoreState) -> std::task::Poll<RuntimeResult<JSValue>> {
        use std::task::Poll;

        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                core.termination.ensure_reason(format!(
                    "Asynchronous function call timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ));
                core.termination.terminate_execution();
                return Poll::Ready(Err(RuntimeError::timeout(format!(
                    "Function call timed out after {}ms",
                    self.timeout_ms.unwrap_or(0)
                ))));
            }
        }

        loop {
            match self.state {
                ResumeFunctionCallJobState::Init => {
                    if let Some(ref locals) = self.task_locals {
                        core.task_locals = Some(locals.clone());
                        core.module_loader.set_task_locals(locals.clone());
                        core.js_runtime
                            .op_state()
                            .borrow_mut()
                            .put(crate::runtime::ops::GlobalTaskLocals(Some(locals.clone())));
                    }
                    self.state = ResumeFunctionCallJobState::Waiting;
                }
                ResumeFunctionCallJobState::Waiting => {
                    let promise_state = {
                        deno_core::scope!(scope, core.js_runtime);
                        let promise_local: v8::Local<v8::Promise> =
                            v8::Local::new(scope, &self.promise);
                        promise_local.state()
                    };

                    return match promise_state {
                        v8::PromiseState::Pending => Poll::Pending,
                        v8::PromiseState::Fulfilled => {
                            let fn_registry = core.fn_registry.clone();
                            let next_fn_id = core.next_fn_id.clone();
                            let limits = core.serialization_limits;
                            let stream_registry = core.js_stream_registry.clone();
                            deno_core::scope!(scope, core.js_runtime);
                            let promise_local: v8::Local<v8::Promise> =
                                v8::Local::new(scope, &self.promise);
                            let result_value = promise_local.result(scope);
                            let result = RuntimeCoreState::value_to_js_value(
                                &fn_registry,
                                &next_fn_id,
                                scope,
                                result_value,
                                limits,
                                stream_registry,
                            );
                            self.state = ResumeFunctionCallJobState::Done;
                            Poll::Ready(result)
                        }
                        v8::PromiseState::Rejected => {
                            let js_error = {
                                deno_core::scope!(scope, core.js_runtime);
                                let promise_local: v8::Local<v8::Promise> =
                                    v8::Local::new(scope, &self.promise);
                                let exception = promise_local.result(scope);
                                *JsError::from_v8_exception(scope, exception)
                            };
                            let error = core.translate_js_error(js_error);
                            self.state = ResumeFunctionCallJobState::Done;
                            Poll::Ready(Err(error))
                        }
                    };
                }
                ResumeFunctionCallJobState::Done => {
                    return Poll::Ready(Err(RuntimeError::internal("Job already completed")));
                }
            }
        }
    }

    fn finish(self: Box<Self>, _core: &mut RuntimeCoreState, result: RuntimeResult<JSValue>) {
        let _ = self.responder.send(result);
    }

    fn start_time(&self) -> Instant {
        self.start_time
    }
}

pub fn spawn_runtime_thread(config: RuntimeConfig) -> RuntimeResult<SpawnRuntimeResult> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RuntimeCommand>();
    let (init_tx, init_rx): InitSignalChannel = std::sync::mpsc::channel();

    thread::Builder::new()
        .name("jsrun-deno-runtime".to_string())
        .spawn(move || {
            let _thread_guard = RuntimeThreadGuard::new();
            let tokio_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");

            let core = match RuntimeCoreState::new(config) {
                Ok(core) => {
                    let termination = core.termination_controller();
                    let inspector_info =
                        match (core.inspector_metadata(), core.inspector_connection_state()) {
                            (Some(meta), Some(state)) => Some((meta, state)),
                            _ => None,
                        };
                    let py_stream_registry = core.py_stream_registry.clone();
                    let _ = init_tx.send(Ok((termination, inspector_info, py_stream_registry)));
                    core
                }
                Err(err) => {
                    let _ = init_tx.send(Err(err));
                    return;
                }
            };

            tokio_rt.block_on(async move {
                let mut dispatcher = RuntimeDispatcher::new(core, cmd_rx);
                dispatcher.run().await;
            });
        })
        .map_err(|e| RuntimeError::internal(format!("Failed to spawn runtime thread: {}", e)))?;

    match init_rx.recv() {
        Ok(Ok((termination, inspector_info, py_stream_registry))) => {
            Ok((cmd_tx, termination, inspector_info, py_stream_registry))
        }
        Ok(Err(err)) => Err(err),
        Err(_) => Err(RuntimeError::internal(
            "Runtime thread initialization failed",
        )),
    }
}

pub fn active_runtime_threads() -> usize {
    ACTIVE_RUNTIME_THREADS.load(Ordering::SeqCst)
}

struct InspectorRuntimeState {
    _server: InspectorServer,
    registration: InspectorRegistration,
    wait_for_connection: bool,
    break_on_next_statement: bool,
    has_waited: bool,
    connection_state: InspectorConnectionState,
}

impl InspectorRuntimeState {
    fn metadata(&self) -> InspectorMetadata {
        self.registration.metadata().clone()
    }

    fn connection_state(&self) -> InspectorConnectionState {
        self.connection_state.clone()
    }
}

/// Core state that holds the V8 isolate and all runtime data.
/// Owned directly by RuntimeDispatcher to enable job polling without RefCell borrows.
struct RuntimeCoreState {
    js_runtime: JsRuntime,
    registry: PythonOpRegistry,
    module_loader: Rc<PythonModuleLoader>,
    task_locals: Option<TaskLocals>,
    execution_timeout: Option<Duration>,
    fn_registry: Rc<RefCell<HashMap<u32, StoredFunction>>>,
    next_fn_id: Rc<RefCell<u32>>,
    pending_calls: Rc<RefCell<HashMap<u64, PendingFunctionCall>>>,
    next_pending_call_id: Rc<RefCell<u64>>,
    stats_state: RuntimeStatsState,
    termination: TerminationController,
    terminated: bool,
    inspector_state: Option<InspectorRuntimeState>,
    #[allow(dead_code)]
    startup_snapshot: Option<SnapshotSource>,
    serialization_limits: SerializationLimits,
    js_stream_registry: Rc<JsStreamRegistry>,
    py_stream_registry: PyStreamRegistry,
}

impl RuntimeCoreState {
    fn new(config: RuntimeConfig) -> RuntimeResult<Self> {
        let registry = PythonOpRegistry::new();
        let extension = python_extension(registry.clone());
        let module_loader = Rc::new(PythonModuleLoader::new());

        let RuntimeConfig {
            max_heap_size,
            initial_heap_size,
            execution_timeout,
            bootstrap_script,
            enable_console,
            inspector,
            snapshot,
            max_serialization_depth,
            max_serialization_bytes,
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

        let serialization_limits =
            SerializationLimits::new(max_serialization_depth, max_serialization_bytes);

        let mut snapshot_source = snapshot.map(SnapshotSource::from_vec);
        let startup_snapshot = snapshot_source.as_mut().map(|source| source.as_static());

        let inspector_enabled = inspector.is_some();
        let mut js_runtime = JsRuntime::new(RuntimeOptions {
            extensions: vec![extension],
            create_params,
            module_loader: Some(module_loader.clone()),
            inspector: inspector_enabled,
            is_main: true,
            startup_snapshot,
            ..Default::default()
        });

        let js_stream_registry = Rc::new(JsStreamRegistry::new());
        let py_stream_registry = PyStreamRegistry::new(serialization_limits);

        js_runtime
            .op_state()
            .borrow_mut()
            .put(py_stream_registry.clone());

        if inspector_enabled {
            js_runtime.maybe_init_inspector();
        }

        // Disable console if enable_console is set to false, since Deno's bootstrap script enables console by default
        if enable_console == Some(false) {
            js_runtime
                .execute_script(
                    "<disable_console>",
                    r#"
                    (() => {
                        const noop = () => {};
                        const stub = new Proxy(Object.create(null), { get: () => noop });
                        const existing = globalThis.console;
                        if (typeof existing === "object" && existing !== null) {
                            for (const key of Reflect.ownKeys(existing)) {
                                try { existing[key] = noop; } catch (_) {} // ignore non-writable properties
                            }
                            return;
                        }
                        globalThis.console = stub;
                    })();
                    "#
                    .to_string(),
                )
                .map_err(|err| RuntimeError::javascript(JsExceptionDetails::from_js_error(*err)))?;
        }

        if let Some(script) = bootstrap_script {
            js_runtime
                .execute_script("<bootstrap>", script)
                .map_err(|err| RuntimeError::javascript(JsExceptionDetails::from_js_error(*err)))?;
        }

        {
            let state = js_runtime.op_state();
            state.borrow_mut().put(serialization_limits);
        }

        let termination = {
            let isolate = js_runtime.v8_isolate();
            let handle = isolate.thread_safe_handle();
            TerminationController::new(handle)
        };

        if let Some(heap_limit_bytes) = max_heap_size {
            let termination_for_heap_limit = termination.clone();
            js_runtime.add_near_heap_limit_callback(move |current_limit, initial_limit| {
                termination_for_heap_limit.ensure_reason("Heap limit exceeded");
                let first_request = termination_for_heap_limit.request();
                if first_request {
                    tracing::error!(
                        configured_heap_limit = heap_limit_bytes,
                        current_heap_limit = current_limit,
                        initial_heap_limit = initial_limit,
                        "V8 isolate is nearing its heap limit; terminating execution",
                    );
                }
                termination_for_heap_limit.terminate_execution();

                // Returning a slightly larger limit gives V8 enough breathing room to unwind
                // after we terminate execution rather than letting it abort the process.
                let extra_headroom = initial_limit
                    .max(heap_limit_bytes / 8)
                    .max(NEAR_HEAP_LIMIT_MIN_HEADROOM_BYTES);
                current_limit.saturating_add(extra_headroom)
            });
        }

        let inspector_state = match inspector {
            Some(inspector_cfg) => {
                let wait_for_connection = inspector_cfg.wait_for_connection;
                let break_on_next_statement = inspector_cfg.break_on_next_statement;
                let connection_state = InspectorConnectionState::default();
                let server =
                    InspectorServer::bind(inspector_cfg.socket_addr(), "jsrun").map_err(|err| {
                        RuntimeError::internal(format!("Failed to start inspector server: {err}"))
                    })?;

                let registration = server
                    .register_runtime(
                        js_runtime.inspector(),
                        InspectorRegistrationParams {
                            target_url: inspector_cfg.target_url.clone(),
                            display_name: inspector_cfg.display_name.clone(),
                            wait_for_connection,
                        },
                        connection_state.clone(),
                    )
                    .map_err(|err| {
                        RuntimeError::internal(format!("Failed to register inspector: {err}"))
                    })?;

                Some(InspectorRuntimeState {
                    _server: server,
                    registration,
                    wait_for_connection,
                    break_on_next_statement,
                    has_waited: false,
                    connection_state,
                })
            }
            None => None,
        };

        Ok(Self {
            js_runtime,
            registry,
            module_loader,
            task_locals: None,
            execution_timeout,
            fn_registry: Rc::new(RefCell::new(HashMap::new())),
            next_fn_id: Rc::new(RefCell::new(0)),
            pending_calls: Rc::new(RefCell::new(HashMap::new())),
            next_pending_call_id: Rc::new(RefCell::new(0)),
            stats_state: RuntimeStatsState::default(),
            termination,
            terminated: false,
            inspector_state,
            startup_snapshot: snapshot_source,
            serialization_limits,
            js_stream_registry,
            py_stream_registry,
        })
    }

    fn inspector_metadata(&self) -> Option<InspectorMetadata> {
        self.inspector_state.as_ref().map(|state| state.metadata())
    }

    fn inspector_connection_state(&self) -> Option<InspectorConnectionState> {
        self.inspector_state
            .as_ref()
            .map(|state| state.connection_state())
    }

    fn ensure_inspector_ready(&mut self) -> RuntimeResult<()> {
        if let Some(state) = self.inspector_state.as_mut() {
            if state.has_waited {
                return Ok(());
            }
            if state.wait_for_connection || state.break_on_next_statement {
                let inspector = self.js_runtime.inspector();
                if state.break_on_next_statement {
                    inspector.wait_for_session_and_break_on_next_statement();
                } else if state.wait_for_connection {
                    inspector.wait_for_session();
                }
            }
            state.has_waited = true;
        }
        Ok(())
    }

    fn termination_controller(&self) -> TerminationController {
        self.termination.clone()
    }

    fn should_reject_new_work(&self) -> bool {
        self.terminated || self.termination.is_requested()
    }

    fn terminated_error(&self) -> RuntimeError {
        self.termination.terminated_error()
    }

    /// Clear task locals after a job completes to prevent stale event loop references
    fn clear_task_locals(&mut self) {
        self.task_locals = None;
        self.module_loader.clear_task_locals();
        self.js_runtime
            .op_state()
            .borrow_mut()
            .put(crate::runtime::ops::GlobalTaskLocals(None));
    }

    fn effective_timeout_ms(&self, timeout_ms: Option<u64>) -> Option<u64> {
        timeout_ms.or_else(|| {
            self.execution_timeout.map(|d| {
                let millis = d.as_millis();
                if millis > u128::from(u64::MAX) {
                    u64::MAX
                } else {
                    millis as u64
                }
            })
        })
    }

    fn is_readable_stream(
        scope: &mut v8::PinScope<'_, '_>,
        value: v8::Local<'_, v8::Value>,
    ) -> bool {
        if !value.is_object() {
            return false;
        }

        let key = match v8::String::new(scope, "ReadableStream") {
            Some(k) => k,
            None => return false,
        };
        let ctor_value = match scope
            .get_current_context()
            .global(scope)
            .get(scope, key.into())
        {
            Some(val) => val,
            None => return false,
        };
        let ctor = match v8::Local::<v8::Function>::try_from(ctor_value) {
            Ok(func) => func,
            Err(_) => return false,
        };

        value.instance_of(scope, ctor.into()).unwrap_or_default()
    }

    fn store_pending_call(
        &self,
        promise: v8::Global<v8::Promise>,
        start_time: Instant,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
    ) -> u64 {
        let mut next_id = self.next_pending_call_id.borrow_mut();
        let call_id = *next_id;
        *next_id = next_id.wrapping_add(1);
        self.pending_calls.borrow_mut().insert(
            call_id,
            PendingFunctionCall {
                promise,
                start_time,
                deadline,
                timeout_ms,
            },
        );
        call_id
    }

    fn take_pending_call(&self, call_id: u64) -> RuntimeResult<PendingFunctionCall> {
        self.pending_calls
            .borrow_mut()
            .remove(&call_id)
            .ok_or_else(|| {
                RuntimeError::internal(format!("Pending function call {} not found", call_id))
            })
    }

    fn start_sync_watchdog(&self, reason: &str) -> RuntimeResult<Option<SyncWatchdog>> {
        self.execution_timeout
            .map(|duration| {
                SyncWatchdog::spawn(duration, self.termination.clone(), reason.to_string())
            })
            .transpose()
    }

    fn start_timeout_watchdog(
        &self,
        timeout_ms: Option<u64>,
        reason: &str,
    ) -> RuntimeResult<Option<SyncWatchdog>> {
        timeout_ms
            .map(|ms| {
                let duration = Duration::from_millis(ms);
                SyncWatchdog::spawn(duration, self.termination.clone(), reason.to_string())
            })
            .transpose()
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

    fn apply_watchdog_result<T>(
        &mut self,
        result: RuntimeResult<T>,
        watchdog: Option<SyncWatchdog>,
        context: &str,
    ) -> RuntimeResult<T> {
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
        self.pending_calls.borrow_mut().clear();
        self.termination.mark_terminated();
        self.terminated = true;
        Ok(())
    }

    fn translate_js_error(&mut self, err: JsError) -> RuntimeError {
        let details = JsExceptionDetails::from_js_error(err);
        if self.should_reject_new_work() && Self::js_error_indicates_termination(&details) {
            let _ = self.finalize_termination();
            self.terminated_error()
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
            self.terminated_error()
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
            RuntimeError::Terminated { .. } => true,
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

    fn bind_object(
        &mut self,
        name: String,
        properties: Vec<BoundObjectProperty>,
    ) -> RuntimeResult<()> {
        deno_core::scope!(scope, self.js_runtime);
        v8::tc_scope!(let try_catch, scope);
        let context = try_catch.get_current_context();
        let global = context.global(try_catch);

        let helper_key = v8::String::new(try_catch, "__jsrun_bind_object")
            .ok_or_else(|| RuntimeError::internal("Failed to allocate helper name"))?;
        let helper_value = global
            .get(try_catch, helper_key.into())
            .ok_or_else(|| RuntimeError::internal("Missing __jsrun_bind_object helper"))?;
        let helper_fn = v8::Local::<v8::Function>::try_from(helper_value)
            .map_err(|_| RuntimeError::internal("__jsrun_bind_object is not callable"))?;

        let global_name = v8::String::new(try_catch, &name)
            .ok_or_else(|| RuntimeError::internal("Failed to allocate target name"))?;

        let assignments = v8::Array::new(try_catch, properties.len() as i32);

        for (index, entry) in properties.into_iter().enumerate() {
            let entry_obj = v8::Object::new(try_catch);

            let key_literal = v8::String::new(try_catch, "key")
                .ok_or_else(|| RuntimeError::internal("Failed to allocate 'key' literal"))?;
            let key_value = v8::String::new(
                try_catch,
                match &entry {
                    BoundObjectProperty::Value { key, .. }
                    | BoundObjectProperty::Op { key, .. } => key,
                },
            )
            .ok_or_else(|| RuntimeError::internal("Failed to allocate property name"))?;
            entry_obj
                .set(try_catch, key_literal.into(), key_value.into())
                .ok_or_else(|| RuntimeError::internal("Failed to set entry key"))?;

            let kind_literal = v8::String::new(try_catch, "kind")
                .ok_or_else(|| RuntimeError::internal("Failed to allocate 'kind' literal"))?;

            match entry {
                BoundObjectProperty::Value { key: _, value } => {
                    let kind_value = v8::String::new(try_catch, "value")
                        .ok_or_else(|| RuntimeError::internal("Failed to allocate kind value"))?;
                    entry_obj
                        .set(try_catch, kind_literal.into(), kind_value.into())
                        .ok_or_else(|| RuntimeError::internal("Failed to set entry kind"))?;

                    let value_literal = v8::String::new(try_catch, "value").ok_or_else(|| {
                        RuntimeError::internal("Failed to allocate 'value' literal")
                    })?;
                    let v8_value =
                        RuntimeCoreState::js_value_to_v8(&self.fn_registry, try_catch, &value)?;
                    entry_obj
                        .set(try_catch, value_literal.into(), v8_value)
                        .ok_or_else(|| RuntimeError::internal("Failed to set entry value"))?;
                }
                BoundObjectProperty::Op {
                    key: _,
                    op_id,
                    mode,
                } => {
                    let kind_value = v8::String::new(try_catch, "op")
                        .ok_or_else(|| RuntimeError::internal("Failed to allocate kind value"))?;
                    entry_obj
                        .set(try_catch, kind_literal.into(), kind_value.into())
                        .ok_or_else(|| RuntimeError::internal("Failed to set entry kind"))?;

                    let op_id_literal = v8::String::new(try_catch, "op_id").ok_or_else(|| {
                        RuntimeError::internal("Failed to allocate 'op_id' literal")
                    })?;
                    let op_id_value = v8::Number::new(try_catch, op_id as f64);
                    entry_obj
                        .set(try_catch, op_id_literal.into(), op_id_value.into())
                        .ok_or_else(|| RuntimeError::internal("Failed to set op id"))?;

                    let mode_literal = v8::String::new(try_catch, "mode").ok_or_else(|| {
                        RuntimeError::internal("Failed to allocate 'mode' literal")
                    })?;
                    let mode_value = v8::String::new(
                        try_catch,
                        match mode {
                            PythonOpMode::Async => "async",
                            PythonOpMode::Sync => "sync",
                        },
                    )
                    .ok_or_else(|| RuntimeError::internal("Failed to allocate mode value"))?;
                    entry_obj
                        .set(try_catch, mode_literal.into(), mode_value.into())
                        .ok_or_else(|| RuntimeError::internal("Failed to set mode"))?;
                }
            }

            assignments
                .set_index(try_catch, index as u32, entry_obj.into())
                .ok_or_else(|| RuntimeError::internal("Failed to store assignment entry"))?;
        }

        match helper_fn.call(
            try_catch,
            global.into(),
            &[global_name.into(), assignments.into()],
        ) {
            Some(_) => Ok(()),
            None => {
                if let Some(exception) = try_catch.exception() {
                    let js_error = JsError::from_v8_exception(try_catch, exception);
                    Err(RuntimeError::javascript(JsExceptionDetails::from_js_error(
                        *js_error,
                    )))
                } else {
                    Err(RuntimeError::internal(
                        "__jsrun_bind_object invocation failed",
                    ))
                }
            }
        }
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

            let fn_registry = this.fn_registry.clone();
            let next_fn_id = this.next_fn_id.clone();
            let limits = this.serialization_limits;
            let stream_registry = this.js_stream_registry.clone();
            deno_core::scope!(scope, this.js_runtime);
            let local = v8::Local::new(scope, global_value);
            Self::value_to_js_value(
                &fn_registry,
                &next_fn_id,
                scope,
                local,
                limits,
                stream_registry,
            )
        })
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
            let fn_registry = this.fn_registry.clone();
            let next_fn_id = this.next_fn_id.clone();
            let limits = this.serialization_limits;
            let stream_registry = this.js_stream_registry.clone();
            deno_core::scope!(scope, this.js_runtime);
            let namespace_obj = v8::Local::new(scope, module_namespace);
            let namespace_value: v8::Local<'_, v8::Value> = namespace_obj.into();
            Self::value_to_js_value(
                &fn_registry,
                &next_fn_id,
                scope,
                namespace_value,
                limits,
                stream_registry,
            )
        })
    }

    fn call_function_sync(
        &mut self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<FunctionCallResult> {
        self.with_timing(RuntimeCallKind::CallFunctionSync, |this| {
            this.invoke_function_sync(fn_id, args, timeout_ms)
        })
    }

    fn invoke_function_sync(
        &mut self,
        fn_id: u32,
        args: Vec<JSValue>,
        timeout_ms: Option<u64>,
    ) -> RuntimeResult<FunctionCallResult> {
        if !self.fn_registry.borrow().contains_key(&fn_id) {
            return Err(RuntimeError::internal(format!(
                "Function ID {} not found",
                fn_id
            )));
        }

        let stream_registry = self.js_stream_registry.clone();

        let start_time = Instant::now();
        let effective_timeout = self.effective_timeout_ms(timeout_ms);
        let deadline = effective_timeout.map(|ms| start_time + Duration::from_millis(ms));

        let fn_registry = self.fn_registry.clone();
        let next_fn_id = self.next_fn_id.clone();
        let limits = self.serialization_limits;

        enum SyncCallOutcome {
            Immediate(JSValue),
            Pending(v8::Global<v8::Promise>),
        }

        enum SyncCallError {
            Runtime(RuntimeError),
            Js(JsError),
        }

        let call_outcome: Result<SyncCallOutcome, SyncCallError> = (|| {
            deno_core::scope!(scope, self.js_runtime);
            v8::tc_scope!(let try_catch, scope);

            let (func, receiver) = {
                let registry = self.fn_registry.borrow();
                let stored = registry.get(&fn_id).unwrap();
                let func = v8::Local::new(try_catch, &stored.function);
                let receiver = stored
                    .receiver
                    .as_ref()
                    .map(|recv| v8::Local::new(try_catch, recv));
                (func, receiver)
            };

            let mut v8_args = Vec::with_capacity(args.len());
            for arg in &args {
                let v8_val = RuntimeCoreState::js_value_to_v8(&fn_registry, try_catch, arg)
                    .map_err(SyncCallError::Runtime)?;
                v8_args.push(v8_val);
            }

            let call_receiver = receiver
                .unwrap_or_else(|| try_catch.get_current_context().global(try_catch).into());

            match func.call(try_catch, call_receiver, &v8_args) {
                Some(result_value) => {
                    try_catch.perform_microtask_checkpoint();

                    if result_value.is_promise() {
                        let promise =
                            v8::Local::<v8::Promise>::try_from(result_value).map_err(|_| {
                                SyncCallError::Runtime(RuntimeError::internal(
                                    "Failed to cast to Promise",
                                ))
                            })?;

                        match promise.state() {
                            v8::PromiseState::Pending => {
                                let promise_global = v8::Global::new(try_catch, promise);
                                Ok(SyncCallOutcome::Pending(promise_global))
                            }
                            v8::PromiseState::Fulfilled => {
                                let fulfilled_value = promise.result(try_catch);
                                RuntimeCoreState::value_to_js_value(
                                    &fn_registry,
                                    &next_fn_id,
                                    try_catch,
                                    fulfilled_value,
                                    limits,
                                    stream_registry.clone(),
                                )
                                .map(SyncCallOutcome::Immediate)
                                .map_err(SyncCallError::Runtime)
                            }
                            v8::PromiseState::Rejected => {
                                let exception = promise.result(try_catch);
                                let js_error = JsError::from_v8_exception(try_catch, exception);
                                Err(SyncCallError::Js(*js_error))
                            }
                        }
                    } else {
                        RuntimeCoreState::value_to_js_value(
                            &fn_registry,
                            &next_fn_id,
                            try_catch,
                            result_value,
                            limits,
                            stream_registry,
                        )
                        .map(SyncCallOutcome::Immediate)
                        .map_err(SyncCallError::Runtime)
                    }
                }
                None => match try_catch.exception() {
                    Some(exception) => {
                        let js_error = JsError::from_v8_exception(try_catch, exception);
                        Err(SyncCallError::Js(*js_error))
                    }
                    None => Err(SyncCallError::Runtime(RuntimeError::internal(
                        "Function call failed with no exception",
                    ))),
                },
            }
        })();

        match call_outcome {
            Ok(SyncCallOutcome::Immediate(value)) => Ok(FunctionCallResult::Immediate(value)),
            Ok(SyncCallOutcome::Pending(promise)) => {
                let call_id =
                    self.store_pending_call(promise, start_time, deadline, effective_timeout);
                Ok(FunctionCallResult::Pending { call_id })
            }
            Err(SyncCallError::Runtime(err)) => Err(err),
            Err(SyncCallError::Js(js_error)) => Err(self.translate_js_error(js_error)),
        }
    }

    /// Remove a function from the registry, freeing its V8 global handle.
    fn release_function(&mut self, fn_id: u32) -> RuntimeResult<()> {
        let mut registry = self.fn_registry.borrow_mut();
        if registry.remove(&fn_id).is_none() {
            log::debug!("Attempted to release unknown function id {}", fn_id);
        }
        Ok(())
    }

    fn release_js_stream(&self, stream_id: u32) -> RuntimeResult<()> {
        self.js_stream_registry.release(stream_id);
        Ok(())
    }

    fn cancel_js_stream(&mut self, stream_id: u32) -> RuntimeResult<()> {
        {
            deno_core::scope!(scope, self.js_runtime);
            if let Ok(reader) = self.js_stream_registry.ensure_reader(scope, stream_id) {
                if let Some(cancel_key) = v8::String::new(scope, "cancel") {
                    if let Some(cancel_value) = reader.get(scope, cancel_key.into()) {
                        if let Ok(cancel_fn) = v8::Local::<v8::Function>::try_from(cancel_value) {
                            let _ = cancel_fn.call(scope, reader.into(), &[]);
                        }
                    }
                }
            }
        }

        self.js_stream_registry.release(stream_id);
        Ok(())
    }

    fn collect_stats(&mut self) -> RuntimeResult<RuntimeStatsSnapshot> {
        let heap = self.snapshot_memory_usage();
        let execution = self.stats_state.snapshot();
        let activity = self.snapshot_activity();
        let mut streams = self.js_stream_registry.stats_snapshot();
        streams.merge(&self.py_stream_registry.stats_snapshot());
        Ok(RuntimeStatsSnapshot::new(
            heap, execution, activity, streams,
        ))
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
        scope: &mut v8::PinScope<'s, '_>,
        value: v8::Local<'s, v8::Value>,
        limits: SerializationLimits,
        stream_registry: Rc<JsStreamRegistry>,
    ) -> RuntimeResult<JSValue> {
        let mut seen = HashSet::new();
        let mut tracker = LimitTracker::new(limits.max_depth, limits.max_bytes);
        Self::value_to_js_value_internal(
            fn_registry,
            next_fn_id,
            scope,
            value,
            &mut seen,
            &mut tracker,
            None,
            stream_registry,
        )
    }

    fn js_value_to_v8<'s>(
        registry: &Rc<RefCell<HashMap<u32, StoredFunction>>>,
        scope: &mut v8::PinScope<'s, '_>,
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
            JSValue::PyStream { id } => {
                let context = scope.get_current_context();
                let global = context.global(scope);
                let helper_key = v8::String::new(scope, "__jsrun_from_py_stream")
                    .ok_or_else(|| RuntimeError::internal("Failed to allocate helper key"))?;
                let helper_value = global.get(scope, helper_key.into()).ok_or_else(|| {
                    RuntimeError::internal("Missing __jsrun_from_py_stream helper")
                })?;
                let helper_fn =
                    v8::Local::<v8::Function>::try_from(helper_value).map_err(|_| {
                        RuntimeError::internal("__jsrun_from_py_stream is not callable")
                    })?;
                let id_value = v8::Number::new(scope, *id as f64);
                helper_fn
                    .call(scope, global.into(), &[id_value.into()])
                    .ok_or_else(|| {
                        RuntimeError::internal("__jsrun_from_py_stream invocation failed")
                    })
            }
            JSValue::JsStream { .. } => Err(RuntimeError::internal(
                "JsStream values cannot be sent back into JavaScript",
            )),
        }
    }

    /// Internal recursive converter with cycle detection and optional receiver capture.
    #[allow(clippy::too_many_arguments)]
    fn value_to_js_value_internal<'s>(
        fn_registry: &Rc<RefCell<HashMap<u32, StoredFunction>>>,
        next_fn_id: &Rc<RefCell<u32>>,
        scope: &mut v8::PinScope<'s, '_>,
        value: v8::Local<'s, v8::Value>,
        seen: &mut HashSet<i32>,
        tracker: &mut LimitTracker,
        receiver: Option<v8::Global<v8::Value>>,
        stream_registry: Rc<JsStreamRegistry>,
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
            let bigint = v8::Local::<v8::BigInt>::try_from(value)
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
            let func = v8::Local::<v8::Function>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to function"))?;

            // Create a Global handle to keep the function alive
            let fn_handle = v8::Global::new(scope, func);

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
            let typed_array = v8::Local::<v8::Uint8Array>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Uint8Array"))?;
            let length = typed_array.byte_length();
            tracker.add_bytes(length)?;
            let mut buffer = vec![0u8; length];
            let view: v8::Local<v8::ArrayBufferView> = typed_array.into();
            view.copy_contents(&mut buffer);
            Ok(JSValue::Bytes(buffer))
        } else if value.is_array_buffer() {
            let array_buffer = v8::Local::<v8::ArrayBuffer>::try_from(value)
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
            let obj = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast array to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            let array = v8::Local::<v8::Array>::try_from(value)
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
                    stream_registry.clone(),
                )?);
            }

            seen.remove(&hash);
            Ok(JSValue::Array(items))
        } else if value.is_set() {
            let obj = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast set to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            let set = v8::Local::<v8::Set>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Set"))?;
            let entries = set.as_array(scope);
            let len = entries.length() as usize;

            tracker.add_bytes(24)?;
            tracker.add_bytes(len.saturating_mul(size_of::<usize>()))?;

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
                    stream_registry.clone(),
                )?);
            }

            seen.remove(&hash);
            Ok(JSValue::Set(values))
        } else if value.is_date() {
            let date = v8::Local::<v8::Date>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to Date"))?;
            let epoch_ms = date.value_of();
            if !epoch_ms.is_finite() || epoch_ms < i64::MIN as f64 || epoch_ms > i64::MAX as f64 {
                return Err(RuntimeError::internal("Date value out of range"));
            }
            tracker.add_bytes(16)?;
            Ok(JSValue::Date(epoch_ms.round() as i64))
        } else if value.is_object() && Self::is_readable_stream(scope, value) {
            let stream_id = stream_registry.register_stream(scope, value);
            tracker.add_bytes(size_of::<u32>())?;
            Ok(JSValue::JsStream { id: stream_id })
        } else if value.is_object() {
            // Check for circular reference using identity hash
            let obj = v8::Local::<v8::Object>::try_from(value)
                .map_err(|_| RuntimeError::internal("Failed to cast to object"))?;
            let hash = obj.get_identity_hash().get();

            if !seen.insert(hash) {
                return Err(RuntimeError::internal(
                    "Cannot serialize circular reference",
                ));
            }

            // Get property names
            let prop_names = obj
                .get_own_property_names(scope, v8::GetPropertyNamesArgs::default())
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
                    let obj_as_value: v8::Local<v8::Value> = obj.into();
                    Some(v8::Global::new(scope, obj_as_value))
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
                        stream_registry.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatcher_exits_when_channel_closes() {
        let baseline = active_runtime_threads();
        let (cmd_tx, termination, _, _) =
            spawn_runtime_thread(RuntimeConfig::default()).expect("spawn runtime");
        assert_eq!(
            active_runtime_threads(),
            baseline + 1,
            "runtime thread should register"
        );

        drop(cmd_tx);

        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if active_runtime_threads() == baseline {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(
            active_runtime_threads(),
            baseline,
            "runtime thread should exit after command channel closes"
        );
        drop(termination);
    }
}
