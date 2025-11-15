//! Streaming registries bridging JavaScript `ReadableStream`s and Python async iterables.

use crate::runtime::conversion::python_to_js_value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::js_value::{JSValue, SerializationLimits};
use deno_core::v8;
use indexmap::IndexMap;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

type PyStreamReleaseCallback = Arc<dyn Fn(u32) + Send + Sync + 'static>;
type PyStreamReleaseListeners = Arc<Mutex<Vec<PyStreamReleaseCallback>>>;

/// Sentinel tag embedded in JSValue objects that represent stream chunks.
const STREAM_CHUNK_TYPE: &str = "StreamChunk";
const STREAM_CHUNK_DONE_KEY: &str = "done";
const STREAM_CHUNK_VALUE_KEY: &str = "value";

/// Snapshot of streaming usage and health for runtime statistics.
///
/// Tracks active and total stream counts, plus bytes transferred in both directions
/// (JS→Python and Python→JS).
#[derive(Debug, Default, Clone)]
pub struct StreamStatsSnapshot {
    /// Number of currently active JavaScript streams.
    pub active_js_streams: u64,
    /// Number of currently active Python streams.
    pub active_py_streams: u64,
    /// Total JavaScript streams created over the runtime's lifetime.
    pub total_js_streams: u64,
    /// Total Python streams created over the runtime's lifetime.
    pub total_py_streams: u64,
    /// Bytes transferred from JavaScript to Python.
    pub bytes_streamed_js_to_py: u64,
    /// Bytes transferred from Python to JavaScript.
    pub bytes_streamed_py_to_js: u64,
}

impl StreamStatsSnapshot {
    pub fn merge(&mut self, other: &StreamStatsSnapshot) {
        self.active_js_streams = self
            .active_js_streams
            .saturating_add(other.active_js_streams);
        self.active_py_streams = self
            .active_py_streams
            .saturating_add(other.active_py_streams);
        self.total_js_streams = self.total_js_streams.saturating_add(other.total_js_streams);
        self.total_py_streams = self.total_py_streams.saturating_add(other.total_py_streams);
        self.bytes_streamed_js_to_py = self
            .bytes_streamed_js_to_py
            .saturating_add(other.bytes_streamed_js_to_py);
        self.bytes_streamed_py_to_js = self
            .bytes_streamed_py_to_js
            .saturating_add(other.bytes_streamed_py_to_js);
    }
}

/// Host-side representation of a chunk pulled from a JS or Python stream.
///
/// Mirrors the structure returned by `ReadableStreamDefaultReader.read()`,
/// with a `done` flag and optional `value`.
#[derive(Debug, Clone)]
pub struct StreamChunk {
    /// True if the stream has ended (no more chunks available).
    pub done: bool,
    /// Optional chunk value (None if done=true or stream was canceled).
    pub value: Option<JSValue>,
}

impl StreamChunk {
    pub fn to_js_value(&self) -> JSValue {
        let mut map = IndexMap::new();
        map.insert(
            "__jsrun_type".to_string(),
            JSValue::String(STREAM_CHUNK_TYPE.to_string()),
        );
        map.insert(STREAM_CHUNK_DONE_KEY.to_string(), JSValue::Bool(self.done));
        if let Some(value) = &self.value {
            map.insert(STREAM_CHUNK_VALUE_KEY.to_string(), value.clone());
        }
        JSValue::Object(map)
    }

    pub fn from_js_value(payload: JSValue) -> RuntimeResult<Self> {
        match payload {
            JSValue::Object(mut map) => {
                let tag = map
                    .shift_remove("__jsrun_type")
                    .and_then(|value| match value {
                        JSValue::String(s) => Some(s),
                        _ => None,
                    })
                    .unwrap_or_default();
                if tag == STREAM_CHUNK_TYPE {
                    let done = match map.shift_remove(STREAM_CHUNK_DONE_KEY) {
                        Some(JSValue::Bool(flag)) => flag,
                        other => {
                            return Err(RuntimeError::internal(format!(
                                "Stream chunk missing done flag: {:?}",
                                other
                            )))
                        }
                    };
                    let value = map.shift_remove(STREAM_CHUNK_VALUE_KEY);
                    Ok(StreamChunk { done, value })
                } else {
                    let done = match map.shift_remove(STREAM_CHUNK_DONE_KEY) {
                        Some(JSValue::Bool(flag)) => flag,
                        Some(JSValue::String(text)) if text == "true" => true,
                        Some(JSValue::String(text)) if text == "false" => false,
                        Some(other) => {
                            return Err(RuntimeError::internal(format!(
                                "Invalid done field for stream chunk: {:?}",
                                other
                            )))
                        }
                        None => {
                            return Err(RuntimeError::internal("Stream chunk missing done field"))
                        }
                    };
                    let value = map.shift_remove(STREAM_CHUNK_VALUE_KEY);
                    Ok(StreamChunk { done, value })
                }
            }
            other => Err(RuntimeError::internal(format!(
                "Unexpected chunk payload: {:?}",
                other
            ))),
        }
    }
}

#[derive(Default)]
struct JsStreamStats {
    active: u64,
    total: u64,
    bytes: u64,
}

impl JsStreamStats {
    fn snapshot(&self) -> StreamStatsSnapshot {
        StreamStatsSnapshot {
            active_js_streams: self.active,
            total_js_streams: self.total,
            bytes_streamed_js_to_py: self.bytes,
            ..StreamStatsSnapshot::default()
        }
    }
}

struct JsStreamEntry {
    stream: v8::Global<v8::Value>,
    reader: Option<v8::Global<v8::Object>>,
    chunks: u64,
    transferred_bytes: u64,
}

impl JsStreamEntry {
    fn new(scope: &mut v8::PinScope<'_, '_>, value: v8::Local<'_, v8::Value>) -> Self {
        Self {
            stream: v8::Global::new(scope, value),
            reader: None,
            chunks: 0,
            transferred_bytes: 0,
        }
    }
}

/// Registry tracking live JS ReadableStream handles.
///
/// Manages V8 global handles for JavaScript `ReadableStream` objects and their readers,
/// enabling Python code to consume chunks from JavaScript streams.
#[derive(Default)]
pub struct JsStreamRegistry {
    /// Active stream entries indexed by stream ID.
    entries: RefCell<HashMap<u32, JsStreamEntry>>,
    /// Next available stream ID.
    next_id: Cell<u32>,
    /// Usage statistics for active/total streams and bytes transferred.
    stats: RefCell<JsStreamStats>,
}

impl JsStreamRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_stream(
        &self,
        scope: &mut v8::PinScope<'_, '_>,
        stream_value: v8::Local<'_, v8::Value>,
    ) -> u32 {
        let id = self.next_id.get();
        self.next_id.set(id.wrapping_add(1));
        self.entries
            .borrow_mut()
            .insert(id, JsStreamEntry::new(scope, stream_value));
        let mut stats = self.stats.borrow_mut();
        stats.active = stats.active.saturating_add(1);
        stats.total = stats.total.saturating_add(1);
        id
    }

    pub fn release(&self, stream_id: u32) {
        if self.entries.borrow_mut().remove(&stream_id).is_some() {
            let mut stats = self.stats.borrow_mut();
            stats.active = stats.active.saturating_sub(1);
        }
    }

    pub fn stats_snapshot(&self) -> StreamStatsSnapshot {
        self.stats.borrow().snapshot()
    }

    pub fn ensure_reader<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        stream_id: u32,
    ) -> RuntimeResult<v8::Local<'s, v8::Object>> {
        let mut entries = self.entries.borrow_mut();
        let entry = entries
            .get_mut(&stream_id)
            .ok_or_else(|| RuntimeError::internal("Unknown stream id"))?;
        if let Some(existing) = &entry.reader {
            return Ok(v8::Local::new(scope, existing));
        }
        let stream_local: v8::Local<v8::Value> = v8::Local::new(scope, &entry.stream);
        let stream_obj = stream_local
            .to_object(scope)
            .ok_or_else(|| RuntimeError::internal("ReadableStream is not an object"))?;
        let key = v8::String::new(scope, "getReader")
            .ok_or_else(|| RuntimeError::internal("Failed to allocate getReader string"))?;
        let getter_value = stream_obj
            .get(scope, key.into())
            .ok_or_else(|| RuntimeError::internal("ReadableStream.getReader missing"))?;
        let getter = v8::Local::<v8::Function>::try_from(getter_value)
            .map_err(|_| RuntimeError::internal("getReader is not a function"))?;
        let reader_value = getter
            .call(scope, stream_obj.into(), &[])
            .ok_or_else(|| RuntimeError::internal("getReader threw"))?;
        let reader_obj = reader_value
            .to_object(scope)
            .ok_or_else(|| RuntimeError::internal("getReader did not return object"))?;
        entry.reader = Some(v8::Global::new(scope, reader_obj));
        Ok(reader_obj)
    }

    pub fn start_read(
        &self,
        scope: &mut v8::PinScope<'_, '_>,
        stream_id: u32,
    ) -> RuntimeResult<v8::Global<v8::Promise>> {
        let reader = self.ensure_reader(scope, stream_id)?;
        let read_key = v8::String::new(scope, "read")
            .ok_or_else(|| RuntimeError::internal("Failed to allocate read key"))?;
        let read_value = reader
            .get(scope, read_key.into())
            .ok_or_else(|| RuntimeError::internal("Reader.read missing"))?;
        let read_fn = v8::Local::<v8::Function>::try_from(read_value)
            .map_err(|_| RuntimeError::internal("Reader.read is not callable"))?;
        let promise_value = read_fn
            .call(scope, reader.into(), &[])
            .ok_or_else(|| RuntimeError::internal("Reader.read threw"))?;
        let promise = v8::Local::<v8::Promise>::try_from(promise_value)
            .map_err(|_| RuntimeError::internal("Reader.read must return a promise"))?;
        Ok(v8::Global::new(scope, promise))
    }

    pub fn update_stats_after_chunk(&self, stream_id: u32, chunk: &StreamChunk) {
        let mut entries = self.entries.borrow_mut();
        if let Some(entry) = entries.get_mut(&stream_id) {
            entry.chunks = entry.chunks.saturating_add(1);
            if let Some(JSValue::Bytes(bytes)) = &chunk.value {
                entry.transferred_bytes =
                    entry.transferred_bytes.saturating_add(bytes.len() as u64);
                let mut stats = self.stats.borrow_mut();
                stats.bytes = stats.bytes.saturating_add(bytes.len() as u64);
            }
        }
    }
}

/// Async iterator registry for Python-provided streams consumed inside JavaScript.
///
/// Manages Python async iterables that are exposed as JavaScript `ReadableStream`s.
/// Thread-safe and clone-able for sharing across the handle and runtime thread.
#[derive(Clone)]
pub struct PyStreamRegistry {
    /// Active stream entries indexed by stream ID.
    entries: Arc<Mutex<HashMap<u32, Arc<PyStreamEntry>>>>,
    /// Next available stream ID.
    next_id: Arc<AtomicU32>,
    /// Number of currently active streams.
    active: Arc<AtomicU64>,
    /// Total streams created over the registry's lifetime.
    total: Arc<AtomicU64>,
    /// Bytes transferred from Python to JavaScript.
    bytes: Arc<AtomicU64>,
    /// Callbacks invoked when a stream is released.
    release_listeners: PyStreamReleaseListeners,
    /// Serialization limits for chunk values.
    serialization_limits: SerializationLimits,
}

impl PyStreamRegistry {
    pub fn new(serialization_limits: SerializationLimits) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU32::new(1)),
            active: Arc::new(AtomicU64::new(0)),
            total: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
            release_listeners: Arc::new(Mutex::new(Vec::new())),
            serialization_limits,
        }
    }

    pub fn add_release_listener<F>(&self, listener: F)
    where
        F: Fn(u32) + Send + Sync + 'static,
    {
        self.release_listeners
            .lock()
            .unwrap()
            .push(Arc::new(listener));
    }

    fn notify_release_listeners(&self, stream_id: u32) {
        let listeners = {
            let guard = self.release_listeners.lock().unwrap();
            guard.iter().cloned().collect::<Vec<_>>()
        };
        for listener in listeners {
            listener(stream_id);
        }
    }

    pub fn register_iterable(
        &self,
        iterable: Py<PyAny>,
        task_locals: TaskLocals,
    ) -> RuntimeResult<u32> {
        let stream_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(PyStreamEntry::new(
            iterable,
            task_locals,
            self.serialization_limits,
        ));
        self.entries.lock().unwrap().insert(stream_id, entry);
        self.active.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
        Ok(stream_id)
    }

    pub async fn pull_next(&self, stream_id: u32) -> RuntimeResult<StreamChunk> {
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| RuntimeError::internal("Unknown Python stream id"))?;

        let chunk = entry.next_chunk().await?;
        if let Some(JSValue::Bytes(bytes)) = &chunk.value {
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        if chunk.done {
            self.release(stream_id);
        }
        Ok(chunk)
    }

    pub async fn cancel(&self, stream_id: u32) -> RuntimeResult<()> {
        if let Some(entry) = self.remove_entry(stream_id) {
            entry.cancel().await?;
        }
        Ok(())
    }

    pub fn release(&self, stream_id: u32) {
        let _ = self.remove_entry(stream_id);
    }

    fn remove_entry(&self, stream_id: u32) -> Option<Arc<PyStreamEntry>> {
        let removed = self.entries.lock().unwrap().remove(&stream_id);
        if removed.is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
            self.notify_release_listeners(stream_id);
        }
        removed
    }

    pub fn stats_snapshot(&self) -> StreamStatsSnapshot {
        StreamStatsSnapshot {
            active_py_streams: self.active.load(Ordering::Relaxed),
            total_py_streams: self.total.load(Ordering::Relaxed),
            bytes_streamed_py_to_js: self.bytes.load(Ordering::Relaxed),
            ..StreamStatsSnapshot::default()
        }
    }
}

struct PyStreamEntry {
    iterable: Py<PyAny>,
    iterator: AsyncMutex<Option<Py<PyAny>>>,
    task_locals: TaskLocals,
    closed: AtomicBool,
    serialization_limits: SerializationLimits,
}

impl PyStreamEntry {
    fn new(
        iterable: Py<PyAny>,
        task_locals: TaskLocals,
        serialization_limits: SerializationLimits,
    ) -> Self {
        Self {
            iterable,
            iterator: AsyncMutex::new(None),
            task_locals,
            closed: AtomicBool::new(false),
            serialization_limits,
        }
    }

    async fn ensure_iterator(&self) -> RuntimeResult<Py<PyAny>> {
        let mut guard = self.iterator.lock().await;
        if let Some(it) = guard.as_ref() {
            return Python::attach(|py| Ok(it.clone_ref(py)));
        }
        let iterator = Python::attach(|py| {
            self.iterable
                .bind(py)
                .call_method0(pyo3::intern!(py, "__aiter__"))
                .map(|obj| obj.into_any().unbind())
                .map_err(|err| RuntimeError::internal(format!("Failed to get __aiter__: {err}")))
        })?;
        Python::attach(|py| {
            *guard = Some(iterator.clone_ref(py));
        });
        Ok(iterator)
    }

    async fn next_chunk(&self) -> RuntimeResult<StreamChunk> {
        let iterator = self.ensure_iterator().await?;
        let future = Python::attach(|py| {
            let iterator_bound = iterator.bind(py);
            let awaitable = iterator_bound
                .call_method0(pyo3::intern!(py, "__anext__"))
                .map_err(|err| RuntimeError::internal(format!("__anext__ failed: {err}")))?;
            pyo3_async_runtimes::into_future_with_locals(&self.task_locals, awaitable)
                .map_err(|err| RuntimeError::internal(format!("Failed to await __anext__: {err}")))
        })?;

        match future.await {
            Ok(value) => {
                let js_value = Python::attach(|py| {
                    python_to_js_value(value.into_bound(py), &self.serialization_limits).map_err(
                        |err| RuntimeError::internal(format!("Chunk conversion failed: {err}")),
                    )
                })?;
                Ok(StreamChunk {
                    done: false,
                    value: Some(js_value),
                })
            }
            Err(err) => {
                let is_stop = Python::attach(|py| err.is_instance_of::<PyStopAsyncIteration>(py));
                if is_stop {
                    Ok(StreamChunk {
                        done: true,
                        value: None,
                    })
                } else {
                    Err(RuntimeError::internal(format!(
                        "Python stream errored: {err}"
                    )))
                }
            }
        }
    }

    async fn cancel(&self) -> RuntimeResult<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let iterator_opt = {
            let guard = self.iterator.lock().await;
            Python::attach(|py| guard.as_ref().map(|it| it.clone_ref(py)))
        };
        let Some(iterator) = iterator_opt else {
            return Ok(());
        };

        let future = Python::attach(|py| {
            let iterator_bound = iterator.bind(py);
            match iterator_bound.getattr(pyo3::intern!(py, "aclose")) {
                Ok(aclose) => {
                    let awaitable = aclose
                        .call0()
                        .map_err(|err| RuntimeError::internal(format!("aclose() failed: {err}")))?;
                    pyo3_async_runtimes::into_future_with_locals(&self.task_locals, awaitable)
                        .map_err(|err| {
                            RuntimeError::internal(format!("Failed to await aclose(): {err}"))
                        })
                        .map(Some)
                }
                Err(_) => Ok(None),
            }
        })?;

        if let Some(fut) = future {
            match fut.await {
                Ok(_) => {}
                Err(err) => {
                    let is_stop =
                        Python::attach(|py| err.is_instance_of::<PyStopAsyncIteration>(py));
                    if !is_stop {
                        return Err(RuntimeError::internal(format!("aclose() errored: {err}")));
                    }
                }
            }
        }
        Ok(())
    }
}
