//! Runtime configuration for isolate-per-tenant execution.
//!
//! This module defines the configuration structure for JavaScript runtimes,
//! including heap limits and bootstrap options.

use crate::runtime::js_value::{SerializationLimits, MAX_JS_BYTES, MAX_JS_DEPTH};
use crate::runtime::python::utils::validate_timeout_seconds;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::net::SocketAddr;
use std::time::Duration;

fn parse_socket_addr(host: &str, port: u16) -> PyResult<SocketAddr> {
    if host.trim().is_empty() {
        return Err(PyValueError::new_err("Inspector host cannot be empty"));
    }
    if port == 0 {
        return Err(PyValueError::new_err(
            "Inspector port must be a positive integer",
        ));
    }

    let candidate = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };

    candidate.parse::<SocketAddr>().map_err(|err| {
        PyValueError::new_err(format!("Invalid inspector address '{candidate}': {err}"))
    })
}

/// Inspector configuration for Chrome DevTools Protocol debugging.
///
/// Configures the WebSocket server that enables debugging via Chrome DevTools
/// or compatible debuggers. The inspector runs on a separate thread from the
/// runtime thread.
#[pyclass(module = "jsrun")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectorConfig {
    /// Socket address (IP and port) for the inspector server.
    pub address: SocketAddr,
    /// If true, runtime waits for debugger connection before executing code.
    pub wait_for_connection: bool,
    /// If true, execution pauses at the first statement for debugging.
    pub break_on_next_statement: bool,
    /// Optional target URL identifier (e.g., module name or script path).
    pub target_url: Option<String>,
    /// Optional display name shown in DevTools (e.g., "Main Runtime").
    pub display_name: Option<String>,
}

impl Default for InspectorConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127, 0, 0, 1], 9229)),
            wait_for_connection: false,
            break_on_next_statement: false,
            target_url: None,
            display_name: None,
        }
    }
}

impl InspectorConfig {
    pub fn socket_addr(&self) -> SocketAddr {
        self.address
    }
}

#[pymethods]
impl InspectorConfig {
    #[new]
    #[pyo3(signature = (
        host = "127.0.0.1",
        port = 9229,
        wait_for_connection = false,
        break_on_next_statement = false,
        target_url = None,
        display_name = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: &str,
        port: u16,
        wait_for_connection: bool,
        break_on_next_statement: bool,
        target_url: Option<String>,
        display_name: Option<String>,
    ) -> PyResult<Self> {
        let address = parse_socket_addr(host, port)?;
        Ok(Self {
            address,
            wait_for_connection,
            break_on_next_statement,
            target_url,
            display_name,
        })
    }

    #[getter]
    fn host(&self) -> String {
        self.address.ip().to_string()
    }

    #[getter]
    fn port(&self) -> u16 {
        self.address.port()
    }

    #[getter]
    fn wait_for_connection(&self) -> bool {
        self.wait_for_connection
    }

    #[setter]
    fn set_wait_for_connection(&mut self, wait: bool) {
        self.wait_for_connection = wait;
    }

    #[getter]
    fn break_on_next_statement(&self) -> bool {
        self.break_on_next_statement
    }

    #[setter]
    fn set_break_on_next_statement(&mut self, should_break: bool) {
        self.break_on_next_statement = should_break;
    }

    #[getter]
    fn target_url(&self) -> Option<String> {
        self.target_url.clone()
    }

    #[setter]
    fn set_target_url(&mut self, url: Option<String>) {
        self.target_url = url;
    }

    #[getter]
    fn display_name(&self) -> Option<String> {
        self.display_name.clone()
    }

    #[setter]
    fn set_display_name(&mut self, name: Option<String>) {
        self.display_name = name;
    }

    fn endpoint(&self) -> String {
        self.address.to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "InspectorConfig(host={:?}, port={}, wait_for_connection={}, break_on_next_statement={}, target_url={:?}, display_name={:?})",
            self.host(),
            self.port(),
            self.wait_for_connection,
            self.break_on_next_statement,
            self.target_url,
            self.display_name,
        )
    }
}

/// Runtime configuration for a single JavaScript isolate.
///
/// Defines heap limits, optional bootstrap code, inspector settings, and
/// serialization constraints for a V8 runtime instance.
#[pyclass(module = "jsrun")]
#[derive(Debug, Clone)]
#[allow(dead_code)] // Exposed to Python bindings; some fields are not wired yet in Rust.
pub struct RuntimeConfig {
    /// Maximum heap size in bytes (None = V8 default ~1.4 GB).
    pub max_heap_size: Option<usize>,

    /// Initial heap size in bytes (None = V8 default).
    pub initial_heap_size: Option<usize>,

    /// Optional timeout for script execution.
    pub execution_timeout: Option<Duration>,

    /// Bootstrap script to execute on startup (before user code).
    pub bootstrap_script: Option<String>,

    /// Enable console.log/error output (default: true).
    pub enable_console: Option<bool>,

    /// Optional inspector configuration for debugging.
    pub inspector: Option<InspectorConfig>,

    /// Startup snapshot bytes for faster initialization.
    pub snapshot: Option<Vec<u8>>,

    /// Maximum nesting depth for Python<->JS value serialization.
    pub max_serialization_depth: usize,

    /// Maximum serialized payload size for Python<->JS transfers (bytes).
    pub max_serialization_bytes: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_heap_size: None,
            initial_heap_size: None,
            execution_timeout: None,
            bootstrap_script: None,
            enable_console: Some(false),
            inspector: None,
            snapshot: None,
            max_serialization_depth: MAX_JS_DEPTH,
            max_serialization_bytes: MAX_JS_BYTES,
        }
    }
}

impl RuntimeConfig {
    fn duration_from_py_timeout(timeout_value: &Bound<'_, PyAny>) -> PyResult<Duration> {
        if let Ok(seconds) = timeout_value.extract::<f64>() {
            validate_timeout_seconds(seconds)?;
            return Ok(Duration::from_secs_f64(seconds));
        }

        if let Ok(seconds) = timeout_value.extract::<u64>() {
            let seconds_f64 = seconds as f64;
            validate_timeout_seconds(seconds_f64)?;
            return Ok(Duration::from_secs(seconds));
        }

        if let Ok(seconds) = timeout_value.extract::<i64>() {
            let seconds_f64 = seconds as f64;
            validate_timeout_seconds(seconds_f64)?;
            return Ok(Duration::from_secs(seconds as u64));
        }

        let py = timeout_value.py();
        let timedelta = py.import("datetime")?.getattr("timedelta")?;
        if timeout_value.is_instance(&timedelta)? {
            let total_seconds: f64 = timeout_value.getattr("total_seconds")?.call0()?.extract()?;
            validate_timeout_seconds(total_seconds)?;
            Ok(Duration::from_secs_f64(total_seconds))
        } else {
            Err(PyValueError::new_err(
                "Timeout must be a number (seconds) or datetime.timedelta object",
            ))
        }
    }

    pub fn serialization_limits(&self) -> SerializationLimits {
        SerializationLimits::new(self.max_serialization_depth, self.max_serialization_bytes)
    }
}

fn validate_serialization_depth(depth: usize) -> PyResult<()> {
    if depth == 0 {
        return Err(PyValueError::new_err(
            "max_serialization_depth must be a positive integer",
        ));
    }
    Ok(())
}

fn validate_serialization_bytes(bytes: usize) -> PyResult<()> {
    if bytes == 0 {
        return Err(PyValueError::new_err(
            "max_serialization_bytes must be a positive integer",
        ));
    }
    Ok(())
}

#[pymethods]
impl RuntimeConfig {
    /// Create a new runtime configuration with default settings.
    #[new]
    #[pyo3(signature = (
        max_heap_size = None,
        initial_heap_size = None,
        bootstrap = None,
        timeout = None,
        enable_console = Some(false),
        inspector = None,
        snapshot = None,
        max_serialization_depth = None,
        max_serialization_bytes = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_heap_size: Option<usize>,
        initial_heap_size: Option<usize>,
        bootstrap: Option<String>,
        timeout: Option<&Bound<'_, PyAny>>,
        enable_console: Option<bool>,
        inspector: Option<InspectorConfig>,
        snapshot: Option<&Bound<'_, PyAny>>,
        max_serialization_depth: Option<usize>,
        max_serialization_bytes: Option<usize>,
    ) -> PyResult<Self> {
        if bootstrap.is_some() && snapshot.is_some() {
            return Err(PyValueError::new_err(
                "snapshot and bootstrap cannot be used together",
            ));
        }

        let mut config = RuntimeConfig {
            inspector,
            ..RuntimeConfig::default()
        };

        // Set max heap size if provided
        if let Some(size) = max_heap_size {
            config.max_heap_size = Some(size);
        }

        // Set initial heap size if provided
        if let Some(size) = initial_heap_size {
            config.initial_heap_size = Some(size);
        }

        // Set bootstrap script if provided
        if let Some(script) = bootstrap {
            config.bootstrap_script = Some(script);
        }

        // Set timeout if provided
        if let Some(timeout_value) = timeout {
            let duration = RuntimeConfig::duration_from_py_timeout(timeout_value)?;
            config.execution_timeout = Some(duration);
        }

        // Set enable console if provided
        if let Some(enable) = enable_console {
            config.enable_console = Some(enable);
        }

        if let Some(snapshot_obj) = snapshot {
            let bytes = snapshot_obj.extract::<Vec<u8>>()?;
            if bytes.is_empty() {
                return Err(PyValueError::new_err("Snapshot bytes cannot be empty"));
            }
            config.snapshot = Some(bytes);
        }

        if let Some(depth) = max_serialization_depth {
            validate_serialization_depth(depth)?;
            config.max_serialization_depth = depth;
        }

        if let Some(bytes) = max_serialization_bytes {
            validate_serialization_bytes(bytes)?;
            config.max_serialization_bytes = bytes;
        }

        Ok(config)
    }

    /// Get maximum heap size in bytes.
    #[getter]
    fn max_heap_size(&self) -> Option<usize> {
        self.max_heap_size
    }

    /// Set maximum heap size in bytes.
    #[setter]
    fn set_max_heap_size(&mut self, bytes: usize) {
        self.max_heap_size = Some(bytes);
    }

    /// Get initial heap size in bytes.
    #[getter]
    fn initial_heap_size(&self) -> Option<usize> {
        self.initial_heap_size
    }

    /// Set initial heap size in bytes.
    #[setter]
    fn set_initial_heap_size(&mut self, bytes: usize) {
        self.initial_heap_size = Some(bytes);
    }

    /// Get bootstrap script.
    #[getter]
    fn bootstrap(&self) -> Option<String> {
        self.bootstrap_script.clone()
    }

    /// Set bootstrap script.
    #[setter]
    fn set_bootstrap(&mut self, source: String) -> PyResult<()> {
        if self.snapshot.is_some() {
            return Err(PyValueError::new_err(
                "snapshot and bootstrap cannot be used together",
            ));
        }
        self.bootstrap_script = Some(source);
        Ok(())
    }

    /// Get execution timeout in seconds.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.execution_timeout.map(|d| d.as_secs_f64())
    }

    /// Set execution timeout.
    /// Accepts float/int as seconds or datetime.timedelta object.
    #[setter]
    fn set_timeout<'py>(&mut self, timeout: &Bound<'py, PyAny>) -> PyResult<()> {
        self.execution_timeout = Some(RuntimeConfig::duration_from_py_timeout(timeout)?);
        Ok(())
    }

    /// Get enable console.
    #[getter]
    fn enable_console(&self) -> Option<bool> {
        self.enable_console
    }

    /// Get inspector configuration if enabled.
    #[getter]
    fn inspector(&self) -> Option<InspectorConfig> {
        self.inspector.clone()
    }

    /// Set inspector configuration.
    #[setter]
    fn set_inspector(&mut self, inspector: Option<InspectorConfig>) {
        self.inspector = inspector;
    }

    /// Get snapshot bytes (copied).
    ///
    /// Warning: this clones the entire snapshot. For large snapshots, avoid
    /// calling this repeatedly.
    #[getter]
    fn snapshot(&self) -> Option<Vec<u8>> {
        self.snapshot.clone()
    }

    /// Set snapshot bytes.
    #[setter]
    fn set_snapshot(&mut self, data: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        if data.is_none() {
            self.snapshot = None;
            return Ok(());
        }
        if self.bootstrap_script.is_some() {
            return Err(PyValueError::new_err(
                "snapshot and bootstrap cannot be used together",
            ));
        }
        let bytes = data.unwrap().extract::<Vec<u8>>()?;
        if bytes.is_empty() {
            return Err(PyValueError::new_err("Snapshot bytes cannot be empty"));
        }
        self.snapshot = Some(bytes);
        Ok(())
    }

    /// Maximum serialization depth for Python<->JS transfers.
    #[getter]
    fn max_serialization_depth(&self) -> usize {
        self.max_serialization_depth
    }

    /// Set maximum serialization depth for Python<->JS transfers.
    #[setter]
    fn set_max_serialization_depth(&mut self, depth: usize) -> PyResult<()> {
        validate_serialization_depth(depth)?;
        self.max_serialization_depth = depth;
        Ok(())
    }

    /// Maximum serialized byte size for Python<->JS transfers.
    #[getter]
    fn max_serialization_bytes(&self) -> usize {
        self.max_serialization_bytes
    }

    /// Set maximum serialized byte size for Python<->JS transfers.
    #[setter]
    fn set_max_serialization_bytes(&mut self, bytes: usize) -> PyResult<()> {
        validate_serialization_bytes(bytes)?;
        self.max_serialization_bytes = bytes;
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!("RuntimeConfig({:?})", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyFloat;

    #[test]
    fn test_default_config() {
        let config = RuntimeConfig::default();
        assert!(config.max_heap_size.is_none());
        assert!(config.initial_heap_size.is_none());
        assert!(config.execution_timeout.is_none());
        assert!(config.bootstrap_script.is_none());
        assert_eq!(config.enable_console, Some(false));
        assert!(config.inspector.is_none());
        assert!(config.snapshot.is_none());
        assert_eq!(config.max_serialization_depth, MAX_JS_DEPTH);
        assert_eq!(config.max_serialization_bytes, MAX_JS_BYTES);
    }

    #[allow(clippy::field_reassign_with_default)]
    #[test]
    fn test_config_builder() {
        let mut config = RuntimeConfig::default();
        config.max_heap_size = Some(100 * 1024 * 1024);
        config.execution_timeout = Some(Duration::from_secs(30));

        assert_eq!(config.max_heap_size, Some(100 * 1024 * 1024));
        assert_eq!(config.execution_timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_inspector_config_defaults() {
        let inspector = InspectorConfig::default();
        assert_eq!(inspector.host(), "127.0.0.1");
        assert_eq!(inspector.port(), 9229);
        assert!(!inspector.wait_for_connection);
        assert!(!inspector.break_on_next_statement);
        assert!(inspector.target_url().is_none());
        assert!(inspector.display_name().is_none());
    }

    #[test]
    fn test_runtime_config_with_inspector() {
        let mut inspector = InspectorConfig::default();
        inspector.set_wait_for_connection(true);
        inspector.set_break_on_next_statement(true);
        inspector.set_target_url(Some("module:main".to_string()));

        let config = RuntimeConfig {
            inspector: Some(inspector.clone()),
            ..RuntimeConfig::default()
        };

        let configured = config.inspector().expect("inspector config");
        assert!(configured.wait_for_connection());
        assert!(configured.break_on_next_statement());
        assert_eq!(configured.target_url(), Some("module:main".to_string()));
    }

    #[test]
    fn rejects_negative_timeout() {
        Python::attach(|py| {
            let timeout = PyFloat::new(py, -1.0).into_any();
            let err = RuntimeConfig::duration_from_py_timeout(&timeout);
            assert!(err.is_err());
        });
    }

    #[test]
    fn rejects_infinite_timeout() {
        Python::attach(|py| {
            let timeout = PyFloat::new(py, f64::INFINITY).into_any();
            let err = RuntimeConfig::duration_from_py_timeout(&timeout);
            assert!(err.is_err());
        });
    }
}
