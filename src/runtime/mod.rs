//! Tokio-based JavaScript runtime for multi-tenant execution.
//!
//! This module implements a Rust-first, async runtime patterned after `deno_core`.
//! Each runtime owns a single V8 isolate running on a dedicated OS thread with a
//! Tokio event loop.

pub mod config;
pub mod conversion;
pub mod handle;
pub mod js_value;
pub mod ops;
pub mod python;
pub mod runner;

#[allow(unused_imports)] // Re-exported for downstream crates.
pub use config::RuntimeConfig;
#[allow(unused_imports)] // Re-exported for downstream crates.
pub use handle::RuntimeHandle;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_runtime_lifecycle() {
        let config = RuntimeConfig::default();
        let mut handle = RuntimeHandle::spawn(config).unwrap();

        assert!(!handle.is_shutdown());

        // Evaluate some code
        let result = handle.eval_sync("40 + 2");
        assert!(result.is_ok());
        let js_value = result.unwrap();
        assert!(matches!(js_value, js_value::JSValue::Int(42)));

        // Shutdown
        handle.close().unwrap();
        assert!(handle.is_shutdown());
    }

    #[test]
    fn test_multiple_runtimes_sequential() {
        for i in 0..3 {
            let config = RuntimeConfig::default();
            let mut handle = RuntimeHandle::spawn(config).unwrap();

            let code = format!("{} * 2", i);
            let result = handle.eval_sync(&code);
            assert!(result.is_ok());
            let js_value = result.unwrap();
            assert!(matches!(js_value, js_value::JSValue::Int(val) if val == i * 2));

            handle.close().unwrap();
        }
    }

    #[test]
    fn test_concurrent_runtimes() {
        let mut handles = vec![];

        // Spawn multiple runtimes
        for _ in 0..3 {
            let config = RuntimeConfig::default();
            let handle = RuntimeHandle::spawn(config).unwrap();
            handles.push(handle);
        }

        // Use them concurrently
        let mut threads = vec![];
        for (i, handle) in handles.into_iter().enumerate() {
            let t = thread::spawn(move || {
                let code = format!("{} + 100", i);
                let result = handle.eval_sync(&code);
                assert!(result.is_ok());
                let js_value = result.unwrap();
                let expected = (i + 100) as i64;
                assert!(matches!(js_value, js_value::JSValue::Int(val) if val == expected));
            });
            threads.push(t);
        }

        // Wait for all threads
        for t in threads {
            t.join().unwrap();
        }
    }

    #[allow(clippy::field_reassign_with_default)]
    #[test]
    fn test_runtime_with_heap_limits() {
        let mut config = RuntimeConfig::default();
        config.max_heap_size = Some(10 * 1024 * 1024); // 10 MB
        config.initial_heap_size = Some(1024 * 1024); // 1 MB

        let handle = RuntimeHandle::spawn(config).unwrap();

        let result = handle.eval_sync("'hello'");
        assert!(result.is_ok());
        let js_value = result.unwrap();
        assert!(matches!(js_value, js_value::JSValue::String(s) if s == "hello"));
    }

    #[allow(clippy::field_reassign_with_default)]
    #[test]
    fn test_runtime_with_bootstrap() {
        let mut config = RuntimeConfig::default();
        config.bootstrap_script = Some("globalThis.VERSION = '1.0.0';".to_string());

        let handle = RuntimeHandle::spawn(config).unwrap();

        let result = handle.eval_sync("globalThis.VERSION");
        assert!(result.is_ok());
        let js_value = result.unwrap();
        assert!(matches!(js_value, js_value::JSValue::String(s) if s == "1.0.0"));
    }

    #[test]
    fn test_runtime_state_persistence() {
        let config = RuntimeConfig::default();
        let handle = RuntimeHandle::spawn(config).unwrap();

        // Set a variable
        let result1 = handle.eval_sync("var counter = 0; counter");
        assert!(matches!(result1.unwrap(), js_value::JSValue::Int(0)));

        // Increment it
        let result2 = handle.eval_sync("++counter");
        assert!(matches!(result2.unwrap(), js_value::JSValue::Int(1)));

        // Verify persistence
        let result3 = handle.eval_sync("counter");
        assert!(matches!(result3.unwrap(), js_value::JSValue::Int(1)));
    }
}
