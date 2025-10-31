//! Internal JSValue type for accurate JavaScript value representation.
//!
//! This module provides a replacement for `serde_json::Value` that can accurately
//! represent JavaScript values including NaN, ±Infinity, and properly detect
//! circular references and enforce depth/size limits.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Maximum depth for JavaScript value serialization
pub const MAX_JS_DEPTH: usize = 100;
/// Maximum size in bytes for JavaScript value serialization
pub const MAX_JS_BYTES: usize = 10 * 1024 * 1024; // 10MB

/// Internal representation of JavaScript values that can round-trip accurately.
///
/// Unlike `serde_json::Value`, this enum can represent special numeric values
/// (NaN, ±Infinity) and enforces proper depth/size limits during conversion.
///
/// Note: The Serialize/Deserialize implementations work directly with serde_v8
/// to preserve special float values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JSValue {
    /// JavaScript null
    Null,
    /// JavaScript boolean
    Bool(bool),
    /// JavaScript integer (within i64 range)
    Int(i64),
    /// JavaScript float (including NaN and ±Infinity)
    Float(f64),
    /// JavaScript string
    String(String),
    /// JavaScript array (preserves order)
    Array(Vec<JSValue>),
    /// JavaScript object (uses IndexMap to preserve insertion order)
    Object(IndexMap<String, JSValue>),
}

impl JSValue {}

/// Tracks depth and size limits during JavaScript value conversion.
///
/// This is used to enforce limits while traversing V8 values to prevent
/// excessive memory usage and stack overflow.
pub struct LimitTracker {
    max_depth: usize,
    max_bytes: usize,
    current_depth: usize,
    current_bytes: usize,
}

impl LimitTracker {
    /// Create a new limit tracker with the specified limits.
    pub fn new(max_depth: usize, max_bytes: usize) -> Self {
        Self {
            max_depth,
            max_bytes,
            current_depth: 0,
            current_bytes: 0,
        }
    }

    /// Enter a new depth level.
    ///
    /// Returns an error if the depth limit is exceeded.
    pub fn enter(&mut self) -> Result<(), String> {
        self.current_depth += 1;
        if self.current_depth > self.max_depth {
            return Err(format!(
                "Depth exceeded maximum limit of {}",
                self.max_depth
            ));
        }
        Ok(())
    }

    /// Exit a depth level.
    pub fn exit(&mut self) {
        self.current_depth = self.current_depth.saturating_sub(1);
    }

    /// Add to the byte count.
    ///
    /// Returns an error if the size limit is exceeded.
    pub fn add_bytes(&mut self, bytes: usize) -> Result<(), String> {
        self.current_bytes += bytes;
        if self.current_bytes > self.max_bytes {
            return Err(format!(
                "Size ({} bytes) exceeded maximum limit of {} bytes",
                self.current_bytes, self.max_bytes
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_js_value_creation() {
        // Test that we can create various JSValue types
        let _null = JSValue::Null;
        let _bool = JSValue::Bool(true);
        let _int = JSValue::Int(42);
        let _float = JSValue::Float(2.5);
        let _string = JSValue::String("hello".to_string());
        let _array = JSValue::Array(vec![JSValue::Int(1), JSValue::Int(2)]);
        let mut map = IndexMap::new();
        map.insert("key".to_string(), JSValue::String("value".to_string()));
        let _object = JSValue::Object(map);
    }

    #[test]
    fn test_js_value_special_floats() {
        let nan = JSValue::Float(f64::NAN);
        let inf = JSValue::Float(f64::INFINITY);
        let neg_inf = JSValue::Float(f64::NEG_INFINITY);

        // Verify they can be created without panicking
        assert!(matches!(nan, JSValue::Float(_)));
        assert!(matches!(inf, JSValue::Float(_)));
        assert!(matches!(neg_inf, JSValue::Float(_)));
    }

    #[test]
    fn test_limit_tracker_basic() {
        let mut tracker = LimitTracker::new(10, 1000);

        assert!(tracker.enter().is_ok());
        assert!(tracker.add_bytes(100).is_ok());
        tracker.exit();
    }

    #[test]
    fn test_limit_tracker_depth_exceeded() {
        let mut tracker = LimitTracker::new(3, 1000);

        assert!(tracker.enter().is_ok()); // depth 1
        assert!(tracker.enter().is_ok()); // depth 2
        assert!(tracker.enter().is_ok()); // depth 3
        assert!(tracker.enter().is_err()); // depth 4 - should fail
    }

    #[test]
    fn test_limit_tracker_size_exceeded() {
        let mut tracker = LimitTracker::new(10, 100);

        assert!(tracker.add_bytes(50).is_ok());
        assert!(tracker.add_bytes(40).is_ok());
        assert!(tracker.add_bytes(20).is_err()); // Total 110 - should fail
    }
}
