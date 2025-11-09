//! Internal JSValue type for accurate JavaScript value representation.
//!
//! This module provides a replacement for `serde_json::Value` that can accurately
//! represent JavaScript values including NaN, ±Infinity, and properly detect
//! circular references and enforce depth/size limits.

use crate::runtime::error::{RuntimeError, RuntimeResult};
use indexmap::IndexMap;
use num_bigint::BigInt;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use serde_bytes::Bytes;

/// Maximum depth for JavaScript value serialization
pub const MAX_JS_DEPTH: usize = 100;
/// Maximum size in bytes for JavaScript value serialization
pub const MAX_JS_BYTES: usize = 10 * 1024 * 1024; // 10MB

/// Configurable serialization limits applied during Python<->JS transfers.
#[derive(Clone, Copy, Debug)]
pub struct SerializationLimits {
    pub max_depth: usize,
    pub max_bytes: usize,
}

impl SerializationLimits {
    pub const fn new(max_depth: usize, max_bytes: usize) -> Self {
        Self {
            max_depth,
            max_bytes,
        }
    }
}

impl Default for SerializationLimits {
    fn default() -> Self {
        Self::new(MAX_JS_DEPTH, MAX_JS_BYTES)
    }
}

const TYPE_TAG: &str = "__jsrun_type";
const UNDEFINED_TYPE: &str = "Undefined";
const DATE_TYPE: &str = "Date";
const DATE_EPOCH_KEY: &str = "epoch_ms";
const SET_TYPE: &str = "Set";
const SET_VALUES_KEY: &str = "values";
const BIGINT_TYPE: &str = "BigInt";
const BIGINT_VALUE_KEY: &str = "value";
const JS_STREAM_TYPE: &str = "JsStream";
const PY_STREAM_TYPE: &str = "PyStream";
const STREAM_ID_KEY: &str = "id";

/// Internal representation of JavaScript values that can round-trip accurately.
///
/// Unlike `serde_json::Value`, this enum can represent special numeric values
/// (NaN, ±Infinity) and enforces proper depth/size limits during conversion.
///
/// Note: The Serialize/Deserialize implementations are manually implemented
/// because the Function variant cannot be serialized.
#[derive(Clone, Debug, PartialEq)]
pub enum JSValue {
    /// JavaScript undefined
    Undefined,
    /// JavaScript null
    Null,
    /// JavaScript boolean
    Bool(bool),
    /// JavaScript integer (within i64 range)
    Int(i64),
    /// JavaScript BigInt
    BigInt(BigInt),
    /// JavaScript float (including NaN and ±Infinity)
    Float(f64),
    /// JavaScript string
    String(String),
    /// JavaScript bytes (Uint8Array / ArrayBuffer)
    Bytes(Vec<u8>),
    /// JavaScript array (preserves order)
    Array(Vec<JSValue>),
    /// JavaScript object (uses IndexMap to preserve insertion order)
    Object(IndexMap<String, JSValue>),
    /// JavaScript Date (epoch milliseconds, UTC)
    Date(i64),
    /// JavaScript Set (preserves insertion order captured from JS)
    Set(Vec<JSValue>),
    /// JavaScript function (proxy via registry ID)
    Function { id: u32 },
    /// JavaScript ReadableStream (proxied via runtime stream registry)
    JsStream { id: u32 },
    /// Python async iterable placeholder forwarded into JavaScript
    PyStream { id: u32 },
}

impl JSValue {}

// Manual Serialize implementation that errors on Function variant
impl Serialize for JSValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        match self {
            JSValue::Undefined => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry(TYPE_TAG, UNDEFINED_TYPE)?;
                map.end()
            }
            JSValue::Null => serializer.serialize_none(),
            JSValue::Bool(b) => serializer.serialize_bool(*b),
            JSValue::Int(i) => serializer.serialize_i64(*i),
            JSValue::BigInt(bigint) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TYPE_TAG, BIGINT_TYPE)?;
                map.serialize_entry(BIGINT_VALUE_KEY, &bigint.to_str_radix(10))?;
                map.end()
            }
            JSValue::Float(f) => serializer.serialize_f64(*f),
            JSValue::String(s) => serializer.serialize_str(s),
            JSValue::Bytes(bytes) => Bytes::new(bytes).serialize(serializer),
            JSValue::Array(arr) => arr.serialize(serializer),
            JSValue::Object(obj) => obj.serialize(serializer),
            JSValue::Date(epoch_ms) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TYPE_TAG, DATE_TYPE)?;
                map.serialize_entry(DATE_EPOCH_KEY, epoch_ms)?;
                map.end()
            }
            JSValue::Set(values) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TYPE_TAG, SET_TYPE)?;
                map.serialize_entry(SET_VALUES_KEY, values)?;
                map.end()
            }
            JSValue::Function { id } => Err(Error::custom(format!(
                "Cannot serialize JSValue::Function (id: {}). Functions must be called, not serialized.",
                id
            ))),
            JSValue::JsStream { id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TYPE_TAG, JS_STREAM_TYPE)?;
                map.serialize_entry(STREAM_ID_KEY, id)?;
                map.end()
            }
            JSValue::PyStream { id } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry(TYPE_TAG, PY_STREAM_TYPE)?;
                map.serialize_entry(STREAM_ID_KEY, id)?;
                map.end()
            }
        }
    }
}

// Manual Deserialize implementation that rejects Function variant
impl<'de> Deserialize<'de> for JSValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de;

        struct JSValueVisitor;

        impl<'de> serde::de::Visitor<'de> for JSValueVisitor {
            type Value = JSValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter
                    .write_str("a JavaScript value (null, bool, number, string, array, or object)")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(JSValue::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(JSValue::Int(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                if value <= i64::MAX as u64 {
                    Ok(JSValue::Int(value as i64))
                } else {
                    Ok(JSValue::Float(value as f64))
                }
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
                Ok(JSValue::Float(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(JSValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(JSValue::String(value))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(JSValue::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(JSValue::Undefined)
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(JSValue::Bytes(value.to_vec()))
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(JSValue::Bytes(value))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element()? {
                    vec.push(elem);
                }
                Ok(JSValue::Array(vec))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut object = IndexMap::new();
                let mut tag: Option<String> = None;

                while let Some((key, value)) = map.next_entry::<String, JSValue>()? {
                    if key == TYPE_TAG {
                        if let JSValue::String(tag_value) = value {
                            tag = Some(tag_value);
                        } else {
                            object.insert(key, value);
                        }
                    } else {
                        object.insert(key, value);
                    }
                }

                if let Some(tag_value) = tag.clone() {
                    match tag_value.as_str() {
                        UNDEFINED_TYPE => {
                            return Ok(JSValue::Undefined);
                        }
                        DATE_TYPE => {
                            if let Some(epoch_value) = object.get(DATE_EPOCH_KEY) {
                                if let JSValue::Int(epoch_ms) = epoch_value {
                                    return Ok(JSValue::Date(*epoch_ms));
                                } else if let JSValue::Float(epoch_float) = epoch_value {
                                    if epoch_float.is_finite() {
                                        return Ok(JSValue::Date(*epoch_float as i64));
                                    }
                                }
                            }
                        }
                        SET_TYPE => {
                            if let Some(JSValue::Array(values)) = object.get(SET_VALUES_KEY) {
                                return Ok(JSValue::Set(values.clone()));
                            }
                        }
                        BIGINT_TYPE => {
                            if let Some(entry) = object.get(BIGINT_VALUE_KEY) {
                                match entry {
                                    JSValue::String(value) => {
                                        let parsed = BigInt::parse_bytes(value.as_bytes(), 10)
                                            .ok_or_else(|| {
                                                de::Error::custom(format!(
                                                    "Invalid BigInt literal '{}'",
                                                    value
                                                ))
                                            })?;
                                        return Ok(JSValue::BigInt(parsed));
                                    }
                                    JSValue::Int(i) => {
                                        return Ok(JSValue::BigInt(BigInt::from(*i)));
                                    }
                                    other => {
                                        return Err(de::Error::custom(format!(
                                            "Invalid BigInt payload: expected string, got {:?}",
                                            other
                                        )));
                                    }
                                }
                            }
                        }
                        JS_STREAM_TYPE => {
                            if let Some(entry) = object.get(STREAM_ID_KEY) {
                                let id = match entry {
                                    JSValue::Int(v) if *v >= 0 => *v as u32,
                                    JSValue::Float(f) if f.is_finite() && *f >= 0.0 => *f as u32,
                                    other => {
                                        return Err(de::Error::custom(format!(
                                            "Invalid JsStream id payload: {:?}",
                                            other
                                        )))
                                    }
                                };
                                return Ok(JSValue::JsStream { id });
                            }
                        }
                        PY_STREAM_TYPE => {
                            if let Some(entry) = object.get(STREAM_ID_KEY) {
                                let id = match entry {
                                    JSValue::Int(v) if *v >= 0 => *v as u32,
                                    JSValue::Float(f) if f.is_finite() && *f >= 0.0 => *f as u32,
                                    other => {
                                        return Err(de::Error::custom(format!(
                                            "Invalid PyStream id payload: {:?}",
                                            other
                                        )))
                                    }
                                };
                                return Ok(JSValue::PyStream { id });
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(tag_value) = tag {
                    object.insert(TYPE_TAG.to_string(), JSValue::String(tag_value));
                }

                Ok(JSValue::Object(object))
            }
        }

        deserializer.deserialize_any(JSValueVisitor)
    }
}

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
    pub fn enter(&mut self) -> RuntimeResult<()> {
        self.current_depth += 1;
        if self.current_depth > self.max_depth {
            return Err(RuntimeError::internal(format!(
                "Depth exceeded maximum limit of {}",
                self.max_depth
            )));
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
    pub fn add_bytes(&mut self, bytes: usize) -> RuntimeResult<()> {
        self.current_bytes += bytes;
        if self.current_bytes > self.max_bytes {
            return Err(RuntimeError::internal(format!(
                "Size ({} bytes) exceeded maximum limit of {} bytes",
                self.current_bytes, self.max_bytes
            )));
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
