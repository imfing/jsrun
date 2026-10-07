//! Conversion helpers between Python objects and JSValue/serde_json values.

use crate::runtime::js_value::{JSValue, LimitTracker, SerializationLimits};
use crate::runtime::python::{runtime_error_to_py, PyStreamSource};
use indexmap::IndexMap;
use num_bigint::BigInt;
use pyo3::conversion::IntoPyObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyFrozenSet,
    PyFrozenSetMethods, PyInt, PyList, PyMemoryView, PySet, PySetMethods, PyString, PyTime,
};
use std::collections::HashSet;

const TYPE_TAG: &str = "__jsrun_type";
const UNDEFINED_TYPE: &str = "Undefined";
const DATE_TYPE: &str = "Date";
const TEMPORAL_TYPE: &str = "Temporal";
const DATE_EPOCH_KEY: &str = "epoch_ms";
const SET_TYPE: &str = "Set";
const SET_VALUES_KEY: &str = "values";
const BIGINT_TYPE: &str = "BigInt";
const BIGINT_VALUE_KEY: &str = "value";

/// Convert a JSValue into a Python object.
///
/// This is the new primary conversion function that supports native JavaScript values
/// including NaN and ±Infinity without sentinel strings.
///
/// Build a tagged Temporal payload, materialized as a real Temporal instance
/// on the JavaScript side by the bridge helpers.
fn temporal_tagged<const N: usize>(kind: &str, fields: [(&str, JSValue); N]) -> JSValue {
    let mut map = IndexMap::new();
    map.insert(TYPE_TAG.to_string(), JSValue::String(TEMPORAL_TYPE.into()));
    map.insert("kind".to_string(), JSValue::String(kind.into()));
    for (key, value) in fields {
        map.insert(key.to_string(), value);
    }
    JSValue::Object(map)
}

fn temporal_field_i64(map: &IndexMap<String, JSValue>, key: &str) -> PyResult<i64> {
    match map.get(key) {
        Some(JSValue::Int(v)) => Ok(*v),
        Some(JSValue::Float(f)) if f.is_finite() && f.fract() == 0.0 => Ok(*f as i64),
        _ => Err(PyRuntimeError::new_err(format!(
            "Invalid Temporal payload: missing or non-integer '{key}'"
        ))),
    }
}

fn temporal_field_i128(map: &IndexMap<String, JSValue>, key: &str) -> PyResult<i128> {
    let text = match map.get(key) {
        Some(JSValue::String(s)) => s.clone(),
        Some(JSValue::Int(v)) => return Ok(*v as i128),
        Some(JSValue::BigInt(b)) => b.to_string(),
        _ => {
            return Err(PyRuntimeError::new_err(format!(
                "Invalid Temporal payload: missing '{key}'"
            )))
        }
    };
    text.parse::<i128>().map_err(|_| {
        PyRuntimeError::new_err(format!("Invalid Temporal payload: non-integer '{key}'"))
    })
}

fn temporal_field_str<'a>(map: &'a IndexMap<String, JSValue>, key: &str) -> PyResult<&'a str> {
    match map.get(key) {
        Some(JSValue::String(s)) => Ok(s),
        _ => Err(PyRuntimeError::new_err(format!(
            "Invalid Temporal payload: missing '{key}'"
        ))),
    }
}

/// Resolve a Temporal time zone identifier to a Python tzinfo: "UTC",
/// fixed offsets like "+05:30", or IANA names via zoneinfo.
fn resolve_timezone<'py>(py: Python<'py>, tz: &str) -> PyResult<pyo3::Bound<'py, PyAny>> {
    let datetime_mod = py.import("datetime")?;
    let timezone_cls = datetime_mod.getattr("timezone")?;
    if tz.eq_ignore_ascii_case("UTC") {
        return timezone_cls.getattr("utc");
    }
    if let Some(rest) = tz.strip_prefix('+').or_else(|| tz.strip_prefix('-')) {
        let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
        let mut parts = rest.split(':');
        let parse = |p: Option<&str>| -> PyResult<i64> {
            match p {
                None => Ok(0),
                Some(s) => s.parse::<i64>().map_err(|_| {
                    PyRuntimeError::new_err(format!("Invalid time zone offset '{tz}'"))
                }),
            }
        };
        let hours = parse(parts.next())?;
        let minutes = parse(parts.next())?;
        let seconds = parse(parts.next())?;
        let offset = sign * (hours * 3600 + minutes * 60 + seconds);
        let kwargs = PyDict::new(py);
        kwargs.set_item("seconds", offset)?;
        let delta = datetime_mod.getattr("timedelta")?.call((), Some(&kwargs))?;
        return timezone_cls.call1((delta,));
    }
    let zoneinfo_cls = py.import("zoneinfo")?.getattr("ZoneInfo")?;
    zoneinfo_cls.call1((tz,)).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "Unknown time zone '{tz}' from Temporal value: {err}"
        ))
    })
}

/// Build an aware datetime from epoch nanoseconds (sub-microsecond precision
/// is truncated), optionally converted into the given time zone.
fn epoch_ns_to_datetime(py: Python<'_>, epoch_ns: i128, tz: Option<&str>) -> PyResult<Py<PyAny>> {
    let datetime_mod = py.import("datetime")?;
    let utc = datetime_mod.getattr("timezone")?.getattr("utc")?;
    let secs = epoch_ns.div_euclid(1_000_000_000);

    // Determine the zone's UTC offset at this instant, then build the local
    // wall-clock fields with integer math. Constructing the intermediate UTC
    // datetime would reject instants whose *local* representation is valid
    // (e.g. 0001-01-01T00:00:00+01:00 is fine, but its UTC form is year 0).
    let mut offset_delta = None;
    let (tzinfo, offset_ns) = match tz {
        None => (utc.clone(), 0i128),
        Some(tz) => {
            let tzinfo = resolve_timezone(py, tz)?;
            // Probe the offset at a clamped, always-representable instant:
            // exact whenever the instant itself is in datetime range, and a
            // best-effort approximation at the extreme year boundaries.
            let probe_secs: i64 = secs.clamp(-62_135_000_000, 253_300_000_000) as i64;
            let probe = datetime_mod
                .getattr("datetime")?
                .call_method1("fromtimestamp", (probe_secs, &utc))?
                .call_method1("astimezone", (&tzinfo,))?;
            let offset = probe.call_method0("utcoffset")?;
            let days: i128 = offset.getattr("days")?.extract()?;
            let seconds: i128 = offset.getattr("seconds")?.extract()?;
            let microseconds: i128 = offset.getattr("microseconds")?.extract()?;
            let offset_ns = ((days * 86_400 + seconds) * 1_000_000 + microseconds) * 1000;
            offset_delta = Some(offset);
            (tzinfo, offset_ns)
        }
    };

    let local_ns = epoch_ns + offset_ns;
    let days = local_ns.div_euclid(86_400_000_000_000);
    let rem_ns = local_ns.rem_euclid(86_400_000_000_000);
    // 719_163 is the proleptic-Gregorian ordinal of 1970-01-01.
    let ordinal: i64 = (days + 719_163)
        .try_into()
        .map_err(|_| PyRuntimeError::new_err("Temporal value out of range for Python datetime"))?;
    let py_date = datetime_mod
        .getattr("date")?
        .call_method1("fromordinal", (ordinal,))
        .map_err(|_| PyRuntimeError::new_err("Temporal value out of range for Python datetime"))?;
    let micros_of_day = rem_ns / 1000;
    let py_time = datetime_mod.getattr("time")?.call1((
        (micros_of_day / 3_600_000_000) as i64,
        (micros_of_day / 60_000_000 % 60) as i64,
        (micros_of_day / 1_000_000 % 60) as i64,
        (micros_of_day % 1_000_000) as i64,
    ))?;
    let mut dt = datetime_mod
        .getattr("datetime")?
        .call_method1("combine", (py_date, py_time, tzinfo))?;
    // Ambiguous wall times (DST fall-back) default to fold=0; pick fold=1
    // when that occurrence is the one matching the instant's actual offset.
    if let Some(offset_delta) = offset_delta {
        if !dt.call_method0("utcoffset")?.eq(&offset_delta)? {
            let kwargs = PyDict::new(py);
            kwargs.set_item("fold", 1)?;
            dt = dt.call_method("replace", (), Some(&kwargs))?;
        }
    }
    Ok(dt.unbind())
}

/// Convert a tagged Temporal payload into the matching Python type.
/// Returns Ok(None) for unknown kinds so the generic dict fallback applies.
fn temporal_to_python(
    py: Python<'_>,
    map: &IndexMap<String, JSValue>,
) -> PyResult<Option<Py<PyAny>>> {
    let kind = match map.get("kind") {
        Some(JSValue::String(kind)) => kind.as_str(),
        _ => return Ok(None),
    };
    let datetime_mod = py.import("datetime")?;
    let converted = match kind {
        "Instant" => epoch_ns_to_datetime(py, temporal_field_i128(map, "epoch_ns")?, None)?,
        "ZonedDateTime" => epoch_ns_to_datetime(
            py,
            temporal_field_i128(map, "epoch_ns")?,
            Some(temporal_field_str(map, "time_zone")?),
        )?,
        "PlainDate" => datetime_mod
            .getattr("date")?
            .call1((
                temporal_field_i64(map, "year")?,
                temporal_field_i64(map, "month")?,
                temporal_field_i64(map, "day")?,
            ))?
            .unbind(),
        "PlainTime" => datetime_mod
            .getattr("time")?
            .call1((
                temporal_field_i64(map, "hour")?,
                temporal_field_i64(map, "minute")?,
                temporal_field_i64(map, "second")?,
                temporal_field_i64(map, "nanosecond")? / 1000,
            ))?
            .unbind(),
        "PlainDateTime" => datetime_mod
            .getattr("datetime")?
            .call1((
                temporal_field_i64(map, "year")?,
                temporal_field_i64(map, "month")?,
                temporal_field_i64(map, "day")?,
                temporal_field_i64(map, "hour")?,
                temporal_field_i64(map, "minute")?,
                temporal_field_i64(map, "second")?,
                temporal_field_i64(map, "nanosecond")? / 1000,
            ))?
            .unbind(),
        "Duration" => {
            // i128 microseconds convert to an arbitrary-precision Python int;
            // timedelta's own constructor enforces its range.
            let micros: i128 = temporal_field_i128(map, "ns")? / 1000;
            let kwargs = PyDict::new(py);
            kwargs.set_item("microseconds", micros)?;
            datetime_mod
                .getattr("timedelta")?
                .call((), Some(&kwargs))?
                .unbind()
        }
        _ => return Ok(None),
    };
    Ok(Some(converted))
}

/// For Function variants, a RuntimeHandle must be provided to create JsFunction proxies.
pub(crate) fn js_value_to_python(
    py: Python<'_>,
    value: &JSValue,
    handle: Option<&super::handle::RuntimeHandle>,
) -> PyResult<Py<PyAny>> {
    match value {
        JSValue::Undefined => super::python::get_js_undefined(py).map(Into::into),
        JSValue::Null => Ok(py.None()),
        JSValue::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any().unbind()),
        JSValue::Int(i) => Ok(PyInt::new(py, *i).into_any().unbind()),
        JSValue::BigInt(bigint) => Ok(bigint.clone().into_pyobject(py)?.into_any().unbind()),
        JSValue::Float(f) => Ok(PyFloat::new(py, *f).into_any().unbind()),
        JSValue::String(s) => Ok(PyString::new(py, s).into_any().unbind()),
        JSValue::Bytes(bytes) => Ok(PyBytes::new(py, bytes).into_any().unbind()),
        JSValue::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(js_value_to_python(py, item, handle)?)?;
            }
            Ok(list.into_any().unbind())
        }
        JSValue::Set(items) => {
            let py_set = PySet::empty(py)?;
            for item in items {
                py_set.add(js_value_to_python(py, item, handle)?)?;
            }
            Ok(py_set.into_any().unbind())
        }
        JSValue::Object(map) => {
            if let Some(JSValue::String(tag)) = map.get(TYPE_TAG) {
                match tag.as_str() {
                    UNDEFINED_TYPE => {
                        return super::python::get_js_undefined(py).map(Into::into);
                    }
                    DATE_TYPE => {
                        if let Some(epoch_value) = map.get(DATE_EPOCH_KEY) {
                            let epoch_ms = match epoch_value {
                                JSValue::Int(i) => *i,
                                JSValue::Float(f) if f.is_finite() => *f as i64,
                                _ => {
                                    return Err(PyRuntimeError::new_err(
                                        "Invalid epoch_ms payload for Date",
                                    ))
                                }
                            };
                            let datetime = py.import("datetime")?;
                            let datetime_cls = datetime.getattr("datetime")?;
                            let timezone = datetime.getattr("timezone")?;
                            let utc = timezone.getattr("utc")?;
                            let seconds = epoch_ms as f64 / 1000.0;
                            let py_dt = datetime_cls
                                .call_method1("fromtimestamp", (seconds, utc))?
                                .into_any()
                                .unbind();
                            return Ok(py_dt);
                        }
                    }
                    SET_TYPE => {
                        if let Some(JSValue::Array(values)) = map.get(SET_VALUES_KEY) {
                            let py_set = PySet::empty(py)?;
                            for item in values {
                                py_set.add(js_value_to_python(py, item, handle)?)?;
                            }
                            return Ok(py_set.into_any().unbind());
                        }
                    }
                    BIGINT_TYPE => {
                        if let Some(JSValue::String(text)) = map.get(BIGINT_VALUE_KEY) {
                            let value =
                                BigInt::parse_bytes(text.as_bytes(), 10).ok_or_else(|| {
                                    PyRuntimeError::new_err(
                                        "Invalid BigInt payload from JavaScript",
                                    )
                                })?;
                            let obj = value.into_pyobject(py)?;
                            return Ok(obj.into_any().unbind());
                        }
                    }
                    TEMPORAL_TYPE => {
                        if let Some(converted) = temporal_to_python(py, map)? {
                            return Ok(converted);
                        }
                    }
                    _ => {}
                }
            }
            let dict = PyDict::new(py);
            for (key, val) in map {
                dict.set_item(key, js_value_to_python(py, val, handle)?)?;
            }
            Ok(dict.into_any().unbind())
        }
        JSValue::Date(epoch_ms) => {
            let datetime = py.import("datetime")?;
            let datetime_cls = datetime.getattr("datetime")?;
            let timezone = datetime.getattr("timezone")?;
            let utc = timezone.getattr("utc")?;
            let seconds = *epoch_ms as f64 / 1000.0;
            let py_dt = datetime_cls.call_method1("fromtimestamp", (seconds, utc))?;
            Ok(py_dt.into_any().unbind())
        }
        JSValue::Function { id } => {
            // Create JsFunction proxy
            let handle = handle.ok_or_else(|| {
                PyRuntimeError::new_err("RuntimeHandle required to convert JSValue::Function")
            })?;

            let js_fn = super::python::JsFunction::new(
                py,
                handle.clone(),
                *id,
                handle.serialization_limits(),
            )?;
            Ok(js_fn.into_any())
        }
        JSValue::JsStream { id } => {
            let handle = handle.ok_or_else(|| {
                PyRuntimeError::new_err("RuntimeHandle required to convert JSValue::JsStream")
            })?;
            let js_stream = super::python::JsStream::new(py, handle.clone(), *id)?;
            Ok(js_stream.into_any())
        }
        JSValue::PyStream { .. } => Err(PyRuntimeError::new_err(
            "PyStream placeholders cannot be materialized on the Python side",
        )),
    }
}

/// Convert a Python object into a JSValue.
///
/// This is used by the ops system to convert Python handler arguments to JSValue.
pub(crate) fn python_to_js_value(
    obj: Bound<'_, PyAny>,
    limits: &SerializationLimits,
) -> PyResult<JSValue> {
    let mut seen: HashSet<usize> = HashSet::new();
    let mut tracker = LimitTracker::new(limits.max_depth, limits.max_bytes);
    python_to_js_value_internal(obj, 0, &mut seen, &mut tracker, limits)
}

fn python_to_js_value_internal(
    obj: Bound<'_, PyAny>,
    depth: usize,
    seen: &mut HashSet<usize>,
    tracker: &mut LimitTracker,
    limits: &SerializationLimits,
) -> PyResult<JSValue> {
    if depth > limits.max_depth {
        return Err(PyRuntimeError::new_err(format!(
            "Depth limit exceeded: {} > {}",
            depth, limits.max_depth
        )));
    }

    tracker.enter().map_err(runtime_error_to_py)?;

    let add_bytes = |bytes: usize, tracker: &mut LimitTracker| {
        tracker.add_bytes(bytes).map_err(runtime_error_to_py)
    };

    let py = obj.py();

    let result = if obj.is_none() {
        add_bytes(4, tracker)?;
        Ok(JSValue::Null)
    } else if obj.extract::<PyRef<super::python::JsUndefined>>().is_ok() {
        add_bytes(0, tracker)?;
        Ok(JSValue::Undefined)
    } else if let Ok(stream) = obj.extract::<PyRef<PyStreamSource>>() {
        add_bytes(size_of::<u32>(), tracker)?;
        let stream_id = stream.stream_id_for_transfer()?;
        Ok(JSValue::PyStream { id: stream_id })
    } else if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        let data = py_bytes.as_bytes();
        add_bytes(data.len(), tracker)?;
        Ok(JSValue::Bytes(data.to_vec()))
    } else if let Ok(py_bytearray) = obj.cast::<PyByteArray>() {
        let data = unsafe { py_bytearray.as_bytes() };
        add_bytes(data.len(), tracker)?;
        Ok(JSValue::Bytes(data.to_vec()))
    } else if let Ok(memory_view) = obj.cast::<PyMemoryView>() {
        let bytes_obj = memory_view.call_method0(pyo3::intern!(py, "tobytes"))?;
        let data: Vec<u8> = bytes_obj.extract()?;
        add_bytes(data.len(), tracker)?;
        Ok(JSValue::Bytes(data))
    } else if let Ok(list) = obj.cast::<PyList>() {
        let ptr = list.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyRuntimeError::new_err(
                "Circular reference detected while converting Python list",
            ));
        }

        add_bytes(16, tracker)?;
        add_bytes(list.len().saturating_mul(size_of::<usize>()), tracker)?;

        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(python_to_js_value_internal(
                item,
                depth + 1,
                seen,
                tracker,
                limits,
            )?);
        }
        seen.remove(&ptr);
        Ok(JSValue::Array(items))
    } else if let Ok(dict) = obj.cast::<PyDict>() {
        let ptr = dict.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyRuntimeError::new_err(
                "Circular reference detected while converting Python dict",
            ));
        }

        add_bytes(24, tracker)?;
        add_bytes(dict.len().saturating_mul(size_of::<usize>() * 2), tracker)?;

        let mut map = IndexMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key_str = key.extract::<String>()?;
            add_bytes(key_str.len(), tracker)?;
            add_bytes(8, tracker)?;
            map.insert(
                key_str,
                python_to_js_value_internal(value, depth + 1, seen, tracker, limits)?,
            );
        }
        seen.remove(&ptr);
        Ok(JSValue::Object(map))
    } else if let Ok(py_set) = obj.cast::<PySet>() {
        let ptr = py_set.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyRuntimeError::new_err(
                "Circular reference detected while converting Python set",
            ));
        }

        add_bytes(24, tracker)?;
        add_bytes(py_set.len().saturating_mul(size_of::<usize>()), tracker)?;

        let mut items = Vec::with_capacity(py_set.len());
        for item in py_set.iter() {
            items.push(python_to_js_value_internal(
                item,
                depth + 1,
                seen,
                tracker,
                limits,
            )?);
        }
        seen.remove(&ptr);
        Ok(JSValue::Set(items))
    } else if let Ok(py_frozenset) = obj.cast::<PyFrozenSet>() {
        let ptr = py_frozenset.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyRuntimeError::new_err(
                "Circular reference detected while converting Python frozenset",
            ));
        }

        add_bytes(24, tracker)?;
        add_bytes(
            py_frozenset.len().saturating_mul(size_of::<usize>()),
            tracker,
        )?;

        let mut items = Vec::with_capacity(py_frozenset.len());
        for item in py_frozenset.iter() {
            items.push(python_to_js_value_internal(
                item,
                depth + 1,
                seen,
                tracker,
                limits,
            )?);
        }
        seen.remove(&ptr);
        Ok(JSValue::Set(items))
    } else if let Ok(py_datetime) = obj.cast::<PyDateTime>() {
        add_bytes(16, tracker)?;
        let datetime_mod = py.import(pyo3::intern!(py, "datetime"))?;
        let timezone = datetime_mod.getattr(pyo3::intern!(py, "timezone"))?;
        let utc = timezone.getattr(pyo3::intern!(py, "utc"))?;

        let dt_any = py_datetime.clone().into_any();
        let offset = dt_any.call_method0(pyo3::intern!(py, "utcoffset"))?;
        let normalized = if offset.is_none() {
            let kwargs = PyDict::new(py);
            kwargs.set_item(pyo3::intern!(py, "tzinfo"), utc)?;
            dt_any.call_method(pyo3::intern!(py, "replace"), (), Some(&kwargs))?
        } else {
            dt_any.call_method1(pyo3::intern!(py, "astimezone"), (utc,))?
        };

        let timestamp = normalized
            .call_method0(pyo3::intern!(py, "timestamp"))?
            .extract::<f64>()?;
        if !timestamp.is_finite() {
            return Err(PyRuntimeError::new_err(
                "datetime.timestamp returned non-finite value",
            ));
        }
        let epoch_ms = timestamp * 1000.0;
        if !epoch_ms.is_finite() || epoch_ms < i64::MIN as f64 || epoch_ms > i64::MAX as f64 {
            return Err(PyRuntimeError::new_err(
                "Datetime value out of range for JavaScript Date",
            ));
        }
        Ok(JSValue::Date(epoch_ms.round() as i64))
    } else if let Ok(py_date) = obj.cast::<PyDate>() {
        // Pure date (datetime is matched above); becomes Temporal.PlainDate.
        add_bytes(16, tracker)?;
        let field = |name: &str| -> PyResult<i64> { py_date.getattr(name)?.extract::<i64>() };
        Ok(temporal_tagged(
            "PlainDate",
            [
                ("year", JSValue::Int(field("year")?)),
                ("month", JSValue::Int(field("month")?)),
                ("day", JSValue::Int(field("day")?)),
            ],
        ))
    } else if let Ok(py_time) = obj.cast::<PyTime>() {
        // Becomes Temporal.PlainTime; aware times have no Temporal analogue.
        if !py_time.getattr("tzinfo")?.is_none() {
            return Err(PyRuntimeError::new_err(
                "datetime.time with tzinfo is not supported; use a full datetime instead",
            ));
        }
        add_bytes(16, tracker)?;
        let field = |name: &str| -> PyResult<i64> { py_time.getattr(name)?.extract::<i64>() };
        Ok(temporal_tagged(
            "PlainTime",
            [
                ("hour", JSValue::Int(field("hour")?)),
                ("minute", JSValue::Int(field("minute")?)),
                ("second", JSValue::Int(field("second")?)),
                ("nanosecond", JSValue::Int(field("microsecond")? * 1000)),
            ],
        ))
    } else if let Ok(py_delta) = obj.cast::<PyDelta>() {
        // Becomes Temporal.Duration (exact: timedelta is microsecond-based).
        add_bytes(24, tracker)?;
        let field = |name: &str| -> PyResult<i128> { py_delta.getattr(name)?.extract::<i128>() };
        let total_ns = ((field("days")? * 86_400 + field("seconds")?) * 1_000_000
            + field("microseconds")?)
            * 1000;
        Ok(temporal_tagged(
            "Duration",
            [("ns", JSValue::String(total_ns.to_string()))],
        ))
    } else if let Ok(b) = obj.extract::<bool>() {
        add_bytes(1, tracker)?;
        Ok(JSValue::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        add_bytes(size_of::<i64>(), tracker)?;
        Ok(JSValue::Int(i))
    } else if let Ok(bigint) = obj.extract::<BigInt>() {
        let (_, magnitude) = bigint.to_bytes_le();
        add_bytes(magnitude.len(), tracker)?;
        Ok(JSValue::BigInt(bigint))
    } else if let Ok(f) = obj.extract::<f64>() {
        add_bytes(size_of::<f64>(), tracker)?;
        Ok(JSValue::Float(f))
    } else if let Ok(s) = obj.extract::<String>() {
        if s.len() > limits.max_bytes {
            return Err(PyRuntimeError::new_err(format!(
                "String size limit exceeded: {} > {}",
                s.len(),
                limits.max_bytes
            )));
        }
        add_bytes(s.len(), tracker)?;
        add_bytes(16, tracker)?;
        Ok(JSValue::String(s))
    } else if let Ok(js_fn) = obj.extract::<PyRef<super::python::JsFunction>>() {
        // JsFunction proxy - extract the function ID for round-trip
        // This validates that the function is not closed and runtime is alive
        let id = js_fn.function_id_for_transfer()?;
        add_bytes(8, tracker)?;
        Ok(JSValue::Function { id })
    } else {
        Err(PyRuntimeError::new_err(
            "Unsupported Python type for JSValue conversion",
        ))
    };

    tracker.exit();
    result
}
