//! Timeout normalization helpers shared by multiple bindings.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::time::Duration;

fn validate_timeout_seconds(seconds: f64) -> PyResult<()> {
    if !seconds.is_finite() {
        return Err(PyValueError::new_err("Timeout must be finite"));
    }
    if seconds < 0.0 {
        return Err(PyValueError::new_err("Timeout cannot be negative"));
    }
    if seconds == 0.0 {
        return Err(PyValueError::new_err("Timeout cannot be zero"));
    }
    if seconds > u64::MAX as f64 {
        return Err(PyValueError::new_err("Timeout is too large"));
    }
    Ok(())
}

pub(crate) fn normalize_timeout_to_ms<'py>(
    timeout: Option<&Bound<'py, PyAny>>,
) -> PyResult<Option<u64>> {
    let Some(timeout_value) = timeout else {
        return Ok(None);
    };

    let duration = if let Ok(seconds) = timeout_value.extract::<f64>() {
        validate_timeout_seconds(seconds)?;
        Duration::from_secs_f64(seconds)
    } else if let Ok(seconds) = timeout_value.extract::<u64>() {
        if seconds == 0 {
            return Err(PyValueError::new_err("Timeout cannot be zero"));
        }
        Duration::from_secs(seconds)
    } else if let Ok(seconds) = timeout_value.extract::<i64>() {
        if seconds < 0 {
            return Err(PyValueError::new_err("Timeout cannot be negative"));
        }
        if seconds == 0 {
            return Err(PyValueError::new_err("Timeout cannot be zero"));
        }
        Duration::from_secs(seconds as u64)
    } else {
        let py = timeout_value.py();
        let timedelta = py.import("datetime")?.getattr("timedelta")?;
        if timeout_value.is_instance(&timedelta)? {
            let total_seconds: f64 = timeout_value.getattr("total_seconds")?.call0()?.extract()?;
            validate_timeout_seconds(total_seconds)?;
            Duration::from_secs_f64(total_seconds)
        } else {
            return Err(PyValueError::new_err(
                "Timeout must be a number (seconds), datetime.timedelta, or None",
            ));
        }
    };

    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        Ok(Some(u64::MAX))
    } else {
        Ok(Some(millis as u64))
    }
}
