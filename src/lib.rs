use pyo3::prelude::*;

mod runtime;

/// Python jsrun module
///
/// This module provides Python bindings to the jsrun JavaScript runtime.
#[pymodule]
fn _jsrun(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<runtime::python::Runtime>()?;
    m.add_class::<runtime::python::JsFunction>()?;
    m.add_class::<runtime::python::JsUndefined>()?;
    m.add_class::<runtime::python::RuntimeStats>()?;
    m.add_class::<runtime::python::JsFunctionFinalizer>()?;
    m.add_class::<runtime::RuntimeConfig>()?;
    m.add(
        "JavaScriptError",
        m.py().get_type::<runtime::python::JavaScriptError>(),
    )?;
    m.add(
        "RuntimeTerminated",
        m.py().get_type::<runtime::python::RuntimeTerminated>(),
    )?;
    let undefined: Py<PyAny> = runtime::python::get_js_undefined(m.py())?.into();
    m.add("undefined", undefined)?;
    Ok(())
}
