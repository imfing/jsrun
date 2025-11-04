//! Simplified runtime error types for bridging JavaScript exceptions to Python.

use std::fmt;

use deno_core::error::{CoreError, CoreErrorKind, JsError, JsStackFrame};
use deno_error::{JsErrorBox, JsErrorClass};
use tokio::time::error::Elapsed;

/// Convenient alias for results produced by the runtime layer.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Minimal frame metadata exposed to Python callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsFrameSummary {
    pub function_name: Option<String>,
    pub file_name: Option<String>,
    pub line_number: Option<i64>,
    pub column_number: Option<i64>,
}

impl From<JsStackFrame> for JsFrameSummary {
    fn from(frame: JsStackFrame) -> Self {
        Self {
            function_name: frame.function_name,
            file_name: frame.file_name,
            line_number: frame.line_number,
            column_number: frame.column_number,
        }
    }
}

/// Structured JavaScript exception metadata captured from V8.
#[derive(Debug, Clone)]
pub struct JsExceptionDetails {
    pub name: Option<String>,
    pub message: Option<String>,
    pub stack: Option<String>,
    pub frames: Vec<JsFrameSummary>,
}

impl JsExceptionDetails {
    pub(crate) fn from_js_error(mut error: JsError) -> Self {
        let frames = error
            .frames
            .into_iter()
            .map(JsFrameSummary::from)
            .collect::<Vec<_>>();

        let message = error.message.take().or_else(|| {
            (!error.exception_message.is_empty()).then(|| error.exception_message.clone())
        });

        let stack = match error.stack.take() {
            Some(stack) if !stack.is_empty() => Some(stack),
            _ if !error.exception_message.is_empty() => Some(error.exception_message.clone()),
            _ => None,
        };

        Self {
            name: error.name.take(),
            message,
            stack,
            frames,
        }
    }

    pub(crate) fn from_js_error_box(error: JsErrorBox) -> Self {
        let class = error.get_class().to_string();
        let message = error.get_message().to_string();

        Self {
            name: Some(class.clone()),
            message: Some(message.clone()),
            stack: None,
            frames: vec![],
        }
    }

    /// Render a concise description combining name and message.
    pub fn summary(&self) -> String {
        let name = self.name.as_deref().unwrap_or("Error");
        match self.message.as_deref() {
            Some(msg) if !msg.is_empty() => format!("{name}: {msg}"),
            _ => name.to_string(),
        }
    }
}

/// Runtime error surfaced to Python bindings and internal callers.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    JavaScript(JsExceptionDetails),
    Timeout { context: String },
    Internal { context: String },
    Terminated,
}

impl RuntimeError {
    pub fn javascript(details: JsExceptionDetails) -> Self {
        Self::JavaScript(details)
    }

    pub fn timeout(context: impl Into<String>) -> Self {
        Self::Timeout {
            context: context.into(),
        }
    }

    pub fn internal(context: impl Into<String>) -> Self {
        Self::Internal {
            context: context.into(),
        }
    }

    pub fn terminated() -> Self {
        Self::Terminated
    }

    /// Access the stored context message for non-JavaScript errors.
    pub fn context(&self) -> Option<&str> {
        match self {
            Self::JavaScript(_) => None,
            Self::Timeout { context } | Self::Internal { context } => Some(context.as_str()),
            Self::Terminated => Some("Runtime terminated"),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JavaScript(details) => write!(f, "{}", details.summary()),
            Self::Timeout { context } | Self::Internal { context } => f.write_str(context),
            Self::Terminated => f.write_str("Runtime terminated"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<CoreError> for RuntimeError {
    fn from(error: CoreError) -> Self {
        let error_message = error.to_string();
        let CoreError(inner) = error;
        match *inner {
            CoreErrorKind::Js(js_error) => {
                RuntimeError::javascript(JsExceptionDetails::from_js_error(*js_error))
            }
            CoreErrorKind::JsBox(js_error_box) => {
                RuntimeError::javascript(JsExceptionDetails::from_js_error_box(js_error_box))
            }
            _ => RuntimeError::internal(error_message),
        }
    }
}

impl From<Elapsed> for RuntimeError {
    fn from(_: Elapsed) -> Self {
        RuntimeError::timeout("JavaScript execution timed out")
    }
}

impl From<String> for RuntimeError {
    fn from(message: String) -> Self {
        RuntimeError::internal(message)
    }
}

impl From<&str> for RuntimeError {
    fn from(message: &str) -> Self {
        RuntimeError::internal(message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_js_frame() -> JsStackFrame {
        JsStackFrame {
            type_name: None,
            function_name: Some("fn".to_string()),
            method_name: None,
            file_name: Some("<eval>".to_string()),
            line_number: Some(1),
            column_number: Some(2),
            eval_origin: None,
            is_top_level: Some(true),
            is_eval: false,
            is_native: false,
            is_constructor: false,
            is_async: false,
            is_promise_all: false,
            promise_index: None,
        }
    }

    fn build_js_error() -> JsError {
        JsError {
            name: Some("TypeError".to_string()),
            message: Some("boom".to_string()),
            stack: Some("TypeError: boom\n    at <eval>:1:1".to_string()),
            cause: None,
            exception_message: "Uncaught TypeError: boom".to_string(),
            frames: vec![sample_js_frame()],
            source_line: None,
            source_line_frame_index: Some(0),
            aggregated: None,
            additional_properties: vec![],
        }
    }

    #[test]
    fn converts_js_error_into_runtime_error() {
        let js_error = build_js_error();
        let core_error = CoreError(Box::new(CoreErrorKind::Js(Box::new(js_error))));
        let runtime_error = RuntimeError::from(core_error);

        match runtime_error {
            RuntimeError::JavaScript(details) => {
                assert_eq!(details.name.as_deref(), Some("TypeError"));
                assert_eq!(details.message.as_deref(), Some("boom"));
                assert!(details
                    .stack
                    .as_deref()
                    .unwrap()
                    .contains("TypeError: boom"));
                assert_eq!(details.frames.len(), 1);
            }
            _ => panic!("Expected JavaScript error"),
        }
    }

    #[test]
    fn derives_message_when_missing() {
        let mut js_error = build_js_error();
        js_error.message = None;
        js_error.stack = None;
        let core_error = CoreError(Box::new(CoreErrorKind::Js(Box::new(js_error))));
        let runtime_error = RuntimeError::from(core_error);

        match runtime_error {
            RuntimeError::JavaScript(details) => {
                assert!(details.message.unwrap().contains("boom"));
                assert!(details.stack.unwrap().contains("boom"));
            }
            _ => panic!("Expected JavaScript error"),
        }
    }

    #[test]
    fn wraps_non_js_error_as_internal() {
        let runtime_error = RuntimeError::from("disk full");
        match runtime_error {
            RuntimeError::Internal { context } => {
                assert!(context.contains("disk full"));
            }
            _ => panic!("Expected internal error"),
        }
    }
}
