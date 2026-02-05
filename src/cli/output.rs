// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Output formatting utilities for CLI.

use std::io::IsTerminal as _;

use crate::cli::CliResult;

use serde::Serialize;

/// Output either as JSON or human-readable format.
///
/// If `json` is true, serializes `value` to JSON and prints to stdout.
/// Otherwise, calls `human_fn` for human-readable output.
pub fn output_json_or<T>(
    json: bool,
    value: &T,
    human_fn: impl FnOnce() -> std::io::Result<()>,
) -> CliResult<()>
where
    T: ?Sized + Serialize,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        human_fn()?;
    }
    Ok(())
}

/// Check if stdout is a terminal (for deciding default output format).
#[allow(dead_code)]
pub fn is_stdout_terminal() -> bool {
    std::io::stdout().is_terminal()
}

/// Check if stderr is a terminal (for deciding progress display).
#[allow(dead_code)]
pub fn is_stderr_terminal() -> bool {
    std::io::stderr().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_output_json_or_with_json_true() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        // Test that json=true outputs JSON
        let result = output_json_or(true, &data, || Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_output_json_or_with_json_false() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let called = &mut std::sync::atomic::AtomicBool::new(false);
        let result = output_json_or(false, &data, || {
            called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        assert!(result.is_ok());
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_is_stdout_terminal() {
        // Just verify the function runs without panicking
        let _ = is_stdout_terminal();
    }

    #[test]
    fn test_is_stderr_terminal() {
        // Just verify the function runs without panicking
        let _ = is_stderr_terminal();
    }

    #[test]
    fn test_output_json_or_human_fn_error() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let result = output_json_or(false, &data, || Err(std::io::Error::other("test error")));
        assert!(result.is_err());
    }

    #[test]
    fn test_output_json_or_empty_struct() {
        #[derive(Serialize)]
        struct Empty {}
        let data = Empty {};
        let result = output_json_or(true, &data, || Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_output_json_or_with_array() {
        let data = vec![1, 2, 3, 4, 5];
        let result = output_json_or(true, &data, || Ok(()));
        assert!(result.is_ok());
    }
}
