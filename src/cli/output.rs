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
pub fn is_stdout_terminal() -> bool {
    std::io::stdout().is_terminal()
}

/// Check if stderr is a terminal (for deciding progress display).
pub fn is_stderr_terminal() -> bool {
    std::io::stderr().is_terminal()
}
