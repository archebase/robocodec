// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Output formatting utilities for CLI.

use anyhow::Result;
use serde::Serialize;

/// Output either as JSON or human-readable format.
///
/// If `json` is true, serializes `value` to JSON and prints to stdout.
/// Otherwise, calls `human_fn` for human-readable output.
pub fn output_json_or<T>(
    json: bool,
    value: &T,
    human_fn: impl FnOnce() -> std::io::Result<()>,
) -> Result<()>
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
