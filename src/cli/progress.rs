// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Simple progress indicator for long-running operations.

use std::io::IsTerminal as _;

/// Simple progress indicator for long-running operations.
pub struct Progress {
    /// Current progress
    current: u64,
    /// Total expected
    total: u64,
    /// Prefix message
    prefix: String,
    /// Whether to show progress
    enabled: bool,
    /// Last update width (for clearing)
    last_width: usize,
}

impl Progress {
    /// Create a new progress indicator.
    pub fn new(total: u64, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        let enabled = std::io::stderr().is_terminal();
        Self {
            current: 0,
            total,
            prefix,
            enabled,
            last_width: 0,
        }
    }

    /// Increment progress by 1.
    #[allow(dead_code)]
    pub fn inc(&mut self) {
        self.current += 1;
        self.draw();
    }

    /// Set progress to a specific value.
    pub fn set(&mut self, value: u64) {
        self.current = value.min(self.total);
        self.draw();
    }

    /// Finish the progress bar with a completion message.
    pub fn finish(mut self, msg: impl Into<String>) {
        self.current = self.total;
        self.draw();
        if self.enabled {
            eprintln!();
        }
        let msg = msg.into();
        if !msg.is_empty() {
            eprintln!("  {}", msg);
        }
    }

    /// Draw the current progress state.
    fn draw(&mut self) {
        if !self.enabled {
            return;
        }

        let percent = if self.total > 0 {
            (self.current * 100 / self.total).min(100)
        } else {
            100
        };

        let bar_width = 30;
        let filled = ((percent as usize) * bar_width / 100).min(bar_width);
        let empty = bar_width.saturating_sub(filled);

        let bar = "=".repeat(filled);
        let rest = " ".repeat(empty);

        let line = format!(
            "\r  {} [{}{}] {}/{} ({:>3}%)",
            self.prefix, bar, rest, self.current, self.total, percent
        );

        // Clear previous output by padding with spaces
        if line.len() < self.last_width {
            eprint!("{}", " ".repeat(self.last_width - line.len()));
        }

        eprint!("{}", line);
        self.last_width = line.len();
    }
}
