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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_new() {
        let progress = Progress::new(100, "Testing");
        assert_eq!(progress.current, 0);
        assert_eq!(progress.total, 100);
    }

    #[test]
    fn test_progress_inc() {
        let mut progress = Progress::new(100, "Testing");
        progress.inc();
        assert_eq!(progress.current, 1);
        progress.inc();
        assert_eq!(progress.current, 2);
    }

    #[test]
    fn test_progress_set() {
        let mut progress = Progress::new(100, "Testing");
        progress.set(50);
        assert_eq!(progress.current, 50);
    }

    #[test]
    fn test_progress_set_clamps_to_total() {
        let mut progress = Progress::new(100, "Testing");
        progress.set(150);
        assert_eq!(progress.current, 100);
    }

    #[test]
    fn test_progress_set_zero() {
        let mut progress = Progress::new(100, "Testing");
        progress.set(0);
        assert_eq!(progress.current, 0);
    }

    #[test]
    fn test_progress_zero_total() {
        let mut progress = Progress::new(0, "Testing");
        assert_eq!(progress.total, 0);
        // Should not panic when drawing with zero total
        progress.set(0);
    }

    #[test]
    fn test_progress_finish_with_message() {
        let progress = Progress::new(100, "Testing");
        // Just verify it doesn't panic - actual output is to stderr
        progress.finish("Done");
    }

    #[test]
    fn test_progress_finish_with_empty_message() {
        let progress = Progress::new(100, "Testing");
        progress.finish("");
    }

    #[test]
    fn test_progress_multiple_sets() {
        let mut progress = Progress::new(100, "Testing");
        for i in 0..=100 {
            progress.set(i);
            assert_eq!(progress.current, i.min(100));
        }
    }

    #[test]
    fn test_progress_large_values() {
        let mut progress = Progress::new(1_000_000_000, "Large");
        progress.set(500_000_000);
        assert_eq!(progress.current, 500_000_000);
    }

    #[test]
    fn test_progress_prefix_variations() {
        let mut progress1 = Progress::new(100, "Prefix1");
        let mut progress2 = Progress::new(100, "");
        let mut progress3 = Progress::new(100, "A very long prefix message here");
        // Verify they don't panic
        progress1.set(10);
        progress2.set(10);
        progress3.set(10);
    }

    #[test]
    fn test_progress_set_same_value() {
        let mut progress = Progress::new(100, "Testing");
        progress.set(50);
        progress.set(50);
        assert_eq!(progress.current, 50);
    }
}
