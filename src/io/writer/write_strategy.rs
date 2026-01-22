// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Writing strategies for optimal data output.

/// Writing strategy selector.
///
/// Determines how data is written to the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteStrategy {
    /// Auto-detect - use parallel if available, fallback to sequential
    #[default]
    Auto,
    /// Sequential writing - processes messages one by one
    Sequential,
    /// Parallel writing - compresses chunks in parallel
    Parallel,
}

impl WriteStrategy {
    /// Resolve Auto strategy to a concrete strategy based on format support.
    ///
    /// For writing, parallel is generally available for all formats,
    /// so Auto resolves to Parallel.
    pub fn resolve(&self) -> WriteStrategy {
        match self {
            WriteStrategy::Auto => WriteStrategy::Parallel,
            other => *other,
        }
    }
}

/// Sequential writing strategy.
///
/// Writes messages one at a time without parallel compression.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialWrite;

impl SequentialWrite {
    pub fn new() -> Self {
        Self
    }
}

/// Parallel writing strategy.
///
/// Compresses chunks in parallel for improved throughput.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelWrite {
    /// Number of compression threads
    pub num_threads: Option<usize>,
}

impl ParallelWrite {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }
}

/// Auto-detect writing strategy.
///
/// Automatically chooses parallel writing when available, falling back
/// to sequential for cases where parallel doesn't provide benefit.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoWrite;

impl AutoWrite {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_strategy_default() {
        let strategy = WriteStrategy::default();
        assert_eq!(strategy, WriteStrategy::Auto);
    }

    #[test]
    fn test_write_strategy_clone() {
        let strategy = WriteStrategy::Parallel;
        let cloned = strategy;
        assert_eq!(strategy, cloned);
    }

    #[test]
    fn test_write_strategy_partial_eq() {
        assert_eq!(WriteStrategy::Sequential, WriteStrategy::Sequential);
        assert_eq!(WriteStrategy::Parallel, WriteStrategy::Parallel);
        assert_ne!(WriteStrategy::Sequential, WriteStrategy::Parallel);
    }

    #[test]
    fn test_sequential_write_new() {
        let _seq = SequentialWrite::new();
    }

    #[test]
    fn test_sequential_write_unit() {
        let _seq = SequentialWrite;
    }

    #[test]
    fn test_parallel_write_new() {
        let parallel = ParallelWrite::new();
        assert_eq!(parallel.num_threads, None);
    }

    #[test]
    fn test_parallel_write_default() {
        let parallel = ParallelWrite::default();
        assert_eq!(parallel.num_threads, None);
    }

    #[test]
    fn test_parallel_write_with_threads() {
        let parallel = ParallelWrite::new().with_threads(4);
        assert_eq!(parallel.num_threads, Some(4));
    }

    #[test]
    fn test_parallel_write_clone() {
        let parallel = ParallelWrite::new().with_threads(8);
        let cloned = parallel;
        assert_eq!(parallel.num_threads, cloned.num_threads);
    }

    #[test]
    fn test_auto_write_new() {
        let _auto = AutoWrite::new();
    }
}
