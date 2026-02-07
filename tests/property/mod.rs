// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Property-based tests for robocodec.
//!
//! This module contains property-based tests that verify invariants across
//! a wide range of randomly generated inputs using the proptest framework.

mod consistency;
mod ordering;
mod round_trip;
mod value_properties;
