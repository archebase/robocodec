#!/bin/bash
# Setup git hooks for this repository
# Run: ./scripts/setup-hooks.sh

set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Set the hooks path to githooks directory
git config core.hooksPath githooks

echo "Git hooks configured to use: githooks/"
echo ""
echo "Installed hooks:"
ls -1 githooks/ 2>/dev/null || echo "  (none yet)"
