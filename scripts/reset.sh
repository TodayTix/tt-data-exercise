#!/usr/bin/env bash
# Reset warehouse to initial state (full reset: init).
# Run from repo root.

set -euo pipefail
cd "$(dirname "$0")/.."

"$(dirname "$0")/init.sh"
