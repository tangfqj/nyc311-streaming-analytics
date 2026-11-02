#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON_BIN="${PYTHON_BIN:-python3.11}"
"$PYTHON_BIN" -m venv .venv
if command -v uv >/dev/null 2>&1; then
  uv pip install --python .venv/bin/python -r requirements.lock -e .
else
  .venv/bin/python -m pip install -r requirements.lock -e .
fi
source scripts/env.sh
.venv/bin/python -m nyc311.cli doctor
