#!/usr/bin/env bash
# ATT SDP workspace installer — thin wrapper around install/sdp_install.py
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if ! python3 -c "import yaml" 2>/dev/null; then
  echo "Installing PyYAML..."
  python3 -m pip install -q pyyaml
fi

exec python3 "$ROOT/install/sdp_install.py" "$@"
