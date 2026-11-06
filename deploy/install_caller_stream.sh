#!/usr/bin/env bash
# Run on the server from the sol-sniper folder:  bash install_caller_stream.sh /path/to/caller_stream_update.tar.gz
set -euo pipefail
TAR="${1:-caller_stream_update.tar.gz}"
STAMP=$(date +%Y%m%d-%H%M%S)
BK="backup-$STAMP"
mkdir -p "$BK"
for f in $(tar -tzf "$TAR"); do [ -f "$f" ] && mkdir -p "$BK/$(dirname "$f")" && cp -p "$f" "$BK/$f"; done
tar -xzf "$TAR"
python3 -m py_compile src/data/caller_stream.py src/observability/dashboard.py src/config/runtime.py src/risk/position_manager.py src/main.py
echo "Installed. Old files saved in $BK  (restore: cp -r $BK/src .)"
echo "Now restart the bot the way you normally start it."
