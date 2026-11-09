#!/usr/bin/env bash
# Run on the server from the sol-sniper folder:  bash install_caller_stream.sh /path/to/caller_stream_update.tar.gz
set -euo pipefail
TAR="${1:-caller_stream_update.tar.gz}"
STAMP=$(date +%Y%m%d-%H%M%S)
BK="backup-$STAMP"
mkdir -p "$BK"
for f in $(tar -tzf "$TAR"); do [ -f "$f" ] && mkdir -p "$BK/$(dirname "$f")" && cp -p "$f" "$BK/$f"; done
tar -xzf "$TAR"
python3 -m py_compile src/observability/exec_history.py src/execution/jupiter.py src/config/file_config.py src/config/runtime.py src/data/caller_stream.py src/data/rpc_client.py src/execution/rpc_pool.py src/execution/trader.py src/execution/tx_route.py src/execution/wallet_tokens.py src/main.py src/observability/dashboard.py src/observability/ledger.py src/risk/position_manager.py 
echo "Installed. Old files saved in $BK  (restore: cp -r $BK/src .)"
echo "Now restart the bot the way you normally start it."
