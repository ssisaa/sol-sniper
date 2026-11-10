#!/usr/bin/env bash
# Restart the bot only when no buy or sell is being sent (no "LANE … sending now" without its "done").
# Usage: scripts/safe_restart.sh <logfile>
set -u
LOG="${1:?log file}"
for i in $(seq 1 120); do
  open=$(tail -400 "$LOG" | awk '/LANE .* — sending now/{n++} /LANE done/{n--} END{print (n>0)?n:0}')
  if [ "$open" = "0" ]; then break; fi
  sleep 0.5
done
P=$(pgrep -f "^venv/bin/python -m src.main")
[ -n "$P" ] && kill -9 $P
sleep 1
cp "$LOG" "${LOG%.out}.prev-$(date +%H%M%S).out" 2>/dev/null
nohup venv/bin/python -m src.main --mode live-feed --config config/sniper.yaml --duration 0 > "$LOG" 2>&1 &
echo "restarted pid $! (waited $((i/2))s for in-flight trades)"
