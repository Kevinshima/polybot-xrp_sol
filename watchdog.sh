#!/bin/bash
# Watchdog: monitors bot health and delegates restarts to systemd.
# systemd (polymarket-bot.service) is the authoritative process manager —
# this script only acts when the process is fully dead AND systemd didn't catch it.
#
# Usage:
#   nohup ./watchdog.sh >> logs/watchdog.log 2>&1 &

HEALTH_URL="http://localhost:8082/api/health"
BOT_DIR="/home/kevi/polymarket-bot"
WATCHDOG_LOG="$BOT_DIR/logs/watchdog.log"
CHECK_INTERVAL=60  # seconds between checks

mkdir -p "$BOT_DIR/logs"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "$(ts) [watchdog] $*" | tee -a "$WATCHDOG_LOG"; }

log "Watchdog started (checking every ${CHECK_INTERVAL}s)"

while true; do
    sleep "$CHECK_INTERVAL"

    # Try to reach the health endpoint
    response=$(curl -sf --max-time 5 "$HEALTH_URL" 2>/dev/null)
    curl_exit=$?

    if [ $curl_exit -ne 0 ]; then
        log "Health endpoint unreachable (curl exit=$curl_exit)"
        # Check if bot process is alive at all
        if pgrep -f "python main.py" > /dev/null 2>&1; then
            log "Bot process is running but dashboard is down — not restarting"
        else
            log "Bot process not found — restarting via systemd"
            sudo systemctl restart polymarket-bot
            log "systemctl restart issued"
        fi
        continue
    fi

    # Parse healthy field from JSON response
    healthy=$(echo "$response" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(str(d.get('healthy',True)).lower())" \
        2>/dev/null)
    age=$(echo "$response" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(d.get('last_activity_seconds_ago',0))" \
        2>/dev/null)

    if [ "$healthy" = "false" ]; then
        log "Bot frozen — last activity ${age}s ago — restarting via systemd"
        sudo systemctl restart polymarket-bot
        log "systemctl restart issued"
    else
        : # healthy — no action needed
    fi
done
