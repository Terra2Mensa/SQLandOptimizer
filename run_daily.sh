#!/bin/bash
# Daily multi-species valuation runner
# Edit the sections below to change what runs each day.
# After editing, no need to reload the agent — changes take effect next run.

CATTLE_OPTIONS="--all-grades --save-db"
PORK_OPTIONS="--save-db"
LAMB_OPTIONS="--save-db"
# Chicken and goat are manual-entry only — uncomment to include if prices are current
# RUN_CHICKEN=true
# RUN_GOAT=true

# Auto-detect PostgreSQL location (Homebrew Apple Silicon, Homebrew Intel, Linux)
for pg_dir in /opt/homebrew/opt/postgresql@*/bin /usr/local/opt/postgresql@*/bin /usr/lib/postgresql/*/bin; do
    [ -d "$pg_dir" ] && export PATH="$pg_dir:$PATH" && break
done
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/src"

LOGDIR="$SCRIPT_DIR/logs"
mkdir -p "$LOGDIR"
LOGFILE="$LOGDIR/valuation_$(date +%Y%m%d_%H%M%S).log"
FAIL=0

echo "=== Multi-Species Valuation Run: $(date) ===" > "$LOGFILE"

echo "" >> "$LOGFILE"
echo "--- CATTLE ---" >> "$LOGFILE"
python3 cattle_valuation.py $CATTLE_OPTIONS >> "$LOGFILE" 2>&1 || FAIL=1

echo "" >> "$LOGFILE"
echo "--- PORK ---" >> "$LOGFILE"
python3 pork_valuation.py $PORK_OPTIONS >> "$LOGFILE" 2>&1 || FAIL=1

echo "" >> "$LOGFILE"
echo "--- LAMB ---" >> "$LOGFILE"
python3 lamb_valuation.py $LAMB_OPTIONS >> "$LOGFILE" 2>&1 || FAIL=1

if [ "${RUN_CHICKEN}" = "true" ]; then
    echo "" >> "$LOGFILE"
    echo "--- CHICKEN ---" >> "$LOGFILE"
    python3 chicken_valuation.py --save-db >> "$LOGFILE" 2>&1 || FAIL=1
fi

if [ "${RUN_GOAT}" = "true" ]; then
    echo "" >> "$LOGFILE"
    echo "--- GOAT ---" >> "$LOGFILE"
    python3 goat_valuation.py --save-db >> "$LOGFILE" 2>&1 || FAIL=1
fi

if [ $FAIL -ne 0 ]; then
    echo "FAILED at $(date)" >> "$LOGFILE"
    command -v osascript &>/dev/null && \
        osascript -e "display notification \"Valuation run failed — check logs\" with title \"Multi-Species Valuation\""
else
    echo "=== All species completed successfully: $(date) ===" >> "$LOGFILE"
fi

# Keep only last 30 days of logs
find "$LOGDIR" -name "valuation_*.log" -mtime +30 -delete 2>/dev/null

exit $FAIL
