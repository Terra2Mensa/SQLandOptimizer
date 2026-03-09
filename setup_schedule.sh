#!/bin/bash
# Install a daily scheduled run for the valuation engine.
# macOS: creates a launchd agent (12:30 PM daily)
# Linux: creates a cron entry

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run_daily.sh"
chmod +x "$RUN_SCRIPT"

if [ "$(uname)" = "Darwin" ]; then
    # macOS — launchd agent
    PLIST_NAME="com.$(whoami).cattle-valuation"
    PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"
    LOG_DIR="$SCRIPT_DIR/logs"
    mkdir -p "$LOG_DIR"

    cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_NAME</string>

    <key>ProgramArguments</key>
    <array>
        <string>$RUN_SCRIPT</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>12</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>$LOG_DIR/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/launchd_stderr.log</string>
</dict>
</plist>
PLIST

    # Unload old agent if running, then load new one
    launchctl bootout "gui/$(id -u)/$PLIST_NAME" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"

    echo "Installed launchd agent: $PLIST_NAME"
    echo "  Runs daily at 12:30 PM"
    echo "  Plist: $PLIST_PATH"

else
    # Linux — cron
    CRON_LINE="30 12 * * * $RUN_SCRIPT"
    if crontab -l 2>/dev/null | grep -qF "$RUN_SCRIPT"; then
        echo "Cron entry already exists."
    else
        (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
        echo "Installed cron entry: daily at 12:30 PM"
    fi
fi
