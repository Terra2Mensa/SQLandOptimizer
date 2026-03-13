#!/bin/bash
# Sets up daily automated valuation runs.
# macOS: installs a launchd agent
# Linux: offers to create a cron entry

set -e

PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SCRIPT="$PROJ_DIR/run_daily.sh"

if [ "$(uname)" = "Darwin" ]; then
    PLIST_NAME="com.$(whoami).terra-mensa"
    PLIST_PATH="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"

    # Unload existing agent if present
    launchctl list "$PLIST_NAME" &>/dev/null && launchctl unload "$PLIST_PATH" 2>/dev/null

    cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_NAME</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
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
    <string>$PROJ_DIR/logs/launchd_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$PROJ_DIR/logs/launchd_stderr.log</string>
    <key>WorkingDirectory</key>
    <string>$PROJ_DIR</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF

    launchctl load "$PLIST_PATH"
    echo "Installed launchd agent: $PLIST_NAME"
    echo "Runs daily at 12:30 PM."
    echo "Plist: $PLIST_PATH"
    echo ""
    echo "To unload:  launchctl unload \"$PLIST_PATH\""
    echo "To test now: launchctl start \"$PLIST_NAME\""

else
    # Linux — offer cron entry
    CRON_LINE="30 12 * * 1-5 $RUN_SCRIPT"
    echo "Suggested cron entry (weekdays at 12:30 PM):"
    echo ""
    echo "  $CRON_LINE"
    echo ""
    read -rp "Add this to your crontab? [y/N] " answer
    if [[ "\$answer" =~ ^[Yy] ]]; then
        (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
        echo "Cron entry added."
    else
        echo "Skipped. You can add it manually with: crontab -e"
    fi
fi
