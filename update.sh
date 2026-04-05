#!/usr/bin/env bash
# AWG Domain List Updater
# Запускается по cron, собирает списки и пушит в GitHub.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$REPO_DIR/update.log"
VENV="$REPO_DIR/venv"
MAX_LOG_LINES=1000
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# Ротация лога (не даём ему расти бесконечно)
if [[ -f "$LOG_FILE" ]]; then
    tail -n "$MAX_LOG_LINES" "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

exec >> "$LOG_FILE" 2>&1
echo ""
echo "════════════════════════════════════════"
echo "▶ $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "════════════════════════════════════════"

# Активируем venv
source "$VENV/bin/activate"

# Собираем списки
python "$REPO_DIR/scripts/build.py"

# Git push
cd "$REPO_DIR"
git add output/
if git diff --staged --quiet; then
    echo "✓ No changes — skip commit"
else
    COUNT=$(git diff --staged --numstat | wc -l | tr -d ' ')
    git commit -m "chore: update lists (${COUNT} files) — $(date -u '+%Y-%m-%d %H:%M UTC')"
    git push
    echo "✓ Pushed to GitHub"
fi
