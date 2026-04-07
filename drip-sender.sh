#!/bin/bash
# Standalone drip email sender for Forge Nightshift
# Sends 1 approved email every 20 minutes during business hours (07:00-19:00)
# Daily cap: 30 emails. Resets at midnight.
# No Ollama, no Tauri — just sqlite3 + curl + Resend API.

set -euo pipefail

DB="$HOME/Library/Application Support/com.fractionalforge.nightshift/nightshift.db"
LOG="$HOME/Library/Logs/nightshift-drip.log"
DAILY_LIMIT=45
WINDOW_START=7
WINDOW_END=19
SLEEP_SECONDS=960  # 16 minutes

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"
}

get_resend_key() {
  sqlite3 "$DB" "SELECT value FROM config WHERE key='resend_api_key';" 2>/dev/null
}

get_sent_today() {
  sqlite3 "$DB" "SELECT COUNT(*) FROM emails WHERE status='sent' AND date(sent_at)=date('now');" 2>/dev/null
}

send_one() {
  # Use python3 for safe DB read + JSON build, curl for API call (urllib blocked by Cloudflare)
  local result
  result=$(python3 << 'PYEOF'
import sqlite3, json, subprocess, sys, os

db_path = os.path.expanduser("~/Library/Application Support/com.fractionalforge.nightshift/nightshift.db")
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# Get one approved email
row = conn.execute(
    "SELECT id, to_email, from_email, subject, body FROM emails WHERE status='approved' ORDER BY created_at ASC LIMIT 1"
).fetchone()

if not row:
    print("NO_EMAILS")
    sys.exit(0)

email_id = row["id"]
to_email = row["to_email"]
from_email = row["from_email"]
subject = row["subject"]
body = row["body"]

# Fallback from_email from config
if not from_email:
    cfg = conn.execute("SELECT value FROM config WHERE key='from_email'").fetchone()
    from_email = cfg[0] if cfg else ""

if not to_email or not from_email:
    conn.execute("UPDATE emails SET status='failed', last_error='Missing to or from email', updated_at=datetime('now') WHERE id=?", (email_id,))
    conn.commit()
    print(f"SKIP|{email_id}|Missing to or from email")
    sys.exit(0)

# Get API key
cfg = conn.execute("SELECT value FROM config WHERE key='resend_api_key'").fetchone()
api_key = cfg[0] if cfg else ""
if not api_key:
    print("ERROR|No resend_api_key configured")
    sys.exit(0)

# Mark as sending
conn.execute("UPDATE emails SET status='sending', updated_at=datetime('now') WHERE id=?", (email_id,))
conn.commit()

# Send via Resend API using curl (urllib gets blocked by Cloudflare)
payload = json.dumps({
    "from": from_email,
    "to": [to_email],
    "subject": subject,
    "html": body
})

try:
    proc = subprocess.run([
        "curl", "-s", "-w", "\n%{http_code}",
        "-X", "POST", "https://api.resend.com/emails",
        "-H", f"Authorization: Bearer {api_key}",
        "-H", "Content-Type: application/json",
        "-d", payload
    ], capture_output=True, text=True, timeout=30)

    lines = proc.stdout.strip().split("\n")
    http_code = lines[-1] if lines else "0"
    resp_body = "\n".join(lines[:-1])

    if http_code in ("200", "201"):
        data = json.loads(resp_body)
        resend_id = data.get("id", "")
        conn.execute(
            "UPDATE emails SET status='sent', resend_id=?, sent_at=datetime('now'), updated_at=datetime('now') WHERE id=?",
            (resend_id, email_id)
        )
        conn.commit()
        sent_today = conn.execute("SELECT COUNT(*) FROM emails WHERE status='sent' AND date(sent_at)=date('now')").fetchone()[0]
        print(f"SENT|{to_email}|{resend_id}|{sent_today}")
    else:
        try:
            err_data = json.loads(resp_body)
            err_msg = err_data.get("message", err_data.get("error", f"HTTP {http_code}"))
        except:
            err_msg = f"HTTP {http_code}"
        conn.execute(
            "UPDATE emails SET status='failed', last_error=?, updated_at=datetime('now') WHERE id=?",
            (err_msg, email_id)
        )
        conn.commit()
        print(f"FAIL|{to_email}|{err_msg}")
except Exception as e:
    conn.execute(
        "UPDATE emails SET status='failed', last_error=?, updated_at=datetime('now') WHERE id=?",
        (str(e), email_id)
    )
    conn.commit()
    print(f"FAIL|unknown|{e}")
finally:
    conn.close()
PYEOF
  )

  # Parse python output
  local status
  status=$(echo "$result" | head -1 | cut -d'|' -f1)

  case "$status" in
    NO_EMAILS)
      log "No approved emails remaining"
      return 1
      ;;
    SKIP)
      log "SKIP $(echo "$result" | cut -d'|' -f2) — $(echo "$result" | cut -d'|' -f3)"
      return 1
      ;;
    ERROR)
      log "ERROR: $(echo "$result" | cut -d'|' -f2)"
      return 1
      ;;
    SENT)
      local to resend_id sent_today
      to=$(echo "$result" | cut -d'|' -f2)
      resend_id=$(echo "$result" | cut -d'|' -f3)
      sent_today=$(echo "$result" | cut -d'|' -f4)
      log "SENT to $to (resend=$resend_id) [$sent_today/$DAILY_LIMIT today]"
      return 0
      ;;
    FAIL)
      local to err
      to=$(echo "$result" | cut -d'|' -f2)
      err=$(echo "$result" | cut -d'|' -f3-)
      log "FAIL $to — $err"
      return 1
      ;;
    *)
      log "UNEXPECTED: $result"
      return 1
      ;;
  esac
}

# Main loop
log "=== Drip sender started (limit=$DAILY_LIMIT, window=$WINDOW_START-$WINDOW_END, interval=${SLEEP_SECONDS}s) ==="

while true; do
  current_hour=$(date '+%H' | sed 's/^0//')

  # Check business hours
  if [ "$current_hour" -lt "$WINDOW_START" ] || [ "$current_hour" -ge "$WINDOW_END" ]; then
    log "Outside business hours ($current_hour:00), sleeping"
    sleep "$SLEEP_SECONDS"
    continue
  fi

  # Check daily limit
  sent_today=$(get_sent_today)
  if [ "$sent_today" -ge "$DAILY_LIMIT" ]; then
    log "Daily limit reached ($sent_today/$DAILY_LIMIT), sleeping"
    sleep "$SLEEP_SECONDS"
    continue
  fi

  # Check any approved emails left
  remaining=$(sqlite3 "$DB" "SELECT COUNT(*) FROM emails WHERE status='approved';" 2>/dev/null)
  if [ "$remaining" -eq 0 ]; then
    log "No more approved emails. All done. Exiting."
    exit 0
  fi

  # Send one
  send_one || true

  log "Next send in $(( SLEEP_SECONDS / 60 )) minutes ($remaining approved remaining)"
  sleep "$SLEEP_SECONDS"
done
