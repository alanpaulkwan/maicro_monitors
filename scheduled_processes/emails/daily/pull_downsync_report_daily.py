#!/usr/bin/env python3
"""
Email wrapper for pull_data_downward_from_cloud.py

Runs the Cloud → chenlin down-sync job and emails the captured stdout/stderr
so you can see exactly what happened on each run.

Environment:
  - RESEND_API_KEY   (required to send email via Resend)
  - ALERT_EMAIL      (recipient, defaults to alanpaulkwan@gmail.com)
  - ALERT_FROM_EMAIL (optional, default 'Maicro Monitors <alerts@resend.dev>')
"""

import io
import os
import sys
import contextlib
from datetime import datetime
from typing import Tuple

import requests

# Make repo modules importable
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

from config.settings import get_secret  # noqa: E402
import scheduled_processes.pull_data_downward_from_cloud as downsync  # noqa: E402

RESEND_API_KEY = get_secret("RESEND_API_KEY")
TO_EMAIL = os.getenv("ALERT_EMAIL", "alanpaulkwan@gmail.com")
FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "Maicro Monitors <alerts@resend.dev>")


def run_downsync_capture_output() -> Tuple[int, str]:
    """Run pull_data_downward_from_cloud.main() and capture all stdout/stderr."""
    buf = io.StringIO()
    code = 0
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        try:
            downsync.main()
        except SystemExit as e:
            # Respect explicit sys.exit codes if used
            code = int(e.code) if isinstance(e.code, int) else 1
        except Exception as e:  # pragma: no cover - defensive
            print(f"[pull_downsync_report_daily] ERROR: {e!r}", file=sys.stderr)
            code = 1
    return code, buf.getvalue()


def send_email(subject: str, text_body: str) -> None:
    """Send plain-text email via Resend."""
    if not RESEND_API_KEY:
        print("RESEND_API_KEY not set; skipping down-sync email send.")
        return

    print(f"Sending down-sync report email to {TO_EMAIL}...")
    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "from": FROM_EMAIL,
        "to": [TO_EMAIL],
        "subject": subject,
        "text": text_body,
    }

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=15)
        resp.raise_for_status()
        print("Down-sync email sent successfully.")
    except Exception as e:  # pragma: no cover - network error path
        print(f"Failed to send down-sync email: {e}")
        if hasattr(e, "response") and getattr(e, "response", None) is not None:
            try:
                print(e.response.text)
            except Exception:
                pass


def main() -> None:
    print("[pull_downsync_report_daily] Starting...")
    started = datetime.utcnow()
    code, output = run_downsync_capture_output()
    finished = datetime.utcnow()

    status = "OK" if code == 0 else f"EXIT {code}"
    subject = f"[MAICRO CRON] Down-sync Cloud → chenlin ({status})"

    header = [
        "Maicro Cloud → chenlin down-sync report",
        "=======================================",
        f"Started (UTC):  {started:%Y-%m-%d %H:%M:%S}",
        f"Finished (UTC): {finished:%Y-%m-%d %H:%M:%S}",
        f"Exit status:    {status}",
        "",
        "Captured output:",
        "----------------",
        output.rstrip(),
        "",
    ]
    body = "\n".join(header)

    print("----- EMAIL BODY BEGIN -----")
    print(body)
    print("----- EMAIL BODY END -----")

    send_email(subject, body)
    print("[pull_downsync_report_daily] Done.")


if __name__ == "__main__":
    main()

