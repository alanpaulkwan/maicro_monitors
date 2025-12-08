#!/usr/bin/env python3
"""
register_cron.py
----------------

Helper script to install the recommended cron entries for
`maicro_monitors` without clobbering unrelated jobs.

Behavior:
  1. Reads the current user crontab (`crontab -l`).
  2. Runs `scripts/generate_cron.py` to get the suggested entries.
  3. Removes any existing lines in the current crontab that correspond
     to the scripts we are about to register (to avoid duplicates).
  4. Appends the fresh cron entries and installs the merged crontab.

Usage:
  cd <repo_root>
  python3 scripts/register_cron.py
"""

import os
import subprocess
import sys
from typing import List, Set


def _run_crontab_list() -> str:
    """Return current crontab as text (or empty string if none)."""
    try:
        res = subprocess.run(
            ["crontab", "-l"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        # crontab command missing; treat as empty
        return ""

    if res.returncode != 0:
        # Common case: "no crontab for user"
        return ""
    return res.stdout or ""


def _run_generate_cron(repo_root: str) -> str:
    """Run scripts/generate_cron.py and capture its output."""
    script_path = os.path.join(repo_root, "scripts", "generate_cron.py")
    res = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        check=True,
    )
    return res.stdout or ""


def _extract_script_keys(generated_lines: List[str]) -> Set[str]:
    """
    From generated cron lines, extract script path tokens we use as
    identifiers. For each non-comment cron line:
      - strip off the 5 cron fields,
      - grab the script path after `python3` (or the first token).
    """
    keys: Set[str] = set()
    for line in generated_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # Split into at most 6 parts: 5 time fields + command
        parts = stripped.split(maxsplit=5)
        if len(parts) < 6:
            # Not a standard cron line; skip
            continue
        cmd = parts[5]

        # If there's a cd/&& prefix, drop it
        if "&&" in cmd:
            cmd = cmd.split("&&", 1)[1].strip()

        tokens = cmd.split()
        if not tokens:
            continue

        script_token = None
        # Common pattern: /usr/bin/python3 <script> ...
        if len(tokens) >= 2 and tokens[0].endswith("python3"):
            script_token = tokens[1]
        else:
            script_token = tokens[0]

        # Only keep something that looks like a path to our repo
        if "scheduled_processes" in script_token or "ops/" in script_token:
            keys.add(script_token)

    return keys


def _install_crontab(new_cron: str) -> None:
    """Replace the user's crontab with new_cron."""
    subprocess.run(
        ["crontab", "-"],
        input=new_cron,
        text=True,
        check=True,
    )


def main() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    current = _run_crontab_list()
    generated = _run_generate_cron(repo_root)

    current_lines = current.splitlines()
    generated_lines = generated.splitlines()

    # Figure out which script paths we are about to register
    script_keys = _extract_script_keys(generated_lines)

    # Filter out any existing lines that already reference those scripts
    filtered_current: List[str] = []
    for line in current_lines:
        if any(key in line for key in script_keys):
            # Drop existing entry for this script so we can add a clean one
            continue
        filtered_current.append(line)

    # Build new crontab text: filtered current + full generated block
    merged_lines: List[str] = []
    if filtered_current:
        merged_lines.extend(filtered_current)
    # Always add a blank line between existing and generated block
    if merged_lines and merged_lines[-1].strip():
        merged_lines.append("")
    merged_lines.extend(generated_lines)

    new_cron = "\n".join(merged_lines).rstrip() + "\n"

    _install_crontab(new_cron)

    print("✅ Cron updated with maicro_monitors scheduled processes.")


if __name__ == "__main__":
    main()

