#!/usr/bin/env python3
import os
import sys

def main():
    # Get the root directory of the project
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    print("# Maicro Monitors & Sync Cron Entry")
    print("# Add this to your crontab (crontab -e)")
    print("")
    
    # Run the combined orchestrator and sync script every minute
    # Redirect output to a log file with rotation or append
    log_file = os.path.join(cwd, "logs", "monitors_and_sync.log")
    script_path = os.path.join(cwd, "scripts", "run_monitors_and_sync.sh")
    
    print(f"* * * * * {script_path} >> {log_file} 2>&1")
    print("")
    print("# Note: Ensure the log directory exists:")
    print(f"# mkdir -p {os.path.join(cwd, 'logs')}")

if __name__ == "__main__":
    main()
