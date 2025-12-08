#!/bin/bash

# Configuration
REPO_URL="git@github.com:alanpaulkwan/maicro_monitors.git"
SSH_KEY_PATH="/home/apkwan/.ssh/id_ed25519_maicro_monitors"

# Ensure we are in the script's directory
cd "$(dirname "$0")"

# Make sure large or deprecated local directories are not pushed
# We want to keep `maicro_ignore_old/` and `deprecated/` out of the repo and avoid pushing them

# Initialize git if needed
if [ ! -d ".git" ]; then
    echo "Initializing git repository..."
    git init
    git branch -M main
fi

# Set specific SSH command for this repo to use the custom key
# This ensures we use the correct key even if others are in ssh-agent
git config core.sshCommand "ssh -i $SSH_KEY_PATH -o IdentitiesOnly=yes"

# Configure user info locally for this repo
git config user.email "alanpaulkwan@gmail.com"
git config user.name "Alan Paul Kwan"

# Add remote if needed
if ! git remote | grep -q origin; then
    echo "Adding remote origin..."
    git remote add origin "$REPO_URL"
else
    # Ensure remote URL is correct
    git remote set-url origin "$REPO_URL"
fi

# Untrack certain directories if they were previously tracked so they don't get pushed
echo "Ensuring maicro_ignore_old/ and deprecated/ are not tracked by git (if they were previously committed)..."
# --ignore-unmatch prevents git from exiting non-zero if those paths don't exist or aren't tracked
git rm -r --cached --ignore-unmatch maicro_ignore_old deprecated || true

# Add all files
echo "Adding files..."
git add .

# Commit
if git diff-index --quiet HEAD --; then
    echo "No changes to commit."
else
    echo "Committing..."
    git commit -m "Automated push $(date '+%Y-%m-%d %H:%M:%S')"
fi

# Push
echo "Pushing to GitHub..."
git push -u origin main

echo "Done."
