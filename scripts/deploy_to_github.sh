#!/usr/bin/env bash

# Usage:
#  scripts/deploy_to_github.sh --owner <GITHUB_OWNER> --repo <REPO_NAME> [--branch <branch>] [--host-alias <ssh-host-alias>] [--key-path <path-to-private-key>]
# Example:
#  scripts/deploy_to_github.sh --owner myuser --repo maicro_monitors --branch main --host-alias github-maicro --key-path ~/.ssh/maicro_deploy_key

set -euo pipefail

GITHUB_OWNER=${GITHUB_OWNER:-alanpaulkwan}
GITHUB_REPO=${GITHUB_REPO:-maicro_monitors}
BRANCH=${BRANCH:-main}
# Prefer environment variables set by ~/.bashrc if present
HOST_ALIAS=${HOST_ALIAS:-${MAICRO_DEPLOY_HOST_ALIAS:-github-alanpaulkwan-maicro}}
KEY_PATH=${KEY_PATH:-${MAICRO_DEPLOY_KEY_PATH:-$HOME/.ssh/maicro_deploy_key}}

usage(){
  echo "Usage: $0 [--owner <OWNER>] [--repo <REPO>] [--branch <branch>] [--host-alias <alias>] [--key-path <path to private key>]"
  echo "Note: defaults are owner=alanpaulkwan and repo=maicro_monitors unless overridden."
  exit 1
}

if [ -f "$HOME/maestral/txt/maicro_deploy_key.pub" ]; then
  echo "Public key for GitHub deploy key is at: $HOME/maestral/txt/maicro_deploy_key.pub"
  echo "Copy this public key into GitHub -> Repository Settings -> Deploy keys (allow write access if you want to push)."
else
  echo "Public key not found at $HOME/maestral/txt/maicro_deploy_key.pub — if you generated a key, place its .pub file there or run scripts/setup_deploy_key.sh"
fi

# Parse args
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --owner)
      GITHUB_OWNER="$2"; shift; shift;;
    --repo)
      GITHUB_REPO="$2"; shift; shift;;
    --branch)
      BRANCH="$2"; shift; shift;;
    --host-alias)
      HOST_ALIAS="$2"; shift; shift;;
    --key-path)
      KEY_PATH="$2"; shift; shift;;
    --help|-h)
      usage
      ;;
    *)
      echo "Unknown arg: $1"; usage
      ;;
  esac
done

if [ -z "$GITHUB_OWNER" ] || [ -z "$GITHUB_REPO" ]; then
  echo "Error: owner and repo are not set; aborting."
  usage
fi

# Build remote url using host alias
REMOTE_URL="git@${HOST_ALIAS}:${GITHUB_OWNER}/${GITHUB_REPO}.git"

# Ensure repo exists (or initialize)
if [ ! -d .git ]; then
  echo "Initializing git repository in $(pwd)"
  git init
fi

ensure_gitignore(){
  GITIGNORE_FILE=".gitignore"
  BLOCK_START="# ---- maicro_monitors deploy block ----"
  BLOCK_END="# ---- end maicro_monitors deploy block ----"
  if grep -F "$BLOCK_START" "$GITIGNORE_FILE" >/dev/null 2>&1; then
    return
  fi
  cat >> "$GITIGNORE_FILE" <<EOF
$BLOCK_START
# Ignore directories containing the word 'ignore'
**/*ignore*/
**/*ignore*/**
# Ignore local deploy/private keys
*.key
*.pem
*.secret
 .deploy_key
deploy_key
maicro_deploy_key
# Python cache
__pycache__/
*.pyc
# Temp files
*.swp
*~
$BLOCK_END
EOF
  echo "Updated .gitignore with deploy defaults."
}

# Ensure .gitignore exists with basic protections
ensure_gitignore

# Configure SSH host alias in ~/.ssh/config if not present
SSH_CONFIG="$HOME/.ssh/config"

grep -F "Host ${HOST_ALIAS}" "$SSH_CONFIG" >/dev/null 2>&1 || {
  echo "Adding SSH host alias to $SSH_CONFIG"
  mkdir -p "$HOME/.ssh"
  chmod 700 "$HOME/.ssh"
  cat >> "$SSH_CONFIG" <<-EOF

Host ${HOST_ALIAS}
  HostName github.com
  User git
  IdentityFile ${KEY_PATH}
  IdentitiesOnly yes

EOF
  chmod 600 "$SSH_CONFIG"
}

# Add remote if not present
if ! git remote | grep -q "deploy-remote"; then
  git remote add deploy-remote "$REMOTE_URL"
  echo "Added remote 'deploy-remote' -> $REMOTE_URL"
else
  git remote set-url deploy-remote "$REMOTE_URL"
  echo "Updated remote 'deploy-remote' -> $REMOTE_URL"
fi

# Check for any staged/committed large files or ignore folders (we already added hooks but be safe)
if git status --porcelain | grep -q 'ignore'; then
  echo "Repository contains tracked files with 'ignore' in their path. Consider removing them before pushing. Aborting deploy."
  exit 1
fi

if git status --porcelain | grep -q 'deprecated'; then
  echo "Repository contains tracked files with 'deprecated' in their path. Consider removing them before pushing. Aborting deploy."
  exit 1
fi

# Find large files already committed (>1MB) -- abort
LARGE_FILES=$(git ls-tree -r --long HEAD | awk '$4 > 1048576 { print $5 "\t" $4 }' || true)
if [ -n "$LARGE_FILES" ]; then
  echo "Found committed large files (>1MB):"
  echo "$LARGE_FILES"
  echo "Please remove them from history or move them elsewhere (or use git lfs). Aborting push."
  exit 1
fi

# Add all files except those in .gitignore
git add --all

# Commit: if nothing to commit, do nothing
if git diff --cached --quiet; then
  echo "No changes to commit"
else
  git commit -m "Repository snapshot: prepare for deploy (automatic)"
fi

# Push
echo "Pushing to deploy-remote ${BRANCH} using SSH key at $KEY_PATH and host alias ${HOST_ALIAS}"
# Ensure we're using key: if ssh-agent is not used, ssh will use ~/.ssh/config and IdentityFile
GIT_SSH_COMMAND="ssh -i ${KEY_PATH} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no" git push deploy-remote HEAD:${BRANCH} --set-upstream

echo "Push complete."

exit 0
