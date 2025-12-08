# Deploying `maicro_monitors` with an SSH deploy key

This document shows a safe flow to create an SSH deploy key, block tracked large files (>1MB), ignore directories containing the string `ignore`, and push to GitHub using a dedicated host alias and key.

⚠️ Security note: Keep the private key secret and never commit it. The scripts add entries to `.gitignore` to help protect private key files, but you must still be careful with how you handle keys.

Files created by the helper scripts:
- `scripts/setup_deploy_key.sh` — generates an ed25519 deploy key pair and prints the public key (for GitHub).
- `scripts/add_gitignore_and_hooks.sh` — adds `.gitignore` patterns for directories containing `ignore`, configures Git hooks to reject files > 1MB, rejects tracked files under 'ignore' directories.
- `scripts/deploy_to_github.sh` — sets up host alias in `~/.ssh/config`, adds remote `deploy-remote` using the host alias, commits and pushes to the remote branch.

Quick summary of usage:

1) Generate keypair

```bash
scripts/setup_deploy_key.sh --key-path ~/.ssh/maicro_deploy_key --host-alias github-maicro
# Copy the printed public key into GitHub repository -> Settings -> Deploy keys
# (Allow write access if you want the key to push)
```

2) Protect sensitive files and block large files via hooks

```bash
scripts/add_gitignore_and_hooks.sh
```

3) Configure remote and push

```bash
scripts/deploy_to_github.sh --owner <your-github-account> --repo <repo-name> --branch main --host-alias github-maicro --key-path ~/.ssh/maicro_deploy_key
```

What the scripts do:
- Don't commit files >1MB (checked at pre-commit and pre-push time)
- Ignore directories containing the string `ignore` via `.gitignore`
- Add an SSH host alias to `~/.ssh/config` which points to `github.com` but uses your deploy key
- Use the alias to push via `git@<alias>:owner/repo.git`

Note: If your repo already contains >1MB committed files or directories named `*ignore*` that are tracked, you'll need to remove them from the git history or untrack them before a push succeeds. We can add an optional script for history rewriting using `git filter-repo` if needed.

If you want me to proceed to: 
- generate the key pair now for you and add the private key to a safe place (e.g., `~/.ssh`), or
- add a script to rewrite history to remove large files or tracked ignore directories, or
- automatically open a PR that updates `.gitignore`, and/or
- create an alternate push script that only copies a snapshot of tracked files to a temporary directory and pushes that snapshot (useful when you can't restructure history),

please let me know and I will proceed.

Cleanup and history rewrite options:

- Use `scripts/cleanup_large_files.sh --dry-run` to find which local files are >1MB in the working tree (excluding `ignore` directories). Re-run it without `--dry-run` to archive and remove them from the repo index for future commits.
- If you need to remove large files from older commits (rewrite history), use `git filter-repo` to remove them safely — this is destructive and requires coordination with other contributors. We can add a helper script for that if you'd like.
