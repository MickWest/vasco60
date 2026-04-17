#!/bin/bash
set -e

# Entrypoint runs as root to fix volume permissions, then drops to 'vasco' user.

# Fix ownership of directories that Docker may create as root
chown vasco:vasco /home/vasco/.claude 2>/dev/null || true

# Set git identity from the repo's most recent commit
GIT_USER_NAME=$(/usr/bin/git log --format="%an" -1 2>/dev/null || echo "Sandbox")
GIT_USER_EMAIL=$(/usr/bin/git log --format="%ae" -1 2>/dev/null || echo "sandbox@localhost")
gosu vasco /usr/bin/git config --global user.name "$GIT_USER_NAME"
gosu vasco /usr/bin/git config --global user.email "$GIT_USER_EMAIL"

# Environment for matplotlib/fontconfig caching
export MPLCONFIGDIR="/workspace/.cache/matplotlib"
export XDG_CACHE_HOME="/workspace/.cache"

cd /workspace
exec gosu vasco claude --dangerously-skip-permissions "$@"
