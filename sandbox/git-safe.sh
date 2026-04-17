#!/bin/bash
# git-safe: blocks destructive git operations inside the sandbox container.
# Installed at /usr/local/bin/git, shadowing /usr/bin/git in PATH.
#
# Allowed: add, commit, push, status, diff, log, branch, stash, fetch, pull, etc.
# Blocked: force push, reset --hard, clean -f, checkout/restore ., branch -D
set -eo pipefail

GIT=/usr/bin/git

block() { printf 'sandbox: BLOCKED — %s\n' "$1" >&2; exit 1; }

cmd="${1:-}"

case "$cmd" in
    push)
        for a in "$@"; do
            case "$a" in
                --force|-f|--force-with-lease|--force-if-includes)
                    block "force push is not allowed in sandbox" ;;
            esac
        done
        ;;
    reset)
        for a in "$@"; do
            [[ "$a" == "--hard" ]] && block "git reset --hard is not allowed in sandbox"
        done
        ;;
    clean)
        for a in "$@"; do
            case "$a" in
                -f|-fd|-df|--force)
                    block "git clean with --force is not allowed in sandbox" ;;
            esac
        done
        ;;
    checkout)
        for a in "$@"; do
            [[ "$a" == "." ]] && block "git checkout . discards all changes"
        done
        ;;
    restore)
        for a in "$@"; do
            [[ "$a" == "." ]] && block "git restore . discards all changes"
        done
        ;;
    branch)
        for a in "$@"; do
            [[ "$a" == "-D" ]] && block "git branch -D (force delete) is not allowed in sandbox"
        done
        ;;
esac

exec "$GIT" "$@"
