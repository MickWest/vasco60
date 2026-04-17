#!/bin/bash
# run.sh — VASCO60 sandbox launcher
# Called by _wt sandbox_worktree with WT_* environment variables set.
#
# Variables from _wt:
#   WT_NAME             worktree name (e.g., "feature-x")
#   WT_PATH             worktree path on host
#   WT_MAIN_REPO        main repo path
#   WT_AGENTS_DIR       agents directory
#   WT_PROJECT_NAME     "vasco60"
#   WT_IMG              Docker image name
#   WT_DOCKERFILE_DIR   path to this sandbox/ directory
#   WT_CRED_TMP         path to credentials temp file (or empty)
#   WT_CLAUDE_LOCAL_TMP path to CLAUDE.local.md temp file
#   WT_REBUILD          "true" or "false"
#   WT_TOOLS_DIR        path to sitrec-tools directory
set -e

RED='\033[0;31m'
GREEN='\033[38;2;0;128;0m'
YELLOW='\033[38;2;128;128;0m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Build Docker image
# ---------------------------------------------------------------------------

# Copy requirements.txt into build context (Dockerfile expects it here)
cp "$WT_MAIN_REPO/requirements.txt" "$WT_DOCKERFILE_DIR/requirements.txt"

if [ "$WT_REBUILD" = "true" ] || ! docker image inspect "$WT_IMG" >/dev/null 2>&1; then
    echo -e "${YELLOW}Building sandbox image (includes SExtractor, PSFEx, STILTS)...${NC}"
    local_flags=()
    [ "$WT_REBUILD" = "true" ] && local_flags=(--no-cache)
    docker build "${local_flags[@]}" -t "$WT_IMG" "$WT_DOCKERFILE_DIR"
fi

# Clean up copied file
rm -f "$WT_DOCKERFILE_DIR/requirements.txt"

# ---------------------------------------------------------------------------
# Prepare mounts
# ---------------------------------------------------------------------------

container_name="${WT_PROJECT_NAME}-sandbox-${WT_NAME//\//-}"

echo -e "${GREEN}Launching vasco60 sandbox for ${WT_NAME}...${NC}"
echo -e "  Worktree: $WT_PATH → /workspace"
echo -e "  Data: ${WT_MAIN_REPO}/data → /workspace/data (read-write)"
echo -e "  Cache: ${WT_MAIN_REPO}/.cache → /workspace/.cache"
echo ""

# Claude Code config mounts
claude_mounts=()
[ -f "${HOME}/.claude/settings.json" ] && \
    claude_mounts+=(-v "${HOME}/.claude/settings.json:/home/vasco/.claude/settings.json:ro")
[ -f "${HOME}/.claude/settings.local.json" ] && \
    claude_mounts+=(-v "${HOME}/.claude/settings.local.json:/home/vasco/.claude/settings.local.json:ro")
[ -d "${HOME}/.claude/projects" ] && \
    claude_mounts+=(-v "${HOME}/.claude/projects:/home/vasco/.claude/projects")
[ -f "${HOME}/.claude.json" ] && \
    claude_mounts+=(-v "${HOME}/.claude.json:/home/vasco/.claude.json")

# OAuth credentials (extracted by _wt from macOS Keychain)
[ -n "$WT_CRED_TMP" ] && [ -f "$WT_CRED_TMP" ] && \
    claude_mounts+=(-v "${WT_CRED_TMP}:/home/vasco/.claude/.credentials.json:ro")

# CLAUDE.local.md
claude_local_mount=()
[ -n "$WT_CLAUDE_LOCAL_TMP" ] && [ -f "$WT_CLAUDE_LOCAL_TMP" ] && \
    claude_local_mount=(-v "${WT_CLAUDE_LOCAL_TMP}:/workspace/CLAUDE.local.md:ro")

# External catalog cache mounts (from .env if configured).
# Mounts host paths at the same absolute path inside the container so env vars work as-is.
catalog_mounts=()
if [ -f "$WT_PATH/.env" ]; then
    while IFS='=' read -r key val; do
        case "$key" in
            VASCO_GAIA_CACHE|VASCO_PS1_CACHE|VASCO_USNOB_CACHE)
                val="${val//\"/}"   # strip quotes
                val="${val//\'/}"
                if [ -d "$val" ]; then
                    catalog_mounts+=(-v "$val:$val:ro" -e "$key=$val")
                    echo -e "  Catalog: $key → $val (read-only)"
                fi
                ;;
        esac
    done < <(grep -v '^#' "$WT_PATH/.env" | grep -v '^$')
fi

# .env file mount (for any other env vars the pipeline reads)
env_mount=()
[ -f "$WT_PATH/.env" ] && env_mount=(--env-file "$WT_PATH/.env")

echo ""

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------

docker run -it --rm \
    --name "$container_name" \
    -v "${WT_PATH}:/workspace" \
    -v "${WT_MAIN_REPO}/.git:${WT_MAIN_REPO}/.git" \
    -v "${WT_MAIN_REPO}/data:/workspace/data" \
    -v "${WT_MAIN_REPO}/.cache:/workspace/.cache" \
    "${claude_mounts[@]}" \
    "${claude_local_mount[@]}" \
    "${catalog_mounts[@]}" \
    "${env_mount[@]}" \
    "$WT_IMG"
