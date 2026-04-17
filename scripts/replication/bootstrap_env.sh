#!/usr/bin/env bash
# bootstrap_env.sh — full-replication environment bootstrap for VASCO60.
#
# Idempotent. Safe to re-run. Writes only to:
#   - ./.venv                                 (Python env)
#   - $HOME/.local/bin/micromamba             (conda manager)
#   - $HOME/.micromamba/envs/vasco-tools      (astro tools env)
#   - ./tools/vendor/stilts/                  (STILTS jar + wrapper)
#   - ./tools/vendor/psfex-src/               (PSFEx source build)
#
# External reproducers: run from the repo root. Requires a POSIX shell, curl,
# tar, git, Java (for STILTS), and standard build tools (autoconf, automake,
# libtool, make, clang/gcc). On macOS, Xcode Command Line Tools is enough.
#
# Pins:
#   Python                      : 3.11.x  (from pyenv or system)
#   numpy et al.                : requirements.txt (numpy==2.2.6, others pinned)
#   micromamba                  : $MICROMAMBA_VERSION (drift warned at install time)
#   astromatic-source-extractor : $SEX_VERSION       (conda-forge)
#   STILTS                      : $STILTS_VERSION    (SHA256-gated; canonical URL serves latest)
#   PSFEx                       : $PSFEX_REF         (git tag; SHA logged)
#
# Env lockfiles are emitted to scripts/replication/ on successful runs:
#   vasco-tools.lock.txt   — conda env (explicit URL + md5 per package)
#   python-venv.lock.txt   — pip freeze of .venv

set -euo pipefail

# ---------------------------------------------------------------------------
# Pinned versions
# ---------------------------------------------------------------------------
MICROMAMBA_VERSION="2.5.0"                      # pinned; drift warned post-install
SEX_VERSION="2.28.2"                            # conda-forge astromatic-source-extractor
STILTS_VERSION="3.5-4"                          # verified; canonical URL always serves latest
STILTS_URL="http://www.star.bris.ac.uk/~mbt/stilts/stilts.jar"
STILTS_SHA256="4861b46a5098decd96b1128dc3a7885d0038130fac3ec49bfa016658b5e5e947"
PSFEX_REPO="https://github.com/astromatic/psfex.git"
PSFEX_REF="3.24.2"                              # git tag; SHA logged at checkout

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

VENV_DIR="$REPO_ROOT/.venv"
MM_BIN="$HOME/.local/bin/micromamba"
export MAMBA_ROOT_PREFIX="$HOME/.micromamba"
TOOLS_ENV="$MAMBA_ROOT_PREFIX/envs/vasco-tools"
VENDOR_DIR="$REPO_ROOT/tools/vendor"
STILTS_DIR="$VENDOR_DIR/stilts"
PSFEX_SRC="$VENDOR_DIR/psfex-src"

# Writable caches to silence matplotlib/fontconfig warnings during pipeline runs.
CACHE_DIR="$REPO_ROOT/.cache"
MPL_CACHE="$CACHE_DIR/matplotlib"
FC_CACHE="$CACHE_DIR/fontconfig"

mkdir -p "$VENDOR_DIR" "$STILTS_DIR" "$MPL_CACHE" "$FC_CACHE"

log() { printf '\n[bootstrap] %s\n' "$*"; }

# ---------------------------------------------------------------------------
# 1) Python venv + pinned requirements
# ---------------------------------------------------------------------------
log "[1/5] Python venv at $VENV_DIR"
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet -r requirements.txt
"$VENV_DIR/bin/python" -c "import astropy, numpy, pandas; print('  astropy', astropy.__version__, 'numpy', numpy.__version__, 'pandas', pandas.__version__)"

# ---------------------------------------------------------------------------
# 2) micromamba + vasco-tools env with source-extractor
# ---------------------------------------------------------------------------
log "[2/5] micromamba"
if [ ! -x "$MM_BIN" ]; then
    mkdir -p "$(dirname "$MM_BIN")"
    tmpdir="$(mktemp -d)"
    # Canonical URL supports versioned paths; fall back to 'latest' if the
    # pinned version is not found on the server.
    if ! (cd "$tmpdir" && curl -Lsf "https://micro.mamba.pm/api/micromamba/osx-arm64/$MICROMAMBA_VERSION" | tar -xj bin/micromamba); then
        printf '[bootstrap] WARN: pinned micromamba %s not available; falling back to latest\n' "$MICROMAMBA_VERSION"
        (cd "$tmpdir" && curl -Ls "https://micro.mamba.pm/api/micromamba/osx-arm64/latest" | tar -xj bin/micromamba)
    fi
    mv -f "$tmpdir/bin/micromamba" "$MM_BIN"
    rm -rf "$tmpdir"
fi
got_mm="$("$MM_BIN" --version)"
if [ "$got_mm" != "$MICROMAMBA_VERSION" ]; then
    printf '[bootstrap] NOTE: micromamba drift — pinned=%s installed=%s (update MICROMAMBA_VERSION to bless)\n' "$MICROMAMBA_VERSION" "$got_mm"
else
    printf '  micromamba %s (pinned, matched)\n' "$got_mm"
fi

log "[3/5] vasco-tools env (astromatic-source-extractor + psfex build deps)"
"$MM_BIN" create -y -n vasco-tools -c conda-forge \
    "astromatic-source-extractor=$SEX_VERSION" \
    fftw cfitsio openblas plplot \
    pkg-config autoconf automake libtool make \
    openjdk \
    2>&1 | tail -10

# Alias source-extractor as sex (VASCO60 code expects `sex` on PATH)
if [ ! -e "$TOOLS_ENV/bin/sex" ]; then
    ln -sf "$TOOLS_ENV/bin/source-extractor" "$TOOLS_ENV/bin/sex"
fi
"$TOOLS_ENV/bin/sex" -v | head -1

# ---------------------------------------------------------------------------
# 4) STILTS jar + wrapper script
# ---------------------------------------------------------------------------
log "[4/5] STILTS $STILTS_VERSION"
if [ ! -f "$STILTS_DIR/stilts.jar" ]; then
    # Use Python (certifi) rather than system curl to avoid CA-bundle drift.
    "$VENV_DIR/bin/python" - <<PY
import ssl, urllib.request, certifi
ctx = ssl.create_default_context(cafile=certifi.where())
url = "$STILTS_URL"
dst = "$STILTS_DIR/stilts.jar"
print(f"  downloading {url}")
with urllib.request.urlopen(url, context=ctx) as r, open(dst, "wb") as f:
    while chunk := r.read(1 << 20):
        f.write(chunk)
print(f"  wrote {dst}")
PY
fi
got_sha="$(shasum -a 256 "$STILTS_DIR/stilts.jar" | awk '{print $1}')"
if [ "$got_sha" != "$STILTS_SHA256" ]; then
    printf '[bootstrap] ERROR: STILTS sha256 mismatch\n  expected %s\n  got      %s\n' "$STILTS_SHA256" "$got_sha" >&2
    printf '  The canonical STILTS URL always serves the latest release. To bless a new\n  version, update STILTS_VERSION and STILTS_SHA256 in this script.\n' >&2
    exit 1
fi
printf '  STILTS sha256 verified (%s)\n' "$got_sha"

# conda-forge openjdk lives under lib/jvm; symlink java into bin/ for convenience
if [ ! -e "$TOOLS_ENV/bin/java" ] && [ -x "$TOOLS_ENV/lib/jvm/bin/java" ]; then
    ln -sf "$TOOLS_ENV/lib/jvm/bin/java" "$TOOLS_ENV/bin/java"
fi

cat > "$TOOLS_ENV/bin/stilts" <<EOF
#!/usr/bin/env bash
# Auto-generated by bootstrap_env.sh
exec "$TOOLS_ENV/lib/jvm/bin/java" -jar "$STILTS_DIR/stilts.jar" "\$@"
EOF
chmod +x "$TOOLS_ENV/bin/stilts"
"$TOOLS_ENV/bin/stilts" -version 2>&1 | head -2

# ---------------------------------------------------------------------------
# 5) Build PSFEx from source, pinned to $PSFEX_REF
# ---------------------------------------------------------------------------
log "[5/5] PSFEx $PSFEX_REF from source"
if [ ! -d "$PSFEX_SRC/.git" ]; then
    git clone "$PSFEX_REPO" "$PSFEX_SRC"
fi
(
    cd "$PSFEX_SRC"
    git fetch --tags --quiet
    git checkout "$PSFEX_REF"
    PSFEX_SHA="$(git rev-parse HEAD)"
    printf '  PSFEx checked out at %s (%s)\n' "$PSFEX_REF" "$PSFEX_SHA"

    if [ ! -x ./configure ]; then
        ./autogen.sh
    fi

    # Point configure at the conda env for all deps.
    # -Wl,-rpath,$TOOLS_ENV/lib bakes the runtime search path into the binary
    # so dyld can find libopenblas.0.dylib etc. without DYLD_LIBRARY_PATH.
    export PATH="$TOOLS_ENV/bin:$PATH"
    export CPPFLAGS="-I$TOOLS_ENV/include"
    export LDFLAGS="-L$TOOLS_ENV/lib -Wl,-rpath,$TOOLS_ENV/lib"
    export PKG_CONFIG_PATH="$TOOLS_ENV/lib/pkgconfig"

    # Clean stale build state so new LDFLAGS actually propagate
    if [ -f Makefile ]; then
        make distclean 2>/dev/null || make clean 2>/dev/null || true
    fi

    # PSFEx uses ATLAS by default; --enable-openblas switches to OpenBLAS.
    # cfitsio has no dedicated flag; picked up via CPPFLAGS/LDFLAGS above.
    ./configure \
        --prefix="$TOOLS_ENV" \
        --enable-openblas \
        --with-openblas-incdir="$TOOLS_ENV/include" \
        --with-openblas-libdir="$TOOLS_ENV/lib" \
        --with-fftw-incdir="$TOOLS_ENV/include" \
        --with-fftw-libdir="$TOOLS_ENV/lib" \
        --with-plplot-incdir="$TOOLS_ENV/include/plplot" \
        --with-plplot-libdir="$TOOLS_ENV/lib"

    make -j
    make install
)
"$TOOLS_ENV/bin/psfex" -v 2>&1 | head -1

# ---------------------------------------------------------------------------
# Env lockfiles (for external reproducers)
# ---------------------------------------------------------------------------
log "Emitting env lockfiles"
CONDA_LOCK="$REPO_ROOT/scripts/replication/vasco-tools.lock.txt"
PY_LOCK="$REPO_ROOT/scripts/replication/python-venv.lock.txt"
"$MM_BIN" env export --explicit -n vasco-tools > "$CONDA_LOCK" 2>/dev/null \
    || "$MM_BIN" list -n vasco-tools > "$CONDA_LOCK"
"$VENV_DIR/bin/pip" freeze > "$PY_LOCK"
printf '  wrote %s (%s bytes)\n' "$CONDA_LOCK" "$(wc -c < "$CONDA_LOCK" | tr -d ' ')"
printf '  wrote %s (%s bytes)\n' "$PY_LOCK" "$(wc -c < "$PY_LOCK" | tr -d ' ')"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log "Done. To use the tools in your shell (copy-paste):"
cat <<EOS

  export PATH="$TOOLS_ENV/bin:\$PATH"
  export MPLCONFIGDIR="$MPL_CACHE"
  export XDG_CACHE_HOME="$CACHE_DIR"
  source "$VENV_DIR/bin/activate"

EOS
printf '[bootstrap] verified binaries:\n'
printf '  sex    -> %s\n' "$TOOLS_ENV/bin/sex"
printf '  psfex  -> %s\n' "$TOOLS_ENV/bin/psfex"
printf '  stilts -> %s\n' "$TOOLS_ENV/bin/stilts"
