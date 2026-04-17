# Sandbox container for running Claude Code on VASCO60 with
# --dangerously-skip-permissions, safely isolated from the host filesystem.
#
# Includes: Python 3.11, SExtractor, PSFEx, STILTS (Java), Claude Code CLI.
#
# Astro tools are installed via micromamba + conda-forge, matching the
# approach in scripts/replication/bootstrap_env.sh. This avoids Debian
# multiarch headaches where OpenBLAS headers/libs are split across packages.
#
# Usage (via wt sandbox):
#   wt sandbox <worktree-name>

FROM python:3.11-bookworm

# System deps: SExtractor from apt (available on all Debian arches),
# build tools for PSFEx compilation. PSFEx's library deps (openblas etc.)
# come from conda-forge below — Debian's split packaging breaks the build.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl gosu \
    source-extractor \
    autoconf automake libtool make gcc g++ gfortran \
    && rm -rf /var/lib/apt/lists/*

# Non-root user (Claude Code refuses --dangerously-skip-permissions as root)
RUN useradd -m -s /bin/bash vasco

# ---------------------------------------------------------------------------
# Astro tools via micromamba (mirrors bootstrap_env.sh)
# ---------------------------------------------------------------------------

# Install micromamba — auto-detect arch for Apple Silicon / x86 compatibility
ENV MAMBA_ROOT_PREFIX=/opt/micromamba
RUN ARCH="$(uname -m)" \
    && case "$ARCH" in \
         x86_64)  MAMBA_ARCH="linux-64" ;; \
         aarch64) MAMBA_ARCH="linux-aarch64" ;; \
         *)       echo "Unsupported arch: $ARCH" && exit 1 ;; \
       esac \
    && curl -Ls "https://micro.mamba.pm/api/micromamba/${MAMBA_ARCH}/latest" \
       | tar -xj -C /usr/local bin/micromamba

# Create vasco-tools env: PSFEx build deps + Java for STILTS.
# SExtractor comes from apt above; only the libraries that PSFEx links
# against need conda-forge (Debian's openblas lacks bundled LAPACKE).
RUN micromamba create -y -n vasco-tools -c conda-forge \
    fftw cfitsio openblas plplot \
    pkg-config autoconf automake libtool make \
    openjdk

ENV TOOLS_ENV=/opt/micromamba/envs/vasco-tools

# SExtractor alias (Debian calls it source-extractor, pipeline expects "sex")
RUN ln -sf "$(command -v source-extractor)" /usr/local/bin/sex

# Java symlink (conda openjdk puts java under lib/jvm/)
RUN [ -x "$TOOLS_ENV/lib/jvm/bin/java" ] \
    && ln -sf "$TOOLS_ENV/lib/jvm/bin/java" "$TOOLS_ENV/bin/java" || true

# Build PSFEx from source — same configure flags as bootstrap_env.sh
ARG PSFEX_REF=3.24.2
RUN git clone --depth 50 https://github.com/astromatic/psfex.git /tmp/psfex \
    && cd /tmp/psfex \
    && git checkout ${PSFEX_REF} \
    && ./autogen.sh \
    && PATH="$TOOLS_ENV/bin:$PATH" \
       CPPFLAGS="-I$TOOLS_ENV/include" \
       LDFLAGS="-L$TOOLS_ENV/lib -Wl,-rpath,$TOOLS_ENV/lib" \
       PKG_CONFIG_PATH="$TOOLS_ENV/lib/pkgconfig" \
       ./configure --prefix="$TOOLS_ENV" --enable-openblas \
        --with-openblas-incdir="$TOOLS_ENV/include" \
        --with-openblas-libdir="$TOOLS_ENV/lib" \
        --with-fftw-incdir="$TOOLS_ENV/include" \
        --with-fftw-libdir="$TOOLS_ENV/lib" \
        --with-plplot-incdir="$TOOLS_ENV/include/plplot" \
        --with-plplot-libdir="$TOOLS_ENV/lib" \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /tmp/psfex

# Download and verify STILTS JAR (same SHA256 as bootstrap_env.sh)
ARG STILTS_SHA256=4861b46a5098decd96b1128dc3a7885d0038130fac3ec49bfa016658b5e5e947
RUN mkdir -p /opt/stilts \
    && curl -fsSL http://www.star.bris.ac.uk/~mbt/stilts/stilts.jar \
       -o /opt/stilts/stilts.jar \
    && echo "${STILTS_SHA256}  /opt/stilts/stilts.jar" | sha256sum -c \
    && printf '#!/bin/bash\nexec %s/bin/java -jar /opt/stilts/stilts.jar "$@"\n' \
       "$TOOLS_ENV" > "$TOOLS_ENV/bin/stilts" \
    && chmod +x "$TOOLS_ENV/bin/stilts"

# Tools env on PATH (sex, psfex, stilts, java all live here)
ENV PATH="$TOOLS_ENV/bin:$PATH"

# ---------------------------------------------------------------------------
# Python + Claude Code
# ---------------------------------------------------------------------------

# Python dependencies (baked into image for fast startup).
# run.sh copies requirements.txt here before building.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Claude Code CLI (native installer, same approach as sitrec sandbox)
RUN gosu vasco bash -c 'curl -fsSL https://claude.ai/install.sh | bash'
ENV PATH="/home/vasco/.local/bin:$PATH"

# Pre-create ~/.claude owned by vasco
RUN mkdir -p /home/vasco/.claude && chown vasco:vasco /home/vasco/.claude

# Safe git wrapper — blocks destructive operations
COPY git-safe.sh /usr/local/bin/git
RUN chmod +x /usr/local/bin/git

ENV DISABLE_AUTOUPDATER=1

WORKDIR /workspace

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
