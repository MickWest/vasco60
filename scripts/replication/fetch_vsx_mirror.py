#!/usr/bin/env python3
"""Fetch the VSX master-slim FITS mirror expected by scripts/stage_vsx_post.py.

This script pulls the AAVSO Variable Star Index (VSX) catalogue from CDS's
STATIC FTP mirror (`cdsarc.cds.unistra.fr/ftp/B/vsx/`) as two simple HTTP
GETs of pre-packaged files:

    vsx.dat    — the catalogue data, CDS fixed-width ASCII (~2.16 GB)
    ReadMe     — the column specification for the CDS-format parser (~49 KB)

This is explicitly NOT a TAP/database query against VizieR's live service.
The files are static snapshots and a single HTTP GET per file; the load on
CDS's infrastructure is one file transfer, not a multi-million-row query.

NOTE on size: CDS does not host a compressed vsx.dat.gz — only the
uncompressed 2.16 GB `vsx.dat`. This is a one-time bandwidth cost; the
download is cached in tools/vendor/vsx/ (gitignored) so re-runs are free.

The downloaded `vsx.dat` is parsed with `astropy.io.ascii.read(format="cds",
readme=...)`, reduced to the columns the S4 pipeline stage actually reads
(position + Name/Type), and written as FITS at:

    data/local-cats/_external_catalogs/vsx/vsx_master_slim.fits

The RAJ2000/DEJ2000 columns are renamed to RAdeg/DEdeg to match the STILTS
tskymatch2 invocation in scripts/stage_vsx_post.py (`ra2=RAdeg dec2=DEdeg`).

Idempotent — skips the download if the target FITS already exists and is
non-empty. Pass `--force` to overwrite. Staged vsx.dat/ReadMe are kept so
re-runs don't re-download unless --force is given.

Usage:
    python scripts/replication/fetch_vsx_mirror.py
    python scripts/replication/fetch_vsx_mirror.py --force
"""
from __future__ import annotations

import argparse
import hashlib
import shutil
import ssl
import sys
import time
import urllib.request
from pathlib import Path

import certifi

_REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUT = _REPO / "data" / "local-cats" / "_external_catalogs" / "vsx" / "vsx_master_slim.fits"
DEFAULT_STAGING = _REPO / "tools" / "vendor" / "vsx"

CDS_BASE = "https://cdsarc.cds.unistra.fr/ftp/B/vsx"
DAT_URL = f"{CDS_BASE}/vsx.dat"
README_URL = f"{CDS_BASE}/ReadMe"


def _http_get(url: str, dst: Path, progress: bool = False) -> int:
    """Download url → dst using certifi's CA bundle. Returns bytes written."""
    ctx = ssl.create_default_context(cafile=certifi.where())
    with urllib.request.urlopen(url, context=ctx) as r:
        total = None
        try:
            total = int(r.headers.get("Content-Length", "") or 0) or None
        except ValueError:
            total = None
        written = 0
        last_print = time.time()
        with open(dst, "wb") as f:
            while True:
                chunk = r.read(1 << 20)  # 1 MB
                if not chunk:
                    break
                f.write(chunk)
                written += len(chunk)
                if progress and time.time() - last_print > 2.0:
                    if total:
                        pct = 100.0 * written / total
                        print(f"      {written / 1e6:.1f} / {total / 1e6:.1f} MB ({pct:.1f}%)")
                    else:
                        print(f"      {written / 1e6:.1f} MB")
                    last_print = time.time()
    return dst.stat().st_size


def _sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT,
                    help=f"Output FITS path (default: {DEFAULT_OUT})")
    ap.add_argument("--staging", type=Path, default=DEFAULT_STAGING,
                    help="Directory for the downloaded dat.gz / ReadMe (kept for reproducibility)")
    ap.add_argument("--force", action="store_true",
                    help="Re-download and overwrite even if target exists")
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.staging.mkdir(parents=True, exist_ok=True)

    if args.out.exists() and args.out.stat().st_size > 0 and not args.force:
        print(f"[VSX] output already exists: {args.out} ({args.out.stat().st_size:,} bytes)")
        print("      pass --force to re-fetch")
        return 0

    readme = args.staging / "ReadMe"
    dat = args.staging / "vsx.dat"

    # ---- step 1: download static files -------------------------------------
    if args.force or not readme.exists() or readme.stat().st_size == 0:
        print(f"[VSX] fetching  {README_URL}")
        n = _http_get(README_URL, readme)
        print(f"      → {readme} ({n:,} bytes)")
    else:
        print(f"[VSX] reusing cached {readme}")

    if args.force or not dat.exists() or dat.stat().st_size == 0:
        print(f"[VSX] fetching  {DAT_URL}  (~2.16 GB — this will take a few minutes)")
        n = _http_get(DAT_URL, dat, progress=True)
        print(f"      → {dat} ({n:,} bytes)")
        print(f"      sha256: {_sha256_of(dat)}")
    else:
        print(f"[VSX] reusing cached {dat} ({dat.stat().st_size:,} bytes)")

    # ---- step 2: parse with astropy CDS reader ------------------------------
    # Lazy-import astropy so the CLI --help is fast.
    from astropy.io import ascii as astropy_ascii
    from astropy.table import Table

    print(f"[VSX] parsing CDS-format ASCII (this takes ~30-90s for ~2.4M rows)...")
    tab = astropy_ascii.read(str(dat), format="cds", readme=str(readme))
    print(f"[VSX] parsed {len(tab):,} rows, columns: {list(tab.colnames)[:12]}...")

    # ---- step 3: slim to what the pipeline needs ---------------------------
    # The CDS ReadMe for B/vsx/vsx uses column names RAdeg/DEdeg directly
    # (not RAJ2000/DEJ2000 like many other VizieR catalogs), which happens to
    # match exactly what scripts/stage_vsx_post.py passes to STILTS, so no
    # rename is needed. We keep a handful of useful context columns too.
    wanted = ("RAdeg", "DEdeg", "Name", "Type", "Period", "OID", "V")
    keep = [c for c in wanted if c in tab.colnames]
    if "RAdeg" not in keep or "DEdeg" not in keep:
        raise RuntimeError(
            f"expected RAdeg/DEdeg columns in VSX table but got {tab.colnames}"
        )
    slim = Table(tab[keep])
    print(f"[VSX] slim table: {len(slim):,} rows × {len(slim.colnames)} cols ({slim.colnames})")

    # ---- step 4: write FITS ------------------------------------------------
    slim.write(args.out, format="fits", overwrite=True)
    print(f"[VSX] wrote {args.out} ({args.out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[VSX] interrupted", file=sys.stderr)
        sys.exit(130)
