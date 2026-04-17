#!/usr/bin/env python3
"""Download all tiles in the VASCO60 tile plan from STScI's DSS cutout service.

Reads plans/tiles_poss1e_ps1.csv and downloads each tile's 60' FITS cutout
via step1-download, skipping tiles that already have a raw/*.fits on disk.
Runs with configurable parallelism (default 5 concurrent downloads).

The downloaded tiles are pixel-identical to what any external reproducer
would get from the same STScI URL — this is the gold standard for
replication. Local plate cutouts (tools/cutout_from_plate.py) produce
scientifically equivalent but numerically different pixels because STScI
reprojects to a TAN grid while the local tool slices raw scanner pixels.

Usage:
    scripts/replication/download_all_tiles.py
    scripts/replication/download_all_tiles.py --workers 10 --dry-run
    scripts/replication/download_all_tiles.py --resume  # skip already-downloaded

Output: data/tiles/<tile_id>/raw/<stem>.fits for each tile in the plan.

Estimated: 11,733 tiles × 9 MB × 2.5 s = ~103 GB, ~8 hrs serial / 1.6 hrs @ 5 workers.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TILE_PLAN = REPO / "plans" / "tiles_poss1e_ps1.csv"
TILES_ROOT = REPO / "data" / "tiles"
VENV = REPO / ".venv"
TOOLS_ENV = Path.home() / ".micromamba" / "envs" / "vasco-tools"


def build_env() -> dict[str, str]:
    env = os.environ.copy()
    prefix = f"{TOOLS_ENV / 'bin'}:{VENV / 'bin'}"
    env["PATH"] = f"{prefix}:{env.get('PATH', '')}"
    env["MPLCONFIGDIR"] = str(REPO / ".cache" / "matplotlib")
    env["XDG_CACHE_HOME"] = str(REPO / ".cache")
    # Skip renders during bulk download — they're not needed yet
    env["VASCO_SKIP_RENDER"] = "1"
    return env


def read_plan() -> list[dict]:
    if not TILE_PLAN.exists():
        raise SystemExit(f"tile plan not found: {TILE_PLAN}")
    with TILE_PLAN.open(newline="") as f:
        return list(csv.DictReader(f))


def tile_has_fits(tile_id: str) -> bool:
    """Check if this tile already has a raw FITS downloaded."""
    raw = TILES_ROOT / tile_id / "raw"
    if not raw.exists():
        return False
    return any(raw.glob("*.fits"))


def download_one(row: dict, env: dict[str, str]) -> tuple[str, str, float]:
    """Download one tile. Returns (tile_id, status, elapsed_sec)."""
    tile_id = row["tile_id"]
    ra = row["ra_deg"]
    dec = row["dec_deg"]
    size = row.get("size_arcmin", "60")

    t0 = time.perf_counter()
    try:
        r = subprocess.run(
            [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
             "step1-download",
             "--ra", str(ra), "--dec", str(dec),
             "--size-arcmin", str(size),
             "--workdir", str(TILES_ROOT)],
            env=env, cwd=str(REPO),
            capture_output=True, text=True,
            timeout=120,
        )
        dt = time.perf_counter() - t0

        if r.returncode != 0:
            # Check for non-POSS skip (expected for some pointings)
            if "non-POSS" in r.stdout or "SKIP" in r.stdout:
                return tile_id, "SKIP", dt
            return tile_id, f"FAIL(rc={r.returncode})", dt

        # Verify the FITS was actually written
        if tile_has_fits(tile_id):
            return tile_id, "OK", dt
        else:
            return tile_id, "FAIL(no-fits)", dt

    except subprocess.TimeoutExpired:
        return tile_id, "TIMEOUT", time.perf_counter() - t0
    except Exception as e:
        return tile_id, f"ERROR({e})", time.perf_counter() - t0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=5,
                    help="Concurrent download threads (default 5)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be downloaded, don't actually fetch")
    ap.add_argument("--resume", action="store_true", default=True,
                    help="Skip tiles that already have raw/*.fits (default: True)")
    ap.add_argument("--force", action="store_true",
                    help="Re-download even if raw/*.fits exists")
    ap.add_argument("--limit", type=int, default=0,
                    help="Download at most N tiles (0 = all)")
    args = ap.parse_args()

    if args.force:
        args.resume = False

    env = build_env()
    plan = read_plan()
    total = len(plan)

    # Filter to tiles needing download
    if args.resume:
        todo = [r for r in plan if not tile_has_fits(r["tile_id"])]
    else:
        todo = list(plan)

    if args.limit > 0:
        todo = todo[:args.limit]

    already = total - len(todo) if args.resume else 0

    print(f"[DOWNLOAD] plan: {total:,} tiles  already downloaded: {already:,}  "
          f"to fetch: {len(todo):,}  workers: {args.workers}")
    if not todo:
        print("[DOWNLOAD] nothing to do")
        return 0

    est_gb = len(todo) * 9 / 1024
    est_hr = len(todo) * 2.5 / 3600 / args.workers
    print(f"[DOWNLOAD] estimated: {est_gb:.0f} GB, {est_hr:.1f} hours @ {args.workers} workers")

    if args.dry_run:
        for r in todo[:20]:
            print(f"  would fetch: {r['tile_id']}  RA={r['ra_deg']}  Dec={r['dec_deg']}")
        if len(todo) > 20:
            print(f"  ... and {len(todo) - 20} more")
        return 0

    # Ensure tiles root exists
    TILES_ROOT.mkdir(parents=True, exist_ok=True)

    ok = skip = fail = 0
    t_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(download_one, r, env): r for r in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            tile_id, status, dt = fut.result()
            if status == "OK":
                ok += 1
            elif status == "SKIP":
                skip += 1
            else:
                fail += 1

            # Progress line
            elapsed = time.perf_counter() - t_start
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(todo) - i) / rate / 60 if rate > 0 else 0
            print(f"[{i:>6}/{len(todo)}] [{status:<12}] {tile_id:<40} "
                  f"{dt:5.1f}s  (ok={ok} skip={skip} fail={fail}  "
                  f"ETA {eta:.0f} min)", flush=True)

    t_total = time.perf_counter() - t_start
    print(f"\n[DOWNLOAD] DONE in {t_total/60:.1f} min")
    print(f"[DOWNLOAD] ok={ok}  skip={skip}  fail={fail}  total={len(todo)}")

    # Report disk usage
    try:
        import shutil
        usage = shutil.disk_usage(str(TILES_ROOT))
        print(f"[DOWNLOAD] tiles dir: {usage.used/1e9:.1f} GB used, "
              f"{usage.free/1e9:.0f} GB free")
    except Exception:
        pass

    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
