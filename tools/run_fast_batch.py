#!/usr/bin/env python3
"""run_fast_batch.py — parallel multi-tile runner for fast_tile_v2.

Usage:
    tools/run_fast_batch.py --tiles-list FILE [--workers N] [--params FILE]
    tools/run_fast_batch.py --tiles-glob 'data/tiles/tile_RA*' --workers 14
    tools/run_fast_batch.py --plan plans/tiles_poss1e_ps1.csv --limit 100

Each worker process runs process_tile() on its assigned tiles. Workers persist
across many tasks (so pyarrow dataset discovery amortizes), and produce:
  - <work_dir>/candidates_raw.parquet   (per tile, for param-sweep reuse)
  - <work_dir>/survivors.parquet        (per tile, final)
  - <work_dir>/fast_tile_v2_summary.json

The runner aggregates per-tile summaries into:
  - <out_dir>/batch_summary.json        (all tile summaries + aggregate stats)
  - <out_dir>/survivors_all.parquet     (concat of all tile survivors)
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))


# ----------------------------------------------------------------------------
# Worker entry point — imports vasco code lazily so each process gets its own
# pyarrow dataset cache.
# ----------------------------------------------------------------------------

def _worker(tile_dir: str, params: Optional[dict],
            reuse_candidates: bool, write_candidates: bool,
            write_survivors: bool, cleanup: bool) -> dict:
    # Import inside worker so ProcessPoolExecutor can fork cleanly.
    from tools.fast_tile_v2 import process_tile
    t0 = time.perf_counter()
    try:
        summary = process_tile(
            Path(tile_dir), params=params,
            reuse_candidates=reuse_candidates,
            write_candidates=write_candidates,
            write_survivors=write_survivors,
            cleanup=cleanup,
            quiet=True,
        )
    except Exception as e:
        summary = {"tile": Path(tile_dir).name, "ok": False, "error": str(e)}
    summary["wall_clock_sec"] = time.perf_counter() - t0
    return summary


# ----------------------------------------------------------------------------
# Tile list resolution
# ----------------------------------------------------------------------------

def _resolve_tiles(args) -> list[Path]:
    tiles: list[Path] = []
    if args.tiles_list:
        for line in Path(args.tiles_list).read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                tiles.append(Path(line).resolve())
    elif args.tiles_glob:
        import glob
        tiles.extend(sorted(Path(p).resolve() for p in glob.glob(args.tiles_glob)))
    elif args.plan:
        import csv
        with open(args.plan) as f:
            r = csv.DictReader(f)
            root = Path(args.tiles_root).resolve()
            for row in r:
                tid = row.get("tile_id")
                if tid:
                    p = root / tid
                    if p.exists():
                        tiles.append(p)
    elif args.workdir:
        tiles.append(Path(args.workdir).resolve())
    else:
        raise ValueError("Must provide --tiles-list, --tiles-glob, --plan, or --workdir")

    if args.limit:
        tiles = tiles[: args.limit]
    return tiles


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--tiles-list", type=Path,
                     help="File with tile directory paths, one per line")
    src.add_argument("--tiles-glob", type=str,
                     help="Glob pattern for tile directories")
    src.add_argument("--plan", type=Path,
                     help="Tile plan CSV with tile_id column")
    src.add_argument("--workdir", type=Path,
                     help="Single tile directory (serial mode, for testing)")
    ap.add_argument("--tiles-root", default="./data/tiles",
                    help="Root for --plan tile resolution")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=min(14, os.cpu_count() or 4))
    ap.add_argument("--params", type=Path, default=None)
    ap.add_argument("--reuse-candidates", action="store_true")
    ap.add_argument("--no-write-candidates", action="store_true")
    ap.add_argument("--no-write-survivors", action="store_true")
    ap.add_argument("--cleanup", action="store_true",
                    help="Delete pass1.ldac/pass1.psf after each tile to save disk")
    ap.add_argument("--out-dir", type=Path, default=None,
                    help="Output directory for aggregated results")
    ap.add_argument("--ensure-hp-index", action="store_true", default=True,
                    help="Pre-build HP5 cache indexes if missing (default: True)")
    args = ap.parse_args(argv)

    # Ensure HP index is built (one-time cost per host)
    if args.ensure_hp_index:
        from vasco.fast_cache_query import _hp_index_path, write_hp_index
        for var in ("VASCO_GAIA_CACHE", "VASCO_PS1_CACHE", "VASCO_USNOB_CACHE"):
            cache = os.getenv(var)
            if not cache:
                continue
            idx_path = _hp_index_path(cache)
            if not idx_path.exists():
                print(f"[BATCH] building HP5 index for {var} (one-time, ~15s)...")
                try:
                    write_hp_index(cache)
                except Exception as e:
                    print(f"[BATCH][WARN] index build for {var} failed: {e}")

    tiles = _resolve_tiles(args)
    if not tiles:
        print("[BATCH] no tiles to process", file=sys.stderr)
        return 1

    params = None
    if args.params:
        params = json.loads(args.params.read_text())

    out_dir = args.out_dir or Path("./work/fast_batch") / time.strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[BATCH] {len(tiles)} tiles  workers={args.workers}  out={out_dir}")
    t0 = time.perf_counter()
    all_summaries: list[dict] = []
    completed = 0
    failed = 0

    with cf.ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(_worker, str(t), params,
                      args.reuse_candidates,
                      not args.no_write_candidates,
                      not args.no_write_survivors,
                      args.cleanup): t
            for t in tiles
        }
        for fut in cf.as_completed(futures):
            summary = fut.result()
            all_summaries.append(summary)
            completed += 1
            if not summary.get("ok"):
                failed += 1
            elapsed = time.perf_counter() - t0
            rate = completed / elapsed * 60 if elapsed > 0 else 0
            print(f"[{completed}/{len(tiles)}] {summary.get('tile'):<40s} "
                  f"surv={summary.get('n_survivors_final', '?')} "
                  f"wall={summary.get('wall_clock_sec', 0):.1f}s  "
                  f"tot_elapsed={elapsed:.1f}s  rate={rate:.1f} tiles/min")

    elapsed = time.perf_counter() - t0
    n_ok = sum(1 for s in all_summaries if s.get("ok"))
    n_total_survivors = sum(s.get("n_survivors_final", 0) for s in all_summaries if s.get("ok"))

    agg = {
        "n_tiles": len(tiles),
        "n_ok": n_ok,
        "n_failed": failed,
        "n_total_survivors": n_total_survivors,
        "elapsed_sec": elapsed,
        "throughput_tiles_per_min": completed / elapsed * 60,
        "workers": args.workers,
        "summaries": all_summaries,
    }
    (out_dir / "batch_summary.json").write_text(json.dumps(agg, indent=2, default=float))

    # Aggregate survivors
    try:
        import pandas as pd
        frames = []
        for s in all_summaries:
            if not s.get("ok"):
                continue
            tile_name = s.get("tile")
            # Find tile dir from its survivor parquet location
            for t in tiles:
                if t.name == tile_name:
                    pq = t / "survivors.parquet"
                    if pq.exists() and pq.stat().st_size > 0:
                        df = pd.read_parquet(pq)
                        df["_tile"] = tile_name
                        frames.append(df)
                    break
        if frames:
            all_surv = pd.concat(frames, ignore_index=True)
            all_surv.to_parquet(out_dir / "survivors_all.parquet", index=False)
    except Exception as e:
        print(f"[BATCH][WARN] aggregation failed: {e}")

    print(f"\n[BATCH] DONE  elapsed={elapsed:.1f}s  rate={agg['throughput_tiles_per_min']:.1f} tiles/min  "
          f"survivors_total={n_total_survivors}  failed={failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
