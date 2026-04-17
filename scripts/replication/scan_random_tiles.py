#!/usr/bin/env python3
"""Unattended end-to-end scan of N random tiles from the POSS-I tile plan.

Picks N tiles from plans/tiles_poss1e_ps1.csv that aren't already under
data/tiles/, fetches each via step1-download (STScI), runs steps 2-6 →
replication-artifact renders on each, and prints a summary. Intended to
be run from the repo root with no manual env setup:

    scripts/replication/scan_random_tiles.py --n 20
    scripts/replication/scan_random_tiles.py --n 5 --seed 12345

The script takes care of its own PATH/MPLCONFIGDIR/XDG_CACHE_HOME wiring
so it doesn't matter whether the vasco-tools env or .venv is activated in
the parent shell. It uses the existing parallel runners under tools/ for
steps 2-3 and 4-5 and then runs step6-summarize per tile (which also fires
the replication renders via the post-step6 hook).

Tiles that already have a directory under data/tiles/ are never picked.
Per-tile failures do not abort the run.

Exit codes:
    0 = all picked tiles reached step6 successfully
    1 = at least one tile failed somewhere in the pipeline
    2 = no tiles could be fetched (nothing to run)
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TILE_PLAN = REPO / "plans" / "tiles_poss1e_ps1.csv"
TILES_ROOT = REPO / "data" / "tiles"
TOOLS_ENV = Path.home() / ".micromamba" / "envs" / "vasco-tools"
VENV = REPO / ".venv"


def build_env() -> dict[str, str]:
    """Return an environment dict with all deps on PATH and caches redirected."""
    env = os.environ.copy()
    prefix = f"{TOOLS_ENV / 'bin'}:{VENV / 'bin'}"
    env["PATH"] = f"{prefix}:{env.get('PATH', '')}"
    env["MPLCONFIGDIR"] = str(REPO / ".cache" / "matplotlib")
    env["XDG_CACHE_HOME"] = str(REPO / ".cache")
    (REPO / ".cache" / "matplotlib").mkdir(parents=True, exist_ok=True)
    (REPO / ".cache").mkdir(parents=True, exist_ok=True)
    return env


def read_tile_plan() -> list[dict]:
    if not TILE_PLAN.exists() or TILE_PLAN.stat().st_size == 0:
        raise SystemExit(f"tile plan not found: {TILE_PLAN}")
    with TILE_PLAN.open(newline="") as f:
        return list(csv.DictReader(f))


def pick_tiles(n: int, seed: int | None) -> list[dict]:
    """Pick N tile rows from the plan that don't already exist under data/tiles/."""
    rows = read_tile_plan()
    pool = [r for r in rows if not (TILES_ROOT / r["tile_id"]).exists()]
    if n > len(pool):
        print(f"[WARN] requested {n} but only {len(pool)} un-processed tiles; "
              f"running all {len(pool)}", flush=True)
        n = len(pool)
    if n == 0:
        raise SystemExit("no un-processed tiles to pick")
    seed_val = seed if seed is not None else int(time.time())
    rng = random.Random(seed_val)
    print(f"[PICK] seed={seed_val}  pool={len(pool)}  picking={n}", flush=True)
    return rng.sample(pool, n)


def fetch_one(row: dict, env: dict[str, str]) -> Path | None:
    """Run step1-download for this tile row. Returns tile_dir on success."""
    tile_id = row["tile_id"]
    tile_dir = TILES_ROOT / tile_id
    r = subprocess.run(
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step1-download",
         "--ra", str(row["ra_deg"]),
         "--dec", str(row["dec_deg"]),
         "--size-arcmin", str(row.get("size_arcmin", 60)),
         "--workdir", str(TILES_ROOT)],
        env=env, cwd=REPO,
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(f"[FETCH FAIL] {tile_id}: rc={r.returncode}  {r.stderr.strip()[-300:]}")
        return None
    if not tile_dir.exists():
        print(f"[FETCH FAIL] {tile_id}: tile dir not created")
        return None
    return tile_dir


def run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
    """Run a subprocess inheriting stdio. Returns rc."""
    print(f"\n$ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, env=env, cwd=REPO)
    return r.returncode


def step6_one(tile_dir: Path, env: dict[str, str]) -> int:
    """Run step6-summarize on a single tile. Also fires the replication renders
    via the post-step6 hook wired in `vasco/cli_pipeline.py:cmd_step6_summarize`."""
    return run_subprocess(
        [str(VENV / "bin" / "python"),
         "-m", "vasco.cli_pipeline", "step6-summarize",
         "--workdir", str(tile_dir)],
        env=env,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=5,
                    help="Number of new tiles to scan (default 5)")
    ap.add_argument("--seed", type=int, default=None,
                    help="Random seed (default: time-based)")
    ap.add_argument("--workers", type=int, default=3,
                    help="Parallel workers for steps 2-3 and 4-5 (default 3)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print picks and exit without running")
    args = ap.parse_args()

    env = build_env()

    required = {
        "sex":   TOOLS_ENV / "bin" / "sex",
        "psfex": TOOLS_ENV / "bin" / "psfex",
        "stilts": TOOLS_ENV / "bin" / "stilts",
        "python (.venv)": VENV / "bin" / "python",
    }
    missing = [name for name, path in required.items() if not path.exists()]
    if missing:
        raise SystemExit(
            "missing required tools: " + ", ".join(missing) +
            "\nrun scripts/replication/bootstrap_env.sh first"
        )

    picks = pick_tiles(args.n, args.seed)
    for row in picks:
        print(f"  {row['tile_id']}  ra={row['ra_deg']}  dec={row['dec_deg']}", flush=True)

    if args.dry_run:
        return 0

    # ---- phase 1: fetch every tile sequentially ---------------------------
    tile_dirs: list[Path] = []
    for row in picks:
        td = fetch_one(row, env)
        if td is None:
            continue
        tile_dirs.append(td)
        print(f"[FETCH OK]  {row['tile_id']}", flush=True)

    if not tile_dirs:
        print("\n[ERROR] no tiles fetched — aborting", file=sys.stderr)
        return 2

    tiles_file = REPO / ".cache" / f"scan_tiles_{os.getpid()}.txt"
    tiles_file.parent.mkdir(parents=True, exist_ok=True)
    tiles_file.write_text("\n".join(str(t) for t in tile_dirs) + "\n")

    try:
        run_subprocess(
            [str(VENV / "bin" / "python"),
             "tools/run_steps_2_3_parallel.py",
             "--tiles-file", str(tiles_file),
             "--workers", str(args.workers)],
            env=env,
        )

        run_subprocess(
            [str(VENV / "bin" / "python"),
             "tools/run_steps_4_5_parallel.py",
             "--tiles-file", str(tiles_file),
             "--workers", str(args.workers)],
            env=env,
        )

        results: list[tuple[Path, int]] = []
        for td in tile_dirs:
            rc = step6_one(td, env)
            results.append((td, rc))
    finally:
        tiles_file.unlink(missing_ok=True)

    n_ok = sum(1 for _, rc in results if rc == 0)
    print("\n=== scan summary ===")
    for td, rc in results:
        tag = "OK" if rc == 0 else f"FAIL(rc={rc})"
        print(f"  [{tag}] {td.name}")
    print(f"\n{n_ok}/{len(tile_dirs)} tiles completed step6 successfully")
    return 0 if n_ok == len(tile_dirs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
