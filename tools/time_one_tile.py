#!/usr/bin/env python3
"""Run one fresh tile end-to-end and time every phase.

Picks an un-processed tile at random from plans/tiles_poss1e_ps1.csv
(or accepts `--tile-id TILE`), runs step1-download + step2..step6
sequentially, captures stdout, and prints a timing breakdown. Outer
wall times come from `time.perf_counter()` wrapped around each
subprocess. Inner sub-phases (fetches, WCSFIX, vetoes, per-render) come
from `[TIMING] phase=… sec=…` lines emitted by `vasco/cli_pipeline.py`
(via the `_phase` context manager added for this).

Usage:
    tools/time_one_tile.py                    # pick a random fresh tile
    tools/time_one_tile.py --tile-id tile_RA2.001_DECp82.807
    tools/time_one_tile.py --seed 12345 --json /tmp/timings.json

The script uses the same PATH/MPLCONFIGDIR/XDG_CACHE_HOME wiring as
`scripts/replication/scan_random_tiles.py` so it can be run from a
fresh shell without manual env setup.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TILE_PLAN = REPO / "plans" / "tiles_poss1e_ps1.csv"
TILES_ROOT = REPO / "data" / "tiles"
TOOLS_ENV = Path.home() / ".micromamba" / "envs" / "vasco-tools"
VENV = REPO / ".venv"

# [TIMING] phase=<name> sec=<x.xxx>
TIMING_RE = re.compile(r"^\[TIMING\]\s+phase=(\S+)\s+sec=([0-9.]+)\s*$")


def build_env() -> dict[str, str]:
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


def pick_random_fresh_tile(seed: int | None) -> dict:
    pool = [r for r in read_tile_plan() if not (TILES_ROOT / r["tile_id"]).exists()]
    if not pool:
        raise SystemExit("no un-processed tiles available")
    rng = random.Random(seed if seed is not None else int(time.time()))
    return rng.choice(pool)


def lookup_tile(tile_id: str) -> dict:
    for r in read_tile_plan():
        if r["tile_id"] == tile_id:
            return r
    raise SystemExit(f"tile_id not found in plan: {tile_id}")


def run_phase(name: str, cmd: list[str], env: dict[str, str]) -> dict:
    """Run a subprocess, tee stdout live, parse [TIMING] lines, return a dict."""
    print(f"\n[{name}] $ {' '.join(cmd)}", flush=True)
    t0 = time.perf_counter()
    # Stream stdout+stderr combined so the user can watch progress. We also
    # collect them into a list so we can parse [TIMING] lines at the end.
    proc = subprocess.Popen(
        cmd, env=env, cwd=REPO,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        lines.append(line)
        sys.stdout.write(line)
        sys.stdout.flush()
    rc = proc.wait()
    dt = time.perf_counter() - t0

    inner: list[tuple[str, float]] = []
    for line in lines:
        m = TIMING_RE.match(line.strip())
        if m:
            inner.append((m.group(1), float(m.group(2))))

    return {
        "name": name,
        "cmd": cmd,
        "wall_sec": dt,
        "returncode": rc,
        "inner": inner,
    }


def fetch_tile(row: dict, env: dict[str, str]) -> tuple[Path, dict]:
    """Run step1-download for this tile row. Returns (tile_dir, phase_result)."""
    result = run_phase(
        "download",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step1-download",
         "--ra", str(row["ra_deg"]),
         "--dec", str(row["dec_deg"]),
         "--size-arcmin", str(row.get("size_arcmin", 60)),
         "--workdir", str(TILES_ROOT)],
        env,
    )
    if result["returncode"] != 0:
        raise SystemExit(f"download failed rc={result['returncode']}")
    tile_dir = TILES_ROOT / row["tile_id"]
    if not tile_dir.exists():
        raise SystemExit(f"download did not create expected tile dir: {tile_dir}")
    return tile_dir, result


def format_breakdown(results: list[dict], tile_dir: Path) -> str:
    """Build a human-readable timing table."""
    lines: list[str] = []
    lines.append("")
    lines.append("=" * 72)
    lines.append(f"TIMING BREAKDOWN — {tile_dir.name}")
    lines.append("=" * 72)

    total_wall = sum(r["wall_sec"] for r in results)

    # Outer phases
    lines.append("")
    lines.append("OUTER (wall time per subprocess):")
    lines.append(f"  {'phase':<34s} {'sec':>10s}  {'% of total':>12s}")
    lines.append(f"  {'-'*34} {'-'*10}  {'-'*12}")
    for r in results:
        pct = 100.0 * r["wall_sec"] / total_wall if total_wall > 0 else 0.0
        lines.append(f"  {r['name']:<34s} {r['wall_sec']:>10.2f}  {pct:>11.1f}%")
    lines.append(f"  {'-'*34} {'-'*10}  {'-'*12}")
    lines.append(f"  {'TOTAL':<34s} {total_wall:>10.2f}  {'100.0%':>12s}")

    # Inner phases (from [TIMING] lines)
    inner_all: list[tuple[str, float]] = []
    for r in results:
        inner_all.extend(r["inner"])
    if inner_all:
        lines.append("")
        lines.append("INNER (from [TIMING] lines emitted by cli_pipeline):")
        lines.append(f"  {'phase':<38s} {'sec':>10s}")
        lines.append(f"  {'-'*38} {'-'*10}")
        for name, dt in inner_all:
            lines.append(f"  {name:<38s} {dt:>10.3f}")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-id", type=str, default=None,
                    help="Specific tile_id to time. If omitted, picks an un-processed tile at random.")
    ap.add_argument("--seed", type=int, default=None,
                    help="Random seed used to pick a fresh tile (ignored if --tile-id is given)")
    ap.add_argument("--json", type=Path, default=None,
                    help="Optional JSON output path for machine-readable timings")
    ap.add_argument("--skip-post-scan-check", action="store_true",
                    help="Run even if another scan looks active (default: warn but proceed)")
    args = ap.parse_args()

    env = build_env()

    # Sanity: required tools
    required = {
        "sex":   TOOLS_ENV / "bin" / "sex",
        "psfex": TOOLS_ENV / "bin" / "psfex",
        "stilts": TOOLS_ENV / "bin" / "stilts",
        "python (.venv)": VENV / "bin" / "python",
    }
    missing = [n for n, p in required.items() if not p.exists()]
    if missing:
        raise SystemExit(
            "missing required tools: " + ", ".join(missing) +
            "\nrun scripts/replication/bootstrap_env.sh first"
        )

    row = lookup_tile(args.tile_id) if args.tile_id else pick_random_fresh_tile(args.seed)
    print(f"[TIME] tile: {row['tile_id']}  ra={row['ra_deg']}  dec={row['dec_deg']}", flush=True)

    overall_t0 = time.perf_counter()

    # Phase 1: download
    tile_dir, r_fetch = fetch_tile(row, env)

    # Phase 2: step2-pass1
    r_s2 = run_phase(
        "step2-pass1",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step2-pass1", "--workdir", str(tile_dir)],
        env,
    )
    if r_s2["returncode"] != 0:
        raise SystemExit(f"step2 failed rc={r_s2['returncode']}")

    # Phase 3: step3-psf-and-pass2
    r_s3 = run_phase(
        "step3-psf-and-pass2",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step3-psf-and-pass2", "--workdir", str(tile_dir)],
        env,
    )
    if r_s3["returncode"] != 0:
        raise SystemExit(f"step3 failed rc={r_s3['returncode']}")

    # Phase 4: step4-xmatch
    r_s4 = run_phase(
        "step4-xmatch",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step4-xmatch", "--workdir", str(tile_dir)],
        env,
    )
    if r_s4["returncode"] != 0:
        raise SystemExit(f"step4 failed rc={r_s4['returncode']}")

    # Phase 5: step5-filter-within5 (no-op in current pipeline, still timed)
    r_s5 = run_phase(
        "step5-filter-within5",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step5-filter-within5", "--workdir", str(tile_dir)],
        env,
    )
    if r_s5["returncode"] != 0:
        raise SystemExit(f"step5 failed rc={r_s5['returncode']}")

    # Phase 6: step6-summarize (fires the replication renders as a side effect)
    r_s6 = run_phase(
        "step6-summarize",
        [str(VENV / "bin" / "python"), "-u", "-m", "vasco.cli_pipeline",
         "step6-summarize", "--workdir", str(tile_dir)],
        env,
    )
    if r_s6["returncode"] != 0:
        raise SystemExit(f"step6 failed rc={r_s6['returncode']}")

    overall_dt = time.perf_counter() - overall_t0

    results = [r_fetch, r_s2, r_s3, r_s4, r_s5, r_s6]
    report = format_breakdown(results, tile_dir)
    print(report)
    print(f"OVERALL wall clock: {overall_dt:.2f} s ({overall_dt/60:.1f} min)")

    if args.json:
        payload = {
            "tile": tile_dir.name,
            "overall_sec": overall_dt,
            "phases": [
                {"name": r["name"], "wall_sec": r["wall_sec"], "returncode": r["returncode"],
                 "inner": [{"phase": n, "sec": s} for n, s in r["inner"]]}
                for r in results
            ],
        }
        args.json.write_text(json.dumps(payload, indent=2))
        print(f"wrote {args.json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
