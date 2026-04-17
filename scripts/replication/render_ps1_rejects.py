#!/usr/bin/env python3
"""PS1-rejects diagnostic: stats + side-by-side POSS-I vs modern panels.

This script reconstructs the pipeline's PS1 veto stage. The PS1 veto only
runs against candidates that ALREADY SURVIVED the Gaia veto, so we match
`sextractor_pass2.after_gaia_veto.csv` against `ps1_neighbourhood.csv`
(not the full pass2 catalog — that would double-count stars already
caught by Gaia). Note: PS1 has no proper motions in the current fetch,
so matches use raw J2013-ish positions (not epoch-propagated).

Each candidate is shown as a POSS-I patch beside a PanSTARRS r-band cutout
at matched angular extent. Cyan circle marks the candidate and red X marks
the matched PS1 position. Green + = PM-propagated catalog source, yellow
+ = raw-epoch. Both panels share a common arcsec grid (east LEFT).

If total rejects ≤ --all-cutoff (default 15), every reject is shown,
sorted by match separation (tightest first). Above the cutoff, the set
is subsampled to `--n-tenuous` most tenuous (largest separations) plus
`--n-random` random draws from the rest (fixed seed for reproducibility).

Output: {out_dir}/{tile_id}/ps1_rejects.png

Usage:
    python scripts/replication/render_ps1_rejects.py \
        --tile-dir data/tiles/tile_RA2.351_DECp84.755
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
os.environ.setdefault("MPLCONFIGDIR", str(_REPO / ".cache" / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_REPO / ".cache"))

sys.path.insert(0, str(Path(__file__).parent))

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Circle  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from astropy.io import fits  # noqa: E402
from astropy.wcs import WCS  # noqa: E402

from _render_common import (  # noqa: E402
    DEFAULT_FOV_ARCMIN,
    cone_match_within,
    draw_pair,
    fetch_modern_cutout,
    load_poss_cutout_at_sky,
    load_sky_markers,
    safe_read_csv,
)


def _safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


MATCH_ARCSEC = 5.0
ALL_CUTOFF = 15
N_TENUOUS = 12
N_RANDOM = 3
RNG_SEED = 42
PAIR_COLS = 3
FOV_ARCMIN = DEFAULT_FOV_ARCMIN


def _panel_title(row, group: str | None) -> str:
    mag = _safe_float(row.get("MAG_AUTO"))
    mag_str = f"{mag:.1f}" if np.isfinite(mag) else "—"
    sep = float(row["_sep_arcsec"])
    rmag = _safe_float(row.get("cat_rmag"))
    rmag_str = f"PS1 r={rmag:.1f}" if np.isfinite(rmag) else ""
    prefix = f"[{group}] " if group else ""
    return (
        f'{prefix}#{int(row["NUMBER"])}  mag={mag_str}  {rmag_str}\n'
        f'PS1 sep={sep:.2f}"'
    )


def _draw_ps1_pair(subfig, row, poss_data, poss_wcs,
                   sky_markers, sex_ra_col, sex_dec_col,
                   group: str | None, fov_arcmin: float):
    cand_ra = float(row[sex_ra_col])
    cand_dec = float(row[sex_dec_col])
    ps1_ra = float(row["_cat_ra"])
    ps1_dec = float(row["_cat_dec"])

    poss_patch, poss_pwcs = load_poss_cutout_at_sky(
        poss_data, poss_wcs, cand_ra, cand_dec, fov_arcmin=fov_arcmin,
    )
    modern_data, modern_wcs = fetch_modern_cutout(
        cand_ra, cand_dec, fov_arcmin=fov_arcmin,
    )

    axes = subfig.subplots(1, 2, gridspec_kw={"wspace": 0})

    to_offset = draw_pair(
        axes[0], axes[1],
        poss_patch, poss_pwcs,
        modern_data, modern_wcs,
        cand_ra, cand_dec,
        fov_arcmin=fov_arcmin,
        sky_markers=sky_markers,
        exclude_sky=[(cand_ra, cand_dec), (ps1_ra, ps1_dec)],
        border_color="red",
    )

    px, py = to_offset(ps1_ra, ps1_dec)
    for ax in axes:
        ax.add_patch(Circle((0, 0), 5.0, fill=False,
                            edgecolor="cyan", lw=1.8, zorder=8))
        ax.plot(px, py, marker="x", color="red",
                markersize=11, markeredgewidth=2.0, zorder=9)

    subfig.suptitle(_panel_title(row, group), fontsize=8)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    ap.add_argument("--match-arcsec", type=float, default=MATCH_ARCSEC)
    ap.add_argument("--all-cutoff", type=int, default=ALL_CUTOFF,
                    help="If rejects > this, subsample to n_tenuous + n_random instead of showing all")
    ap.add_argument("--n-tenuous", type=int, default=N_TENUOUS)
    ap.add_argument("--n-random", type=int, default=N_RANDOM)
    ap.add_argument("--seed", type=int, default=RNG_SEED)
    ap.add_argument("--fov-arcmin", type=float, default=FOV_ARCMIN)
    args = ap.parse_args()

    cat = args.tile_dir / "catalogs"
    fits_files = sorted((args.tile_dir / "raw").glob("*.fits"))
    if not fits_files:
        print(f"[ERROR] no FITS under {args.tile_dir / 'raw'}")
        return 2

    with fits.open(fits_files[0]) as hdul:
        poss_data = hdul[0].data.astype(float)
        poss_wcs = WCS(hdul[0].header)

    # The PS1 veto only runs on candidates that already survived Gaia.
    post_gaia_path = cat / "sextractor_pass2.after_gaia_veto.csv"
    if not (post_gaia_path.exists() and post_gaia_path.stat().st_size > 0):
        print(f"[ERROR] missing {post_gaia_path.name} — run step4-xmatch first")
        return 2
    sex_df = pd.read_csv(post_gaia_path)
    if "RA_corr" in sex_df.columns and "Dec_corr" in sex_df.columns:
        sex_ra_col, sex_dec_col = "RA_corr", "Dec_corr"
    else:
        sex_ra_col, sex_dec_col = "ALPHA_J2000", "DELTA_J2000"
    print(f"Candidates entering PS1 veto (post-Gaia): {len(sex_df)}  "
          f"(using {sex_ra_col}/{sex_dec_col})")

    ps1_df = safe_read_csv(cat / "ps1_neighbourhood.csv")
    print(f"PS1 catalog: {len(ps1_df)} rows (no epoch propagation — PS1 fetch has no PMs)")

    if not len(ps1_df):
        print("[ERROR] empty PS1 neighborhood — nothing to match")
        return 2

    rejects = cone_match_within(
        sex_df, sex_ra_col, sex_dec_col,
        ps1_df, "ra", "dec",
        max_arcsec=args.match_arcsec,
    )
    n_rejects = len(rejects)
    print(f"PS1 rejects (sex within {args.match_arcsec}\" of a PS1 source): {n_rejects}")

    if n_rejects == 0:
        print("  no PS1 rejects — nothing to render")
        return 0

    seps = rejects["_sep_arcsec"].values
    stats = dict(
        median=float(np.median(seps)),
        p25=float(np.percentile(seps, 25)),
        p75=float(np.percentile(seps, 75)),
        p95=float(np.percentile(seps, 95)),
        max=float(np.max(seps)),
        min=float(np.min(seps)),
    )
    print(f"separation stats: median={stats['median']:.2f}\" p75={stats['p75']:.2f}\" "
          f"p95={stats['p95']:.2f}\" max={stats['max']:.2f}\"")

    if n_rejects <= args.all_cutoff:
        rejects = rejects.sort_values("_sep_arcsec", ascending=True).reset_index(drop=True)
        panels_list = [(row, None) for _, row in rejects.iterrows()]
        mode_label = "ALL shown"
    else:
        tenuous = rejects.sort_values("_sep_arcsec", ascending=False).head(args.n_tenuous)
        remaining = rejects.drop(tenuous.index)
        rng = random.Random(args.seed)
        if len(remaining) >= args.n_random:
            rand_idx = rng.sample(list(remaining.index), args.n_random)
        else:
            rand_idx = list(remaining.index)
        random_rejects = remaining.loc[rand_idx]
        panels_list = (
            [(row, "tenuous") for _, row in tenuous.iterrows()]
            + [(row, "random") for _, row in random_rejects.iterrows()]
        )
        mode_label = f"{len(tenuous)} most tenuous + {len(random_rejects)} random"

    sky_markers = load_sky_markers(args.tile_dir)
    print(f"sky markers: {len(sky_markers)} unique positions")

    n_panels = len(panels_list)
    n_rows = (n_panels + PAIR_COLS - 1) // PAIR_COLS
    fig = plt.figure(figsize=(5.4 * PAIR_COLS, 3.2 * n_rows + 1.8),
                     layout="constrained")
    subfigs = fig.subfigures(n_rows, PAIR_COLS, squeeze=False)

    for idx in range(n_rows * PAIR_COLS):
        r_i, c_i = idx // PAIR_COLS, idx % PAIR_COLS
        sf = subfigs[r_i, c_i]
        if idx >= n_panels:
            sf.set_facecolor("none")
            continue
        row, group = panels_list[idx]
        _draw_ps1_pair(
            sf, row, poss_data, poss_wcs,
            sky_markers, sex_ra_col, sex_dec_col,
            group, args.fov_arcmin,
        )

    stats_line = (
        f'{args.tile_dir.name} — PS1 rejects ({args.match_arcsec:.0f}" gate, {mode_label})\n'
        f'total: {n_rejects}  |  sep median={stats["median"]:.2f}"  '
        f'p75={stats["p75"]:.2f}"  p95={stats["p95"]:.2f}"  max={stats["max"]:.2f}"\n'
        f'PS1: no epoch propagation (positions ~J2013)  |  '
        f'left = POSS-I plate, right = PanSTARRS DR1 r  ({args.fov_arcmin*60:.0f}" FOV)'
    )
    fig.suptitle(stats_line, fontsize=10)

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "ps1_rejects.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
