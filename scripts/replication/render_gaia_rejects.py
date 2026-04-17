#!/usr/bin/env python3
"""Gaia-rejects diagnostic: stats + side-by-side POSS-I vs modern panels.

The pipeline's Gaia xmatch file (`xmatch/sex_gaia_xmatch.csv`) is deleted
after HPM processing, so this script reconstructs the match on the fly via
astropy `match_coordinates_sky` against the epoch-propagated Gaia catalog
(falling back to the raw file if the propagated variant is missing).

Each candidate is shown as a POSS-I patch beside a PanSTARRS r-band cutout
at matched angular extent. Both panels share a common arcsec grid centred
on the candidate, with east on the LEFT. A cyan circle marks the candidate
and a red X marks the matched Gaia position; green + markers are catalog
sources with PM-propagated positions, yellow + markers are raw-epoch.

Rejects are shown in two groups (default 12 most tenuous + 3 random).

Output: {out_dir}/{tile_id}/gaia_rejects.png

Usage:
    python scripts/replication/render_gaia_rejects.py \
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
    load_sex_with_wcsfix,
    load_sky_markers,
    prefer_plate,
    safe_read_csv,
)


MATCH_ARCSEC = 5.0
N_TENUOUS = 12
N_RANDOM = 3
RNG_SEED = 42
PAIR_COLS = 3
FOV_ARCMIN = DEFAULT_FOV_ARCMIN


def _panel_title(row, group: str, propagated: bool) -> str:
    mag = row.get("MAG_AUTO")
    mag_str = f"{float(mag):.1f}" if mag is not None and np.isfinite(float(mag)) else "—"
    sep = float(row["_sep_arcsec"])
    gmag = row.get("cat_Gmag")
    gmag_str = f"Gmag={float(gmag):.1f}" if gmag is not None and np.isfinite(float(gmag)) else ""
    prop_tag = "prop" if propagated else "raw"
    return (
        f'[{group}] #{int(row["NUMBER"])}  mag={mag_str}  {gmag_str}\n'
        f'Gaia sep={sep:.2f}" ({prop_tag})'
    )


def _draw_gaia_pair(subfig, row, poss_data, poss_wcs,
                    sky_markers, sex_ra_col, sex_dec_col,
                    propagated: bool, group: str, fov_arcmin: float):
    cand_ra = float(row[sex_ra_col])
    cand_dec = float(row[sex_dec_col])
    gaia_ra = float(row["_cat_ra"])
    gaia_dec = float(row["_cat_dec"])

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
        exclude_sky=[(cand_ra, cand_dec), (gaia_ra, gaia_dec)],
        border_color="red",
    )

    # Candidate circle (5" radius) and Gaia match X on both panels.
    gx, gy = to_offset(gaia_ra, gaia_dec)
    for ax in axes:
        ax.add_patch(Circle((0, 0), 5.0, fill=False,
                            edgecolor="cyan", lw=1.8, zorder=8))
        ax.plot(gx, gy, marker="x", color="red",
                markersize=11, markeredgewidth=2.0, zorder=9)

    subfig.suptitle(_panel_title(row, group, propagated), fontsize=8)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    ap.add_argument("--match-arcsec", type=float, default=MATCH_ARCSEC)
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

    sex_df, (sex_ra_col, sex_dec_col) = load_sex_with_wcsfix(args.tile_dir)
    print(f"SExtractor detections: {len(sex_df)}  (using {sex_ra_col}/{sex_dec_col})")

    gaia_path = prefer_plate(args.tile_dir, "gaia_neighbourhood")
    propagated = gaia_path.name.endswith("_at_plate.csv")
    gaia_df = safe_read_csv(gaia_path)
    print(f"Gaia catalog: {len(gaia_df)} rows from {gaia_path.name} (propagated={propagated})")

    if not len(gaia_df):
        print("[ERROR] empty Gaia neighborhood — nothing to match")
        return 2

    rejects = cone_match_within(
        sex_df, sex_ra_col, sex_dec_col,
        gaia_df, "ra", "dec",
        max_arcsec=args.match_arcsec,
    )
    n_rejects = len(rejects)
    print(f"Gaia rejects (sex within {args.match_arcsec}\" of a Gaia source): {n_rejects}")

    if n_rejects == 0:
        print("  no Gaia rejects — nothing to render")
        return 0

    seps = rejects["_sep_arcsec"].values
    stats = dict(
        n=len(rejects),
        median=float(np.median(seps)),
        mean=float(np.mean(seps)),
        p25=float(np.percentile(seps, 25)),
        p75=float(np.percentile(seps, 75)),
        p95=float(np.percentile(seps, 95)),
        max=float(np.max(seps)),
        min=float(np.min(seps)),
    )
    print(f"separation stats: median={stats['median']:.2f}\" p75={stats['p75']:.2f}\" "
          f"p95={stats['p95']:.2f}\" max={stats['max']:.2f}\"")

    tenuous = rejects.sort_values("_sep_arcsec", ascending=False).head(args.n_tenuous)
    remaining = rejects.drop(tenuous.index)
    rng = random.Random(args.seed)
    if len(remaining) >= args.n_random:
        rand_idx = rng.sample(list(remaining.index), args.n_random)
    else:
        rand_idx = list(remaining.index)
    random_rejects = remaining.loc[rand_idx]

    panels: list[tuple[pd.Series, str]] = []
    for _, r in tenuous.iterrows():
        panels.append((r, "tenuous"))
    for _, r in random_rejects.iterrows():
        panels.append((r, "random"))

    sky_markers = load_sky_markers(args.tile_dir)
    print(f"sky markers: {len(sky_markers)} unique positions")

    n_panels = len(panels)
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
        row, group = panels[idx]
        _draw_gaia_pair(
            sf, row, poss_data, poss_wcs,
            sky_markers, sex_ra_col, sex_dec_col,
            propagated, group, args.fov_arcmin,
        )

    stats_line = (
        f'{args.tile_dir.name} — Gaia rejects ({args.match_arcsec:.0f}" gate)\n'
        f'total: {n_rejects}  |  sep median={stats["median"]:.2f}"  '
        f'p75={stats["p75"]:.2f}"  p95={stats["p95"]:.2f}"  max={stats["max"]:.2f}"\n'
        f'Gaia file: {gaia_path.name} (propagated={propagated})  |  '
        f'showing {len(tenuous)} most tenuous + {len(random_rejects)} random  |  '
        f'left = POSS-I plate, right = PanSTARRS DR1 r  ({args.fov_arcmin*60:.0f}" FOV)'
    )
    fig.suptitle(stats_line, fontsize=10)

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "gaia_rejects.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
