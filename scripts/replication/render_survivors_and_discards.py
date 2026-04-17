#!/usr/bin/env python3
"""Render survivors + USNO-B rejects as side-by-side POSS-I vs modern panels.

Survivors are shown first (green border) followed by USNO-B rejects
(red border). Each pair is a POSS-I plate cutout beside a PanSTARRS r-band
cutout on a common arcsec grid (east LEFT). For rejects, a cyan circle
marks the candidate and a red X marks the matched USNO-B1.0 position.

Output: {out_dir}/{tile_id}/survivors_vs_discards.png

Usage:
    python scripts/replication/render_survivors_and_discards.py \
        --tile-dir data/tiles/tile_RA2.351_DECp84.755
"""
from __future__ import annotations

import argparse
import os
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
    draw_pair,
    fetch_modern_cutout,
    load_poss_cutout_at_sky,
    load_sky_markers,
    load_sex_with_wcsfix,
)


PAIR_COLS = 3
FOV_ARCMIN = DEFAULT_FOV_ARCMIN


def _safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _survivor_title(row) -> str:
    mag = _safe_float(row.get("MAG_AUTO"))
    fwhm = _safe_float(row.get("FWHM_IMAGE"))
    ell = _safe_float(row.get("ELONGATION"))
    return (
        f'SURVIVOR #{int(row["NUMBER"])}  mag={mag:.1f}\n'
        f'FWHM={fwhm:.1f}  ell={ell:.2f}'
    )


def _discard_title(row) -> str:
    mag = _safe_float(row.get("MAG_AUTO"))
    sep = _safe_float(row.get("Separation"))
    usnob = row.get("USNO-B1.0", "")
    return (
        f'USNO REJECT #{int(row["NUMBER"])}  mag={mag:.1f}\n'
        f'sep={sep:.2f}"  {usnob}'
    )


def _draw_survivor(subfig, row, poss_data, poss_wcs, sky_markers,
                   sex_ra_col, sex_dec_col, fov_arcmin):
    cand_ra = float(row[sex_ra_col])
    cand_dec = float(row[sex_dec_col])

    poss_patch, poss_pwcs = load_poss_cutout_at_sky(
        poss_data, poss_wcs, cand_ra, cand_dec, fov_arcmin=fov_arcmin,
    )
    modern_data, modern_wcs = fetch_modern_cutout(
        cand_ra, cand_dec, fov_arcmin=fov_arcmin,
    )

    axes = subfig.subplots(1, 2, gridspec_kw={"wspace": 0})

    draw_pair(
        axes[0], axes[1],
        poss_patch, poss_pwcs,
        modern_data, modern_wcs,
        cand_ra, cand_dec,
        fov_arcmin=fov_arcmin,
        sky_markers=sky_markers,
        exclude_sky=[(cand_ra, cand_dec)],
        border_color="limegreen",
    )

    for ax in axes:
        ax.add_patch(Circle((0, 0), 5.0, fill=False,
                            edgecolor="lime", lw=1.8, zorder=8))

    subfig.suptitle(_survivor_title(row), fontsize=8, color="darkgreen")


def _draw_discard(subfig, row, poss_data, poss_wcs, sky_markers, fov_arcmin):
    cand_ra = float(row["ALPHA_J2000"])
    cand_dec = float(row["DELTA_J2000"])
    usnob_ra = float(row["RAJ2000"])
    usnob_dec = float(row["DEJ2000"])

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
        exclude_sky=[(cand_ra, cand_dec), (usnob_ra, usnob_dec)],
        border_color="red",
    )

    ux, uy = to_offset(usnob_ra, usnob_dec)
    for ax in axes:
        ax.add_patch(Circle((0, 0), 5.0, fill=False,
                            edgecolor="cyan", lw=1.8, zorder=8))
        ax.plot(ux, uy, marker="x", color="red",
                markersize=11, markeredgewidth=2.0, zorder=9)

    subfig.suptitle(_discard_title(row), fontsize=8, color="darkred")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    ap.add_argument("--max-discards", type=int, default=15)
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
    df_final = pd.read_csv(cat / "sextractor_pass2.filtered.csv")
    # Join survivors against the WCSFIX rows so we get RA_corr/Dec_corr.
    df_final_full = sex_df[sex_df["NUMBER"].isin(df_final["NUMBER"])].copy()
    df_xmatch = pd.read_csv(args.tile_dir / "xmatch" / "sex_usnob_xmatch.csv")
    df_xmatch = df_xmatch.sort_values("Separation").reset_index(drop=True)

    n_surv = len(df_final_full)
    n_disc = min(args.max_discards, len(df_xmatch))
    print(f"survivors={n_surv}  usnob_discards={len(df_xmatch)}  showing {n_disc}")

    sky_markers = load_sky_markers(args.tile_dir)
    print(f"sky markers: {len(sky_markers)} unique positions")

    n_panels = n_surv + n_disc
    if n_panels == 0:
        print("  nothing to render")
        return 0

    n_rows = (n_panels + PAIR_COLS - 1) // PAIR_COLS
    fig = plt.figure(figsize=(5.4 * PAIR_COLS, 3.2 * n_rows + 1.8),
                     layout="constrained")
    subfigs = fig.subfigures(n_rows, PAIR_COLS, squeeze=False)

    idx = 0
    for _, row in df_final_full.iterrows():
        sf = subfigs[idx // PAIR_COLS, idx % PAIR_COLS]
        _draw_survivor(sf, row, poss_data, poss_wcs, sky_markers,
                       sex_ra_col, sex_dec_col, args.fov_arcmin)
        idx += 1

    for i in range(n_disc):
        sf = subfigs[idx // PAIR_COLS, idx % PAIR_COLS]
        _draw_discard(sf, df_xmatch.iloc[i], poss_data, poss_wcs, sky_markers,
                      args.fov_arcmin)
        idx += 1

    for j in range(idx, n_rows * PAIR_COLS):
        subfigs[j // PAIR_COLS, j % PAIR_COLS].set_facecolor("none")

    fig.suptitle(
        f"{args.tile_dir.name} — {n_surv} survivors (green) vs "
        f"{n_disc} of {len(df_xmatch)} USNO-B rejects (red)\n"
        f"cyan ○ = candidate, red × = matched USNO-B1.0 position\n"
        f"left = POSS-I plate, right = PanSTARRS DR1 r  ({args.fov_arcmin*60:.0f}\" FOV)",
        fontsize=10,
    )

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "survivors_vs_discards.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
