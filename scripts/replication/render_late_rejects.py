#!/usr/bin/env python3
"""Render late-stage rejects as side-by-side POSS-I vs modern panels.

Shows each reject from the extract/morphology/spike gates as a POSS-I plate
cutout beside a PanSTARRS r-band cutout on a common arcsec grid (east LEFT).

Border colours:
  - orange  = extract reject (FLAGS or SNR)
  - magenta = morphology reject (FWHM/ELONGATION/SPREAD_MODEL)
  - cyan    = spike reject (CONST and/or LINE rule)

Spike rejects also draw a cyan line from the candidate to the nearest PS1
bright star that triggered the rule, with a yellow star at the bright.

Output: {out_dir}/{tile_id}/late_rejects.png

Usage:
    python scripts/replication/render_late_rejects.py \
        --tile-dir data/tiles/tile_RA2.351_DECp84.755
"""
from __future__ import annotations

import argparse
import math
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
    safe_read_csv,
)


PAIR_COLS = 3
FOV_ARCMIN = DEFAULT_FOV_ARCMIN


def _sf(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def reason_extract(r):
    if _sf(r.get("FLAGS")) != 0:
        return f'FLAGS={int(_sf(r["FLAGS"]))}'
    if _sf(r.get("SNR_WIN")) < 30:
        return f'SNR={_sf(r["SNR_WIN"]):.0f}<30'
    return "extract"


def reason_morph(r):
    bits = []
    fwhm = _sf(r.get("FWHM_IMAGE"))
    ell = _sf(r.get("ELONGATION"))
    sm = _sf(r.get("SPREAD_MODEL"))
    if fwhm < 2:
        bits.append(f'FWHM={fwhm:.2f}<2')
    if fwhm > 7:
        bits.append(f'FWHM={fwhm:.1f}>7')
    if ell > 1.3:
        bits.append(f'ell={ell:.2f}>1.3')
    if sm <= -0.002:
        bits.append(f'SM={sm:.3f}')
    return " ".join(bits) if bits else "morph"


def reason_spike(r):
    sep = _sf(r.get("_bs_sep_arcsec"))
    m = _sf(r.get("_bs_rmag"))
    bits = []
    if m <= 12.4:
        bits.append(f"CONST(m*={m:.1f})")
    line_thresh = -0.09 * sep + 15.3
    if m < line_thresh:
        bits.append(f"LINE(m*={m:.1f}<{line_thresh:.1f})")
    return "  ".join(bits) if bits else "spike"


def _nearest_bright_star(ra, dec, bright_df):
    """Return (sep_arcsec, rmag, bs_ra, bs_dec) for the closest bright star."""
    if bright_df is None or not len(bright_df):
        return None
    r1, d1 = math.radians(ra), math.radians(dec)
    best = None
    for _, bs in bright_df.iterrows():
        r2, d2 = math.radians(bs["ra"]), math.radians(bs["dec"])
        s = 2 * math.asin(math.sqrt(
            math.sin((d2 - d1) / 2) ** 2 +
            math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2
        ))
        sep = math.degrees(s) * 3600.0
        if best is None or sep < best[0]:
            best = (sep, float(bs["rmag"]), float(bs["ra"]), float(bs["dec"]))
    return best


def _draw_late_pair(subfig, row, poss_data, poss_wcs, sky_markers, fov_arcmin):
    cand_ra = float(row["ALPHA_J2000"])
    cand_dec = float(row["DELTA_J2000"])
    cat = row["_cat"]
    color = row["_color"]

    exclude_sky = [(cand_ra, cand_dec)]
    bs_ra = bs_dec = None
    if cat == "SPIKE" and pd.notna(row.get("_bs_sep_arcsec")):
        bs_ra = float(row["_bs_ra"])
        bs_dec = float(row["_bs_dec"])
        exclude_sky.append((bs_ra, bs_dec))

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
        exclude_sky=exclude_sky,
        border_color=color,
    )

    for ax in axes:
        ax.add_patch(Circle((0, 0), 5.0, fill=False,
                            edgecolor=color, lw=1.8, zorder=8))

    if bs_ra is not None:
        bx, by = to_offset(bs_ra, bs_dec)
        for ax in axes:
            ax.plot([0, bx], [0, by], color="cyan", linewidth=1.5, zorder=6)
            ax.plot(bx, by, marker="*", color="yellow",
                    markeredgecolor="black", markersize=16,
                    markeredgewidth=0.7, zorder=9)

    mag = _sf(row.get("MAG_AUTO"))
    title = (
        f'#{int(row["NUMBER"])} {cat}  mag={mag:.1f}\n'
        f'{row["_reason"]}'
    )
    subfig.suptitle(title, fontsize=8)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
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

    df_ext = safe_read_csv(cat / "sextractor_pass2.after_usnob_veto.rejected_extract.csv")
    df_mor = safe_read_csv(cat / "sextractor_pass2.after_usnob_veto.rejected_morphology.csv")
    df_spk = safe_read_csv(cat / "sextractor_spike_rejected.csv")
    bright = safe_read_csv(cat / "ps1_bright_stars_r16_rad3.csv")

    if len(df_ext):
        df_ext["_reason"] = df_ext.apply(reason_extract, axis=1)
        df_ext["_cat"] = "EXTRACT"
        df_ext["_color"] = "orange"
        df_ext["_bs_sep_arcsec"] = np.nan
        df_ext["_bs_rmag"] = np.nan
        df_ext["_bs_ra"] = np.nan
        df_ext["_bs_dec"] = np.nan

    if len(df_mor):
        df_mor["_reason"] = df_mor.apply(reason_morph, axis=1)
        df_mor["_cat"] = "MORPH"
        df_mor["_color"] = "magenta"
        df_mor["_bs_sep_arcsec"] = np.nan
        df_mor["_bs_rmag"] = np.nan
        df_mor["_bs_ra"] = np.nan
        df_mor["_bs_dec"] = np.nan

    if len(df_spk):
        bs_sep: list[float] = []
        bs_mag: list[float] = []
        bs_ra: list[float] = []
        bs_dec: list[float] = []
        for _, r in df_spk.iterrows():
            nb = _nearest_bright_star(r["ALPHA_J2000"], r["DELTA_J2000"], bright)
            if nb:
                bs_sep.append(nb[0]); bs_mag.append(nb[1])
                bs_ra.append(nb[2]); bs_dec.append(nb[3])
            else:
                bs_sep.append(np.nan); bs_mag.append(np.nan)
                bs_ra.append(np.nan); bs_dec.append(np.nan)
        df_spk["_bs_sep_arcsec"] = bs_sep
        df_spk["_bs_rmag"] = bs_mag
        df_spk["_bs_ra"] = bs_ra
        df_spk["_bs_dec"] = bs_dec
        df_spk["_cat"] = "SPIKE"
        df_spk["_color"] = "cyan"
        df_spk["_reason"] = df_spk.apply(reason_spike, axis=1)

    combined = pd.concat(
        [d for d in (df_ext, df_mor, df_spk) if len(d)],
        ignore_index=True,
    )
    n = len(combined)
    print(f"late rejects: {n} (ext={len(df_ext)} morph={len(df_mor)} spike={len(df_spk)})")

    if n == 0:
        print("  no late rejects — skipping render")
        return 0

    sky_markers = load_sky_markers(args.tile_dir)
    print(f"sky markers: {len(sky_markers)} unique positions")

    n_rows = (n + PAIR_COLS - 1) // PAIR_COLS
    fig = plt.figure(figsize=(5.4 * PAIR_COLS, 3.2 * n_rows + 1.8),
                     layout="constrained")
    subfigs = fig.subfigures(n_rows, PAIR_COLS, squeeze=False)

    for i in range(n_rows * PAIR_COLS):
        sf = subfigs[i // PAIR_COLS, i % PAIR_COLS]
        if i >= n:
            sf.set_facecolor("none")
            continue
        _draw_late_pair(sf, combined.iloc[i], poss_data, poss_wcs,
                        sky_markers, args.fov_arcmin)

    fig.suptitle(
        f"{args.tile_dir.name} — late-stage rejects "
        f"({len(df_ext)} extract + {len(df_mor)} morph + {len(df_spk)} spike)\n"
        "orange=flags/snr  magenta=morphology  cyan=spike "
        "(yellow ★ = nearest bright, cyan line = connection)\n"
        f"left = POSS-I plate, right = PanSTARRS DR1 r  ({args.fov_arcmin*60:.0f}\" FOV)",
        fontsize=10,
    )

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "late_rejects.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
