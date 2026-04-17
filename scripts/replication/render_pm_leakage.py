#!/usr/bin/env python3
"""Render spike rejects with PM-leakage diagnosis as side-by-side panels.

For each spike reject on a tile, this looks up the nearest Gaia source
with proper motion and computes whether the observed offset between the
SExtractor plate detection and the PS1 "bright star" that triggered the
spike rule is actually consistent with proper motion over the plate-to-
catalog epoch gap. If so, the "spike" is actually a PM leaker: the same
star seen at two epochs, not an artifact from a nearby bright neighbor.

Each candidate is shown as a POSS-I plate cutout beside a PanSTARRS r-band
cutout on a common arcsec grid (east LEFT). Markers: cyan ○ = SExtractor
(plate epoch), yellow ★ = PS1 bright (modern epoch), lime ◆ = Gaia source.
Red border = PM LEAKER verdict; cyan border = true spike reject.

Output: {out_dir}/{tile_id}/pm_leakage.png

Usage:
    python scripts/replication/render_pm_leakage.py \
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


PLATE_TO_GAIA_EPOCH_YR_DEFAULT = 63.5
PAIR_COLS = 3
FOV_ARCMIN = 2.5   # spike rejects need more context than the default 1.5'


def _sep_arcsec(ra1, dec1, ra2, dec2):
    r1, d1 = math.radians(ra1), math.radians(dec1)
    r2, d2 = math.radians(ra2), math.radians(dec2)
    s = 2 * math.asin(math.sqrt(
        math.sin((d2 - d1) / 2) ** 2 +
        math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2
    ))
    return math.degrees(s) * 3600.0


def _safe_float(x):
    if x is None:
        return float("nan")
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def nearest_gaia_with_pm(ra, dec, gaia):
    """Return (sep_arcsec, Gmag, pmRA, pmDE, ra, dec) for the closest Gaia
    source within 25" that has non-null PMs; None if nothing qualifies."""
    if not len(gaia):
        return None
    ra_col = "ra" if "ra" in gaia.columns else "RA_ICRS"
    dec_col = "dec" if "dec" in gaia.columns else "DE_ICRS"
    dd = (gaia[dec_col].astype(float).values - dec) * 3600
    dr = (gaia[ra_col].astype(float).values - ra) * 3600 * math.cos(math.radians(dec))
    s = np.sqrt(dr * dr + dd * dd)
    mask = s < 25
    if not mask.any():
        return None
    sub = gaia[mask].copy()
    sub["_sep"] = s[mask]
    sub = sub.sort_values("_sep")
    for _, g in sub.iterrows():
        pmra = _safe_float(g.get("pmRA", g.get("pmra")))
        pmde = _safe_float(g.get("pmDE", g.get("pmde")))
        if not (math.isfinite(pmra) and math.isfinite(pmde)):
            continue
        gmag = _safe_float(g.get("Gmag", g.get("phot_g_mean_mag")))
        return (float(g["_sep"]), gmag, pmra, pmde,
                float(g[ra_col]), float(g[dec_col]))
    return None


def nearest_bright_ps1(ra, dec, bright):
    if not len(bright):
        return None
    best = None
    for _, b in bright.iterrows():
        d = _sep_arcsec(ra, dec, b["ra"], b["dec"])
        if best is None or d < best[0]:
            best = (d, float(b["rmag"]), float(b["ra"]), float(b["dec"]))
    return best


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    ap.add_argument("--epoch-gap-yr", type=float, default=PLATE_TO_GAIA_EPOCH_YR_DEFAULT,
                    help="Years between POSS plate and Gaia DR3 epoch (default 63.5)")
    ap.add_argument("--pm-match-tol-arcsec", type=float, default=1.5,
                    help="Tolerance (arcsec) for treating observed offset as consistent with expected PM drift")
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

    rej = safe_read_csv(cat / "sextractor_spike_rejected.csv")
    bright = safe_read_csv(cat / "ps1_bright_stars_r16_rad3.csv")
    gaia = safe_read_csv(cat / "gaia_neighbourhood.csv")

    if not len(rej):
        print("no spike rejects on this tile — nothing to render")
        return 0

    infos = []
    for _, r in rej.iterrows():
        ra, dec = float(r["ALPHA_J2000"]), float(r["DELTA_J2000"])
        ps1_nb = nearest_bright_ps1(ra, dec, bright)
        gaia_nb = nearest_gaia_with_pm(ra, dec, gaia)
        expected = None
        consistent = False
        pm_tot = None
        if gaia_nb:
            _, _, pmra, pmde, _, _ = gaia_nb
            pm_tot = math.sqrt(pmra ** 2 + pmde ** 2)
            expected = pm_tot * args.epoch_gap_yr / 1000.0
            if ps1_nb:
                consistent = abs(expected - ps1_nb[0]) <= args.pm_match_tol_arcsec
        infos.append(dict(
            row=r, ps1=ps1_nb, gaia=gaia_nb,
            pm_total=pm_tot, expected=expected, is_pm_leaker=consistent,
        ))

    sky_markers = load_sky_markers(args.tile_dir)
    print(f"spike rejects: {len(infos)}  sky markers: {len(sky_markers)}")

    n_panels = len(infos)
    n_rows = (n_panels + PAIR_COLS - 1) // PAIR_COLS
    fig = plt.figure(figsize=(5.4 * PAIR_COLS, 3.4 * n_rows + 2.0),
                     layout="constrained")
    subfigs = fig.subfigures(n_rows, PAIR_COLS, squeeze=False)

    for idx in range(n_rows * PAIR_COLS):
        sf = subfigs[idx // PAIR_COLS, idx % PAIR_COLS]
        if idx >= n_panels:
            sf.set_facecolor("none")
            continue

        info = infos[idx]
        r = info["row"]
        ps1 = info["ps1"]
        gaia_nb = info["gaia"]

        cand_ra = float(r["ALPHA_J2000"])
        cand_dec = float(r["DELTA_J2000"])

        exclude = [(cand_ra, cand_dec)]
        if ps1 is not None:
            exclude.append((ps1[2], ps1[3]))
        if gaia_nb is not None:
            exclude.append((gaia_nb[4], gaia_nb[5]))

        poss_patch, poss_pwcs = load_poss_cutout_at_sky(
            poss_data, poss_wcs, cand_ra, cand_dec, fov_arcmin=args.fov_arcmin,
        )
        modern_data, modern_wcs = fetch_modern_cutout(
            cand_ra, cand_dec, fov_arcmin=args.fov_arcmin,
        )

        axes = sf.subplots(1, 2, gridspec_kw={"wspace": 0})

        border = "red" if info["is_pm_leaker"] else "cyan"
        to_offset = draw_pair(
            axes[0], axes[1],
            poss_patch, poss_pwcs,
            modern_data, modern_wcs,
            cand_ra, cand_dec,
            fov_arcmin=args.fov_arcmin,
            sky_markers=sky_markers,
            exclude_sky=exclude,
            border_color=border,
        )

        for ax in axes:
            ax.add_patch(Circle((0, 0), 5.0, fill=False,
                                edgecolor="cyan", lw=1.8, zorder=8))
            if ps1 is not None:
                px, py = to_offset(ps1[2], ps1[3])
                ax.plot(px, py, marker="*", color="yellow",
                        markeredgecolor="black", markersize=18,
                        markeredgewidth=0.8, zorder=9)
            if gaia_nb is not None:
                gx, gy = to_offset(gaia_nb[4], gaia_nb[5])
                ax.plot(gx, gy, marker="D", color="lime",
                        markeredgecolor="black", markersize=10,
                        markeredgewidth=0.7, zorder=9)

        mag = _safe_float(r.get("MAG_AUTO"))
        label = "PM LEAKER" if info["is_pm_leaker"] else "true spike reject"
        title_lines = [
            f'#{int(r["NUMBER"])}  mag={mag:.1f}  [{label}]',
        ]
        if gaia_nb and ps1:
            title_lines.append(
                f'PS1 off={ps1[0]:.2f}"  vs  PM exp={info["expected"]:.2f}"'
                f' (|pm|={info["pm_total"]:.0f})'
            )
        sf.suptitle("\n".join(title_lines), fontsize=8)

    fig.suptitle(
        f'{args.tile_dir.name} — spike rejects vs PM-leakage diagnosis\n'
        f'PM LEAKER criterion: |PS1 offset − expected PM drift over {args.epoch_gap_yr:.0f} yr| < {args.pm_match_tol_arcsec}"\n'
        f'cyan ○ = SExtractor (plate), yellow ★ = PS1 bright, lime ◆ = Gaia  '
        f'|  left = POSS-I plate, right = PanSTARRS DR1 r  ({args.fov_arcmin*60:.0f}" FOV)',
        fontsize=10,
    )

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "pm_leakage.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"wrote {out_path}")
    for info in infos:
        r = info["row"]
        status = "PM LEAKER" if info["is_pm_leaker"] else "spike/unknown"
        ps1_d = f'{info["ps1"][0]:.2f}"' if info["ps1"] else "n/a"
        exp = f'{info["expected"]:.2f}"' if info["expected"] is not None else "n/a"
        print(f"  #{int(r['NUMBER'])}: {status}  ps1_offset={ps1_d}  pm_expected={exp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
