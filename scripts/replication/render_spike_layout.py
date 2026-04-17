#!/usr/bin/env python3
"""Render the spatial layout of the spike veto for a tile.

Shows the tile image with overlays:
  - PS1 bright stars used for the veto (size/color by rmag)
  - Candidates that entered the spike stage (green = survived, red X = rejected)
  - Tile center
  - 3' PS1 fetch radius (cyan dashed, around tile center)
  - 90" paper-intent spike radius around the brightest star (lime solid)
  - 90' code-actual spike radius around the brightest star (red dotted)

The 90' circle is 60x larger than the 90" circle and extends beyond the tile
boundary — the spatial absurdity is the point of the visualization.

Output: {out_dir}/{tile_id}/spike_layout.png

Usage:
    python scripts/replication/render_spike_layout.py \
        --tile-dir data/tiles/tile_RA2.351_DECp84.755
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
os.environ.setdefault("MPLCONFIGDIR", str(_REPO / ".cache" / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_REPO / ".cache"))

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Circle  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from astropy.coordinates import SkyCoord  # noqa: E402
from astropy.io import fits  # noqa: E402
import astropy.units as u  # noqa: E402
from astropy.visualization import ImageNormalize, LinearStretch, ZScaleInterval  # noqa: E402
from astropy.wcs import WCS  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    args = ap.parse_args()

    cat = args.tile_dir / "catalogs"
    fits_files = sorted((args.tile_dir / "raw").glob("*.fits"))
    if not fits_files:
        print(f"[ERROR] no FITS under {args.tile_dir / 'raw'}")
        return 2

    with fits.open(fits_files[0]) as hdul:
        data = hdul[0].data.astype(float)
        wcs = WCS(hdul[0].header)
    norm = ImageNormalize(data, interval=ZScaleInterval(), stretch=LinearStretch())

    def _safe_read(path: Path) -> pd.DataFrame:
        """Tolerate the pipeline's 0-byte 'no rejects' output."""
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    bright = pd.read_csv(cat / "ps1_bright_stars_r16_rad3.csv")
    rej = _safe_read(cat / "sextractor_spike_rejected.csv")
    surv = _safe_read(cat / "sextractor_pass2.filtered.csv")

    # Pixel scale from WCS (Dec direction, arcsec/px)
    scales = wcs.proj_plane_pixel_scales()
    pix_scale = float(scales[1].to(u.arcsec).value)

    # Convert bright-star sky positions to pixel coordinates
    bright_sky = SkyCoord(bright["ra"].values * u.deg,
                          bright["dec"].values * u.deg, frame="icrs")
    bright_px = wcs.world_to_pixel(bright_sky)
    bright_x, bright_y = np.asarray(bright_px[0]), np.asarray(bright_px[1])

    # Rejects in pixel coordinates (may be empty)
    if len(rej):
        rej_sky = SkyCoord(rej["ALPHA_J2000"].values * u.deg,
                           rej["DELTA_J2000"].values * u.deg, frame="icrs")
        rej_px = wcs.world_to_pixel(rej_sky)
        rej_x, rej_y = np.asarray(rej_px[0]), np.asarray(rej_px[1])
    else:
        rej_sky = None
        rej_x = rej_y = np.array([])

    # Survivors in pixel coordinates (using existing X_IMAGE/Y_IMAGE; 1-indexed)
    if len(surv):
        surv_x = surv["X_IMAGE"].values - 1
        surv_y = surv["Y_IMAGE"].values - 1
    else:
        surv_x = surv_y = np.array([])

    # Brightest bright star
    brightest_idx = int(bright["rmag"].idxmin())
    bx, by = float(bright_x[brightest_idx]), float(bright_y[brightest_idx])
    bmag = float(bright.iloc[brightest_idx]["rmag"])

    # Tile center
    ny, nx = data.shape
    cx, cy = (nx - 1) / 2.0, (ny - 1) / 2.0

    # Radii in pixels
    r_paper = 90.0 / pix_scale        # 90 arcsec
    r_fetch = 3.0 * 60.0 / pix_scale  # 3 arcmin
    r_code = 90.0 * 60.0 / pix_scale  # 90 arcmin

    # Extend axis limits to show the 90' circle
    pad = r_code * 1.05
    xlim = (cx - pad, cx + pad)
    ylim = (cy - pad, cy + pad)

    fig, ax = plt.subplots(figsize=(12, 12))
    ax.imshow(data, cmap="gray_r", norm=norm, origin="lower",
              extent=(-0.5, nx - 0.5, -0.5, ny - 0.5))

    # Tile boundary rectangle
    from matplotlib.patches import Rectangle
    ax.add_patch(Rectangle((-0.5, -0.5), nx, ny, fill=False,
                            edgecolor="black", linewidth=1.5, linestyle="-"))

    # 90' code-actual circle around brightest star (red dotted — huge)
    ax.add_patch(Circle((bx, by), r_code, fill=False,
                         edgecolor="red", linewidth=2, linestyle=":"))

    # 3' PS1 fetch radius around tile center (cyan dashed)
    ax.add_patch(Circle((cx, cy), r_fetch, fill=False,
                         edgecolor="cyan", linewidth=1.5, linestyle="--"))

    # 90" paper-intent circle around brightest star (lime solid — tiny)
    ax.add_patch(Circle((bx, by), r_paper, fill=False,
                         edgecolor="lime", linewidth=2))

    # Bright stars, sized/colored by rmag
    sizes = np.clip(400 * (16.0 - bright["rmag"].values) / 6.0, 30, 400)
    sc = ax.scatter(bright_x, bright_y, s=sizes, c=bright["rmag"].values,
                    cmap="YlOrRd_r", edgecolors="black", linewidths=0.8,
                    marker="*", zorder=10)

    # Survivors (green circles)
    if len(surv):
        ax.scatter(surv_x, surv_y, s=120, marker="o", facecolors="none",
                   edgecolors="lime", linewidths=2, zorder=11)
        for i, (_, row) in enumerate(surv.iterrows()):
            ax.annotate(f'#{int(row["NUMBER"])}',
                        xy=(surv_x[i], surv_y[i]),
                        xytext=(surv_x[i] + 25, surv_y[i] + 25),
                        fontsize=8, color="lime", weight="bold")

    # Rejects (red X) — annotate with the NEAREST bright star (the one that
    # actually triggered the CONST/LINE rule), not the brightest-overall star.
    # Draw a cyan line from each reject to its nearest bright star.
    if len(rej):
        ax.scatter(rej_x, rej_y, s=250, marker="x", c="red", linewidths=3, zorder=12)
        for i, (_, row) in enumerate(rej.iterrows()):
            sky = SkyCoord(row["ALPHA_J2000"] * u.deg, row["DELTA_J2000"] * u.deg, frame="icrs")
            # Find the nearest bright star to this reject
            seps = bright_sky.separation(sky).arcsec
            nearest_i = int(np.argmin(seps))
            sep_near = float(seps[nearest_i])
            rmag_near = float(bright.iloc[nearest_i]["rmag"])
            nbx = float(bright_x[nearest_i])
            nby = float(bright_y[nearest_i])
            # Cyan line to the actually-rule-triggering star
            ax.plot([float(rej_x[i]), nbx], [float(rej_y[i]), nby],
                    color="cyan", linewidth=1.5, zorder=11)
            ax.annotate(
                f'#{int(row["NUMBER"])}\nd={sep_near:.1f}"  rmag={rmag_near:.2f}',
                xy=(float(rej_x[i]), float(rej_y[i])),
                xytext=(float(rej_x[i]) + 250, float(rej_y[i]) - 250),
                fontsize=9, color="red", weight="bold",
                arrowprops=dict(arrowstyle="->", color="red", lw=1.5),
            )

    # Tile center marker
    ax.plot(cx, cy, marker="+", color="cyan", markersize=20, markeredgewidth=2, zorder=11)

    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_aspect("equal")
    ax.set_xlabel("X pixel")
    ax.set_ylabel("Y pixel")
    ax.set_title(
        f"{args.tile_dir.name} — spike veto spatial layout\n"
        f"brightest PS1 bright star: rmag={bmag:.2f} near tile center\n"
        f"LIME solid = 90\" paper intent   RED dotted = 90' code actual (60× too large)"
    )

    cbar = plt.colorbar(sc, ax=ax, shrink=0.5, pad=0.02, label="PS1 rmag")

    legend = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor="orange",
               markeredgecolor="black", markersize=14,
               label=f"PS1 bright stars ({len(bright)})"),
        Line2D([0], [0], marker="o", markerfacecolor="none",
               markeredgecolor="lime", markersize=12,
               linewidth=0, label=f"spike-stage survivors ({len(surv)})"),
        Line2D([0], [0], marker="x", color="red", markersize=12, linewidth=0,
               label=f"spike-rejected ({len(rej)})"),
        Line2D([0], [0], color="cyan", linewidth=1.5,
               label="reject → nearest bright star"),
        Line2D([0], [0], marker="+", color="cyan", markersize=14, linewidth=0,
               label="tile center"),
        Line2D([0], [0], color="black", linewidth=1.5,
               label="tile boundary (60x60')"),
        Line2D([0], [0], color="lime", linewidth=2,
               label='spike radius — paper (90")'),
        Line2D([0], [0], color="red", linewidth=2, linestyle=":",
               label="spike radius — code-PRE-FIX (90')"),
    ]
    ax.legend(handles=legend, loc="upper right", fontsize=9, framealpha=0.9)

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "spike_layout.png"
    plt.tight_layout()
    fig.savefig(out_path, dpi=100, bbox_inches="tight")
    plt.close(fig)

    print(f"wrote {out_path}")
    print(f"  pixel scale: {pix_scale:.3f} arcsec/px")
    print(f"  90\" paper radius:  {r_paper:.1f} px")
    print(f"  3'  fetch radius:  {r_fetch:.1f} px")
    print(f"  90' code radius:   {r_code:.1f} px  (tile width = {nx} px)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
