#!/usr/bin/env python3
"""Render a zscale preview of a tile's raw cutout FITS.

Output: {out_dir}/{tile_id}/cutout_preview.png

Usage:
    python scripts/replication/render_cutout_preview.py \
        --tile-dir data/tiles/tile_RA2.351_DECp84.755
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

# Silence matplotlib/fontconfig in unusual shells; the bootstrap exports these
# too, but we set defaults so the script also runs standalone.
_REPO = Path(__file__).resolve().parents[2]
os.environ.setdefault("MPLCONFIGDIR", str(_REPO / ".cache" / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_REPO / ".cache"))

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from astropy.io import fits  # noqa: E402
from astropy.visualization import ImageNormalize, LinearStretch, ZScaleInterval  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tile-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path,
                    default=_REPO / "work" / "replication_artifacts")
    args = ap.parse_args()

    fits_files = sorted((args.tile_dir / "raw").glob("*.fits"))
    if not fits_files:
        print(f"[ERROR] no FITS under {args.tile_dir / 'raw'}")
        return 2
    fits_path = fits_files[0]

    data = fits.getdata(fits_path).astype(float)
    stats = (data.min(), float(np.median(data)), data.max())
    print(f"shape={data.shape}  min={stats[0]:.0f}  median={stats[1]:.0f}  max={stats[2]:.0f}")

    norm = ImageNormalize(data, interval=ZScaleInterval(), stretch=LinearStretch())

    out_sub = args.out_dir / args.tile_dir.name
    out_sub.mkdir(parents=True, exist_ok=True)
    out_path = out_sub / "cutout_preview.png"

    fig, ax = plt.subplots(figsize=(8, 8), dpi=100)
    ax.imshow(data, cmap="gray_r", norm=norm, origin="lower")
    ax.set_title(f"{args.tile_dir.name} — raw cutout ({fits_path.name})")
    ax.set_xlabel("X pixel")
    ax.set_ylabel("Y pixel")
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)

    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
