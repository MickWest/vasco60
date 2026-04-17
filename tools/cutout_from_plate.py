#!/usr/bin/env python3
"""Cut a 60x60' tile from a local POSS-I plate FITS — smoke-test helper.

Bypasses step1-download by slicing a region out of a full plate and writing
it into a tile_dir that step2-pass1 can pick up.

Usage:
    python tools/cutout_from_plate.py \
        --plate-fits /Volumes/SANDISK/poss_1_raw/poss_red_raw/dss1red_XE002.fits

Outputs under --tiles-root (default ./data/tiles):
    <tile_id>/raw/<stem>.fits   the cutout, header inherited + cutout WCS
    <tile_id>/RUN_INDEX.json    minimal tile metadata
    <tile_id>/tile_status.json  step1.status=ok (source=local_cutout)
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.wcs import WCS
import astropy.units as u

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from vasco.utils.tile_id import format_tile_id


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--plate-fits", required=True, type=Path)
    ap.add_argument("--tiles-root", type=Path, default=Path("./data/tiles"))
    ap.add_argument("--size-arcmin", type=float, default=60.0)
    ap.add_argument("--ra", type=float, default=None,
                    help="tile center RA (deg); default = plate center")
    ap.add_argument("--dec", type=float, default=None,
                    help="tile center Dec (deg); default = plate center")
    args = ap.parse_args()

    if not args.plate_fits.exists() or args.plate_fits.stat().st_size == 0:
        print(f"[ERROR] plate FITS missing or zero length: {args.plate_fits}", file=sys.stderr)
        return 2

    with fits.open(args.plate_fits) as hdul:
        hdu = hdul[0]
        data = hdu.data
        hdr = hdu.header
        w = WCS(hdr)

        ny, nx = data.shape
        if args.ra is None or args.dec is None:
            sky = w.pixel_to_world((nx - 1) / 2.0, (ny - 1) / 2.0)
            ra, dec = float(sky.ra.deg), float(sky.dec.deg)
        else:
            ra, dec = args.ra, args.dec

        center = SkyCoord(ra * u.deg, dec * u.deg, frame="icrs")
        size = (args.size_arcmin * u.arcmin, args.size_arcmin * u.arcmin)
        cutout = Cutout2D(data, position=center, size=size, wcs=w, mode="trim")

    tile_id = format_tile_id(ra, dec)
    tile_dir = args.tiles_root / tile_id
    raw_dir = tile_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    stem = f"possi-e_{ra:.6f}_{dec:.6f}_{int(round(args.size_arcmin))}arcmin"
    out_fits = raw_dir / f"{stem}.fits"

    # Strip legacy DSS plate-solution keys; carry only instrumental metadata
    # and let cutout.wcs.to_header() provide a clean TAN WCS.
    keep_keys = {
        "DATE-OBS", "SURVEY", "REGION", "PLATEID", "EMULSION", "FILTER",
        "EXPOSURE", "BANDPASS", "PLTLABEL", "OBSHA", "OBSZD", "AIRMASS",
        "EQUINOX", "ORIGIN", "TELESCOP", "INSTRUME", "COPYRGHT",
        "SITELAT", "SITELONG", "SCANNUM", "DSCNDNUM", "TELESCID",
    }
    new_hdr = fits.Header()
    for k in keep_keys:
        if k in hdr:
            new_hdr[k] = (hdr[k], hdr.comments[k])
    new_hdr.update(cutout.wcs.to_header())
    fits.PrimaryHDU(data=cutout.data, header=new_hdr).writeto(out_fits, overwrite=True)

    (tile_dir / "RUN_INDEX.json").write_text(json.dumps(
        [{"tile": stem, "ra": ra, "dec": dec,
          "size_arcmin": args.size_arcmin,
          "source_plate": args.plate_fits.name}],
        indent=2,
    ))

    (tile_dir / "tile_status.json").write_text(json.dumps({
        "tile_id": tile_id,
        "steps": {
            "step1": {
                "status": "ok",
                "ts": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "local_cutout",
                "parent_plate": args.plate_fits.name,
            }
        },
    }, indent=2))

    print(f"[CUTOUT] tile_id   = {tile_id}")
    print(f"[CUTOUT] cutout    = {out_fits}")
    print(f"[CUTOUT] shape     = {cutout.data.shape} (y, x)")
    print(f"[CUTOUT] center    = RA {ra:.6f}°  Dec {dec:.6f}°")
    print(f"[CUTOUT] size      = {args.size_arcmin}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
