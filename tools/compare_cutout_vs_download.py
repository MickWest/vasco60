#!/usr/bin/env python3
"""Janne protocol: local cutout vs STScI download service.

For each sampled POSS-I plate, cut a 60' tile locally at the plate center,
download the same tile from STScI, then compare WCS headers and pixels.
Writes per-plate diagnostic JSON + both FITS files under
data/.compare_cutout_vs_download/ (gitignored via /data).

Usage:
    python tools/compare_cutout_vs_download.py --n 5
    python tools/compare_cutout_vs_download.py --n 10 --seed 12345

Exit code 0 iff every sampled plate passes both pixel and WCS checks.

How to validate:
    1. Run with --n 5 and check the summary — every plate should print PASS.
    2. Inspect data/.compare_cutout_vs_download/summary_*.json for per-plate deltas.
    3. For any FAIL, open the stored local_cutout.fits and download.fits in SAOImage
       or astropy and compare visually.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.wcs import WCS
import astropy.units as u

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from vasco.downloader import fetch_skyview_dss  # noqa: E402

PLATE_DIR = Path("/Volumes/SANDISK/poss_1_raw/poss_red_raw")
OUT_DIR = REPO / "data" / ".compare_cutout_vs_download"

ZERO_LEN_PLATES = {"XE000", "XE001", "XE722", "XE853", "XE876"}

CRITICAL_WCS_KEYS = (
    "NAXIS1", "NAXIS2",
    "CRVAL1", "CRVAL2", "CRPIX1", "CRPIX2",
    "CD1_1", "CD1_2", "CD2_1", "CD2_2",
    "CDELT1", "CDELT2",
    "CTYPE1", "CTYPE2",
    "EQUINOX",
)
METADATA_KEYS = ("DATE-OBS", "SURVEY", "BANDPASS", "EXPOSURE")


def pick_plates(n: int, seed: int | None) -> list[Path]:
    if not PLATE_DIR.is_dir():
        raise SystemExit(f"plate dir not found: {PLATE_DIR}")
    pool: list[Path] = []
    for p in sorted(PLATE_DIR.glob("dss1red_XE*.fits")):
        if p.name.startswith("._"):
            continue
        stem = p.stem.split("_", 1)[1]
        if stem in ZERO_LEN_PLATES:
            continue
        try:
            if p.stat().st_size == 0:
                continue
        except OSError:
            continue
        pool.append(p)
    if not pool:
        raise SystemExit("plate pool is empty")
    seed_val = seed if seed is not None else int(time.time())
    rng = random.Random(seed_val)
    n = min(n, len(pool))
    picked = rng.sample(pool, n)
    print(f"[PICK] seed={seed_val} pool={len(pool)} picked={n}", flush=True)
    return picked


def local_cutout(plate: Path, size_arcmin: float):
    with fits.open(plate, memmap=True) as hdul:
        hdu = hdul[0]
        data = hdu.data
        hdr = hdu.header
        w = WCS(hdr)
        ny, nx = data.shape
        sky = w.pixel_to_world((nx - 1) / 2.0, (ny - 1) / 2.0)
        ra, dec = float(sky.ra.deg), float(sky.dec.deg)
        center = SkyCoord(ra * u.deg, dec * u.deg, frame="icrs")
        size = (size_arcmin * u.arcmin, size_arcmin * u.arcmin)
        cutout = Cutout2D(data, position=center, size=size, wcs=w, mode="trim")
    new_hdr = fits.Header()
    new_hdr.update(cutout.wcs.to_header())
    return ra, dec, np.asarray(cutout.data), new_hdr


def write_fits(path: Path, data: np.ndarray, hdr: fits.Header) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fits.PrimaryHDU(data=data, header=hdr).writeto(path, overwrite=True)


def diff_headers(local_hdr, remote_hdr, keys):
    out = {}
    for k in keys:
        lv = local_hdr.get(k, None)
        rv = remote_hdr.get(k, None)
        entry = {"local": lv, "remote": rv}
        if lv is None and rv is None:
            entry["delta"] = None
        elif lv is None or rv is None:
            entry["delta"] = "MISSING"
        else:
            try:
                entry["delta"] = float(rv) - float(lv)
            except (TypeError, ValueError):
                entry["delta"] = "EQ" if lv == rv else "NE"
        out[k] = entry
    return out


def wcs_keys_match(diff: dict, float_tol: float = 1e-6) -> bool:
    for k, entry in diff.items():
        d = entry.get("delta")
        if d is None:
            continue  # key absent on both sides is fine
        if d == "MISSING" or d == "NE":
            return False
        if isinstance(d, (int, float)) and abs(d) > float_tol:
            return False
    return True


def diff_pixels(local: np.ndarray, remote: np.ndarray) -> dict:
    out = {
        "local_shape": list(local.shape),
        "remote_shape": list(remote.shape),
        "local_dtype": str(local.dtype),
        "remote_dtype": str(remote.dtype),
    }
    if local.shape != remote.shape:
        out["same_shape"] = False
        return out
    out["same_shape"] = True
    lf = local.astype(np.float64)
    rf = remote.astype(np.float64)
    diff = rf - lf
    out["max_abs"] = float(np.max(np.abs(diff)))
    out["mean_abs"] = float(np.mean(np.abs(diff)))
    out["exact_fraction"] = float(np.mean(diff == 0))
    out["local_sum"] = float(lf.sum())
    out["remote_sum"] = float(rf.sum())
    return out


def compare_one(plate: Path, out_dir: Path, size_arcmin: float) -> dict:
    plate_id = plate.stem.split("_", 1)[1]
    print(f"\n[PLATE] {plate_id}", flush=True)
    t0 = time.time()

    ra, dec, lpix, lhdr = local_cutout(plate, size_arcmin)
    t_cut = time.time() - t0
    print(f"  local cutout: shape={tuple(lpix.shape)} ra={ra:.4f} dec={dec:.4f} ({t_cut:.2f}s)", flush=True)

    plate_out = out_dir / plate_id
    plate_out.mkdir(parents=True, exist_ok=True)
    write_fits(plate_out / "local_cutout.fits", lpix, lhdr)

    t1 = time.time()
    try:
        remote_path = fetch_skyview_dss(
            ra, dec,
            size_arcmin=size_arcmin,
            out_dir=plate_out,
            basename="download.fits",
        )
    except Exception as e:
        print(f"  [DOWNLOAD FAIL] {e}", flush=True)
        return {
            "plate_id": plate_id,
            "ra": ra, "dec": dec,
            "verdict": "ERROR",
            "download_error": str(e),
        }
    t_dl = time.time() - t1
    print(f"  remote download: ({t_dl:.2f}s)", flush=True)

    with fits.open(remote_path, memmap=False) as hdul:
        rpix = np.asarray(hdul[0].data)
        rhdr = hdul[0].header.copy()

    wcs_diff = diff_headers(lhdr, rhdr, CRITICAL_WCS_KEYS)
    meta_diff = diff_headers(lhdr, rhdr, METADATA_KEYS)
    pix = diff_pixels(lpix, rpix)

    wcs_ok = wcs_keys_match(wcs_diff)
    pix_ok = pix.get("same_shape") and pix.get("max_abs", 1.0) == 0.0
    verdict = "PASS" if (wcs_ok and pix_ok) else "FAIL"

    print(f"  wcs_ok={wcs_ok} pix_same_shape={pix.get('same_shape')} "
          f"max_abs={pix.get('max_abs', 'NA')} verdict={verdict}", flush=True)

    result = {
        "plate_id": plate_id,
        "ra": ra, "dec": dec,
        "local_shape": list(lpix.shape),
        "remote_shape": list(rpix.shape),
        "wcs_ok": wcs_ok,
        "pixels": pix,
        "wcs_diff": wcs_diff,
        "meta_diff": meta_diff,
        "verdict": verdict,
        "timings_sec": {"local_cut": round(t_cut, 3), "download": round(t_dl, 3)},
    }
    (plate_out / "compare.json").write_text(json.dumps(result, indent=2))
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--size-arcmin", type=float, default=60.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plates = pick_plates(args.n, args.seed)
    for p in plates:
        print(f"  {p.stem}", flush=True)

    results: list[dict] = []
    for p in plates:
        try:
            results.append(compare_one(p, OUT_DIR, args.size_arcmin))
        except Exception as e:
            print(f"  [ERROR] {p.stem}: {e}", flush=True)
            results.append({"plate_id": p.stem, "verdict": "ERROR", "error": str(e)})

    ts = int(time.time())
    summary_path = OUT_DIR / f"summary_{ts}.json"
    summary_path.write_text(json.dumps(results, indent=2))

    n_pass = sum(1 for r in results if r.get("verdict") == "PASS")
    n_fail = sum(1 for r in results if r.get("verdict") == "FAIL")
    n_err = sum(1 for r in results if r.get("verdict") == "ERROR")

    print("\n=== SUMMARY ===")
    for r in results:
        pid = r.get("plate_id", "?")
        v = r.get("verdict", "?")
        dec = r.get("dec")
        dec_str = f"dec={dec:+6.2f}" if isinstance(dec, (int, float)) else "dec=?"
        max_abs = r.get("pixels", {}).get("max_abs", "NA")
        print(f"  [{v:5s}] {pid}  {dec_str}  max|Δ|={max_abs}")
    print(f"\n{n_pass} PASS / {n_fail} FAIL / {n_err} ERROR of {len(results)} plates")
    print(f"[SUMMARY] wrote {summary_path}")

    return 0 if (n_fail == 0 and n_err == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
