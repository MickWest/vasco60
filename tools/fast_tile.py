#!/usr/bin/env python3
"""Fast veto-first pipeline for a single tile.

Reorders the standard VASCO60 pipeline to eliminate known stars BEFORE
the expensive PSF-model pass2, reducing wall clock from ~60 min to ~1 min
on dense galactic-plane tiles.

Standard pipeline:  pass1 → PSFEx → pass2 (ALL) → catalog veto → filters
Fast pipeline:      pass1 → catalog veto → PSFEx → pass2 (survivors) → filters

The key insight: pass1 positions are accurate to ~0.85" on POSS-I plates,
well within the 5" veto gate. So 99% of detections (known catalog stars)
can be eliminated before the expensive per-source PSF fit.

Pass2 runs only on the ~100 survivors via small postage-stamp sub-images,
each processed independently by SExtractor. PSFEx still builds the PSF
model from the full pass1.ldac (it needs ~500 stars across the full field).

Usage:
    tools/fast_tile.py --workdir data/tiles/tile_RA2.351_DECp84.755
    tools/fast_tile.py --plate-fits /Volumes/SANDISK/.../dss1red_XE002.fits

Output: same artifacts as the standard pipeline (catalogs/*.csv, xmatch/*,
MNRAS_SUMMARY.json, replication renders), so downstream stages (S0–S4) and
the scan driver work unchanged.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from astropy.coordinates import SkyCoord, match_coordinates_sky
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.wcs import WCS
import astropy.units as u

from vasco.pipeline_split import run_pass1, run_psfex
from vasco.external_fetch_online import fetch_gaia_neighbourhood, fetch_ps1_neighbourhood
from vasco.external_fetch_usnob_vizier import fetch_usnob_neighbourhood
from vasco.mnras.filters_mnras import apply_extract_filters, apply_morphology_filters
from vasco.mnras.spikes import (
    BrightStar, fetch_bright_ps1, apply_spike_cuts,
    SpikeConfig, SpikeRuleConst, SpikeRuleLine,
)
from vasco.mnras.buckets import init_buckets, finalize
from vasco.mnras.report import write_summary
from vasco.cli_pipeline import (
    _plate_epoch_year_from_fits,
    _propagate_catalog_epoch,
    _run_replication_renders,
)

STAMP_PX = 91       # postage stamp half-width × 2 + 1 (odd)
VETO_ARCSEC = 5.0
BACK_SIZE_STAMP = 32


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _phase(name: str):
    """Print [TIMING] lines for the outer driver to parse."""
    import contextlib
    @contextlib.contextmanager
    def _ctx():
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            print(f"[TIMING] phase={name} sec={dt:.3f}", flush=True)
    return _ctx()


def _safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _safe_read_csv(path: Path):
    import pandas as pd
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _ldac_to_csv(ldac_path: Path, csv_path: Path) -> None:
    """Convert an LDAC FITS catalog to CSV using astropy (no STILTS needed).

    Drops multi-dimensional columns (VIGNET) that can't serialize to flat CSV.
    """
    from astropy.table import Table
    with fits.open(ldac_path) as hdul:
        if len(hdul) > 2:
            tab = Table(hdul[2].data)
        else:
            tab = Table(hdul[1].data)
    # Drop any columns with ndim > 1 (VIGNET is 45×45)
    drop = [c for c in tab.colnames if tab[c].ndim > 1]
    if drop:
        tab.remove_columns(drop)
    tab.write(csv_path, format="csv", overwrite=True)


def _tile_center(tile_dir: Path) -> tuple[float, float] | None:
    """Get tile center RA/Dec from RUN_INDEX.json or tile dirname."""
    try:
        recs = json.loads((tile_dir / "RUN_INDEX.json").read_text())
        if recs:
            stem = Path(recs[0].get("tile", "")).name
            parts = stem.split("_")
            return float(parts[1]), float(parts[2])
    except Exception:
        pass
    from vasco.utils.tile_id import parse_tile_id_center
    return parse_tile_id_center(tile_dir.name)


# ---------------------------------------------------------------------------
# Core: Python-native merged-catalog veto
# ---------------------------------------------------------------------------

def veto_against_catalogs(
    src_ra: np.ndarray,
    src_dec: np.ndarray,
    cat_ra: np.ndarray,
    cat_dec: np.ndarray,
    max_arcsec: float = VETO_ARCSEC,
) -> np.ndarray:
    """Return a boolean mask: True = SURVIVES (no catalog match within max_arcsec)."""
    if len(cat_ra) == 0:
        return np.ones(len(src_ra), dtype=bool)

    src_sky = SkyCoord(src_ra * u.deg, src_dec * u.deg, frame="icrs")
    cat_sky = SkyCoord(cat_ra * u.deg, cat_dec * u.deg, frame="icrs")

    idx, sep, _ = match_coordinates_sky(src_sky, cat_sky)
    sep_arcsec = sep.to(u.arcsec).value

    return sep_arcsec > max_arcsec


def load_merged_catalog(tile_dir: Path) -> tuple[np.ndarray, np.ndarray]:
    """Load and merge all three neighborhood catalogs (epoch-propagated preferred)."""
    catdir = tile_dir / "catalogs"
    ras, decs = [], []

    def _prefer(name):
        p = catdir / f"{name}_at_plate.csv"
        r = catdir / f"{name}.csv"
        return p if (p.exists() and p.stat().st_size > 0) else r

    def _add(path, ra_col, dec_col):
        df = _safe_read_csv(path)
        if not len(df) or ra_col not in df.columns or dec_col not in df.columns:
            return
        r = df[ra_col].apply(_safe_float).values
        d = df[dec_col].apply(_safe_float).values
        ok = np.isfinite(r) & np.isfinite(d)
        ras.append(r[ok])
        decs.append(d[ok])

    _add(_prefer("gaia_neighbourhood"), "ra", "dec")
    _add(catdir / "ps1_neighbourhood.csv", "ra", "dec")
    _add(_prefer("usnob_neighbourhood"), "RAJ2000", "DEJ2000")

    if not ras:
        return np.array([]), np.array([])
    return np.concatenate(ras), np.concatenate(decs)


# ---------------------------------------------------------------------------
# Core: stamp-based pass2
# ---------------------------------------------------------------------------

def run_stamp_pass2(
    tile_dir: Path,
    raw_fits: Path,
    psf_path: Path,
    survivors_csv: Path,
    out_csv: Path,
    stamp_px: int = STAMP_PX,
) -> int:
    """Run SExtractor pass2 on small postage stamps centered on each survivor.

    Returns the number of survivors successfully measured.
    """
    import pandas as pd

    sex_bin = shutil.which("sex") or shutil.which("sextractor")
    if not sex_bin:
        raise RuntimeError("SExtractor not found in PATH")

    surv = pd.read_csv(survivors_csv)
    if not len(surv):
        # No survivors: write empty CSV with header
        out_csv.write_text("")
        return 0

    # Read the full tile image + WCS
    with fits.open(raw_fits) as hdul:
        full_data = hdul[0].data.astype(float)
        full_hdr = hdul[0].header
        full_wcs = WCS(full_hdr)

    ny, nx = full_data.shape
    half = stamp_px // 2

    # Stage configs into tile_dir for sex
    for cfg in ["sex_pass2_stamp.sex", "default.param", "default.nnw", "default.conv"]:
        src = REPO / "configs" / cfg
        dst = tile_dir / cfg
        if src.exists() and (not dst.exists() or src.resolve() != dst.resolve()):
            shutil.copy2(src, dst)

    results = []
    stamp_dir = tile_dir / "_stamps"
    stamp_dir.mkdir(exist_ok=True)

    for _, row in surv.iterrows():
        num = int(row["NUMBER"])
        # Pass1 uses 1-indexed pixel coords
        cx = int(round(row["X_IMAGE"])) - 1
        cy = int(round(row["Y_IMAGE"])) - 1

        # Clamp stamp boundaries
        x1 = max(0, cx - half)
        x2 = min(nx, cx + half + 1)
        y1 = max(0, cy - half)
        y2 = min(ny, cy + half + 1)

        if x2 - x1 < 20 or y2 - y1 < 20:
            continue  # too close to edge

        stamp_data = full_data[y1:y2, x1:x2]

        # Build a stamp header with correct WCS (shift CRPIX)
        stamp_hdr = full_hdr.copy()
        stamp_hdr["NAXIS1"] = x2 - x1
        stamp_hdr["NAXIS2"] = y2 - y1
        if "CRPIX1" in stamp_hdr:
            stamp_hdr["CRPIX1"] = float(stamp_hdr["CRPIX1"]) - x1
        if "CRPIX2" in stamp_hdr:
            stamp_hdr["CRPIX2"] = float(stamp_hdr["CRPIX2"]) - y1

        stamp_fits = stamp_dir / f"stamp_{num}.fits"
        fits.PrimaryHDU(data=stamp_data, header=stamp_hdr).writeto(
            stamp_fits, overwrite=True
        )

        stamp_ldac = stamp_dir / f"stamp_{num}.ldac"
        cmd = [
            sex_bin,
            str(stamp_fits),
            "-c", "sex_pass2_stamp.sex",
            "-CATALOG_NAME", str(stamp_ldac),
            "-CATALOG_TYPE", "FITS_LDAC",
            "-PSF_NAME", str(psf_path.name),
        ]

        rc = subprocess.run(
            cmd, cwd=str(tile_dir),
            capture_output=True, text=True,
        ).returncode

        if rc != 0 or not stamp_ldac.exists():
            continue

        # Read the stamp LDAC and find the detection closest to stamp center
        try:
            from astropy.table import Table
            with fits.open(stamp_ldac) as sh:
                if len(sh) < 3:
                    continue
                stab = Table(sh[2].data)
            if len(stab) == 0:
                continue

            # Source closest to the stamp center (in pixel coords)
            scx = (x2 - x1) / 2.0
            scy = (y2 - y1) / 2.0
            dx = np.array(stab["X_IMAGE"], dtype=float) - scx
            dy = np.array(stab["Y_IMAGE"], dtype=float) - scy
            best = np.argmin(dx * dx + dy * dy)

            # Convert stamp-local pixel coords back to full-image coords
            result_row = {col: stab[col][best] for col in stab.colnames}
            result_row["X_IMAGE"] = float(stab["X_IMAGE"][best]) + x1
            result_row["Y_IMAGE"] = float(stab["Y_IMAGE"][best]) + y1
            # Keep the original pass1 NUMBER for traceability
            result_row["NUMBER"] = num
            results.append(result_row)
        except Exception:
            continue

    # Write merged CSV
    if results:
        import pandas as pd
        df = pd.DataFrame(results)
        df.to_csv(out_csv, index=False)
    else:
        out_csv.write_text("")

    # Clean up stamps
    shutil.rmtree(stamp_dir, ignore_errors=True)

    return len(results)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, default=None,
                    help="Tile directory (must have raw/*.fits)")
    ap.add_argument("--plate-fits", type=Path, default=None,
                    help="Plate FITS to cut a fresh tile from (alternative to --workdir)")
    ap.add_argument("--stamp-px", type=int, default=STAMP_PX,
                    help=f"Stamp side length in pixels (default {STAMP_PX})")
    ap.add_argument("--veto-arcsec", type=float, default=VETO_ARCSEC)
    ap.add_argument("--skip-renders", action="store_true",
                    help="Skip replication renders (saves ~2 min of hips2fits calls)")
    args = ap.parse_args()

    overall_t0 = time.perf_counter()

    # ---- resolve tile dir --------------------------------------------------
    if args.plate_fits:
        print(f"[FAST] cutting tile from {args.plate_fits.name}")
        from tools.cutout_from_plate import main as cutout_main
        sys.argv = ["cutout_from_plate", "--plate-fits", str(args.plate_fits)]
        cutout_main()
        # Find the newest tile dir
        tiles_root = REPO / "data" / "tiles"
        tile_dir = max(
            (d for d in tiles_root.iterdir() if d.is_dir()),
            key=lambda d: d.stat().st_mtime,
        )
    elif args.workdir:
        tile_dir = args.workdir.resolve()
    else:
        ap.error("provide --workdir or --plate-fits")
        return 2

    raw_fits = next(iter(sorted((tile_dir / "raw").glob("*.fits"))), None)
    if not raw_fits:
        print(f"[FAST][ERROR] no FITS under {tile_dir / 'raw'}")
        return 2

    catdir = tile_dir / "catalogs"
    catdir.mkdir(parents=True, exist_ok=True)
    xdir = tile_dir / "xmatch"
    xdir.mkdir(parents=True, exist_ok=True)

    center = _tile_center(tile_dir)
    if not center:
        print("[FAST][ERROR] cannot determine tile center")
        return 2
    ra_t, dec_t = center
    radius_arcmin = 60.0 * (2 ** 0.5) * 0.5 + 3.0  # same as cli_pipeline

    print(f"[FAST] tile={tile_dir.name}  center=({ra_t:.3f}, {dec_t:.3f})")

    # ---- phase 1: SExtractor pass1 ----------------------------------------
    with _phase("pass1"):
        p1, _ = run_pass1(raw_fits, tile_dir, config_root="configs")
    print(f"[FAST] pass1 -> {p1}")

    # ---- phase 2: convert pass1.ldac to CSV --------------------------------
    with _phase("pass1_to_csv"):
        pass1_csv = catdir / "sextractor_pass1.csv"
        _ldac_to_csv(p1, pass1_csv)

    import pandas as pd
    p1_df = pd.read_csv(pass1_csv)
    n_pass1 = len(p1_df)
    print(f"[FAST] pass1 detections: {n_pass1}")

    # ---- phase 3: catalog fetches + epoch propagation ----------------------
    with _phase("catalog_fetch"):
        try:
            gaia_cache = catdir / "gaia_neighbourhood.csv"
            if not (gaia_cache.exists() and gaia_cache.stat().st_size > 0):
                fetch_gaia_neighbourhood(tile_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print(f"[FAST][WARN] Gaia fetch: {e}")

        try:
            ps1_cache = catdir / "ps1_neighbourhood.csv"
            if not (ps1_cache.exists() and ps1_cache.stat().st_size > 0):
                if dec_t >= -30.0:
                    fetch_ps1_neighbourhood(tile_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print(f"[FAST][WARN] PS1 fetch: {e}")

        try:
            usnob_cache = catdir / "usnob_neighbourhood.csv"
            if not (usnob_cache.exists() and usnob_cache.stat().st_size > 0):
                fetch_usnob_neighbourhood(tile_dir, ra_t, dec_t, radius_arcmin)
        except Exception as e:
            print(f"[FAST][WARN] USNO-B fetch: {e}")

    with _phase("epoch_propagate"):
        plate_epoch = _plate_epoch_year_from_fits(raw_fits)
        if plate_epoch:
            print(f"[FAST] plate epoch = {plate_epoch:.3f}")
            gaia_csv = catdir / "gaia_neighbourhood.csv"
            if gaia_csv.exists() and gaia_csv.stat().st_size > 0:
                _propagate_catalog_epoch(
                    gaia_csv, catdir / "gaia_neighbourhood_at_plate.csv",
                    2016.0, plate_epoch, "ra", "dec", "pmRA", "pmDE",
                )
            usnob_csv = catdir / "usnob_neighbourhood.csv"
            if usnob_csv.exists() and usnob_csv.stat().st_size > 0:
                _propagate_catalog_epoch(
                    usnob_csv, catdir / "usnob_neighbourhood_at_plate.csv",
                    2000.0, plate_epoch, "RAJ2000", "DEJ2000", "pmRA", "pmDE",
                )

    # ---- phase 3b: WCSFIX on pass1 positions --------------------------------
    # WCSFIX shifts detection positions using a Gaia-based polynomial WCS
    # correction. Typical shifts are 1–3″, enough to cross the 5″ veto boundary.
    # Without WCSFIX, the veto under/over-counts by ~5–10 sources per tile.
    with _phase("wcsfix_pass1"):
        from vasco.wcsfix_early import ensure_wcsfix_catalog, WcsFixConfig
        gaia_csv_for_wcsfix = catdir / "gaia_neighbourhood_at_plate.csv"
        if not (gaia_csv_for_wcsfix.exists() and gaia_csv_for_wcsfix.stat().st_size > 0):
            gaia_csv_for_wcsfix = catdir / "gaia_neighbourhood.csv"
        try:
            wcsfix_cfg = WcsFixConfig(
                bootstrap_radius_arcsec=5.0, degree=2, min_matches=20,
            )
            wcsfix_out, wcsfix_status = ensure_wcsfix_catalog(
                tile_dir, pass1_csv, gaia_csv_for_wcsfix,
                center=center, cfg=wcsfix_cfg,
            )
            if wcsfix_status.get("ok"):
                p1_df = pd.read_csv(wcsfix_out)
                print(f"[FAST] WCSFIX OK -> using RA_corr/Dec_corr for veto")
            else:
                print(f"[FAST] WCSFIX failed ({wcsfix_status.get('reason', '?')}) -> using raw pass1 positions")
        except Exception as e:
            print(f"[FAST] WCSFIX error: {e} -> using raw pass1 positions")

    # Determine which RA/Dec columns to use for the veto
    if "RA_corr" in p1_df.columns and "Dec_corr" in p1_df.columns:
        veto_ra_col, veto_dec_col = "RA_corr", "Dec_corr"
    else:
        veto_ra_col, veto_dec_col = "ALPHA_J2000", "DELTA_J2000"

    # ---- phase 4: Python cone-match veto -----------------------------------
    with _phase("veto_merged"):
        cat_ra, cat_dec = load_merged_catalog(tile_dir)
        print(f"[FAST] merged catalog: {len(cat_ra)} entries")

        src_ra = p1_df[veto_ra_col].apply(_safe_float).values
        src_dec = p1_df[veto_dec_col].apply(_safe_float).values

        survives = veto_against_catalogs(src_ra, src_dec, cat_ra, cat_dec,
                                         max_arcsec=args.veto_arcsec)
        n_surv = int(survives.sum())
        n_vetoed = n_pass1 - n_surv
        print(f"[FAST] veto: {n_vetoed} eliminated, {n_surv} survivors")

    survivors_df = p1_df[survives].copy()
    survivors_csv = catdir / "_fast_survivors_pass1.csv"
    survivors_df.to_csv(survivors_csv, index=False)

    # ---- phase 5: PSFEx (full pass1.ldac — needs ~500 stars) ---------------
    with _phase("psfex"):
        psf = run_psfex(p1, tile_dir, config_root="configs")
    print(f"[FAST] psf -> {psf}")

    # ---- phase 6: stamp-based pass2 on survivors only ----------------------
    with _phase("stamp_pass2"):
        pass2_csv = catdir / "sextractor_pass2.csv"
        n_measured = run_stamp_pass2(
            tile_dir, raw_fits, psf, survivors_csv, pass2_csv,
            stamp_px=args.stamp_px,
        )
    print(f"[FAST] stamp pass2: {n_measured} sources measured")

    if n_measured == 0:
        print("[FAST] no sources measured — writing empty results")
        (catdir / "sextractor_pass2.filtered.csv").write_text("")
        write_summary(tile_dir, finalize(init_buckets()),
                      md_path="MNRAS_SUMMARY.md", json_path="MNRAS_SUMMARY.json")
        return 0

    # ---- phase 7: late filters + spike cuts --------------------------------
    with _phase("late_filters"):
        buckets = init_buckets()

        # Apply extract + morphology filters (cfg={} uses all defaults)
        from astropy.table import Table
        tab = Table.read(pass2_csv, format="csv")
        kept_extract = apply_extract_filters(tab, {})
        rej_extract = tab[~np.isin(np.array(tab["NUMBER"]), np.array(kept_extract["NUMBER"]))] if "NUMBER" in tab.colnames and len(kept_extract) else tab

        kept_morph = apply_morphology_filters(kept_extract, {})
        rej_morph = kept_extract[~np.isin(np.array(kept_extract["NUMBER"]), np.array(kept_morph["NUMBER"]))] if "NUMBER" in kept_extract.colnames and len(kept_morph) else kept_extract

        buckets["morphology_rejected"] = len(rej_morph)

        # Write intermediate files for render scripts
        rej_ext_csv = catdir / "sextractor_pass2.after_usnob_veto.rejected_extract.csv"
        rej_morph_csv = catdir / "sextractor_pass2.after_usnob_veto.rejected_morphology.csv"
        if len(rej_extract):
            rej_extract.write(rej_ext_csv, format="csv", overwrite=True)
        else:
            rej_ext_csv.write_text("")
        if len(rej_morph):
            rej_morph.write(rej_morph_csv, format="csv", overwrite=True)
        else:
            rej_morph_csv.write_text("")

        # Write the post-filter set for spike cuts (also acts as after_usnob_veto
        # since the fast pipeline merges the catalog veto upstream)
        post_filter_csv = catdir / "sextractor_pass2.after_usnob_veto.csv"
        if len(kept_morph):
            kept_morph.write(post_filter_csv, format="csv", overwrite=True)
        else:
            post_filter_csv.write_text("")

        # Spike cuts
        bright = []
        bright_cache = catdir / "ps1_bright_stars_r16_rad3.csv"
        if center:
            try:
                if bright_cache.exists() and bright_cache.stat().st_size > 0:
                    bright_df = pd.read_csv(bright_cache)
                    bright = [
                        BrightStar(ra=float(r["ra"]), dec=float(r["dec"]), rmag=float(r["rmag"]))
                        for _, r in bright_df.iterrows()
                    ]
                else:
                    bright = fetch_bright_ps1(
                        ra_t, dec_t, radius_arcmin=45.0,
                        rmag_max=16.0, mindetections=2,
                    )
                    if bright:
                        pd.DataFrame(bright).to_csv(bright_cache, index=False)
            except Exception:
                bright = []

        if len(kept_morph) and bright:
            rows = [dict(zip(kept_morph.colnames, r)) for r in kept_morph]
            kept_spike, rej_spike = apply_spike_cuts(
                rows, bright,
                SpikeConfig(
                    search_radius_arcmin=1.5,
                    rules=[
                        SpikeRuleConst(const_max_mag=12.4),
                        SpikeRuleLine(a=-0.09, b=15.3),
                    ],
                ),
            )
            buckets["spikes_rejected"] = len(rej_spike)
        else:
            kept_spike = [dict(zip(kept_morph.colnames, r)) for r in kept_morph] if len(kept_morph) else []
            rej_spike = []

        # Write spike rejects
        spike_rej_csv = catdir / "sextractor_spike_rejected.csv"
        if rej_spike:
            pd.DataFrame(rej_spike).to_csv(spike_rej_csv, index=False)
        else:
            spike_rej_csv.write_text("")

        # Write final filtered survivors
        filtered_csv = catdir / "sextractor_pass2.filtered.csv"
        if kept_spike:
            pd.DataFrame(kept_spike).to_csv(filtered_csv, index=False)
        else:
            filtered_csv.write_text("")
        n_final = len(kept_spike)
        print(f"[FAST] final survivors: {n_final}")

    # Write summary
    extra = {
        "pipeline": "fast_tile",
        "pass1_detections": n_pass1,
        "catalog_entries_merged": len(cat_ra),
        "veto_eliminated": n_vetoed,
        "veto_survivors": n_surv,
        "stamp_measured": n_measured,
        "final_survivors": n_final,
    }
    write_summary(tile_dir, finalize(buckets),
                  md_path="MNRAS_SUMMARY.md", json_path="MNRAS_SUMMARY.json")
    # Augment JSON
    try:
        summary_json = tile_dir / "MNRAS_SUMMARY.json"
        d = json.loads(summary_json.read_text())
        d.update(extra)
        summary_json.write_text(json.dumps(d, indent=2))
    except Exception:
        pass

    # ---- phase 8: step6 renders (optional) ---------------------------------
    if not args.skip_renders:
        with _phase("renders"):
            try:
                _run_replication_renders(tile_dir)
            except Exception as e:
                print(f"[FAST][RENDER] error: {e}")

    overall_dt = time.perf_counter() - overall_t0
    print(f"\n[FAST] DONE in {overall_dt:.1f} s ({overall_dt / 60:.1f} min)")
    print(f"[FAST] funnel: {n_pass1} pass1 → {n_surv} after veto → "
          f"{n_measured} measured → {n_final} final survivors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
