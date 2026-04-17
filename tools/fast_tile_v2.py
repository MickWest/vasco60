#!/usr/bin/env python3
"""fast_tile_v2.py — unified per-tile pipeline, parallel-friendly, no intermediate CSVs.

Design goals:
  - Single Python process from pass1 → PSFEx → pass2 → late filters.
  - All intermediates in-memory (pandas DataFrames, astropy Tables) or passed as
    file handles; the only disk writes are the input FITS + one parquet output.
  - Catalog fetches (Gaia/PS1/USNO-B + bright-PS1) go through local HEALPix-5
    Parquet caches (VASCO_{GAIA,PS1,USNOB}_CACHE) — no network calls.
  - Veto-first ordering (pass1 positions are ≤1" on POSS-I; catalog match
    at 5" eliminates ~95% of detections before the expensive PSF pass2).
  - Stamp-based pass2 on survivors only (one subprocess per survivor, but
    only O(100) survivors vs O(2000) total detections).
  - Two-phase structure so parameter sweeps can reuse the expensive stage:
      phase A (expensive): pass1 → veto → psfex → stamp pass2 → raw candidates
      phase B (cheap):     late filters (gates + spikes) → survivor list
    Pass 'candidates_parquet' to skip phase A.

Locked invariants preserved (from context/02_DECISIONS.md):
  - 5″ veto against merged (Gaia, PS1, USNO-B) catalogs, find=best1 semantics
  - Gates: FLAGS=0, SNR_WIN>30, SPREAD_MODEL>-0.002, ELONGATION<1.3, FWHM 2–7
  - Spike rules: CONST(<=12.4), LINE(a=-0.09, b=15.3), radius 1.5 arcmin
  - Pixel-extent constraints as in filters_mnras.apply_morphology_filters

Outputs per tile (configurable, default minimal):
  - candidates_raw.parquet — all pass2-measured sources, post-veto, with SExtractor cols
  - survivors.parquet      — subset after late filters (gates + spike cuts)
  - summary dict (returned / JSON) with funnel counts

CLI:
  fast_tile_v2.py --workdir data/tiles/tile_XYZ
  fast_tile_v2.py --workdir data/tiles/tile_XYZ --params configs/params_default.json
  fast_tile_v2.py --workdir data/tiles/tile_XYZ --reuse-candidates   # skip phase A
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from astropy.wcs import WCS
import astropy.units as u

# ----------------------------------------------------------------------------
# Defaults (locked invariants — match context/02_DECISIONS.md)
# ----------------------------------------------------------------------------

VETO_ARCSEC = 5.0
STAMP_PX = 91
BACK_SIZE_STAMP = 32
WCSFIX_BOOTSTRAP_ARCSEC = 5.0
WCSFIX_DEGREE = 2
WCSFIX_MIN_MATCHES = 20

# Late filter gates (MNRAS locked defaults)
DEFAULT_PARAMS = dict(
    flags_equal=0,
    snr_win_min=30.0,
    spread_model_min=-0.002,
    fwhm_lower=2.0,
    fwhm_upper=7.0,
    elongation_lt=1.3,
    extent_delta_lt=2.0,
    extent_min=1.0,
    sigma_clip=True,
    sigma_k=2.0,
    spike_search_arcmin=1.5,
    spike_const_max=12.4,
    spike_line_a=-0.09,
    spike_line_b=15.3,
    spike_radius_arcmin=45.0,
    spike_rmag_max=16.0,
    spike_mindetections=2,
    veto_arcsec=VETO_ARCSEC,
    stamp_px=STAMP_PX,
    wcsfix_enabled=True,
    stamp_threads=1,  # within-tile thread pool for stamp pass2 subprocesses
    stamp_center_max=1e9,  # filter: reject if stamp-local detection is this far from stamp center (px)
    spread_model_max=1e9,  # upper bound; default effectively disabled
    psfex_config="psfex.conf",  # filename under configs/ (try psfex_matched.conf for paper-calibrated SPREAD_MODEL)
)


# ----------------------------------------------------------------------------
# Timing
# ----------------------------------------------------------------------------

@dataclass
class PhaseTimer:
    records: dict = field(default_factory=dict)
    enabled: bool = True

    def run(self, name):
        import contextlib
        @contextlib.contextmanager
        def _ctx():
            t0 = time.perf_counter()
            try:
                yield
            finally:
                dt = time.perf_counter() - t0
                self.records[name] = self.records.get(name, 0.0) + dt
                if self.enabled:
                    print(f"[TIMING] phase={name} sec={dt:.3f}", flush=True)
        return _ctx()


# ----------------------------------------------------------------------------
# Tile helpers
# ----------------------------------------------------------------------------

def _tile_center(tile_dir: Path) -> tuple[float, float] | None:
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


def _plate_epoch_year(raw_fits: Path) -> float | None:
    from vasco.cli_pipeline import _plate_epoch_year_from_fits
    return _plate_epoch_year_from_fits(raw_fits)


# ----------------------------------------------------------------------------
# In-memory LDAC → DataFrame
# ----------------------------------------------------------------------------

def _ldac_to_df(ldac_path: Path) -> pd.DataFrame:
    with fits.open(ldac_path) as hdul:
        # SExtractor LDAC: HDU[2] is the OBJECTS table; HDU[1] is the header table.
        idx = 2 if len(hdul) > 2 else 1
        data = hdul[idx].data
        cols = {}
        for name in data.columns.names:
            arr = data[name]
            if arr.ndim != 1:
                continue
            # FITS stores big-endian; convert to native byte order for pandas.
            if arr.dtype.byteorder not in ("=", "|"):
                arr = arr.astype(arr.dtype.newbyteorder("="))
            cols[name] = np.asarray(arr)
    return pd.DataFrame(cols)


# ----------------------------------------------------------------------------
# Stage to run folder — single-shot copy of configs
# ----------------------------------------------------------------------------

_CONFIG_CACHE: dict[str, bytes] = {}

def _stage_configs(tile_dir: Path, names: list[str]) -> None:
    """Copy config files into tile_dir (bare names). Memo-cached by filename."""
    cfg_root = REPO / "configs"
    for name in names:
        dst = tile_dir / name
        if dst.exists():
            continue
        src = cfg_root / name
        if src.exists():
            shutil.copy2(src, dst)


# ----------------------------------------------------------------------------
# pass1: SExtractor without PSF model
# ----------------------------------------------------------------------------

def run_pass1(raw_fits: Path, tile_dir: Path) -> Path:
    _stage_configs(tile_dir, ["sex_pass1.sex", "sex_default.param",
                              "default.nnw", "default.conv"])
    img_rel = raw_fits.relative_to(tile_dir) if raw_fits.is_absolute() and raw_fits.is_relative_to(tile_dir) else Path("raw") / raw_fits.name
    ldac = tile_dir / "pass1.ldac"
    cmd = ["sex", str(img_rel), "-c", "sex_pass1.sex",
           "-CATALOG_NAME", "pass1.ldac", "-CATALOG_TYPE", "FITS_LDAC",
           "-PSF_NAME", ""]
    with open(tile_dir / "sex.out", "w") as out, open(tile_dir / "sex.err", "w") as err:
        rc = subprocess.run(cmd, stdout=out, stderr=err, cwd=str(tile_dir)).returncode
    if rc != 0:
        raise RuntimeError(f"pass1 failed rc={rc}")
    return ldac


def run_psfex(pass1_ldac: Path, tile_dir: Path,
              config_name: str = "psfex.conf") -> Path:
    """Run PSFEx with the named config file (under configs/). Default is
    configs/psfex.conf; configs/psfex_matched.conf uses PSF_SAMPLING=0.5 which
    shifts SPREAD_MODEL to match the Solano 2022 calibration."""
    cfg_name = config_name if config_name.endswith(".conf") else config_name + ".conf"
    # Stage the chosen config under a stable name `psfex.conf` in tile_dir so
    # subsequent psfex invocations see it.
    src = REPO / "configs" / cfg_name
    if not src.exists():
        raise FileNotFoundError(f"psfex config not found: {src}")
    shutil.copy2(src, tile_dir / "psfex.conf")
    cmd = ["psfex", "pass1.ldac", "-c", "psfex.conf"]
    with open(tile_dir / "psfex.out", "w") as out, open(tile_dir / "psfex.err", "w") as err:
        rc = subprocess.run(cmd, stdout=out, stderr=err, cwd=str(tile_dir)).returncode
    if rc != 0:
        raise RuntimeError(f"psfex failed rc={rc}")
    psf = tile_dir / "pass1.psf"
    if not psf.exists():
        raise RuntimeError("psfex produced no output")
    return psf


# ----------------------------------------------------------------------------
# Catalog fetches — local cache only (no network)
# ----------------------------------------------------------------------------

def fetch_catalogs_local(tile_dir: Path, ra: float, dec: float,
                          radius_arcmin: float) -> dict[str, pd.DataFrame]:
    """Query local HP5 parquet caches directly; return DataFrames (no CSV)."""
    from vasco.fast_cache_query import cone_query as _cone_query
    import pyarrow.compute as pc

    out: dict[str, pd.DataFrame] = {}

    cache_gaia = os.getenv("VASCO_GAIA_CACHE")
    if cache_gaia:
        df = _cone_query(cache_gaia, ra, dec, radius_arcmin,
                         columns=["ra", "dec", "phot_g_mean_mag", "pmra", "pmdec"])
        df = df.rename(columns={"phot_g_mean_mag": "Gmag",
                                "pmra": "pmRA", "pmdec": "pmDE"})
        out["gaia"] = df
    else:
        out["gaia"] = pd.DataFrame(columns=["ra", "dec", "Gmag", "pmRA", "pmDE"])

    cache_ps1 = os.getenv("VASCO_PS1_CACHE")
    if cache_ps1 and dec >= -30.0:
        df = _cone_query(cache_ps1, ra, dec, radius_arcmin,
                         columns=["objID", "ra", "dec", "nDetections",
                                  "gmag", "rmag", "imag", "zmag", "ymag"],
                         parquet_filter=pc.field("nDetections") >= 3)
        out["ps1"] = df
    else:
        out["ps1"] = pd.DataFrame(columns=["ra", "dec", "rmag"])

    cache_usnob = os.getenv("VASCO_USNOB_CACHE")
    if cache_usnob:
        df = _cone_query(cache_usnob, ra, dec, radius_arcmin,
                         columns=["ra", "dec", "B1mag", "R1mag", "B2mag",
                                  "R2mag", "Imag", "pmRA", "pmDE"])
        out["usnob"] = df
    else:
        out["usnob"] = pd.DataFrame(columns=["ra", "dec", "pmRA", "pmDE"])

    return out


def fetch_bright_ps1_local(ra: float, dec: float, radius_arcmin: float,
                            rmag_max: float, mindetections: int) -> pd.DataFrame:
    """Query local PS1 cache for bright stars (for spike veto). No network."""
    cache = os.getenv("VASCO_PS1_CACHE")
    if not cache:
        return pd.DataFrame(columns=["ra", "dec", "rmag"])
    from vasco.fast_cache_query import cone_query as _cone_query
    import pyarrow.compute as pc
    df = _cone_query(cache, ra, dec, radius_arcmin,
                     columns=["ra", "dec", "rmag", "nDetections"],
                     parquet_filter=pc.field("nDetections") >= mindetections)
    if len(df) == 0:
        return df
    # Filter rmag
    df = df[np.isfinite(df["rmag"]) & (df["rmag"] > 0) & (df["rmag"] <= rmag_max)]
    return df.reset_index(drop=True)


# ----------------------------------------------------------------------------
# Epoch propagation — numpy vectorized, in-memory
# ----------------------------------------------------------------------------

def epoch_propagate_inplace(df: pd.DataFrame, *, plate_epoch: float,
                             catalog_epoch: float,
                             ra_col: str = "ra", dec_col: str = "dec",
                             pmra_col: str = "pmRA", pmde_col: str = "pmDE") -> pd.DataFrame:
    if len(df) == 0 or pmra_col not in df.columns or pmde_col not in df.columns:
        return df
    dt = plate_epoch - catalog_epoch
    ra = pd.to_numeric(df[ra_col], errors="coerce").values
    dec = pd.to_numeric(df[dec_col], errors="coerce").values
    pmra = pd.to_numeric(df[pmra_col], errors="coerce").values
    pmde = pd.to_numeric(df[pmde_col], errors="coerce").values
    has_pm = np.isfinite(pmra) & np.isfinite(pmde) & np.isfinite(ra) & np.isfinite(dec)
    ddec = np.where(has_pm, pmde * dt / 3.6e6, 0.0)
    cos_dec = np.cos(np.radians(np.where(has_pm, dec, 0.0)))
    dra = np.where(has_pm, (pmra * dt / 3.6e6) / np.where(cos_dec == 0, 1, cos_dec), 0.0)
    out = df.copy()
    out[ra_col] = ra + dra
    out[dec_col] = dec + ddec
    valid = np.isfinite(out[ra_col].values) & np.isfinite(out[dec_col].values)
    return out.loc[valid].reset_index(drop=True)


# ----------------------------------------------------------------------------
# In-memory WCSFIX — astropy KDTree match, no STILTS
# ----------------------------------------------------------------------------

def fit_wcsfix_poly(sex_ra: np.ndarray, sex_dec: np.ndarray,
                    gaia_ra: np.ndarray, gaia_dec: np.ndarray,
                    center: tuple[float, float],
                    bootstrap_arcsec: float = WCSFIX_BOOTSTRAP_ARCSEC,
                    degree: int = WCSFIX_DEGREE,
                    min_matches: int = WCSFIX_MIN_MATCHES) -> dict | None:
    """Fit a 2D polynomial correction to SExtractor RA/Dec positions using Gaia.

    Returns a dict with the fit coefficients and status, or None on failure.
    """
    if len(sex_ra) < min_matches or len(gaia_ra) < min_matches:
        return None
    sex_sky = SkyCoord(sex_ra * u.deg, sex_dec * u.deg, frame="icrs")
    gaia_sky = SkyCoord(gaia_ra * u.deg, gaia_dec * u.deg, frame="icrs")
    idx, sep, _ = sex_sky.match_to_catalog_sky(gaia_sky)
    sep_arcsec = sep.to(u.arcsec).value
    keep = sep_arcsec <= bootstrap_arcsec
    if keep.sum() < min_matches:
        return None

    # Match pairs
    dra = (gaia_ra[idx][keep] - sex_ra[keep])
    ddec = gaia_dec[idx][keep] - sex_dec[keep]
    # Use sex positions (relative to center) as features
    cra, cdec = center
    x = (sex_ra[keep] - cra) * np.cos(np.radians(cdec))
    y = sex_dec[keep] - cdec

    def _features(x, y, deg):
        cols = [np.ones_like(x)]
        if deg >= 1:
            cols.extend([x, y])
        if deg >= 2:
            cols.extend([x * x, x * y, y * y])
        return np.stack(cols, axis=1)

    F = _features(x, y, degree)
    try:
        cra_coef, *_ = np.linalg.lstsq(F, dra, rcond=None)
        cdec_coef, *_ = np.linalg.lstsq(F, ddec, rcond=None)
    except Exception:
        return None

    return dict(
        center=center,
        degree=degree,
        n_matches=int(keep.sum()),
        cra_coef=cra_coef,
        cdec_coef=cdec_coef,
    )


def apply_wcsfix(fit: dict, ra: np.ndarray, dec: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    cra, cdec = fit["center"]
    degree = fit["degree"]
    x = (ra - cra) * np.cos(np.radians(cdec))
    y = dec - cdec
    cols = [np.ones_like(x)]
    if degree >= 1:
        cols.extend([x, y])
    if degree >= 2:
        cols.extend([x * x, x * y, y * y])
    F = np.stack(cols, axis=1)
    dra = F @ fit["cra_coef"]
    ddec = F @ fit["cdec_coef"]
    return ra + dra, dec + ddec


# ----------------------------------------------------------------------------
# Merged veto (Gaia + PS1 + USNO-B)
# ----------------------------------------------------------------------------

def merged_veto_mask(src_ra: np.ndarray, src_dec: np.ndarray,
                      cats: dict[str, pd.DataFrame],
                      max_arcsec: float = VETO_ARCSEC) -> np.ndarray:
    """Return bool mask; True = SURVIVES (no catalog match within max_arcsec)."""
    ras = []
    decs = []
    for name, df in cats.items():
        if len(df) and "ra" in df.columns and "dec" in df.columns:
            r = pd.to_numeric(df["ra"], errors="coerce").values
            d = pd.to_numeric(df["dec"], errors="coerce").values
            ok = np.isfinite(r) & np.isfinite(d)
            ras.append(r[ok])
            decs.append(d[ok])
    if not ras:
        return np.ones(len(src_ra), dtype=bool)
    cat_ra = np.concatenate(ras)
    cat_dec = np.concatenate(decs)
    src_sky = SkyCoord(src_ra * u.deg, src_dec * u.deg, frame="icrs")
    cat_sky = SkyCoord(cat_ra * u.deg, cat_dec * u.deg, frame="icrs")
    _, sep, _ = src_sky.match_to_catalog_sky(cat_sky)
    return sep.to(u.arcsec).value > max_arcsec


# ----------------------------------------------------------------------------
# Stamp-based pass2
# ----------------------------------------------------------------------------

def _stamp_write(args):
    """Phase 1 worker: write one stamp FITS. Thread-unsafe parts (astropy) only here."""
    (stamp_dir, full_data, full_hdr, num, cx, cy, half, nx, ny) = args
    x1 = max(0, cx - half); x2 = min(nx, cx + half + 1)
    y1 = max(0, cy - half); y2 = min(ny, cy + half + 1)
    if x2 - x1 < 20 or y2 - y1 < 20:
        return None
    stamp_data = full_data[y1:y2, x1:x2]
    hdr = full_hdr.copy()
    hdr["NAXIS1"] = x2 - x1
    hdr["NAXIS2"] = y2 - y1
    if "CRPIX1" in hdr: hdr["CRPIX1"] = float(hdr["CRPIX1"]) - x1
    if "CRPIX2" in hdr: hdr["CRPIX2"] = float(hdr["CRPIX2"]) - y1
    stamp_fits = stamp_dir / f"s{num}.fits"
    fits.PrimaryHDU(data=stamp_data, header=hdr).writeto(stamp_fits, overwrite=True)
    return (num, x1, y1, x2, y2)


def _stamp_sex(args):
    """Phase 2 worker: run sex on a pre-written stamp. Pure subprocess — thread-safe."""
    (tile_dir, stamp_dir, psf_name, num) = args
    stamp_fits = stamp_dir / f"s{num}.fits"
    stamp_ldac = stamp_dir / f"s{num}.ldac"
    if not stamp_fits.exists():
        return num, False
    cmd = ["sex", str(stamp_fits.relative_to(tile_dir)),
           "-c", "sex_pass2_stamp.sex",
           "-CATALOG_NAME", str(stamp_ldac.relative_to(tile_dir)),
           "-CATALOG_TYPE", "FITS_LDAC",
           "-PSF_NAME", psf_name]
    rc = subprocess.run(cmd, cwd=str(tile_dir),
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL).returncode
    return num, (rc == 0 and stamp_ldac.exists())


def _stamp_read(stamp_dir: Path, num: int, x1: int, y1: int, x2: int, y2: int):
    """Phase 3: read the LDAC result (serial, astropy)."""
    stamp_ldac = stamp_dir / f"s{num}.ldac"
    if not stamp_ldac.exists():
        return None
    try:
        with fits.open(stamp_ldac) as h:
            if len(h) < 3:
                return None
            data = h[2].data
            if len(data) == 0:
                return None
            scx = (x2 - x1) / 2.0; scy = (y2 - y1) / 2.0
            xd = np.asarray(data["X_IMAGE"], dtype=float) - scx
            yd = np.asarray(data["Y_IMAGE"], dtype=float) - scy
            best = int(np.argmin(xd * xd + yd * yd))
            row = {}
            for name in data.columns.names:
                v = data[name][best]
                if np.ndim(v) > 0:
                    continue
                row[name] = v
            row["_stamp_center_dist"] = float(np.hypot(xd[best], yd[best]))
            row["X_IMAGE"] = float(data["X_IMAGE"][best]) + x1
            row["Y_IMAGE"] = float(data["Y_IMAGE"][best]) + y1
            row["NUMBER"] = num
            return row
    except Exception:
        return None


def stamp_pass2(tile_dir: Path, raw_fits: Path, psf_path: Path,
                survivors_df: pd.DataFrame, stamp_px: int = STAMP_PX,
                n_threads: int = 1) -> pd.DataFrame:
    """Run SExtractor pass2 on postage stamps around each survivor.

    Three phases:
      1. Write stamp FITS files (serial; astropy is not perfectly thread-safe).
      2. Run sex subprocess per stamp (parallel; pure subprocess calls).
      3. Read LDAC results (serial).

    n_threads controls Phase 2 parallelism. Set to 1 for multi-process callers.
    """
    if not len(survivors_df):
        return pd.DataFrame()

    _stage_configs(tile_dir, ["sex_pass2_stamp.sex", "default.param",
                              "default.nnw", "default.conv"])

    with fits.open(raw_fits) as hdul:
        full_data = hdul[0].data.astype(np.float32)
        full_hdr = hdul[0].header

    ny, nx = full_data.shape
    half = stamp_px // 2

    stamp_dir = tile_dir / "_stamps_v2"
    stamp_dir.mkdir(exist_ok=True)

    numbers = survivors_df["NUMBER"].astype(int).values
    xs = survivors_df["X_IMAGE"].astype(float).round().astype(int).values - 1
    ys = survivors_df["Y_IMAGE"].astype(float).round().astype(int).values - 1

    # Phase 1: write all stamps (serial)
    stamp_info: dict[int, tuple[int, int, int, int]] = {}
    for k in range(len(numbers)):
        info = _stamp_write((stamp_dir, full_data, full_hdr,
                              int(numbers[k]), int(xs[k]), int(ys[k]),
                              half, nx, ny))
        if info is not None:
            stamp_info[info[0]] = info[1:]

    # Phase 2: run sex (parallel)
    tasks = [(tile_dir, stamp_dir, psf_path.name, num) for num in stamp_info]
    try:
        if n_threads <= 1:
            sex_results = [_stamp_sex(t) for t in tasks]
        else:
            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor(max_workers=n_threads) as ex:
                sex_results = list(ex.map(_stamp_sex, tasks))

        # Phase 3: read LDACs (serial)
        results: list[dict] = []
        for num, ok in sex_results:
            if not ok:
                continue
            x1, y1, x2, y2 = stamp_info[num]
            r = _stamp_read(stamp_dir, num, x1, y1, x2, y2)
            if r is not None:
                results.append(r)
    finally:
        shutil.rmtree(stamp_dir, ignore_errors=True)

    return pd.DataFrame(results)


# ----------------------------------------------------------------------------
# Late filters (gates + spike cuts) — vectorized
# ----------------------------------------------------------------------------

def _robust_sigma_mask(x: np.ndarray, k: float = 2.0) -> np.ndarray:
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    sigma = 1.4826 * mad
    if not np.isfinite(sigma) or sigma <= 0:
        return np.isfinite(x)
    return np.isfinite(x) & (np.abs(x - med) <= k * sigma)


def apply_late_filters(df: pd.DataFrame, params: dict,
                       bright: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Apply gates + spike cuts. Returns (kept_df, funnel_counts)."""
    counts = {"in": len(df)}
    if not len(df):
        return df, {**counts, "out": 0, "reject_extract": 0,
                    "reject_morph": 0, "reject_spike": 0}

    # ----- extract gates (FLAGS, SNR_WIN) -----
    keep = np.ones(len(df), dtype=bool)
    if "FLAGS" in df.columns:
        keep &= df["FLAGS"].astype(float).values == params["flags_equal"]
    if "SNR_WIN" in df.columns:
        keep &= df["SNR_WIN"].astype(float).values > params["snr_win_min"]
    n_after_extract = int(keep.sum())
    counts["reject_extract"] = len(df) - n_after_extract

    df1 = df.loc[keep].reset_index(drop=True)
    if not len(df1):
        counts["out"] = 0; counts["reject_morph"] = 0; counts["reject_spike"] = 0
        return df1, counts

    # ----- morphology gates -----
    m = np.ones(len(df1), dtype=bool)
    if params.get("sigma_clip", True):
        if "FWHM_IMAGE" in df1.columns:
            m &= _robust_sigma_mask(df1["FWHM_IMAGE"].astype(float).values,
                                     k=params["sigma_k"])
        if "ELONGATION" in df1.columns:
            m &= _robust_sigma_mask(df1["ELONGATION"].astype(float).values,
                                     k=params["sigma_k"])

    if "SPREAD_MODEL" in df1.columns:
        sm = df1["SPREAD_MODEL"].astype(float).values
        m &= sm > params["spread_model_min"]
        m &= sm < params.get("spread_model_max", 1e9)

    if "FWHM_IMAGE" in df1.columns:
        f = df1["FWHM_IMAGE"].astype(float).values
        m &= np.isfinite(f) & (f > params["fwhm_lower"]) & (f < params["fwhm_upper"])

    if "ELONGATION" in df1.columns:
        e = df1["ELONGATION"].astype(float).values
        m &= np.isfinite(e) & (e < params["elongation_lt"])

    if "_stamp_center_dist" in df1.columns:
        sd = df1["_stamp_center_dist"].astype(float).values
        m &= np.isfinite(sd) & (sd <= params.get("stamp_center_max", 1e9))

    need = {"XMAX_IMAGE", "XMIN_IMAGE", "YMAX_IMAGE", "YMIN_IMAGE"}
    if need.issubset(df1.columns):
        dx = df1["XMAX_IMAGE"].astype(float).values - df1["XMIN_IMAGE"].astype(float).values
        dy = df1["YMAX_IMAGE"].astype(float).values - df1["YMIN_IMAGE"].astype(float).values
        m &= np.isfinite(dx) & np.isfinite(dy)
        m &= np.abs(dx - dy) < params["extent_delta_lt"]
        m &= (dx > params["extent_min"]) & (dy > params["extent_min"])

    n_after_morph = int(m.sum())
    counts["reject_morph"] = len(df1) - n_after_morph
    df2 = df1.loc[m].reset_index(drop=True)
    if not len(df2):
        counts["out"] = 0; counts["reject_spike"] = 0
        return df2, counts

    # ----- spike cuts -----
    if not len(bright):
        counts["reject_spike"] = 0; counts["out"] = len(df2)
        return df2, counts

    # Detection positions
    ra_col = "ALPHA_J2000" if "ALPHA_J2000" in df2.columns else ("RA_corr" if "RA_corr" in df2.columns else None)
    dec_col = "DELTA_J2000" if "DELTA_J2000" in df2.columns else ("Dec_corr" if "Dec_corr" in df2.columns else None)
    if ra_col is None or dec_col is None:
        counts["reject_spike"] = 0; counts["out"] = len(df2)
        return df2, counts

    det_ra = df2[ra_col].astype(float).values
    det_dec = df2[dec_col].astype(float).values
    b_ra = bright["ra"].astype(float).values
    b_dec = bright["dec"].astype(float).values
    b_mag = bright["rmag"].astype(float).values

    d_sky = SkyCoord(det_ra * u.deg, det_dec * u.deg, frame="icrs")
    b_sky = SkyCoord(b_ra * u.deg, b_dec * u.deg, frame="icrs")
    idx, sep, _ = d_sky.match_to_catalog_sky(b_sky)
    d_arcsec = sep.to(u.arcsec).value
    m_near = b_mag[idx]

    max_arcsec = params["spike_search_arcmin"] * 60.0
    has_bright = (d_arcsec <= max_arcsec) & np.isfinite(m_near) & (m_near > 0) & (m_near <= params["spike_rmag_max"])

    reject = np.zeros(len(d_arcsec), dtype=bool)
    # CONST rule: reject if m_near <= const_max_mag
    reject |= has_bright & (m_near <= params["spike_const_max"])
    # LINE rule: reject if m_near < a*d_arcsec + b
    thresh = params["spike_line_a"] * d_arcsec + params["spike_line_b"]
    reject |= has_bright & (m_near < thresh)

    keep_spike = ~reject
    counts["reject_spike"] = int(reject.sum())
    counts["out"] = int(keep_spike.sum())
    df3 = df2.loc[keep_spike].reset_index(drop=True)
    return df3, counts


# ----------------------------------------------------------------------------
# Main per-tile driver
# ----------------------------------------------------------------------------

def process_tile(tile_dir: Path, params: Optional[dict] = None,
                  *, reuse_candidates: bool = False,
                  write_candidates: bool = True,
                  write_survivors: bool = True,
                  cleanup: bool = False,
                  quiet: bool = False) -> dict:
    """Run the full pipeline for one tile. Returns a summary dict.

    cleanup: delete pass1.ldac and pass1.psf after stamp_pass2 completes to save disk.
             Re-running phase A will re-create them (~1s overhead).
    """
    p = dict(DEFAULT_PARAMS)
    if params:
        p.update(params)

    timer = PhaseTimer(enabled=not quiet)
    tile_dir = Path(tile_dir)
    raw_fits = next(iter(sorted((tile_dir / "raw").glob("*.fits"))), None)
    if not raw_fits:
        return {"tile": tile_dir.name, "ok": False, "error": "no raw FITS"}

    center = _tile_center(tile_dir)
    if not center:
        return {"tile": tile_dir.name, "ok": False, "error": "no center"}
    ra_t, dec_t = center

    candidates_parquet = tile_dir / "candidates_raw.parquet"
    survivors_parquet = tile_dir / "survivors.parquet"

    # -------- phase A: produce raw candidates (pass1→veto→psfex→stamp pass2) --
    if reuse_candidates and candidates_parquet.exists():
        with timer.run("read_candidates"):
            cand = pd.read_parquet(candidates_parquet)
        summary = {
            "tile": tile_dir.name, "center": [ra_t, dec_t],
            "ok": True, "phase_a_skipped": True,
        }
    else:
        # pass1
        with timer.run("pass1"):
            p1_ldac = run_pass1(raw_fits, tile_dir)
        with timer.run("pass1_to_df"):
            p1_df = _ldac_to_df(p1_ldac)
        n_pass1 = len(p1_df)

        # Now run [catalog_fetch + epoch + wcsfix + veto] concurrently with [psfex].
        # PSFEx only depends on pass1_ldac. Catalog ops only depend on pass1_df.
        # Saves ~0.5s on typical tiles (psfex short) where catalog ops would
        # otherwise run serially; on dense tiles PSFEx dominates and catalog is
        # entirely masked.
        radius_arcmin = 60.0 * (2 ** 0.5) * 0.5 + 3.0

        import concurrent.futures as _cf
        _results: dict = {}

        def _catalog_pipeline():
            t_cat = time.perf_counter()
            cats = fetch_catalogs_local(tile_dir, ra_t, dec_t, radius_arcmin)
            _results["cat_sec"] = time.perf_counter() - t_cat

            t_ep = time.perf_counter()
            pe = _plate_epoch_year(raw_fits)
            if pe:
                cats["gaia"] = epoch_propagate_inplace(
                    cats["gaia"], plate_epoch=pe, catalog_epoch=2016.0)
                cats["usnob"] = epoch_propagate_inplace(
                    cats["usnob"], plate_epoch=pe, catalog_epoch=2000.0)
            _results["ep_sec"] = time.perf_counter() - t_ep

            t_wf = time.perf_counter()
            wcsfix_ok = False
            if p.get("wcsfix_enabled", True) and len(cats["gaia"]):
                sex_ra = p1_df["ALPHA_J2000"].astype(float).values
                sex_dec = p1_df["DELTA_J2000"].astype(float).values
                g_ra = cats["gaia"]["ra"].astype(float).values
                g_dec = cats["gaia"]["dec"].astype(float).values
                fit = fit_wcsfix_poly(sex_ra, sex_dec, g_ra, g_dec, center=center)
                if fit is not None:
                    new_ra, new_dec = apply_wcsfix(fit, sex_ra, sex_dec)
                    p1_df["RA_corr"] = new_ra
                    p1_df["Dec_corr"] = new_dec
                    wcsfix_ok = True
            _results["wcsfix_ok"] = wcsfix_ok
            _results["wf_sec"] = time.perf_counter() - t_wf
            _results["plate_epoch"] = pe

            t_v = time.perf_counter()
            veto_ra_col = "RA_corr" if wcsfix_ok else "ALPHA_J2000"
            veto_dec_col = "Dec_corr" if wcsfix_ok else "DELTA_J2000"
            sr = p1_df[veto_ra_col].astype(float).values
            sd = p1_df[veto_dec_col].astype(float).values
            mask = merged_veto_mask(sr, sd, cats, max_arcsec=p["veto_arcsec"])
            _results["mask"] = mask
            _results["n_veto_elim"] = int((~mask).sum())
            _results["v_sec"] = time.perf_counter() - t_v

        def _psfex():
            t = time.perf_counter()
            psf = run_psfex(p1_ldac, tile_dir, config_name=p.get("psfex_config", "psfex.conf"))
            _results["psf"] = psf
            _results["psfex_sec"] = time.perf_counter() - t

        with timer.run("parallel_cat_psfex"):
            with _cf.ThreadPoolExecutor(max_workers=2) as ex:
                f_cat = ex.submit(_catalog_pipeline)
                f_psf = ex.submit(_psfex)
                f_cat.result(); f_psf.result()

        # Backfill timer records for detail
        timer.records["catalog_fetch"] = _results["cat_sec"]
        timer.records["epoch_propagate"] = _results["ep_sec"]
        timer.records["wcsfix"] = _results["wf_sec"]
        timer.records["veto_merged"] = _results["v_sec"]
        timer.records["psfex"] = _results["psfex_sec"]

        wcsfix_ok = _results["wcsfix_ok"]
        plate_epoch = _results["plate_epoch"]
        mask = _results["mask"]
        n_veto_elim = _results["n_veto_elim"]
        psf = _results["psf"]
        survivors_df = p1_df.loc[mask].reset_index(drop=True)

        # stamp pass2
        with timer.run("stamp_pass2"):
            cand = stamp_pass2(tile_dir, raw_fits, psf, survivors_df,
                                stamp_px=p["stamp_px"],
                                n_threads=int(p.get("stamp_threads", 1)))

        # carry WCSFIX correction onto cand rows (match by NUMBER)
        if wcsfix_ok and len(cand):
            lookup = dict(zip(p1_df["NUMBER"].astype(int),
                              zip(p1_df["RA_corr"].values, p1_df["Dec_corr"].values)))
            ras, decs = [], []
            for n in cand["NUMBER"].astype(int).values:
                rdec = lookup.get(int(n), (np.nan, np.nan))
                ras.append(rdec[0]); decs.append(rdec[1])
            cand["RA_corr"] = ras
            cand["Dec_corr"] = decs

        if write_candidates and len(cand):
            cand.to_parquet(candidates_parquet, index=False)

        summary = {
            "tile": tile_dir.name, "center": [ra_t, dec_t],
            "ok": True,
            "n_pass1": n_pass1,
            "n_veto_eliminated": n_veto_elim,
            "n_veto_survivors": int(mask.sum()),
            "n_stamp_measured": len(cand),
            "wcsfix_ok": wcsfix_ok,
            "plate_epoch": plate_epoch,
        }

    # -------- phase B: late filters ------------------------------------------
    with timer.run("bright_ps1"):
        bright_cache = tile_dir / "catalogs" / "ps1_bright_stars_r16_rad3.csv"
        if bright_cache.exists() and bright_cache.stat().st_size > 0:
            bright = pd.read_csv(bright_cache)
        else:
            bright = fetch_bright_ps1_local(
                ra_t, dec_t,
                radius_arcmin=p["spike_radius_arcmin"],
                rmag_max=p["spike_rmag_max"],
                mindetections=p["spike_mindetections"],
            )
            if len(bright):
                bright_cache.parent.mkdir(parents=True, exist_ok=True)
                bright.to_csv(bright_cache, index=False)

    with timer.run("late_filters"):
        survivors, counts = apply_late_filters(cand, p, bright)

    summary.update(counts)
    summary["n_survivors_final"] = len(survivors)
    summary["timings"] = dict(timer.records)

    if write_survivors:
        # Write a parquet even if empty (with header, if possible)
        if len(survivors):
            survivors.to_parquet(survivors_parquet, index=False)
        else:
            # empty sentinel
            survivors_parquet.write_bytes(b"")

    if cleanup and not reuse_candidates:
        # Delete big intermediates. candidates_raw.parquet is kept for reruns.
        for f in ("pass1.ldac", "pass1.psf", "pass2.ldac"):
            p = tile_dir / f
            if p.exists():
                try: p.unlink()
                except Exception: pass

    return summary


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, required=True)
    ap.add_argument("--params", type=Path, default=None,
                    help="JSON file with param overrides")
    ap.add_argument("--reuse-candidates", action="store_true",
                    help="Skip phase A; read candidates_raw.parquet if present")
    ap.add_argument("--no-write-candidates", action="store_true")
    ap.add_argument("--no-write-survivors", action="store_true")
    ap.add_argument("--cleanup", action="store_true",
                    help="Delete pass1.ldac/pass1.psf after processing to save disk")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    params = None
    if args.params:
        params = json.loads(Path(args.params).read_text())

    t0 = time.perf_counter()
    summary = process_tile(
        args.workdir, params=params,
        reuse_candidates=args.reuse_candidates,
        write_candidates=not args.no_write_candidates,
        write_survivors=not args.no_write_survivors,
        cleanup=args.cleanup,
        quiet=args.quiet,
    )
    dt = time.perf_counter() - t0
    summary["wall_clock_sec"] = dt

    out_json = args.workdir / "fast_tile_v2_summary.json"
    out_json.write_text(json.dumps(summary, indent=2, default=float))

    if not args.quiet:
        print(f"\n[V2] DONE in {dt:.2f}s  survivors={summary.get('n_survivors_final', 0)}")
    return 0 if summary.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
