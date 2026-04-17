"""Faster HEALPix-5 parquet cache queries.

Skip pyarrow dataset discovery by reading parquet files directly after a
one-time scan of HP5 partition directories. The partition map is cached
per-process and optionally persisted to <cache>/_hp_index.json.

Public API:
    cone_query(cache_dir, ra, dec, radius_arcmin, columns, filter=None)
        -> pandas.DataFrame with '_r' in arcmin.

Env activation (set these to the cache root dirs, same as local_cache_query):
    VASCO_GAIA_CACHE / VASCO_PS1_CACHE / VASCO_USNOB_CACHE
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np


_HP = None
_HP_INDEX_CACHE: dict[str, dict[int, list[str]]] = {}


def _get_hp():
    global _HP
    if _HP is None:
        from astropy_healpix import HEALPix
        _HP = HEALPix(nside=32, order="nested")
    return _HP


def _build_hp_index(cache_dir: str) -> dict[int, list[str]]:
    """Scan cache_dir/parquet/healpix_5=N/ → {N: [parquet paths]}."""
    root = Path(cache_dir) / "parquet"
    out: dict[int, list[str]] = {}
    with os.scandir(root) as it:
        for d in it:
            if not d.is_dir() or not d.name.startswith("healpix_5="):
                continue
            try:
                hp = int(d.name.split("=", 1)[1])
            except Exception:
                continue
            with os.scandir(d.path) as it2:
                files = [f.path for f in it2 if f.is_file() and f.name.endswith(".parquet")]
            if files:
                out[hp] = files
    return out


def _hp_index_path(cache_dir: str) -> Path:
    """External (writable) cache path for the HP5 index.

    The cache_dir itself may be read-only in sandboxed environments, so we
    store the index under the repo's .cache directory keyed by the cache_dir
    basename. Env override: VASCO_HP_INDEX_DIR.
    """
    base = os.getenv("VASCO_HP_INDEX_DIR")
    if base:
        root = Path(base)
    else:
        # Default: <repo>/.cache/hp_index/
        root = Path(__file__).resolve().parents[1] / ".cache" / "hp_index"
    root.mkdir(parents=True, exist_ok=True)
    name = Path(cache_dir).name or "default"
    return root / f"{name}.json"


def _get_hp_index(cache_dir: str) -> dict[int, list[str]]:
    """Memo-cached per-process; also persisted to a writable JSON index."""
    if cache_dir in _HP_INDEX_CACHE:
        return _HP_INDEX_CACHE[cache_dir]

    idx_path = _hp_index_path(cache_dir)
    if idx_path.exists():
        try:
            data = json.loads(idx_path.read_text())
            idx = {int(k): v for k, v in data.items()}
            _HP_INDEX_CACHE[cache_dir] = idx
            return idx
        except Exception:
            pass

    idx = _build_hp_index(cache_dir)
    # Persist for future runs (best-effort)
    try:
        idx_path.write_text(
            json.dumps({str(k): v for k, v in idx.items()}, separators=(",", ":"))
        )
    except Exception:
        pass
    _HP_INDEX_CACHE[cache_dir] = idx
    return idx


def write_hp_index(cache_dir: str) -> Path:
    """Explicitly (re)build and persist the HP5→files mapping."""
    idx = _build_hp_index(cache_dir)
    out = _hp_index_path(cache_dir)
    out.write_text(json.dumps({str(k): v for k, v in idx.items()}, separators=(",", ":")))
    _HP_INDEX_CACHE[cache_dir] = idx
    return out


def cone_query(cache_dir: str, ra: float, dec: float, radius_arcmin: float,
               columns: list[str], *, parquet_filter=None):
    """Load rows from a HP5 cache within a cone. Returns pandas DataFrame.

    `parquet_filter` is applied at parquet-read time (pyarrow.compute expression).
    """
    import astropy.units as u
    import pyarrow as pa
    import pyarrow.parquet as pq

    hp = _get_hp()
    pixels = hp.cone_search_lonlat(ra * u.deg, dec * u.deg,
                                   radius=radius_arcmin * u.arcmin)
    pixels = set(int(p) for p in pixels.tolist())

    idx = _get_hp_index(cache_dir)

    tables = []
    for p in pixels:
        for f in idx.get(p, []):
            tables.append(pq.read_table(f, columns=columns, filters=parquet_filter))

    if not tables:
        return _empty_df(columns)

    tbl = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    df = tbl.to_pandas()

    if len(df) == 0:
        df["_r"] = []
        return df

    ra_r = np.deg2rad(df["ra"].to_numpy())
    dec_r = np.deg2rad(df["dec"].to_numpy())
    cra, cdec = np.deg2rad(ra), np.deg2rad(dec)
    cos_sep = (np.sin(dec_r) * np.sin(cdec) +
               np.cos(dec_r) * np.cos(cdec) * np.cos(ra_r - cra))
    cos_sep = np.clip(cos_sep, -1.0, 1.0)
    sep_arcmin = np.rad2deg(np.arccos(cos_sep)) * 60.0

    df["_r"] = sep_arcmin
    df = df[df["_r"] <= radius_arcmin].copy()
    return df


def _empty_df(columns):
    import pandas as pd
    cols = {c: [] for c in columns}
    cols["_r"] = []
    return pd.DataFrame(cols)
