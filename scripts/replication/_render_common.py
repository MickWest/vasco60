"""Shared helpers for scripts/replication/render_*.py renders.

Kept as a plain module (no package structure) so the render scripts can
import it via a minimal sys.path manipulation:

    sys.path.insert(0, str(Path(__file__).parent))
    from _render_common import (...)

Nothing here is pipeline-critical; these are all visualization utilities
that read pipeline artifacts and produce cropped-panel overlays.
"""
from __future__ import annotations

import hashlib
import math
import ssl
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord, match_coordinates_sky
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.wcs import WCS
import astropy.units as u


# ---------------------------------------------------------------------------
# Small I/O helpers
# ---------------------------------------------------------------------------

def safe_read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV that may be truly empty (0 bytes, no header)."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def prefer_plate(tile_dir: Path, name: str) -> Path:
    """Return `<tile_dir>/catalogs/<name>_at_plate.csv` if present+nonempty,
    else `<tile_dir>/catalogs/<name>.csv` (may not exist)."""
    cat = tile_dir / "catalogs"
    p = cat / f"{name}_at_plate.csv"
    r = cat / f"{name}.csv"
    return p if (p.exists() and p.stat().st_size > 0) else r


def load_sex_with_wcsfix(tile_dir: Path) -> tuple[pd.DataFrame, tuple[str, str]]:
    """Load SExtractor pass2, preferring the WCSFIX-corrected variant.

    Returns (dataframe, (ra_col, dec_col)) where the column names match the
    convention used by the pipeline's veto chain:
      * RA_corr/Dec_corr if present (WCSFIX output)
      * ALPHA_J2000/DELTA_J2000 otherwise
    """
    cat = tile_dir / "catalogs"
    wcsfix = cat / "sextractor_pass2.wcsfix.csv"
    raw = cat / "sextractor_pass2.csv"
    path = wcsfix if wcsfix.exists() and wcsfix.stat().st_size > 0 else raw
    df = pd.read_csv(path)
    if "RA_corr" in df.columns and "Dec_corr" in df.columns:
        return df, ("RA_corr", "Dec_corr")
    if "ALPHA_J2000" in df.columns and "DELTA_J2000" in df.columns:
        return df, ("ALPHA_J2000", "DELTA_J2000")
    raise RuntimeError(f"no RA/Dec columns found in {path}")


# ---------------------------------------------------------------------------
# Cone match (for render scripts that need to reconstruct Gaia/PS1 rejects)
# ---------------------------------------------------------------------------

def cone_match_within(
    src_df: pd.DataFrame,
    src_ra_col: str,
    src_dec_col: str,
    cat_df: pd.DataFrame,
    cat_ra_col: str,
    cat_dec_col: str,
    max_arcsec: float = 5.0,
) -> pd.DataFrame:
    """Nearest-in-catalog match for each source row, kept if within max_arcsec.

    Returns a copy of src_df filtered to matched rows, with added columns:
      _sep_arcsec, _cat_ra, _cat_dec, plus any other cat_* columns present
      in cat_df (prefixed with cat_).
    """
    def _to_float(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return math.nan

    src_ra = np.asarray([_to_float(v) for v in src_df[src_ra_col]])
    src_dec = np.asarray([_to_float(v) for v in src_df[src_dec_col]])
    cat_ra = np.asarray([_to_float(v) for v in cat_df[cat_ra_col]])
    cat_dec = np.asarray([_to_float(v) for v in cat_df[cat_dec_col]])

    src_ok = np.isfinite(src_ra) & np.isfinite(src_dec)
    cat_ok = np.isfinite(cat_ra) & np.isfinite(cat_dec)

    if not src_ok.any() or not cat_ok.any():
        return src_df.iloc[:0].copy()

    src_coords = SkyCoord(src_ra[src_ok] * u.deg, src_dec[src_ok] * u.deg, frame="icrs")
    cat_coords = SkyCoord(cat_ra[cat_ok] * u.deg, cat_dec[cat_ok] * u.deg, frame="icrs")

    idx, sep2d, _ = match_coordinates_sky(src_coords, cat_coords)
    sep_arcsec = sep2d.to(u.arcsec).value

    within = sep_arcsec <= max_arcsec
    src_sub_idx = np.where(src_ok)[0][within]
    cat_sub_idx = np.where(cat_ok)[0][idx[within]]

    result = src_df.iloc[src_sub_idx].copy().reset_index(drop=True)
    result["_sep_arcsec"] = sep_arcsec[within]

    cat_sub = cat_df.iloc[cat_sub_idx].reset_index(drop=True)
    result["_cat_ra"] = cat_sub[cat_ra_col].astype(float).values
    result["_cat_dec"] = cat_sub[cat_dec_col].astype(float).values

    # Copy a few additional catalog columns if present, prefixed with cat_
    for col in ("Gmag", "BPmag", "RPmag", "pmRA", "pmDE", "rmag", "gmag",
                "B1mag", "R1mag", "B2mag", "R2mag", "USNO-B1.0", "Source"):
        if col in cat_sub.columns:
            result[f"cat_{col}"] = cat_sub[col].values

    return result


# ---------------------------------------------------------------------------
# Crop helpers
# ---------------------------------------------------------------------------

def crop_bbox(data: np.ndarray, sx_x_1idx: float, sx_y_1idx: float, half: int = 30):
    """Crop `half`-px around a SExtractor 1-indexed centroid.

    Returns (patch, (x1, y1)) where (x1, y1) is the crop top-left in 0-indexed
    image coordinates.
    """
    ny, nx = data.shape
    sy0, sx0 = int(round(sx_y_1idx)) - 1, int(round(sx_x_1idx)) - 1
    y1, y2 = max(0, sy0 - half), min(ny, sy0 + half + 1)
    x1, x2 = max(0, sx0 - half), min(nx, sx0 + half + 1)
    return data[y1:y2, x1:x2], (x1, y1)


# ---------------------------------------------------------------------------
# Catalog-source marker overlay
# ---------------------------------------------------------------------------

def load_catalog_pixels(tile_dir: Path, wcs: WCS, dedup_px: float = 2.0):
    """Return (px_x, px_y, is_propagated_bool) for every Gaia+PS1+USNO-B entry.

    Prefers `*_at_plate.csv` variants. `is_propagated=True` iff the source has
    a finite PM in the catalog file — i.e., `_propagate_catalog_epoch` would
    have applied a PM shift to its position. PS1 sources are always False
    (no PMs in the current fetch).

    Dedup is a 2-px grid. Propagated entries win ties.
    """
    cat = tile_dir / "catalogs"
    ras: list[float] = []
    decs: list[float] = []
    props: list[bool] = []

    def _sf(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return float("nan")

    def _add(df: pd.DataFrame, ra_col: str, dec_col: str,
             pmra_col: str | None, pmde_col: str | None) -> None:
        if not len(df) or ra_col not in df.columns or dec_col not in df.columns:
            return
        has_pm = (
            pmra_col is not None and pmde_col is not None
            and pmra_col in df.columns and pmde_col in df.columns
        )
        for _, r in df.iterrows():
            ra = _sf(r[ra_col]); dec = _sf(r[dec_col])
            if not (math.isfinite(ra) and math.isfinite(dec)):
                continue
            is_prop = False
            if has_pm:
                pmra = _sf(r[pmra_col]); pmde = _sf(r[pmde_col])
                if math.isfinite(pmra) and math.isfinite(pmde):
                    is_prop = True
            ras.append(ra); decs.append(dec); props.append(is_prop)

    _add(safe_read_csv(prefer_plate(tile_dir, "gaia_neighbourhood")),
         "ra", "dec", "pmRA", "pmDE")
    _add(safe_read_csv(cat / "ps1_neighbourhood.csv"),
         "ra", "dec", None, None)
    _add(safe_read_csv(prefer_plate(tile_dir, "usnob_neighbourhood")),
         "RAJ2000", "DEJ2000", "pmRA", "pmDE")

    if not ras:
        return np.array([]), np.array([]), np.array([], dtype=bool)

    sky = SkyCoord(np.asarray(ras) * u.deg, np.asarray(decs) * u.deg, frame="icrs")
    px_x, px_y = wcs.world_to_pixel(sky)
    px_x = np.asarray(px_x, dtype=float)
    px_y = np.asarray(px_y, dtype=float)
    is_prop = np.asarray(props, dtype=bool)

    finite = np.isfinite(px_x) & np.isfinite(px_y)
    px_x = px_x[finite]; px_y = px_y[finite]; is_prop = is_prop[finite]

    if dedup_px > 0 and len(px_x):
        order = np.argsort(~is_prop)  # propagated first
        px_x = px_x[order]; px_y = px_y[order]; is_prop = is_prop[order]
        kx = np.round(px_x / dedup_px).astype(np.int64)
        ky = np.round(px_y / dedup_px).astype(np.int64)
        seen: set[tuple[int, int]] = set()
        keep = np.zeros(len(px_x), dtype=bool)
        for i in range(len(px_x)):
            k = (int(kx[i]), int(ky[i]))
            if k not in seen:
                seen.add(k)
                keep[i] = True
        px_x = px_x[keep]; px_y = px_y[keep]; is_prop = is_prop[keep]

    return px_x, px_y, is_prop


def draw_catalog_crosses(ax, cat_px, x1, y1, crop_w, crop_h,
                         exclude: Iterable[tuple[float, float]] = (),
                         exclude_tol_px: float = 5.0) -> None:
    """Draw + markers for catalog sources inside the current crop.

    Green for PM-propagated sources, yellow for raw-epoch sources. `cat_px`
    is the 3-tuple returned by `load_catalog_pixels`.
    """
    px_all, py_all, is_prop_all = cat_px
    if not len(px_all):
        return
    in_bbox = (
        (px_all >= x1) & (px_all < x1 + crop_w)
        & (py_all >= y1) & (py_all < y1 + crop_h)
    )
    if not in_bbox.any():
        return
    px = px_all[in_bbox]
    py = py_all[in_bbox]
    is_prop = is_prop_all[in_bbox]
    if exclude:
        keep = np.ones(len(px), dtype=bool)
        tol2 = exclude_tol_px * exclude_tol_px
        for ex, ey in exclude:
            keep &= ((px - ex) ** 2 + (py - ey) ** 2) > tol2
        px = px[keep]; py = py[keep]; is_prop = is_prop[keep]
    if not len(px):
        return
    if is_prop.any():
        ax.scatter((px - x1)[is_prop], (py - y1)[is_prop],
                   marker="+", color="limegreen", s=80, linewidths=1.2, zorder=7)
    if (~is_prop).any():
        ax.scatter((px - x1)[~is_prop], (py - y1)[~is_prop],
                   marker="+", color="yellow", s=80, linewidths=1.2, zorder=7)


# ---------------------------------------------------------------------------
# Modern-image pair helpers (POSS-I vs PanSTARRS side-by-side)
# ---------------------------------------------------------------------------

DEFAULT_FOV_ARCMIN = 1.5              # 90"-square panels
DEFAULT_MODERN_HIPS = "CDS/P/PanSTARRS/DR1/r"
DEFAULT_MODERN_PX = 300


def _modern_cache_dir() -> Path:
    """Repo-global disk cache for hips2fits cutouts (SHA256(url) keyed)."""
    repo = Path(__file__).resolve().parents[2]
    d = repo / ".cache" / "modern_cutouts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def fetch_modern_cutout(
    ra_deg: float,
    dec_deg: float,
    fov_arcmin: float = DEFAULT_FOV_ARCMIN,
    width: int = DEFAULT_MODERN_PX,
    height: int = DEFAULT_MODERN_PX,
    hips: str | None = None,
    timeout: float = 60.0,
    force: bool = False,
):
    """Fetch a FITS cutout from CDS hips2fits at (ra, dec).

    For dec < -30 (outside PanSTARRS footprint), auto-fall-back to SkyMapper R
    unless the caller overrides `hips` explicitly.

    Cached on disk under {repo}/.cache/modern_cutouts/ keyed by SHA256(url).
    Returns (data, wcs) on success or (None, None) on any failure.
    """
    import certifi  # heavy import kept local

    if hips is None:
        hips = "CDS/P/skymapper-R" if dec_deg < -30.0 else DEFAULT_MODERN_HIPS

    url = (
        "https://alasky.cds.unistra.fr/hips-image-services/hips2fits"
        "?hips=" + urllib.parse.quote(hips)
        + f"&ra={ra_deg:.8f}&dec={dec_deg:.8f}"
        + f"&fov={fov_arcmin / 60.0:.8f}"
        + f"&width={width}&height={height}"
        + "&projection=TAN&format=fits"
    )
    key = hashlib.sha256(url.encode("utf-8")).hexdigest()
    cache = _modern_cache_dir() / f"{key}.fits"

    if cache.exists() and cache.stat().st_size > 0 and not force:
        try:
            with fits.open(cache) as hdul:
                return hdul[0].data.astype(float), WCS(hdul[0].header)
        except Exception:
            cache.unlink(missing_ok=True)

    try:
        ctx = ssl.create_default_context(cafile=certifi.where())
        with urllib.request.urlopen(url, context=ctx, timeout=timeout) as r:
            cache.write_bytes(r.read())
        with fits.open(cache) as hdul:
            return hdul[0].data.astype(float), WCS(hdul[0].header)
    except Exception as e:
        print(f"  [modern] fetch failed at ({ra_deg:.4f}, {dec_deg:.4f}): {e}")
        if cache.exists():
            cache.unlink(missing_ok=True)
        return None, None


def load_poss_cutout_at_sky(
    poss_data: np.ndarray,
    poss_wcs: WCS,
    ra_deg: float,
    dec_deg: float,
    fov_arcmin: float = DEFAULT_FOV_ARCMIN,
):
    """Angular-size-exact POSS-I cutout via astropy Cutout2D.

    The returned patch has EXACTLY `fov_arcmin` on each side regardless of the
    plate's pixel scale — so both panels of a POSS/modern pair cover the same
    physical sky area. Returns (data, wcs) or (None, None) on failure.
    """
    try:
        cut = Cutout2D(
            poss_data,
            position=SkyCoord(ra_deg * u.deg, dec_deg * u.deg, frame="icrs"),
            size=(fov_arcmin * u.arcmin, fov_arcmin * u.arcmin),
            wcs=poss_wcs,
            mode="trim",
        )
        return cut.data, cut.wcs
    except Exception as e:
        print(f"  [poss] cutout failed at ({ra_deg:.4f}, {dec_deg:.4f}): {e}")
        return None, None


def load_sky_markers(tile_dir: Path) -> list[tuple[float, float, bool]]:
    """Load all Gaia+PS1+USNO-B positions as (ra_deg, dec_deg, is_propagated).

    Sky-space analogue of `load_catalog_pixels` — deduped on a ~0.5" grid,
    propagated entries winning ties. Used by `draw_pair` to overlay + markers
    on both panels of a POSS/modern comparison at their true sky positions.
    """
    cat = tile_dir / "catalogs"
    rows: list[tuple[float, float, bool]] = []

    def _sf(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return float("nan")

    def _add(df: pd.DataFrame, ra_col: str, dec_col: str,
             pmra_col: str | None, pmde_col: str | None) -> None:
        if not len(df) or ra_col not in df.columns or dec_col not in df.columns:
            return
        has_pm = (
            pmra_col is not None and pmde_col is not None
            and pmra_col in df.columns and pmde_col in df.columns
        )
        for _, r in df.iterrows():
            ra = _sf(r[ra_col]); dec = _sf(r[dec_col])
            if not (math.isfinite(ra) and math.isfinite(dec)):
                continue
            is_prop = False
            if has_pm:
                pmra = _sf(r[pmra_col]); pmde = _sf(r[pmde_col])
                if math.isfinite(pmra) and math.isfinite(pmde):
                    is_prop = True
            rows.append((ra, dec, is_prop))

    _add(safe_read_csv(prefer_plate(tile_dir, "gaia_neighbourhood")),
         "ra", "dec", "pmRA", "pmDE")
    _add(safe_read_csv(cat / "ps1_neighbourhood.csv"),
         "ra", "dec", None, None)
    _add(safe_read_csv(prefer_plate(tile_dir, "usnob_neighbourhood")),
         "RAJ2000", "DEJ2000", "pmRA", "pmDE")

    # Dedup on a ~0.5" sky grid; propagated entries win ties.
    rows.sort(key=lambda t: not t[2])  # propagated first
    seen: set[tuple[int, int]] = set()
    out: list[tuple[float, float, bool]] = []
    for ra, dec, is_prop in rows:
        kx = int(round(ra * 3600 / 0.5))
        ky = int(round(dec * 3600 / 0.5))
        k = (kx, ky)
        if k in seen:
            continue
        seen.add(k)
        out.append((ra, dec, is_prop))
    return out


def draw_pair(
    ax_left,
    ax_right,
    poss_data,
    poss_wcs,
    modern_data,
    modern_wcs,
    ra_deg: float,
    dec_deg: float,
    fov_arcmin: float = DEFAULT_FOV_ARCMIN,
    sky_markers: Sequence[tuple[float, float, bool]] = (),
    exclude_sky: Sequence[tuple[float, float]] = (),
    exclude_tol_arcsec: float = 1.0,
    border_color: str = "red",
):
    """Render a POSS-I + modern pair on a common arcsec grid (east LEFT, N up).

    Both panels share a `(Δα·cos δ, Δδ)` arcsec coordinate system centred on
    (ra_deg, dec_deg). The axes are drawn flush (caller sets `wspace=0`).
    Inner spines are hidden; the four outer spines are coloured with
    `border_color`. Sky markers are drawn as + symbols (green = PM-propagated,
    yellow = raw-epoch) on both panels at their true sky positions, with
    anything within `exclude_tol_arcsec` of any `exclude_sky` entry skipped.

    Returns `to_offset(ra, dec) -> (dx_arcsec, dy_arcsec)` so the caller can
    place candidate-specific overlays (circles, match markers, bright-star
    glyphs, connecting lines, etc.) on either axis using the same grid.
    """
    from astropy.visualization import (
        ImageNormalize, LinearStretch, ZScaleInterval,
    )

    half = fov_arcmin * 60.0 / 2.0
    cos_dec = math.cos(math.radians(dec_deg))

    def to_offset(ra: float, dec: float) -> tuple[float, float]:
        dx = (ra - ra_deg) * cos_dec * 3600.0
        dy = (dec - dec_deg) * 3600.0
        return dx, dy

    def _image_extent(data, wcs):
        ny, nx = data.shape
        corners_px = [(0, 0), (nx - 1, 0), (0, ny - 1), (nx - 1, ny - 1)]
        xs: list[float] = []
        ys: list[float] = []
        for cx, cy in corners_px:
            sc = wcs.pixel_to_world(cx, cy)
            dx, dy = to_offset(sc.ra.deg, sc.dec.deg)
            xs.append(dx); ys.append(dy)
        return min(xs), max(xs), min(ys), max(ys)

    def _paint(ax, data, wcs, label: str):
        if data is None or wcs is None or data.size == 0 or not np.isfinite(data).any():
            ax.set_facecolor("black")
            ax.text(0.5, 0.5, f"no {label}\ncutout",
                    ha="center", va="center", color="white",
                    fontsize=9, transform=ax.transAxes, zorder=10)
        else:
            xmin, xmax, ymin, ymax = _image_extent(data, wcs)
            finite = data[np.isfinite(data)]
            if finite.size > 0:
                norm = ImageNormalize(finite, interval=ZScaleInterval(), stretch=LinearStretch())
            else:
                norm = None
            # extent: xmax first → east pixel column lands on screen LEFT.
            ax.imshow(data, cmap="gray_r", norm=norm, origin="lower",
                      extent=(xmax, xmin, ymin, ymax))
        ax.set_xlim(half, -half)   # east left
        ax.set_ylim(-half, half)
        ax.set_aspect("equal")

    _paint(ax_left, poss_data, poss_wcs, "POSS-I")
    _paint(ax_right, modern_data, modern_wcs, "modern")

    # Catalog + markers on both panels
    if sky_markers:
        exclude_off = [to_offset(r, d) for r, d in exclude_sky]

        def _keep(dx: float, dy: float) -> bool:
            for ex, ey in exclude_off:
                if (dx - ex) ** 2 + (dy - ey) ** 2 < exclude_tol_arcsec ** 2:
                    return False
            return True

        prop_x: list[float] = []; prop_y: list[float] = []
        raw_x: list[float] = []; raw_y: list[float] = []
        for ra, dec, is_prop in sky_markers:
            dx, dy = to_offset(ra, dec)
            if abs(dx) > half or abs(dy) > half:
                continue
            if not _keep(dx, dy):
                continue
            if is_prop:
                prop_x.append(dx); prop_y.append(dy)
            else:
                raw_x.append(dx); raw_y.append(dy)

        for ax in (ax_left, ax_right):
            if prop_x:
                ax.scatter(prop_x, prop_y, marker="+", color="limegreen",
                           s=90, linewidths=1.3, zorder=7)
            if raw_x:
                ax.scatter(raw_x, raw_y, marker="+", color="yellow",
                           s=90, linewidths=1.3, zorder=7)

    # Styling: no ticks, hide INNER spines, colour OUTER spines.
    for ax in (ax_left, ax_right):
        ax.set_xticks([]); ax.set_yticks([])
        for s in ax.spines.values():
            s.set_edgecolor(border_color)
            s.set_linewidth(2)
    ax_left.spines["right"].set_visible(False)
    ax_right.spines["left"].set_visible(False)

    return to_offset
