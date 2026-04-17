# fast_tile_v2 — optimized per-tile pipeline

A single-process implementation of the VASCO60 core pipeline (Steps 2–5) designed
for high throughput and parameter-sweep workflows. Preserves all locked
invariants from `context/02_DECISIONS.md`.

## What it does

Per tile:
1. SExtractor pass1 (no PSF)
2. Load Gaia/PS1/USNO-B from local HEALPix-5 parquet caches (no network)
3. Epoch-propagate catalog positions to plate epoch
4. In-memory WCSFIX polynomial (astropy-only; no STILTS)
5. 5″ merged-catalog veto
6. PSFEx on pass1.ldac
7. Stamp-based pass2 on veto survivors (with optional threading)
8. Late filters (FLAGS/SNR/SPREAD_MODEL/FWHM/ELONGATION + MNRAS spike cuts)
9. Write `candidates_raw.parquet` + `survivors.parquet` per tile

No intermediate CSVs. Two-phase design: pass1→pass2 produces
`candidates_raw.parquet`; late filters run off that for cheap param sweeps.

## Single-tile CLI

```bash
python tools/fast_tile_v2.py --workdir data/tiles/tile_RA.../
```

Options:
- `--params FILE` — JSON with parameter overrides (see `DEFAULT_PARAMS`)
- `--reuse-candidates` — skip Phase A if `candidates_raw.parquet` exists
- `--cleanup` — delete `pass1.ldac/pass1.psf` after run (saves ~32MB/tile)
- `--quiet` — suppress per-phase timing prints

Example params file (SNR gate sweep):
```json
{"snr_win_min": 50.0, "stamp_threads": 4}
```

## Batch CLI

```bash
python tools/run_fast_batch.py \
    --tiles-list tiles.txt \
    --workers 4 \
    --params params.json
```

Options:
- `--tiles-list FILE` | `--tiles-glob PATTERN` | `--plan CSV` — tile source
- `--workers N` — process pool size
- `--reuse-candidates` — param-sweep mode (~1000× faster)
- `--cleanup` — disk-saving mode
- `--out-dir PATH` — aggregated output location (default: `work/fast_batch/<ts>/`)

Outputs:
- `batch_summary.json` — per-tile summaries + aggregate stats
- `survivors_all.parquet` — concatenated survivors across all tiles

## Performance (docker sandbox: 8 cores / 8GB)

Single tile (2200 pass1 detections, 100 veto-survivors):
- Cold (with HP5 index):   **6.2s** (1 thread) / **3.5s** (8 threads)
- Reuse-candidates:        **0.08s**

Batch (14 tiles):
- Cold, 4 workers × 2 threads:  11.7 tiles/min
- Reuse, 6 workers × 1 thread:  **1330 tiles/min**

Batch (50 tiles):
- Cold, 4 workers × 2 threads:  10.0 tiles/min (mean 23.6s/tile, dominated by PSFEx on dense galactic fields)

## Tuning: workers × threads

`stamp_threads` in params controls within-tile stamp-pass2 parallelism. Pick a
config where `workers × stamp_threads ≈ N_cores`:

| cores | workers | stamp_threads | notes |
|---|---|---|---|
| 8  | 4 | 2 | balanced (docker sandbox) |
| 8  | 6 | 1 | works, slightly better on cold outliers |
| 14 | 7 | 2 | recommended for native M4 Pro |
| 14 | 14 | 1 | max workers, no within-tile threading |

## HP5 index cache

Pyarrow hive discovery is slow on remote/USB-mounted caches. `fast_cache_query`
pre-builds a HP5→file JSON index at `.cache/hp_index/<cache-name>.json`.
First run of `run_fast_batch.py` auto-builds if missing (~20s one-time).
Override location with `VASCO_HP_INDEX_DIR`.

## Parity

Verified equivalence vs `tools/fast_tile.py` (and therefore `cli_pipeline.py`) by
SExtractor NUMBER on a 14-tile sample: **107 / 107 survivors match exactly**
across both pipelines at default parameters.

## Files

- `tools/fast_tile_v2.py` — per-tile driver + `process_tile()` function
- `tools/run_fast_batch.py` — ProcessPoolExecutor batch runner
- `vasco/fast_cache_query.py` — fast HP5 parquet queries (bypasses pyarrow hive)

