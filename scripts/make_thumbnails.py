#!/usr/bin/env python3
"""Create 1/8-scale JPG thumbnails of POSS-I plate FITS files."""
import sys
import os
import time
import numpy as np
from pathlib import Path
from astropy.io import fits
from PIL import Image

def make_thumbnail(fits_path, out_path, scale=8):
    """Read FITS, downsample by `scale`, linear stretch, save as JPG."""
    with fits.open(str(fits_path), memmap=True) as hdul:
        data = hdul[0].data
    if data is None:
        return False

    # Downsample by taking every Nth pixel (fast, no anti-alias needed for thumbnails)
    thumb = data[::scale, ::scale].astype(np.float32)

    # Linear stretch: 1st to 99.5th percentile → 0-255
    # POSS plates are negative images (bright stars = low pixel values)
    lo = np.nanpercentile(thumb, 0.5)
    hi = np.nanpercentile(thumb, 99.5)
    if hi <= lo:
        hi = lo + 1
    thumb = (thumb - lo) / (hi - lo)
    thumb = np.clip(thumb, 0, 1)
    # Invert so stars are bright (white on black) for the sky map
    thumb = 1.0 - thumb
    thumb = (thumb * 255).astype(np.uint8)

    img = Image.fromarray(thumb, mode='L')
    img.save(str(out_path), 'JPEG', quality=85)
    return True

def process_batch(src_dir, out_dir, pattern, label):
    src = Path(src_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    fits_files = sorted(src.glob(pattern))
    total = len(fits_files)
    print(f'[{label}] {total} plates to process → {out}')

    done = 0
    skipped = 0
    failed = 0
    t0 = time.time()

    for i, fp in enumerate(fits_files):
        stem = fp.stem  # e.g. dss1red_XE002
        jpg = out / f'{stem}.jpg'
        if jpg.exists():
            skipped += 1
            continue
        try:
            make_thumbnail(fp, jpg)
            done += 1
        except Exception as e:
            print(f'  FAIL {fp.name}: {e}')
            failed += 1

        if (done + failed) % 50 == 0:
            elapsed = time.time() - t0
            rate = (done + failed) / elapsed if elapsed > 0 else 0
            remaining = (total - i - 1) / rate if rate > 0 else 0
            print(f'  [{label}] {i+1}/{total}  done={done} skip={skipped} fail={failed}  '
                  f'{rate:.1f} plates/s  ETA {remaining/60:.0f}m')

    elapsed = time.time() - t0
    print(f'[{label}] DONE: {done} created, {skipped} skipped, {failed} failed in {elapsed:.0f}s')

if __name__ == '__main__':
    base = '/Volumes/SANDISK/poss_1_raw'
    band = sys.argv[1] if len(sys.argv) > 1 else 'red'

    if band == 'red':
        process_batch(f'{base}/poss_red_raw', f'{base}/poss_red_thumb',
                      'dss1red_XE*.fits', 'RED')
    elif band == 'blue':
        process_batch(f'{base}/poss_blue_raw', f'{base}/poss_blue_thumb',
                      'dss1blue_XO*.fits', 'BLUE')
    else:
        print(f'Unknown band: {band}. Use "red" or "blue".')
        sys.exit(1)
