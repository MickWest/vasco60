#!/usr/bin/env python3
"""Render an 8K all-sky map with POSS-I plate thumbnails as background,
plate boundaries in red, and tile plan positions in yellow.

Output: work/sky_map_8k.jpg
"""
import csv
import math
import os
import sys
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib_cache")

import numpy as np
from PIL import Image, ImageDraw

# --- Config ---
WIDTH = 7680
HEIGHT = 4320
SANDISK = Path("/Volumes/SANDISK/poss_1_raw")
RED_THUMB_DIR = SANDISK / "poss_red_thumb"
BLUE_THUMB_DIR = SANDISK / "poss_blue_thumb"
RED_HEADERS = SANDISK / "poss_red_headers.csv"
BLUE_HEADERS = SANDISK / "poss_blue_headers.csv"
TILE_PLAN = Path("plans/tiles_poss1e_ps1.csv")
OUT_PATH = Path("work/sky_map_8k.jpg")

# Plate Carrée projection: RA 360→0 left-to-right (astronomical convention), Dec -40→+90 bottom-to-top
DEC_MIN = -40.0
DEC_MAX = 90.0
RA_MIN = 0.0
RA_MAX = 360.0

def ra_to_x(ra):
    """RA in degrees to pixel x. RA increases right-to-left (flipped)."""
    return int((360.0 - (ra % 360.0)) / 360.0 * WIDTH) % WIDTH

def dec_to_y(dec):
    """Dec in degrees to pixel y. Dec increases bottom-to-top."""
    return int((DEC_MAX - dec) / (DEC_MAX - DEC_MIN) * HEIGHT)

def load_plates(headers_csv, thumb_dir, prefix):
    """Load plate metadata and thumbnail paths."""
    plates = []
    with open(headers_csv, newline='') as f:
        for row in csv.DictReader(f):
            region = row.get('REGION', '').strip()
            ra_s = row.get('PLATERA', '').strip()
            dec_s = row.get('PLATEDEC', '').strip()
            if not (region and ra_s and dec_s):
                continue
            try:
                ra = float(ra_s)
                dec = float(dec_s)
                corners = []
                for i in range(1, 5):
                    cra = float(row[f'WCS_CORNER_RA_{i}'])
                    cdec = float(row[f'WCS_CORNER_DEC_{i}'])
                    corners.append((cra, cdec))
            except (KeyError, ValueError):
                continue
            thumb = thumb_dir / f"{prefix}{region}.jpg"
            plates.append({
                'region': region, 'ra': ra, 'dec': dec,
                'corners': corners, 'thumb': thumb,
            })
    return plates

def load_tiles(tile_plan_csv):
    """Load tile centers from the tile plan."""
    tiles = []
    with open(tile_plan_csv, newline='') as f:
        rdr = csv.reader(f)
        header = next(rdr)
        for row in rdr:
            if len(row) < 5:
                continue
            try:
                ra = float(row[3])
                dec = float(row[4])
                tiles.append((ra, dec))
            except ValueError:
                continue
    return tiles

def paste_plate_thumbnail(canvas, plate):
    """Paste a plate thumbnail onto the canvas at its sky position."""
    if not plate['thumb'].exists():
        return
    corners = plate['corners']
    decs = [c[1] for c in corners]
    dec_lo = min(decs)
    dec_hi = max(decs)

    # RA handling: deal with wrap-around near RA=0/360
    ras = [c[0] for c in corners]
    # Check for wrap: if spread > 180°, some corners crossed 0/360
    ra_spread = max(ras) - min(ras)
    if ra_spread > 180:
        # Shift to [0, 360) → [-180, 180) for span calculation
        ras_shifted = [(r - 360 if r > 180 else r) for r in ras]
        ra_lo = min(ras_shifted)
        ra_hi = max(ras_shifted)
    else:
        ra_lo = min(ras)
        ra_hi = max(ras)

    # Pixel bounds on canvas
    # RA is flipped: higher RA → lower x
    x_right = ra_to_x(ra_lo)
    x_left = ra_to_x(ra_hi)
    y_top = dec_to_y(dec_hi)
    y_bot = dec_to_y(dec_lo)

    pw = x_right - x_left
    ph = y_bot - y_top

    # Handle wrap-around
    if pw <= 0:
        pw += WIDTH
    if ph <= 0 or pw <= 0:
        return
    if pw > WIDTH // 2 or ph > HEIGHT // 2:
        return  # sanity cap

    try:
        thumb = Image.open(plate['thumb']).convert('L')
        thumb = thumb.resize((pw, ph), Image.LANCZOS)
        # Invert so stars are white on black background
        arr = 255 - np.array(thumb, dtype=np.uint8)
        rgba = np.zeros((ph, pw, 4), dtype=np.uint8)
        rgba[:, :, 0] = arr  # R
        rgba[:, :, 1] = arr  # G
        rgba[:, :, 2] = arr  # B
        rgba[:, :, 3] = 200  # alpha
        thumb_rgba = Image.fromarray(rgba, 'RGBA')

        # Handle RA wrap
        if x_left >= 0 and x_left + pw <= WIDTH:
            canvas.alpha_composite(thumb_rgba, (x_left, y_top))
        else:
            # Wraps around — paste in two parts
            part1_w = WIDTH - x_left
            if part1_w > 0 and part1_w < pw:
                canvas.alpha_composite(thumb_rgba.crop((0, 0, part1_w, ph)), (x_left, y_top))
                canvas.alpha_composite(thumb_rgba.crop((part1_w, 0, pw, ph)), (0, y_top))
            else:
                xl = x_left % WIDTH
                canvas.alpha_composite(thumb_rgba, (xl, y_top))
    except Exception as e:
        pass  # skip broken thumbnails

def draw_plate_boundary(draw, plate, color=(255, 60, 60, 180)):
    """Draw plate boundary as polygon."""
    corners = plate['corners']
    # Convert corners to pixel coords
    pts = []
    for ra, dec in corners:
        pts.append((ra_to_x(ra), dec_to_y(dec)))
    # Close polygon
    pts.append(pts[0])

    # Check for RA wrap (points jumping across the image)
    xs = [p[0] for p in pts]
    if max(xs) - min(xs) > WIDTH // 2:
        return  # skip wrap-around plates for boundary drawing (they'll look wrong)

    draw.line(pts, fill=color, width=2)

def draw_tile_marker(draw, ra, dec, color=(255, 255, 0, 200)):
    """Draw a small square for a tile."""
    x = ra_to_x(ra)
    y = dec_to_y(dec)
    # 1° tile = about 21 pixels, but draw smaller markers
    half = max(1, int(1.0 / 360.0 * WIDTH / 2))  # half-tile in pixels
    draw.rectangle([x - half, y - half, x + half, y + half],
                   outline=color, width=1)

def main():
    print(f"Canvas: {WIDTH}x{HEIGHT}")
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Dark background
    canvas = Image.new('RGBA', (WIDTH, HEIGHT), (10, 10, 20, 255))

    # Load data
    print("Loading plate headers...")
    red_plates = load_plates(RED_HEADERS, RED_THUMB_DIR, "dss1red_")
    print(f"  Red plates: {len(red_plates)}")

    blue_plates = []
    if BLUE_HEADERS.exists():
        blue_plates = load_plates(BLUE_HEADERS, BLUE_THUMB_DIR, "dss1blue_")
        print(f"  Blue plates: {len(blue_plates)}")

    print("Loading tile plan...")
    tiles = load_tiles(TILE_PLAN)
    print(f"  Tiles: {len(tiles)}")

    # Paste plate thumbnails as background
    # Blue first (under), then red on top
    all_plates = blue_plates + red_plates
    print(f"Pasting {len(all_plates)} plate thumbnails...")
    for i, plate in enumerate(all_plates):
        paste_plate_thumbnail(canvas, plate)
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(all_plates)}")

    # Draw overlay on top
    overlay = Image.new('RGBA', (WIDTH, HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    # Plate boundaries in red
    print("Drawing plate boundaries...")
    for plate in red_plates:
        draw_plate_boundary(draw, plate)

    # Tile markers in yellow
    print(f"Drawing {len(tiles)} tile markers...")
    for ra, dec in tiles:
        draw_tile_marker(draw, ra, dec)

    # Add RA/Dec grid lines
    print("Drawing grid...")
    grid_color = (80, 80, 80, 120)
    for ra_line in range(0, 360, 30):
        x = ra_to_x(ra_line)
        draw.line([(x, 0), (x, HEIGHT)], fill=grid_color, width=1)
    for dec_line in range(-30, 91, 15):
        y = dec_to_y(dec_line)
        draw.line([(0, y), (WIDTH, y)], fill=grid_color, width=1)

    canvas = Image.alpha_composite(canvas, overlay)

    # Convert to RGB for JPG
    final = canvas.convert('RGB')
    final.save(str(OUT_PATH), 'JPEG', quality=95)
    print(f"Saved: {OUT_PATH} ({os.path.getsize(OUT_PATH) / 1e6:.1f} MB)")

if __name__ == '__main__':
    main()
