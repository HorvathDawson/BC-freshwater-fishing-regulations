#!/usr/bin/env python3
"""
(T886, owner 2026-11-18) THE "HATCHERY OR WILD?" SHEET'S TWO DRAWINGS, on the loading screen's coho.

    .venv/bin/python -I app/tools/fish-loader/adipose_art.py --ink "#hex" --red "#hex" --ring "#hex" \
        --width 1200 --wild OUT.webp --clipped OUT.webp

The owner, on the old drawings (a rainbow trout whose mouth curved up): "the how to tell hatchery fish is very smile
creepy we want to make it more realistic I think. Better shape more natural smile. Could use the salmon general shape
from loading maybe?" So the fish IS the loader's: `fish_loader.build()` (the owner's drawing, beside this file) at rest
(no tail beat, no bob or pitch, no water marks), drawn the same way (an ink block with carved marks over the red block),
with a plain face: the open kype and its teeth closed into one straight mouth line, the jaw tab, gill swirl, plate,
brow and cheek lines replaced by a plain eye and one gill arc. Two drawings, 2:1, the fish along the bottom:

  wild     a clear adipose fin (fleshy, no rays) on the back between the dorsal fin and the tail, its base carved
  clipped  no fin: the back runs on bare, a short red seam (the healed scar) just under it

On both the spot is ringed and drawn again 2.5x in a close-up circle over the back, joined to the ring, so the
difference reads at phone size. The ring and the close-up's rim are the third colour. All three colours are the theme's
tokens, handed in by `tools/build-adipose.mjs` (`color.ui.coho.ink`, `.red`, `color.ui.accent`); nothing is typed here.
Transparent ground, lossless WebP: the sheet's card shows through, as the loader's ground does.
"""
import argparse
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # -I leaves the script's folder off the path

from PIL import Image, ImageColor, ImageDraw  # noqa: E402

from fish_loader import build, densify, roughen, shift, stroke  # noqa: E402

SS = 4                                   # supersampling, as the loader
# the view: the loader's design space, 2:1 — the fish along the bottom, the close-up circle over its back on the right
VX0, VX1 = 6.0, 774.0
VW = VX1 - VX0
VH = VW / 2
VY0 = 420.0 - VH                         # the tail fin's lowest point (412) just above the bottom edge
ASPECT = (2, 1)
# the adipose fin: the loader's is a 38 x 15 sliver, 7 px tall on a phone; drawn here as a bigger fleshy fin with no rays,
# on the back between the dorsal fin and the tail (fish_loader.top_y: the back runs 259 -> 266 over x 498 -> 522)
FIN = [(480, 266), (488, 254), (497, 247), (508, 242.5), (519, 242), (528, 246), (533, 253), (536, 262), (536, 272), (480, 270)]
FIN_BASE = [(484, 263.5), (510, 263.0), (535, 267.0)]
SPOT = (509.0, 258.0)                    # the fin / the scar, the back line through the middle
RING_R, LINE_W = 34.0, 4.2
# the close-up: SPOT's neighbourhood (radius ZOOM_R) at ZOOM x, in a circle centred at INSET
ZOOM, ZOOM_R = 2.5, 30.0
INSET = (612.0, 126.0)
INSET_R = ZOOM * ZOOM_R


def is_adipose(poly):
    """the rest pose's adipose fin among the ink blocks: every point within a few px of the fin's own box"""
    xs, ys = [p[0] for p in poly], [p[1] for p in poly]
    return min(xs) > 484 and max(xs) < 534 and min(ys) > 246 and max(ys) < 272


def is_tooth(poly):
    """a tooth: one of the three small triangles fish_loader.build() puts in the mouth's gap"""
    return len(poly) == 3 and max(p[0] for p in poly) < 120 and min(p[1] for p in poly) > 310


def is_face_mark(poly):
    """a carved mark of the loader's head (the jaw tab, the gill swirl and plate, the brow, the cheek lines, a pectoral
    base mark): anything carved there bigger than a spot. A still picture read them as a cartoon face."""
    xs, ys = [p[0] for p in poly], [p[1] for p in poly]
    big = max(xs) - min(xs) > 10 or max(ys) - min(ys) > 10
    return big and min(xs) > 125 and max(xs) < 250 and min(ys) > 270 and max(ys) < 385


# the loader's open kype (a spawning male's hooked jaw, three teeth in the gap) read as a grin: closed, one straight line
MOUTH_GAP = [(57, 328), (63, 330), (100, 335), (146, 338), (152, 332), (110, 326), (70, 321), (57, 322)]
MOUTH_LINE = [(56, 326.5), (148, 335.5)]
# one plain gill arc behind the head
GILL = [(150 + 62 * math.cos(a), 316 + 60 * math.sin(a)) for a in [math.radians(-58 + 116 * i / 20) for i in range(21)]]
# the healed scar where the fin was: the back runs on unbroken, a short red seam just under it
SCAR = [(495, 263.5), (503, 262.0), (512, 262.6), (521, 265.0), (521, 268.4), (512, 266.4), (503, 266.0), (495, 267.0)]


def circle(c, r, n=96):
    return [(c[0] + r * math.cos(a), c[1] + r * math.sin(a)) for a in [2 * math.pi * i / n for i in range(n + 1)]]


def fish_layers(clipped):
    """the fish's ink and red blocks, as polygons in design space (the carves to cut from the ink)"""
    ink, carve, accent, eye, chips = build(0.0, 0.0)
    fins = [p for p in ink if is_adipose(p)]
    teeth = [p for p in ink if is_tooth(p)]
    if len(fins) != 1 or len(teeth) != 3:
        raise SystemExit(f"adipose_art: fish_loader.build() drew {len(fins)} adipose fins and {len(teeth)} teeth, wanted 1 and 3")
    ink = [p for p in ink if not is_adipose(p) and not is_tooth(p)] + [MOUTH_GAP]
    if not clipped:
        ink.append(roughen(densify(FIN, 3), 0.5, 5))
        carve.append(stroke(densify(FIN_BASE, 4, False), 3.0, "lens", 59))   # its base, carved as the dorsal's
    marks = [p for p in carve if is_face_mark(p)]
    if len(marks) < 5:
        raise SystemExit(f"adipose_art: only {len(marks)} face marks found in fish_loader.build(); the head changed")
    carve = [p for p in carve if not is_face_mark(p)]
    carve += [stroke(densify(MOUTH_LINE, 4, False), 3.2, "even", 57), stroke(GILL, 4.6, "lens", 58)]
    red = [shift(p, 3.5, 3.0) for p in accent]                    # the loader's red block, its registration offset kept
    if clipped:
        red.append(SCAR)
    return ink, carve, red, eye, chips


def draw(clipped, ink_hex, red_hex, ring_hex, width):
    W = width * SS
    H = width * ASPECT[1] // ASPECT[0] * SS
    k = W / VW

    def tf(pts):
        return [((x - VX0) * k, (y - VY0) * k) for x, y in pts]

    ink, carve, red, eye, chips = fish_layers(clipped)
    rmask = Image.new("L", (W, H), 0)
    for poly in red:
        ImageDraw.Draw(rmask).polygon(tf(poly), fill=255)
    imask = Image.new("L", (W, H), 0)
    d = ImageDraw.Draw(imask)
    for poly in ink:
        d.polygon(tf(poly), fill=255)
    for poly in carve:
        d.polygon(tf(poly), fill=0)
    for poly in chips:
        d.polygon(tf(poly), fill=255)
    (ex, ey), = tf([eye])
    for r, v in ((10.5, 0), (6.0, 255)):                          # a plain eye: a carved ring round an inked pupil
        d.ellipse([ex - r * k, ey - r * k, ex + r * k, ey + r * k], fill=v)
    if clipped:
        d.polygon(tf(SCAR), fill=0)                               # the scar shows red through the ink

    fish = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    fish.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(red_hex) + (255,)), (0, 0), rmask)
    fish.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(ink_hex) + (255,)), (0, 0), imask)

    # the close-up: SPOT's neighbourhood, ZOOM x, clipped to a circle
    (sx, sy), = tf([SPOT])
    zr = ZOOM_R * k
    crop = fish.crop((round(sx - zr), round(sy - zr), round(sx + zr), round(sy + zr)))
    side = round(2 * zr * ZOOM)
    crop = crop.resize((side, side), Image.LANCZOS)
    cmask = Image.new("L", (side, side), 0)
    ImageDraw.Draw(cmask).ellipse([0, 0, side - 1, side - 1], fill=255)
    (ix, iy), = tf([INSET])
    out = fish.copy()
    clear = Image.new("RGBA", (side, side), (0, 0, 0, 0))
    out.paste(clear, (round(ix - side / 2), round(iy - side / 2)), cmask)
    out.paste(crop, (round(ix - side / 2), round(iy - side / 2)), Image.composite(crop, clear, cmask).getchannel("A"))

    # the ring on the fish, the close-up's rim, and the two lines joining them (tangents of the two circles)
    ring = Image.new("L", (W, H), 0)
    rg = ImageDraw.Draw(ring)
    for c, r in ((SPOT, RING_R), (INSET, INSET_R)):
        rg.polygon(tf(stroke(circle(c, r), LINE_W, "even", 0)), fill=255)
    dx, dy = INSET[0] - SPOT[0], INSET[1] - SPOT[1]
    dist = math.hypot(dx, dy)
    base = math.atan2(dy, dx)
    off = math.acos((RING_R - INSET_R) / dist)                    # the outer tangents: n . (c2 - c1) = r1 - r2
    for sgn in (1, -1):
        a = base + sgn * off
        p0 = (SPOT[0] + RING_R * math.cos(a), SPOT[1] + RING_R * math.sin(a))
        p1 = (INSET[0] + INSET_R * math.cos(a), INSET[1] + INSET_R * math.sin(a))
        rg.polygon(tf(stroke(densify([p0, p1], 4, False), LINE_W * 0.7, "even", 1)), fill=255)
    out.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(ring_hex) + (255,)), (0, 0), ring)
    return out.resize((W // SS, H // SS), Image.LANCZOS)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ink", required=True)
    ap.add_argument("--red", required=True)
    ap.add_argument("--ring", required=True)
    ap.add_argument("--width", type=int, required=True)
    ap.add_argument("--wild", required=True)
    ap.add_argument("--clipped", required=True)
    a = ap.parse_args()
    for out, clipped in ((a.wild, False), (a.clipped, True)):
        img = draw(clipped, a.ink, a.red, a.ring, a.width)
        img.save(out, lossless=True, quality=100, method=6, exact=False)
        print(f"{out} {img.width}x{img.height}")


if __name__ == "__main__":
    main()
