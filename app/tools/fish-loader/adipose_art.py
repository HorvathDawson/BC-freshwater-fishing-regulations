#!/usr/bin/env python3
"""
(T886, owner 2026-11-18) THE "HATCHERY OR WILD?" SHEET'S TWO DRAWINGS, on the loading screen's coho.

    .venv/bin/python -I app/tools/fish-loader/adipose_art.py --ink "#hex" --red "#hex" --ring "#hex" \
        --width 1200 --wild OUT.webp --clipped OUT.webp

The owner, on the old drawings (a rainbow trout whose mouth curved up): "the how to tell hatchery fish is very smile
creepy we want to make it more realistic I think. Better shape more natural smile. Could use the salmon general shape
from loading maybe?" So the fish IS the loader's: `fish_loader.build()` (the owner's drawing, beside this file) at rest
(no tail beat, jaw closed, no bob or pitch, no water marks), drawn the same way (an ink block with carved marks over the
red block), still. Two drawings:

  wild     the adipose fin there, ringed
  clipped  no adipose fin: a healed scar where it was (a low mound in the red block with a carved line across it), ringed

The ring (a circle and a short leader above it) is the third colour. All three colours are the theme's tokens, handed in
by `tools/build-adipose.mjs` (`color.ui.coho.ink`, `.red`, `color.ui.accent`); nothing is typed here. Transparent ground,
lossless WebP: the sheet's card shows through, as the loader's ground does.
"""
import argparse
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # -I leaves the script's folder off the path

from PIL import Image, ImageColor, ImageDraw  # noqa: E402

from fish_loader import build, densify, roughen, shift, stroke  # noqa: E402

SS = 4                                   # supersampling, as the loader
# the view: the loader's design space, cropped to the fish and the ring (8:3, the sheet's picture box)
VX0, VX1 = 6.0, 774.0
VW = VX1 - VX0
VH = VW * 3 / 8
VY0 = 310.0 - VH / 2                     # the fish's middle (tail fin 203..412, the ring's leader 209) at the middle
# the adipose fin at rest (fish_loader.build: `ad`), and the ring around it
ADIPOSE = [(490, 263), (504, 256), (518, 252), (527, 255), (528, 263), (520, 267)]
RING_C, RING_R, RING_W = (510.0, 258.0), 27.0, 4.2
LEADER = ((510.0, RING_C[1] - RING_R), (510.0, RING_C[1] - RING_R - 22.0))


def is_adipose(poly):
    """the rest pose's adipose fin among the ink blocks: every point within a few px of the fin's own box"""
    xs, ys = [p[0] for p in poly], [p[1] for p in poly]
    return min(xs) > 484 and max(xs) < 534 and min(ys) > 246 and max(ys) < 272


def scar():
    """the healed scar on the back where the fin was: a low mound (the red block) and a carved line along it"""
    mound = [(489, 268), (495, 262), (503, 258.5), (513, 258), (522, 260.5), (531, 267)]
    line = [(496, 263.5), (503, 260.8), (512, 260.4), (520, 261.8), (526, 264.5)]
    return roughen(densify(mound, 3), 0.4, 7), stroke(line, 3.0, "lens", 9)


# the loader's open kype (a spawning male's hooked jaw, three teeth in the gap) read as a grin in a still picture: closed here
MOUTH_GAP = [(57, 328), (63, 330), (100, 335), (146, 338), (152, 332), (110, 326), (70, 321), (57, 322)]
MOUTH_LINE = [(54, 326), (80, 328), (110, 331.5), (138, 335), (150, 336)]


def is_tooth(poly):
    """a tooth: one of the three small triangles fish_loader.build() puts in the mouth's gap"""
    return len(poly) == 3 and max(p[0] for p in poly) < 120 and min(p[1] for p in poly) > 310


def ring():
    pts = [(RING_C[0] + RING_R * math.cos(a), RING_C[1] + RING_R * math.sin(a))
           for a in [2 * math.pi * i / 72 for i in range(73)]]
    return [stroke(pts, RING_W, "even", 0), stroke(list(LEADER), RING_W, "even", 1)]


def draw(clipped, ink_hex, red_hex, ring_hex, width):
    W = width * SS
    H = round(width * VH / VW) * SS
    k = W / VW

    def tf(pts):
        return [((x - VX0) * k, (y - VY0) * k) for x, y in pts]

    ink, carve, accent, eye, chips = build(0.0, 0.0)
    fins = [p for p in ink if is_adipose(p)]
    if len(fins) != 1:
        raise SystemExit(f"adipose_art: found {len(fins)} adipose fins in fish_loader.build(), wanted 1")
    if clipped:
        ink = [p for p in ink if not is_adipose(p)]
    teeth = [p for p in ink if is_tooth(p)]
    if len(teeth) != 3:
        raise SystemExit(f"adipose_art: found {len(teeth)} teeth in fish_loader.build(), wanted 3")
    ink = [p for p in ink if not is_tooth(p)] + [MOUTH_GAP]
    carve = carve + [stroke(MOUTH_LINE, 3.2, "taper", 57)]

    red = Image.new("L", (W, H), 0)
    rd = ImageDraw.Draw(red)
    for poly in accent:
        rd.polygon(tf(shift(poly, 3.5, 3.0)), fill=255)          # the loader's red block, its registration offset kept
    msk = Image.new("L", (W, H), 0)
    d = ImageDraw.Draw(msk)
    for poly in ink:
        d.polygon(tf(poly), fill=255)
    for poly in carve:
        d.polygon(tf(poly), fill=0)
    for poly in chips:
        d.polygon(tf(poly), fill=255)
    (ex, ey), = tf([eye])
    px = -1.6                                                     # the pupil at rest (fish_loader.render_frame)
    for (ox, oy, r, v) in [(0, 0, 12.5, 0), (px, -0.4, 7.2, 255), (px - 2.4, -2.6, 2.4, 0), (px + 2.6, 2.4, 1.1, 0)]:
        d.ellipse([ex + (ox - r) * k, ey + (oy - r) * k, ex + (ox + r) * k, ey + (oy + r) * k], fill=v)
    if clipped:
        mound, cut = scar()
        d.polygon(tf(mound), fill=0)                              # no ink on the scar: the red block shows
        rd.polygon(tf(mound), fill=255)
        rd.polygon(tf(cut), fill=0)                               # the scar's line, carved through to the card
    rng = Image.new("L", (W, H), 0)
    for poly in ring():
        ImageDraw.Draw(rng).polygon(tf(poly), fill=255)

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    img.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(red_hex) + (255,)), (0, 0), red)
    img.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(ink_hex) + (255,)), (0, 0), msk)
    img.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(ring_hex) + (255,)), (0, 0), rng)
    return img.resize((W // SS, H // SS), Image.LANCZOS)


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
