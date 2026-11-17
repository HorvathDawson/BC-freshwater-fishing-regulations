#!/usr/bin/env python3
"""
fish_loader.py -- linocut coho salmon loading animation -> sprite sheet + CSS.

HANDOFF NOTES FOR AN AGENT
==========================
This one file is everything needed. It draws every frame procedurally (no
input images), writes a sprite sheet, a ready-to-use CSS file and a demo page.

1. Requirements
   Python 3.8+ and Pillow:   pip install pillow

2. Generate (defaults are the approved look -- don't change them unless asked)
   python fish_loader.py
   -> writes ./fish_loader_out/ :
      fish_loader_sheet.png    80 frames, 480x200 each, 8 columns x 10 rows,
                               transparent background (3840x2000 px)
      fish_loader_static.png   one resting frame, shown when the visitor has
                               "reduce motion" on
      fish_loader.css          the .fish-loader class that plays the sheet
      fish_loader.json         frame size, count, grid layout, fps, loop length
      demo.html                open in a browser to see it at several sizes
      fish_loader_preview.gif  960x400 preview on a paper-coloured background

   Takes a few seconds (add --no-preview to skip the large GIF).

3. Use it in a web page
   Copy fish_loader_sheet.png, fish_loader_static.png and fish_loader.css
   into the site (same folder, or fix the url(...) paths in the CSS), then:

     <link rel="stylesheet" href="fish_loader.css">
     <div role="status" aria-live="polite">
       <div class="fish-loader" style="width:160px" aria-hidden="true"></div>
       <span class="visually-hidden">Loading...</span>
     </div>

   Any width works -- the height follows the 480:200 frame ratio, and the CSS
   uses percentage positions so it scales cleanly. prefers-reduced-motion is
   handled in the CSS (the still frame is shown instead).

   Other engines: frame i sits at column i % 8, row i // 8 of the sheet; play
   80 frames at 20 fps (4 s loop). fish_loader.json has these numbers.

4. Common options
   --width 240           smaller frames / smaller sheet (height follows)
   --cols 0              single-row sheet instead of a grid (very wide)
   --face right          mirror so the fish swims to the right
   --swim 1.0 --beats 4  swim the whole loop (no glide)
   --no-water            remove the ripple marks under the fish
   --ink / --accent / --paper "#hex"   recolour (accent = the red side band)
   --frames / --fps      change smoothness or speed (keep beats ~1 s each)
   --out DIR             output folder
   Fine-tuning lives in the code: beat_curves() (swim motion), motion() (glide,
   bob), render_frame() (pitch -0.045 rad, jaw 0.05 rad), build() (shapes).

5. Check the result
   Open demo.html; the fish should swim two slow beats, then glide while
   bobbing and tilting gently, with the mouth opening slightly. The loop must
   have no jump between the last and first frame.

WHAT IT LOOKS LIKE
==================
A spawning male coho facing left: hooked snout over a kype, a few teeth, dark
ink back and belly with a carved red band along the side, spots on the back
and the upper tail lobe only, a round tail joint. Two-block linocut look: ink
block with carved (transparent) marks over a slightly mis-registered red block.

Motion per loop (4 s): two side-to-side tail beats (~1 s each) where the head
stays rigid, the tail joint slides, the tail fin foreshortens to ~40% width,
and the dorsal, adipose and anal fins sway; then a glide with fins at rest.
Throughout: a few pixels of bob with the nose pitching up as it rises, and the
jaw opening a touch twice per loop. Geometry is in design pixels on a
732 x 305 canvas, then scaled.
"""
import argparse
import json
import math
import os

from PIL import Image, ImageColor, ImageDraw, ImageOps

SS = 4                                # supersampling
VX0, VY0, VW, VH = 28, 160, 732, 305  # design-space view box


# ------------------------------------------------------------------ helpers
def lerp(a, b, t):
    return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)


def smooth(t):
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)


def ease(t):
    return 0.5 - 0.5 * math.cos(math.pi * max(0.0, min(1.0, t)))


def noise(i, k=0):
    """Deterministic wobble in [-1, 1]; identical every frame so edges don't boil."""
    return 0.5 * math.sin(i * 12.9898 + k * 78.233) + 0.5 * math.sin(i * 4.1414 + k * 3.7)


def densify(pts, step=6.0, closed=True):
    out = []
    n = len(pts)
    for i in range(n if closed else n - 1):
        a, b = pts[i], pts[(i + 1) % n]
        m = max(1, int(math.hypot(b[0] - a[0], b[1] - a[1]) / step))
        out += [lerp(a, b, j / m) for j in range(m)]
    if not closed:
        out.append(pts[-1])
    return out


def roughen(pts, amp, k=0):
    # slow-varying wobble: reads as a hand-cut edge rather than a zigzag
    def w(i, k):
        return (0.55 * math.sin(i * 0.17 + k * 1.3) + 0.3 * math.sin(i * 0.41 + k * 2.1)
                + 0.15 * math.sin(i * 1.13 + k * 0.7))
    return [(x + amp * w(i, k), y + amp * w(i, k + 5)) for i, (x, y) in enumerate(pts)]


def stroke(pts, w, profile="even", k=0):
    """Gouge mark along a polyline -> polygon.
    profile: 'even', 'taper' (w -> 0), 'lens' (0 -> w -> 0)."""
    n = len(pts)
    left, right = [], []
    for i in range(n):
        t = i / (n - 1)
        ww = {"taper": w * (1 - t) ** 0.7, "lens": w * math.sin(math.pi * t) ** 0.5,
              "even": w}[profile]
        ww *= 1 + 0.12 * noise(i, k)
        a, b = pts[max(i - 1, 0)], pts[min(i + 1, n - 1)]
        tx, ty = b[0] - a[0], b[1] - a[1]
        ln = math.hypot(tx, ty) or 1.0
        nx, ny = -ty / ln, tx / ln
        x, y = pts[i]
        left.append((x + nx * ww / 2, y + ny * ww / 2))
        right.append((x - nx * ww / 2, y - ny * ww / 2))
    return left + right[::-1]


def line_pts(a, b, n=6):
    return [lerp(a, b, i / (n - 1)) for i in range(n)]


def fleck(cx, cy, rx, ry, k):
    return [(cx + rx * math.cos(a) * (1 + 0.3 * noise(k * 7 + i)),
             cy + ry * math.sin(a) * (1 + 0.3 * noise(k * 7 + i, 3)))
            for i, a in enumerate([2 * math.pi * i / 8 for i in range(8)])]


def shift(pts, dx, dy=0.0):
    return [(x + dx, y + dy) for x, y in pts]


# ------------------------------------------------------------------ motion
def beat_curves(phi):
    """Offsets (design px) measured from the reference, relative to the rest
    pose at phi = 0. phi advances 2*pi per tail beat."""
    def c(amp, lag):
        return amp * (math.cos(phi - lag) - math.cos(-lag))
    return {
        "cap": -c(12.5, 1.70),        # tail joint slides back / forward
        "edge": c(37.0, 0.0),         # tail fin trailing edge
        "adipose": -c(8.0, 0.52),
        "anal": -c(11.0, 0.13),
        "dorsal_x": c(6.0, 1.96),
        "dorsal_y": -c(3.0, 2.60),
        "pec": math.sin(phi + 1.0) - math.sin(1.0),
    }


def motion(t, swim, beats):
    """t in [0,1) -> (phi, surge, bob)."""
    if t < swim:
        u = t / swim
        warp = u - 0.7 * math.sin(2 * math.pi * u) / (2 * math.pi)   # eases in/out
        phi = 2 * math.pi * beats * warp
        surge = -6 * ease(u)                        # drifts a touch forward (left)
    else:
        u = (t - swim) / (1 - swim)
        phi = 0.0                                   # glide: rest pose
        surge = -6 * (1 - ease(u))                  # eases back
    bob = 4 * math.sin(2 * math.pi * t)
    return phi, surge, bob


# ------------------------------------------------------------------ salmon
# spawning male coho: hooked snout and kype; the back keeps the reference
# line with no hump (a hump reads as a pink salmon)
TOP_FRONT = [(41, 320), (40, 310), (46, 301), (56, 295), (68, 290), (80, 285), (92, 276),
             (105, 267), (120, 259), (140, 250), (165, 243), (195, 238), (230, 234),
             (270, 232), (310, 232), (340, 235)]
TOP_REAR = [(378, 241), (402, 243), (426, 246), (450, 250), (474, 255), (498, 259),
            (522, 266), (546, 272), (570, 279), (604, 279)]
BOT_REAR = [(604, 337), (546, 337), (534, 339), (522, 342), (498, 355), (474, 368), (462, 378)]
BELLY = [(400, 378), (300, 378), (200, 378), (138, 378)]
HEAD_BOT = [(118, 372), (100, 361), (86, 353), (72, 351), (60, 347), (53, 340), (53, 333),
            (57, 328), (63, 330), (100, 335), (146, 338), (152, 332), (110, 326), (70, 321),
            (57, 322), (50, 328)]
CAP_C, CAP_R = (604, 308), 29


def top_y(x):
    pts = TOP_FRONT + TOP_REAR
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / ((x1 - x0) or 1)
    return pts[-1][1]


JAW_HINGE = (152, 334)


def jaw_open(pts, ang):
    """Rotate lower-jaw points about the hinge; full effect in front of x=100,
    fading out toward the throat so the chin line stays smooth."""
    out = []
    for x, y in pts:
        w = 1.0 if x <= 100 else max(0.0, 1 - (x - 100) / 30)
        a = -ang * w
        dx, dy = x - JAW_HINGE[0], y - JAW_HINGE[1]
        out.append((JAW_HINGE[0] + dx * math.cos(a) - dy * math.sin(a),
                    JAW_HINGE[1] + dx * math.sin(a) + dy * math.cos(a)))
    return out


def build(phi, jaw=0.0):
    m = beat_curves(phi)
    cap_dx = m["cap"]

    def top_shift(x):
        if x <= 380:
            return 0.0
        s = smooth((x - 380) / (498 - 380)) * m["adipose"]
        return s + (cap_dx - s) * smooth((x - 546) / (604 - 546))

    def bot_shift(x):
        if x <= 400:
            return 0.0
        s = smooth((x - 400) / (474 - 400)) * m["anal"]
        return s + (cap_dx - s) * smooth((x - 534) / (604 - 534))

    ink, carve = [], []

    # ---- body silhouette
    cap = [(CAP_C[0] + cap_dx + CAP_R * math.cos(a), CAP_C[1] + CAP_R * math.sin(a))
           for a in [-math.pi / 2 + math.pi * i / 12 for i in range(13)]]
    top = TOP_FRONT + [(x + top_shift(x), y) for x, y in TOP_REAR]
    bot = [(x + bot_shift(x), y) for x, y in BOT_REAR] + [(x + bot_shift(x), y) for x, y in BELLY]
    lower = jaw_open(HEAD_BOT[:11], jaw)          # throat .. mouth corner
    body = top + cap[1:-1] + bot + lower + HEAD_BOT[11:]
    ink.append(roughen(densify(body, 5), 1.3, 1))

    # ---- tail fin: swivels at the joint, so it foreshortens
    root_x = CAP_C[0] + cap_dx
    edge_rest = 722 - CAP_C[0]
    ts = max(0.25, (edge_rest + m["edge"] - cap_dx) / edge_rest)
    fin = [(612, 279), (700, 203), (712, 216), (720, 236), (722, 258), (717, 280), (707, 300),
           (709, 313), (718, 332), (722, 356), (716, 386), (700, 412), (612, 337)]
    fin = [(root_x + (x - CAP_C[0]) * ts, y) for x, y in fin]
    ink.insert(0, roughen(densify(fin, 7), 1.0, 2))
    trailing = fin[1:12]
    for j, f in enumerate([0.04, 0.18, 0.32, 0.45, 0.58, 0.72, 0.86, 0.97]):
        e = trailing[min(int(f * (len(trailing) - 1) + 0.5), len(trailing) - 1)]
        ry = CAP_C[1] - 22 + 44 * f
        rim = root_x + math.sqrt(max(CAP_R ** 2 - (ry - CAP_C[1]) ** 2, 0.0))
        r0 = (rim + 7, ry)
        if e[0] - r0[0] < 9:          # fin nearly edge-on: no room for this ray
            continue
        carve.append(stroke(line_pts(r0, lerp(r0, e, 0.82)), 4.5 * min(1.0, 0.4 + ts), "taper", 100 + j))
    for k, (u, v) in enumerate([(0.62, 236), (0.80, 262), (0.48, 254), (0.78, 228)]):
        fx = root_x + (edge_rest * u) * ts
        if fx > root_x + CAP_R + 8:
            carve.append(fleck(fx, v, 3.5 * max(ts, 0.5), 3.0, 110 + k))
    # carved rim of the round tail joint
    carve.append(stroke([(c[0] - 1, c[1]) for c in cap], 4.5, "even", 120))

    # ---- dorsal fin (above the back)
    ddx, ddy = m["dorsal_x"], m["dorsal_y"]
    dorsal = [(342, 239), (408 + ddx, 211 + ddy), (431 + ddx, 219 + ddy * 0.7),
              (452 + ddx * 0.8, 232 + ddy * 0.3), (469 + ddx * 0.6, 243), (461, 252), (400, 246)]
    ink.append(roughen(densify(dorsal, 6), 0.9, 3))
    for j, f in enumerate([0.2, 0.4, 0.6, 0.8]):
        b0 = (350 + 105 * f, top_y(350 + 105 * f) - 2)
        tip = lerp(dorsal[1], dorsal[4], f * 0.95)
        carve.append(stroke(line_pts(b0, lerp(b0, tip, 0.8)), 4.0, "taper", 10 + j))
    base = [(x, top_y(x) + 1) for x in range(346, 466, 8)]
    carve.append(stroke([(x + top_shift(x), y) for x, y in base], 3.5, "lens", 14))

    # ---- adipose fin
    ad = shift([(490, 263), (504, 256), (518, 252), (527, 255), (528, 263), (520, 267)], m["adipose"])
    ink.append(roughen(densify(ad, 5), 0.7, 5))

    # ---- fins below: carved outlines where they overlap the body
    pa = 0.05 * m["pec"]
    pec = [(226, 352), (276, 359), (292, 377), (301, 402), (218, 377)]
    pec = [(226 + (x - 226) * math.cos(pa) - (y - 352) * math.sin(pa),
            352 + (x - 226) * math.sin(pa) + (y - 352) * math.cos(pa)) for x, y in pec]
    pel = shift([(410, 364), (440, 365), (450, 372), (456, 386), (406, 376)], 0.4 * m["anal"])
    an = m["anal"]
    anal = [(462 + an * 0.8, 377), (536 + an, 340), (537 + an * 1.2, 355), (531 + an * 1.4, 377),
            (522 + an * 1.4, 389), (470 + an, 384)]
    for k, poly in enumerate([pec, pel, anal]):
        ink.append(roughen(densify(poly, 6), 0.8, 6 + k))
        carve.append(stroke(densify(poly + [poly[0]], 5, False), 4.0, "even", 30 + k))
    carve.append(stroke(line_pts(lerp(pec[4], pec[0], 0.5), lerp(pec[2], pec[3], 0.6)), 3.5, "taper", 40))
    carve.append(stroke(line_pts((213, 362), (219, 379), 4), 4.0, "even", 41))
    carve.append(stroke(line_pts(lerp(anal[0], anal[5], 0.5), lerp(anal[2], anal[3], 0.5)), 3.5, "taper", 42))
    carve.append(stroke(line_pts((409, 361), (407, 375), 4), 3.5, "even", 43))

    # ---- head: mouth gap is in the silhouette; carve the maxilla tab, gill, plate
    carve.append(stroke([(147, 330), (143, 342), (147, 352), (157, 356), (164, 351),
                         (162, 340), (154, 331)], 4.5, "even", 50))
    # three small, even teeth in the gap
    for x in (82, 112):                                     # upper
        y = 321 + (x - 70) * 5 / 40
        ink.append([(x - 3.5, y - 2), (x + 3.5, y - 2), (x, y + 4.5)])
    y = 330 + (97 - 63) * 5 / 37                            # lower (moves with the jaw)
    ink.append(jaw_open([(93.5, y + 2), (100.5, y + 2), (97, y - 4.5)], jaw))
    for k, (x0, y0, x1, y1) in enumerate([(168, 360, 198, 356), (172, 367, 203, 364),
                                          (178, 373, 206, 371)]):
        carve.append(stroke(line_pts((x0, y0), (x1, y1), 6), 3.6, "lens", 53 + k))
    head_spots = [(104, 286), (126, 273), (152, 262), (182, 254), (212, 249), (122, 297),
                  (244, 247), (300, 246), (228, 263)]
    for k, (sx, sy) in enumerate(head_spots):
        carve.append(fleck(sx, sy, 3.8, 3.0, 140 + k))
    # brow: a carved arc over the eye gives it some intent
    brow = [(136, 292), (142, 287), (150, 285), (159, 286), (166, 290)]
    carve.append(stroke(brow, 4.2, "lens", 56))
    gill_c = [(178 + 22 * math.cos(a), 320 + 23 * math.sin(a))
              for a in [-1.9 + 3.8 * i / 12 for i in range(13)]]
    carve.append(stroke(gill_c, 5.5, "even", 51))
    plate = [(188, 300), (210, 283), (226, 302), (239, 328), (226, 352), (205, 372)]
    carve.append(stroke(densify(plate, 6, False), 5.5, "even", 52))

    # ---- lateral line, spots, belly lines
    x = 250
    i = 0
    while x < 585:
        y0 = 313 - 6 * (x - 250) / 335
        x1 = x + 14
        y1 = 313 - 6 * (x1 - 250) / 335
        carve.append(stroke([(x + top_shift(x) * 0.5, y0), (x1 + top_shift(x1) * 0.5, y1)],
                            3.5, "even", 60 + i))
        x += 24
        i += 1
    spots = [(240, 262), (272, 249), (300, 268), (330, 250), (356, 265), (386, 254), (412, 270),
             (440, 262), (468, 277), (497, 283), (528, 290), (558, 295), (262, 288), (420, 290)]
    for k, (sx, sy) in enumerate(spots):
        carve.append(fleck(sx + top_shift(sx), sy, 4.6, 3.6, 70 + k))
    for k, yy in enumerate([351, 358, 365, 372]):
        carve.append(stroke(line_pts((304, yy), (401 - 4 * k, yy), 10), 3.6, "lens", 90 + k))

    # ---- red block: belly, under the ink
    accent = [(140, 379)] + [(x + bot_shift(x), y) for x, y in BELLY[::-1]][1:] + \
             [(462 + bot_shift(462), 378), (474 + bot_shift(474), 368), (466, 372), (300, 374),
              (210, 374), (150, 375)]
    accent = roughen(densify(accent, 8), 1.0, 90)

    # ---- spawning coho red side: a band carved out of the ink, red block beneath
    up = [(232, 324), (270, 319), (320, 316), (380, 315), (440, 314), (500, 314), (548, 314),
          (590, 315)]
    lo = [(590, 328), (548, 333), (510, 344), (470, 352), (420, 355), (360, 356), (300, 354),
          (250, 350), (226, 340)]
    band = [(x + top_shift(x) * 0.6, y) for x, y in up] + [(x + bot_shift(x) * 0.6, y) for x, y in lo]
    band = roughen(densify(band, 4), 2.2, 150)
    carve.append(band)
    red = [(x + top_shift(x) * 0.6, y - 6) for x, y in up] + \
          [(x + bot_shift(x) * 0.6, y + 6) for x, y in lo]
    red = shift(roughen(densify(red, 8), 1.0, 151), -3.5, -3.0)   # cancels the offset inside
    # a few uncut slivers left in the band, as happens when cutting lino by hand
    chips = [[(300, 330), (322, 327), (318, 331), (299, 333)],
             [(410, 340), (436, 336), (433, 340), (409, 343)],
             [(498, 330), (514, 327), (512, 330), (497, 332)]]
    chips = [[(x + top_shift(x) * 0.6, y) for x, y in c] for c in chips]

    eye = (150, 302)
    return ink, carve, [red, accent], eye, chips


def water_marks():
    marks = []
    for k, (x0, x1, y) in enumerate([(120, 250, 436), (330, 440, 446), (540, 630, 434)]):
        pts = [(x0 + (x1 - x0) * i / 11, y + 3.5 * math.sin(i * 0.9 + k)) for i in range(12)]
        marks.append(stroke(pts, 9, "lens", 130 + k))
    return marks


# ------------------------------------------------------------------ render
def render_frame(t, a, width):
    W = width * SS
    H = round(width * VH / VW) * SS
    k = W / VW
    phi, surge, bob = motion(t, a.swim, a.beats)
    u = t / a.swim if t < a.swim else 1.0
    look = math.sin(math.pi * u) if t < a.swim else 0.0      # 0 rest .. 1 mid-swim

    pitch = -0.045 * math.cos(2 * math.pi * t)     # follows the bob: nose leads
    pc, ps = math.cos(pitch), math.sin(pitch)
    PX, PY = 400, 310                               # pivot near the body centre

    def tf(pts, move=True):
        out = []
        for x, y in pts:
            if move:
                x, y = (PX + (x - PX) * pc - (y - PY) * ps + surge,
                        PY + (x - PX) * ps + (y - PY) * pc + bob)
            out.append(((x - VX0) * k, (y - VY0) * k))
        return out

    jaw = 0.05 * (0.5 - 0.5 * math.cos(2 * math.pi * 2 * t))   # opens a touch, twice a loop
    ink, carve, accent, eye, chips = build(phi, jaw)

    acc = Image.new("L", (W, H), 0)
    for poly in accent:
        ImageDraw.Draw(acc).polygon(tf(shift(poly, 3.5, 3.0)), fill=255)

    msk = Image.new("L", (W, H), 0)
    d = ImageDraw.Draw(msk)
    for poly in ink:
        d.polygon(tf(poly), fill=255)
    if a.water:
        for poly in water_marks():
            d.polygon(tf(poly, move=False), fill=255)
    for poly in carve:
        d.polygon(tf(poly), fill=0)
    for poly in chips:
        d.polygon(tf(poly), fill=255)
    (ex, ey), = tf([eye])
    px = -1.6 - 1.4 * look                     # pupil looks ahead (left)
    for (ox, oy, r, v) in [(0, 0, 12.5, 0),            # carved eye
                           (px, -0.4, 7.2, 255),      # inked pupil
                           (px - 2.4, -2.6, 2.4, 0),  # main glint
                           (px + 2.6, 2.4, 1.1, 0)]:  # small second glint
        d.ellipse([ex + (ox - r) * k, ey + (oy - r) * k, ex + (ox + r) * k, ey + (oy + r) * k], fill=v)

    img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    img.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(a.accent) + (255,)), (0, 0), acc)
    img.paste(Image.new("RGBA", (W, H), ImageColor.getrgb(a.ink) + (255,)), (0, 0), msk)
    img = img.resize((W // SS, H // SS), Image.LANCZOS)
    return ImageOps.mirror(img) if a.face == "right" else img


# ------------------------------------------------------------------ main
def frame_css(meta, cls="fish-loader"):
    """CSS that plays the sheet at any display size. Positions are percentages,
    so the same rules work for a single row or a grid, and the element can be
    scaled freely (only its aspect ratio is fixed)."""
    cols, rows, n = meta["columns"], meta["rows"], meta["frames"]
    stops = []
    for i in range(n):
        c, r = i % cols, i // cols
        px = 0 if cols == 1 else c / (cols - 1) * 100
        py = 0 if rows == 1 else r / (rows - 1) * 100
        stops.append(f"  {i / n * 100:.4f}% {{ background-position: {px:.4f}% {py:.4f}%; }}")
    return f"""/* Generated by fish_loader.py -- {n} frames, {cols}x{rows} sheet, {meta['loop_ms']} ms loop */
.{cls} {{
  width: {meta['frame_width']}px;            /* any width works; height follows */
  max-width: 100%;
  aspect-ratio: {meta['frame_width']} / {meta['frame_height']};
  background-image: url({meta['image']});
  background-repeat: no-repeat;
  background-size: {cols * 100}% {rows * 100}%;
  background-position: 0% 0%;
  animation: {cls}-swim {meta['loop_ms']}ms step-end infinite;
}}
@keyframes {cls}-swim {{
{chr(10).join(stops)}
}}
.visually-hidden {{
  position: absolute; width: 1px; height: 1px; margin: -1px; padding: 0;
  overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; border: 0;
}}
@media (prefers-reduced-motion: reduce) {{
  .{cls} {{
    animation: none;
    background-image: url({meta['static_image']});
    background-size: 100% 100%;
    background-position: 0 0;
  }}
}}
"""


DEMO_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fish loader demo</title>
<link rel="stylesheet" href="fish_loader.css">
<style>
  body {{ margin: 0; font: 15px/1.5 system-ui, sans-serif; background: {paper}; color: {ink}; }}
  main {{ max-width: 880px; margin: 0 auto; padding: 32px 20px 60px; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  .row {{ display: flex; flex-wrap: wrap; gap: 28px; align-items: end; margin: 24px 0; }}
  .cell {{ text-align: center; font-size: 13px; opacity: .8; }}
  .busy {{ display: inline-flex; align-items: center; gap: 12px; padding: 10px 16px;
          border: 2px solid {ink}; border-radius: 6px; }}
  pre {{ background: #fffaf0; border: 1px solid #d8ccb4; padding: 14px; overflow: auto;
        font-size: 13px; border-radius: 6px; }}
  label {{ display: inline-flex; gap: 8px; align-items: center; cursor: pointer; }}
  .force-static .fish-loader {{ animation: none; background-image: url({static});
                               background-size: 100% 100%; background-position: 0 0; }}
</style>
</head>
<body>
<main id="root">
  <h1>Fish loader</h1>
  <p>Sprite-sheet loading animation: {frames} frames, {loop} ms loop.
     Visitors with &ldquo;reduce motion&rdquo; turned on see a still frame.</p>
  <label><input type="checkbox" id="rm"> Preview the reduced-motion version</label>

  <div class="row">
    <div class="cell"><div class="fish-loader" style="width:{w}px"></div>{w} px (native)</div>
    <div class="cell"><div class="fish-loader" style="width:240px"></div>240 px</div>
    <div class="cell"><div class="fish-loader" style="width:120px"></div>120 px</div>
  </div>

  <p>Inline with text:</p>
  <span class="busy" role="status" aria-live="polite">
    <span class="fish-loader" style="width:96px" aria-hidden="true"></span>
    Loading catch data&hellip;
  </span>

  <h2>Use it</h2>
<pre>&lt;link rel="stylesheet" href="fish_loader.css"&gt;

&lt;div role="status" aria-live="polite"&gt;
  &lt;div class="fish-loader" style="width:160px" aria-hidden="true"&gt;&lt;/div&gt;
  &lt;span class="visually-hidden"&gt;Loading&hellip;&lt;/span&gt;
&lt;/div&gt;</pre>
  <p>Keep <code>fish_loader_sheet.png</code> and <code>fish_loader_static.png</code>
     next to <code>fish_loader.css</code>, or edit the <code>url(...)</code> paths in it.
     Set any width; the height follows the frame's aspect ratio.</p>
</main>
<script>
  document.getElementById('rm').addEventListener('change', e =>
    document.getElementById('root').classList.toggle('force-static', e.target.checked));
</script>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--width", type=int, default=480, help="sprite frame width in px (height follows, 732:305)")
    ap.add_argument("--preview-width", type=int, default=960, help="preview GIF width")
    ap.add_argument("--no-preview", dest="preview", action="store_false", help="skip the preview GIF (faster)")
    ap.add_argument("--frames", type=int, default=80, help="frames per loop")
    ap.add_argument("--fps", type=int, default=20)
    ap.add_argument("--beats", type=int, default=2, help="tail beats per swim")
    ap.add_argument("--swim", type=float, default=0.6,
                    help="fraction of the loop spent swimming (rest is gliding)")
    ap.add_argument("--cols", type=int, default=8, help="sheet columns (0 = single row)")
    ap.add_argument("--face", choices=["left", "right"], default="left")
    ap.add_argument("--ink", default="#1f2e3b", help="main ink colour")
    ap.add_argument("--accent", default="#cf5a3c", help="second block colour (coho red)")
    ap.add_argument("--paper", default="#f2e8d5", help="preview / demo background")
    ap.add_argument("--no-water", dest="water", action="store_false",
                    help="drop the carved ripple marks under the fish")
    ap.add_argument("--out", default="fish_loader_out", help="output directory")
    # (app, T814) options the app's build (app/tools/build-coho.mjs, `pnpm coho`) adds; the defaults are unchanged
    ap.add_argument("--format", choices=["png", "webp"], default="png",
                    help="sheet file format (webp = lossless WebP: same pixels, smaller file)")
    ap.add_argument("--name", default="fish_loader", help="file name prefix (<name>_sheet.<format>, <name>.json)")
    ap.add_argument("--sheet-only", dest="extras", action="store_false",
                    help="write only the sheet and its .json (no still frame, CSS, demo page or preview GIF)")
    a = ap.parse_args()
    if not a.extras:
        a.preview = False
    os.makedirs(a.out, exist_ok=True)

    ts = [i / a.frames for i in range(a.frames)]
    frames = [render_frame(t, a, a.width) for t in ts]
    fw, fh = frames[0].size

    cols = a.cols if a.cols > 0 else a.frames
    rows = math.ceil(a.frames / cols)
    sheet = Image.new("RGBA", (cols * fw, rows * fh), (0, 0, 0, 0))
    for i, f in enumerate(frames):
        sheet.paste(f, ((i % cols) * fw, (i // cols) * fh))
    sheet_file = f"{a.name}_sheet.{a.format}"
    if a.format == "webp":
        sheet.save(os.path.join(a.out, sheet_file), lossless=True, quality=100, method=6, exact=False)
    else:
        sheet.save(os.path.join(a.out, sheet_file), optimize=True)

    still = int(a.frames * (a.swim + (1 - a.swim) * 0.5))
    if a.extras:
        frames[still].save(os.path.join(a.out, f"{a.name}_static.png"))

    if a.preview:
        big = [render_frame(t, a, a.preview_width) for t in ts]
        bg = Image.new("RGBA", big[0].size, ImageColor.getrgb(a.paper) + (255,))
        gif = [Image.alpha_composite(bg, f).convert("RGB") for f in big]
        gif[0].save(os.path.join(a.out, "fish_loader_preview.gif"), save_all=True,
                    append_images=gif[1:], duration=round(1000 / a.fps), loop=0)

    meta = {"image": sheet_file, "static_image": f"{a.name}_static.png" if a.extras else None,
            "frame_width": fw, "frame_height": fh, "frames": a.frames,
            "columns": cols, "rows": rows, "fps": a.fps,
            "loop_ms": round(1000 * a.frames / a.fps), "static_frame": still,
            "layout": "row-major: frame i is at column i % columns, row i // columns"}
    with open(os.path.join(a.out, f"{a.name}.json"), "w") as fp:
        json.dump(meta, fp, indent=2)
    if not a.extras:
        print(f"Done: {a.frames} frames of {fw}x{fh}, {cols}x{rows} sheet ({cols * fw}x{rows * fh} px), "
              f"{meta['loop_ms']} ms loop -> {a.out}/{sheet_file}")
        return
    with open(os.path.join(a.out, "fish_loader.css"), "w") as fp:
        fp.write(frame_css(meta))
    with open(os.path.join(a.out, "demo.html"), "w") as fp:
        fp.write(DEMO_HTML.format(paper=a.paper, ink=a.ink, frames=a.frames,
                                  loop=meta["loop_ms"], w=fw, static=meta["static_image"]))

    print(f"Done: {a.frames} frames of {fw}x{fh}, {cols}x{rows} sheet "
          f"({cols * fw}x{rows * fh} px), {meta['loop_ms']} ms loop -> {a.out}/")
    print("Files: fish_loader_sheet.png, fish_loader_static.png, fish_loader.css, "
          "fish_loader.json, demo.html" + (", fish_loader_preview.gif" if a.preview else ""))


if __name__ == "__main__":
    main()
