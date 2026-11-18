#!/usr/bin/env python3
"""
(T844, owner 2026-11-17: "make way smaller for web") THE WEB'S COHO SHEET, made smaller from a sheet the owner's
generator drew (`fish_loader.py`, 40 frames for the web): every pixel's colour is put back on the line between the two
inks it is printed in (`--ink`, `--red`) at MIX steps, and its alpha on ALPHA steps, so the sheet holds a few dozen
colours and the lossless WebP packs it as a palette (~105 KB, from 477 KB). The two inks stay exact (`pnpm coho` reads
them back); the linocut's faint grain inside a block (a step or two of a channel) is what goes.

    .venv/bin/python app/tools/fish-loader/web_sheet.py IN.webp OUT.webp --ink '#1F2E3B' --red '#CF5A3C' [--mix 4] [--alpha 16]
"""
import argparse

import numpy as np
from PIL import Image


def hexrgb(s):
    s = s.lstrip("#")
    return np.array([int(s[i:i + 2], 16) for i in (0, 2, 4)], dtype=np.float64)


def snap(im, ink, red, mix, alpha):
    a = np.asarray(im.convert("RGBA")).astype(np.float64)
    d = red - ink
    t = np.clip(((a[..., :3] - ink) @ d) / (d @ d), 0.0, 1.0)
    t = np.round(t * (mix - 1)) / (mix - 1)
    out = np.zeros_like(a)
    out[..., :3] = np.round(ink + t[..., None] * d)
    out[..., 3] = np.round(np.round(a[..., 3] / 255.0 * (alpha - 1)) / (alpha - 1) * 255.0)
    out[out[..., 3] == 0] = 0
    return Image.fromarray(out.astype(np.uint8), "RGBA")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--ink", required=True)
    ap.add_argument("--red", required=True)
    ap.add_argument("--mix", type=int, default=4, help="steps between the two inks, ends included")
    ap.add_argument("--alpha", type=int, default=16, help="alpha steps, 0 and 255 included")
    a = ap.parse_args()
    snap(Image.open(a.src), hexrgb(a.ink), hexrgb(a.red), a.mix, a.alpha).save(
        a.out, "WEBP", lossless=True, quality=100, method=6, exact=False)
