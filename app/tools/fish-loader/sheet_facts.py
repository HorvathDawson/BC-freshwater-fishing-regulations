#!/usr/bin/env python3
"""
(T814) What a baked coho sheet IS, read back from the committed file: its size and the two colours it is printed in.

    .venv/bin/python app/tools/fish-loader/sheet_facts.py SHEET.webp [SHEET.webp ...]   -> one JSON line per sheet
    .venv/bin/python app/tools/fish-loader/sheet_facts.py --frame0 W H SHEET.webp        -> frame 0, RGBA, base64 (T843)
    .venv/bin/python app/tools/fish-loader/sheet_facts.py --counts FILE.webp "#HEX" ...   -> {hex: opaque pixels} (T886)

`pnpm coho` (app/tools/build-coho.mjs --check) compares these with the colour tokens and the generated module, so a
token changed without `pnpm coho:build` fails. Decoding a lossless WebP is exact on every machine, which re-rendering
the frames is not proven to be (the drawing is floating point; the Mac and CI's Linux were never compared), so the check
reads the committed pixels and never re-draws them.

The two colours: every fully opaque pixel of the sheet is either the ink block or the red block (only the anti-aliased
edges blend), so the two commonest opaque colours, most common first, are the ink and the red.
"""
import json
import sys

from PIL import Image


def facts(path):
    im = Image.open(path)
    im.load()
    rgba = im.convert("RGBA")
    opaque = sorted(((n, c[:3]) for n, c in rgba.getcolors(rgba.width * rgba.height) if c[3] == 255), reverse=True)
    top = ["#%02X%02X%02X" % c for _, c in opaque[:2]]
    return {"file": path, "format": im.format,
            "width": rgba.width, "height": rgba.height, "colours": top}


def frame_zero(path, width, height):
    """(T843) The sheet's first frame (top-left `width` x `height` px) as straight-alpha RGBA bytes, base64: what the
    native splash is drawn from (`tools/build-splash.mjs`)."""
    import base64
    im = Image.open(path)
    im.load()
    return base64.b64encode(im.convert("RGBA").crop((0, 0, width, height)).tobytes()).decode("ascii")


def counts(path, hexes):
    """(T886) How many fully opaque pixels are exactly each colour: the hatch sheet's drawings hold a third (the ring) that
    is too thin to be among the two commonest (`tools/build-adipose.mjs --check`)."""
    im = Image.open(path)
    im.load()
    rgba = im.convert("RGBA")
    have = {"#%02X%02X%02X" % c[:3]: n for n, c in rgba.getcolors(rgba.width * rgba.height) if c[3] == 255}
    return {h.upper(): have.get(h.upper(), 0) for h in hexes}


if __name__ == "__main__":
    if sys.argv[1:2] == ["--counts"]:
        print(json.dumps(counts(sys.argv[2], sys.argv[3:])))
        sys.exit(0)
    if sys.argv[1:2] == ["--frame0"]:
        _, _, w, h, sheet = sys.argv
        print(frame_zero(sheet, int(w), int(h)))
        sys.exit(0)
    for p in sys.argv[1:]:
        print(json.dumps(facts(p)))
