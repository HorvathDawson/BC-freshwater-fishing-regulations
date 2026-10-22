"""Contact sheet: the given shots side by side (same height), each captioned with its file name.

Usage: python sheet.py <out.png> <height px> <shot.png> [<shot.png> ...]
"""
import sys
from PIL import Image, ImageDraw

out, height, *paths = sys.argv[1:]
height = int(height)
ims = []
for p in paths:
    im = Image.open(p).convert("RGB")
    w = round(im.width * height / im.height)
    ims.append((p.rsplit("/", 1)[-1], im.resize((w, height), Image.LANCZOS)))
W = sum(im.width for _, im in ims) + 8 * (len(ims) - 1)
sheet = Image.new("RGB", (W, height + 22), "white")
d = ImageDraw.Draw(sheet)
x = 0
for name, im in ims:
    sheet.paste(im, (x, 22))
    d.text((x + 4, 4), name, fill="black")
    x += im.width + 8
sheet.save(out)
