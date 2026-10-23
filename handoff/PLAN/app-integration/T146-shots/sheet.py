"""T146: a contact sheet of option shots, side by side with labels. python sheet.py <shots dir> <out.jpg> <view> <scheme> <id...>"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

src, out, view, scheme, *ids = sys.argv[1:]
ims = [Image.open(Path(src) / f"{i}--{view}--{scheme}.jpg") for i in ids]
w, h = ims[0].size
pad = 60
sheet = Image.new("RGB", (w * len(ims), h + pad), "white")
d = ImageDraw.Draw(sheet)
try:
    font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 40)
except OSError:
    font = ImageFont.load_default()
for k, (i, im) in enumerate(zip(ids, ims)):
    sheet.paste(im, (k * w, pad))
    d.text((k * w + 16, 8), i, fill="black", font=font)
sheet.save(out, quality=80)
