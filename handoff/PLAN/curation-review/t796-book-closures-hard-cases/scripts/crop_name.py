"""Crop a band of a synopsis page around a water's name (to see the ✱ tributary mark and the row's cells):
crop_name.py <pdf page> <text> <out.png> [band height in pt, default 60]"""
import sys, pypdfium2 as pdfium
pdf = pdfium.PdfDocument('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277/synopsis-80614b20045f.pdf')
pg = pdf[int(sys.argv[1]) - 1]; tp = pg.get_textpage()
s = tp.search(sys.argv[2], match_case=True); r = s.get_next()
if not r: sys.exit(f"not found: {sys.argv[2]}")
idx, n = r; box = tp.get_charbox(idx)   # (left, bottom, right, top) in pt
h = float(sys.argv[4]) if len(sys.argv) > 4 else 60
W, H = pg.get_size(); scale = 2
img = pg.render(scale=scale).to_pil()
top = max(0, (H - box[3] - 12) * scale); bot = min(H * scale, (H - box[1] + h) * scale)
img.crop((0, int(top), int(W * scale), int(bot))).save(sys.argv[3]); print(img.size, top, bot)
