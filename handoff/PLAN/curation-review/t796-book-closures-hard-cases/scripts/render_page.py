"""Render a synopsis PDF page (1-based PDF index) to PNG, optionally cropped: render_page.py <pdf> <out.png> [x0 y0 x1 y1 as fractions] [scale]"""
import sys, pypdfium2 as pdfium
pdf = pdfium.PdfDocument('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277/synopsis-80614b20045f.pdf')
i = int(sys.argv[1]); out = sys.argv[2]
crop = [float(x) for x in sys.argv[3:7]] if len(sys.argv) >= 7 else None
scale = float(sys.argv[7]) if len(sys.argv) >= 8 else 2.0
img = pdf[i - 1].render(scale=scale).to_pil()
if crop:
    w, h = img.size; img = img.crop((int(crop[0]*w), int(crop[1]*h), int(crop[2]*w), int(crop[3]*h)))
img.save(out); print(img.size)
