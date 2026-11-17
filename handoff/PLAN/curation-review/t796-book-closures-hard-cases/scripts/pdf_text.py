"""Extract the synopsis PDF's text per PDF page (pypdfium2) into out/pages/NNN.txt. Re-run with the repo venv python."""
import pypdfium2 as pdfium, sys
from pathlib import Path
pdf = pdfium.PdfDocument('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277/synopsis-80614b20045f.pdf')
o = Path(__file__).parent / 'out/pages'; o.mkdir(parents=True, exist_ok=True)
for i in range(len(pdf)):
    (o / f'{i+1:03d}.txt').write_text(pdf[i].get_textpage().get_text_range())
print(len(pdf))
