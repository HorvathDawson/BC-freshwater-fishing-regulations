"""Re-encodes the shots as JPEG (quality 85, full 2x resolution) so ~200 MB of PNG fits a repo. Removes the PNGs.

Usage: python to_jpeg.py <dir> [<dir> ...]
"""
import os
import sys
from PIL import Image

for d in sys.argv[1:]:
    for n in sorted(os.listdir(d)):
        if not n.endswith(".png"):
            continue
        p = os.path.join(d, n)
        Image.open(p).convert("RGB").save(p[:-4] + ".jpg", quality=85, optimize=True, progressive=True)
        os.remove(p)
