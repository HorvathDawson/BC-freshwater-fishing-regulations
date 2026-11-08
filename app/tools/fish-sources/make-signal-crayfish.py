"""T505: Faxon 1885 "Astacus leniusculus Dana, Male" (FMIB 53513, public domain) -> the app's 800x400 RGBA picture:
the animal cut from the paper, its shell filled, tinted signal-crayfish brown-olive, head to the left."""
import sys
from PIL import Image, ImageFilter
import numpy as np
from scipy import ndimage
src, out_path = sys.argv[1], sys.argv[2]
g = np.asarray(Image.open(src).convert("L")).astype(float) / 255
paper = np.percentile(g, 90)
ink = np.clip((paper - g) / (paper - 0.15), 0, 1); ink[ink < 0.06] = 0
mask = ndimage.binary_dilation(ink > 0.12, iterations=4)            # the animal: the largest inked region (no specks/caption)
lab, n = ndimage.label(mask); sizes = ndimage.sum(mask, lab, range(1, n + 1)); ink = ink * (lab == (1 + int(np.argmax(sizes))))
edge = ndimage.binary_closing(ink > 0.08, iterations=2)
holes = ndimage.binary_fill_holes(edge) & ~edge
hl, hn = ndimage.label(holes)
dens = np.asarray(ndimage.mean(ink > 0.05, hl, range(1, hn + 1))); area = np.asarray(ndimage.sum(holes, hl, range(1, hn + 1)))
# shell insides (stippled, or pinholes) filled; the bare paper between touching legs is not
sil = edge | np.isin(hl, 1 + np.nonzero((dens >= 0.03) | (area < 400))[0])
thin = (ink > 0.2) & ~sil
rows = np.nonzero((sil | thin).any(1))[0]; cols = np.nonzero((sil | thin).any(0))[0]
sl = (slice(rows.min(), rows.max() + 1), slice(cols.min(), cols.max() + 1))
ink, sil, thin = ink[sl], sil[sl], thin[sl]
lo, mid, hi = np.array([160, 128, 78]), np.array([118, 76, 42]), np.array([48, 30, 18])
t = np.clip(ink * 1.5, 0, 1)[..., None]
rgb = np.where(t < 0.5, lo * (1 - 2 * t) + mid * (2 * t), mid * (2 - 2 * t) + hi * (2 * t - 1))
a = np.where(sil, 1.0, np.where(thin, np.clip(ink * 1.6, 0, 1), 0.0))
a = np.asarray(Image.fromarray((a * 255).astype(np.uint8)).filter(ImageFilter.GaussianBlur(0.8))).astype(float)
im = Image.fromarray(np.dstack([rgb, a]).astype(np.uint8), "RGBA").rotate(90, expand=True)
im.thumbnail((780, 380), Image.LANCZOS)
out = Image.new("RGBA", (800, 400), (0, 0, 0, 0)); out.paste(im, ((800 - im.width) // 2, (400 - im.height) // 2), im)
out.save(out_path, "WEBP", quality=88)
