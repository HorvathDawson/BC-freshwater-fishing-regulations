// T41 — one PNG from two: [prototype | app | per-pixel diff] composed in headless Chrome's canvas (no image dependency).
// The diff band covers the overlap of the two columns (both aligned at their regs header's top): red where any channel
// differs by more than 24, else the app's pixel faded. Returns the PNG and the diff % over that overlap.
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath } from 'node:url';
import { open } from './cdp.mjs';

/** a theme's colour by token (packages/map/style/themes/<theme>.json, aliases resolved): the canvas holds no colour of its own */
export function themeColour(theme, key) {
  const f = path.resolve(path.dirname(fileURLToPath(import.meta.url)), `../../packages/map/style/themes/${theme}.json`);
  const v = JSON.parse(fs.readFileSync(f, 'utf8')).values;
  const get = (k, n = 0) => { const x = v[k]; return typeof x === 'string' && x.startsWith('@') && n < 8 ? get(x.slice(1), n + 1) : x; };
  return get(key);
}

let page = null;
async function canvas() { if (!page) page = await open('about:blank', { width: 400, height: 300 }); return page; }
export async function closeCompose() { if (page) { await page.close(); page = null; } }

/** `scale` < 1 and `jpeg` (a quality 0–1) make the smaller copies a report commits; the diff % is always full size */
export async function compose(left, right, { labels = ['prototype', 'app', 'diff'], gap = 16, theme = 'light', scale = 1, jpeg = null } = {}) {
  const b = await canvas();
  const bg = themeColour(theme, 'color.ui.card'), fg = themeColour(theme, 'color.ui.ink'), red = themeColour(theme, 'color.status.closed');
  const r = await b.evaluate(`(async () => {
    const load = (b64) => new Promise((ok, no) => { const i = new Image(); i.onload = () => ok(i); i.onerror = no; i.src = 'data:image/png;base64,' + b64; });
    const [L, A] = await Promise.all([load(${JSON.stringify(left.toString('base64'))}), load(${JSON.stringify(right.toString('base64'))})]);
    const ow = Math.min(L.width, A.width), oh = Math.min(L.height, A.height), G = ${gap} * 2, head = 56;
    const c = document.createElement('canvas'); c.width = L.width + A.width + ow + 2 * G; c.height = Math.max(L.height, A.height) + head;
    const x = c.getContext('2d'); x.fillStyle = ${JSON.stringify(bg)}; x.fillRect(0, 0, c.width, c.height);
    x.drawImage(L, 0, head); x.drawImage(A, L.width + G, head);
    const pick = (img) => { const k = document.createElement('canvas'); k.width = ow; k.height = oh; const kx = k.getContext('2d'); kx.drawImage(img, 0, 0); return kx.getImageData(0, 0, ow, oh).data; };
    const p = pick(L), q = pick(A), d = x.createImageData(ow, oh); let n = 0;
    const RED = [1, 3, 5].map((k) => parseInt(${JSON.stringify(red)}.slice(k, k + 2), 16));
    for (let i = 0; i < p.length; i += 4) { const dd = Math.max(Math.abs(p[i] - q[i]), Math.abs(p[i + 1] - q[i + 1]), Math.abs(p[i + 2] - q[i + 2]));
      if (dd > 24) { n++; d.data[i] = RED[0]; d.data[i + 1] = RED[1]; d.data[i + 2] = RED[2]; d.data[i + 3] = 255; }
      else { const g = 255 - (255 - (q[i] + q[i + 1] + q[i + 2]) / 3) * 0.25; d.data[i] = d.data[i + 1] = d.data[i + 2] = g; d.data[i + 3] = 255; } }
    x.putImageData(d, L.width + A.width + 2 * G, head);
    x.fillStyle = ${JSON.stringify(fg)}; x.font = '600 28px system-ui, sans-serif';
    ${JSON.stringify(labels)}.forEach((t, k) => x.fillText(t, [0, L.width + G, L.width + A.width + 2 * G][k] + 8, 38));
    let o = c; if (${scale} !== 1) { o = document.createElement('canvas'); o.width = Math.round(c.width * ${scale}); o.height = Math.round(c.height * ${scale});
      const ox = o.getContext('2d'); ox.imageSmoothingQuality = 'high'; ox.drawImage(c, 0, 0, o.width, o.height); }
    return { png: (${jpeg === null} ? o.toDataURL('image/png') : o.toDataURL('image/jpeg', ${jpeg ?? 0})).split(',')[1], pct: 100 * n / (ow * oh) };
  })()`);
  return { png: Buffer.from(r.png, 'base64'), pct: r.pct };
}

/** Stack PNGs vertically (a tall shot made of a header clip and a section clip) */
export async function stack(parts, theme = 'light') {
  const b = await canvas(), bg = themeColour(theme, 'color.ui.card');
  const r = await b.evaluate(`(async () => {
    const load = (b64) => new Promise((ok, no) => { const i = new Image(); i.onload = () => ok(i); i.onerror = no; i.src = 'data:image/png;base64,' + b64; });
    const ims = await Promise.all(${JSON.stringify(parts.map((p) => p.toString('base64')))}.map(load));
    const c = document.createElement('canvas'); c.width = Math.max(...ims.map((i) => i.width)); c.height = ims.reduce((a, i) => a + i.height, 0);
    const x = c.getContext('2d'); x.fillStyle = ${JSON.stringify(bg)}; x.fillRect(0, 0, c.width, c.height); let y = 0; for (const i of ims) { x.drawImage(i, 0, y); y += i.height; }
    return c.toDataURL('image/png').split(',')[1]; })()`);
  return Buffer.from(r, 'base64');
}
