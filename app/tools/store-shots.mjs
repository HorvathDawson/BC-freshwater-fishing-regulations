#!/usr/bin/env node
// STORE SCREENSHOTS (T319) — the store listing's images from the CURRENT staging app, in one command:
//   node tools/store-shots.mjs [--theme light|dark] [--only play-phone,appstore-6.9] [--no-shoot]
// 1. play-shots.mjs takes the raw screenshots (headless Chrome on staging) for every raw size the targets need, into
//    <store>/graphics/raw/ (skipped with --no-shoot: the raw shots already there are framed again).
// 2. Each raw shot is framed at the store's exact pixel size: a title + one line in the app's own faces
//    (tools/seo/fonts, the self-hosted WOFF2) and colours (the theme files, no colour literal here), the screen below in a
//    rounded card. The page is drawn and captured by the same headless Chrome (a file:// page, dpr 1 = the store's pixels).
// A shot with `compose` is two (or more) captures of the same size stacked into one picture, each a crop (from..to, fractions of
// its height, summing to 1 so nothing is rescaled) with a label pill on the join: the Conditions map in Change over the map in
// Forecast; a river's sections on the map over one section's rules. Each part is a real capture, none is drawn.
// A shot with `"device": true` is a phone-only screen (spots, the app's Help page): it is not shot here but read from
// <raw>/device-<scene>.png, which store-shots/device-ios.mjs writes from the iOS simulator; it is cropped (its status bar,
// `cropTop` as a fraction of its height, then to each raw size's shape, from the top) so every card has the same shape.
// A shot with `only` (target names) is in those targets' sets alone (Play takes at most 8 phone shots, the App Store 10); each
// set is numbered in its own order.
// The shot list (order, titles, lines) and the target sizes are tools/store-shots.json: the owner's to pick and reword.
// Output: <store>/graphics/store/<theme>/<target>/<nn>-<scene>.png. Refuses a missing raw shot (no silent gap).
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { PNG } from 'pngjs';
import { open } from './regs-web/cdp.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '..');
const STORE = process.env.STORE_SHOTS_DIR ? path.resolve(process.env.STORE_SHOTS_DIR) : path.resolve(APP, '../handoff/PLAN/app-integration/store/graphics');   // (T701) run.mjs points it at its own folder
const RAW = path.join(STORE, 'raw');
const args = process.argv.slice(2), flag = (k) => args.includes(k), val = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
const THEME = val('--theme', 'light');
const ONLY = val('--only', null)?.split(',');
const { shots, targets } = JSON.parse(fs.readFileSync(path.join(HERE, 'store-shots.json'), 'utf8'));
const use = Object.entries(targets).filter(([k]) => !ONLY || ONLY.includes(k));
if (!use.length) throw new Error(`store-shots: no target named ${ONLY}; targets: ${Object.keys(targets).join(', ')}`);
for (const s of shots) for (const t of s.only ?? []) if (!targets[t]) throw new Error(`store-shots: ${s.scene}: only names no target ${t}`);
// each target's set, in the list's order; the shots any chosen target (or any target on one raw size) uses
const listFor = (name) => shots.filter((s) => !s.only || s.only.includes(name));
const usedBy = (pairs) => shots.filter((s) => pairs.some(([name]) => listFor(name).includes(s)));
const onRaw = (raw) => usedBy(use.filter(([, t]) => t.raw === raw));

const RAW_PRE = { phone: '', tablet7: 'tablet7-', tablet10: 'tablet10-', iphone69: 'iphone69-', iphone67: 'iphone67-', iphone55: 'iphone55-', ipad13: 'ipad13-' };
// each raw size's shape (width / height, play-shots.mjs SIZES): a device shot is cropped to it
const RAW_SHAPE = { phone: 360 / 640, tablet7: 600 / 960, tablet10: 800 / 1280, iphone69: 440 / 956, iphone67: 430 / 932, iphone55: 414 / 736, ipad13: 1024 / 1366 };
const rawFile = (raw, scene) => path.join(RAW, `${RAW_PRE[raw] ?? `${raw}-`}${scene}.png`);

// every capture the list needs: a shot's own scene, or each part of a composed one; device shots are the simulator's
const parts = (shot) => (shot.compose ?? [{ scene: shot.scene }]).map((p) => p.scene);
// a device capture is a whole shot (`device: true`) or one part of a composed one (the part's `device: true`)
const deviceScenes = (list) => list.flatMap((s) => (s.device ? [s] : (s.compose ?? []).filter((p) => p.device)));
const needed = [...new Set(usedBy(use).filter((s) => !s.device).flatMap((s) => (s.compose ?? [s]).filter((p) => !p.device).map((p) => p.scene)))];
const deviceFile = (scene) => path.join(RAW, `device-${scene}.png`);
// 1. the raw shots, one play-shots run for every raw size wanted
if (!flag('--no-shoot')) {
  const sizes = [...new Set(use.map(([, t]) => t.raw))].join(',');
  const r = spawnSync(process.execPath, [path.join(HERE, 'play-shots.mjs'), RAW, '--only', needed.join(',')],
    { stdio: 'inherit', env: { ...process.env, SIZES: sizes }, cwd: APP });
  if (r.status !== 0) throw new Error(`store-shots: play-shots failed (exit ${r.status}); nothing framed`);
}
const missingDevice = deviceScenes(usedBy(use)).filter((s) => !fs.existsSync(deviceFile(s.scene))).map((s) => deviceFile(s.scene));
if (missingDevice.length) throw new Error(`store-shots: device shots missing (store-shots/device-ios.mjs makes them):\n  ${missingDevice.join('\n  ')}`);
// a device shot, cropped to each raw size's shape: its status bar off the top, then as tall as the shape allows
for (const raw of new Set(use.map(([, t]) => t.raw))) for (const s of deviceScenes(onRaw(raw))) {
  const src = PNG.sync.read(fs.readFileSync(deviceFile(s.scene)));
  const top = Math.round((s.cropTop ?? 0) * src.height), h = Math.min(src.height - top, Math.round(src.width / RAW_SHAPE[raw]));
  const out = new PNG({ width: src.width, height: h });
  PNG.bitblt(src, out, 0, top, src.width, h, 0, 0);
  fs.writeFileSync(rawFile(raw, s.scene), PNG.sync.write(out));
}
const missing = use.flatMap(([name, t]) => listFor(name).flatMap((s) => parts(s).map((sc) => rawFile(t.raw, sc)))).filter((f) => !fs.existsSync(f));
if (missing.length) throw new Error(`store-shots: raw shots missing (run without --no-shoot):\n  ${missing.join('\n  ')}`);

// 2. the frame: the app's colours and faces
const theme = JSON.parse(fs.readFileSync(path.join(APP, `packages/map/style/themes/${THEME}.json`), 'utf8')).values;
const c = (k) => { const v = theme[`color.ui.${k}`]; if (!v) throw new Error(`store-shots: theme ${THEME} has no color.ui.${k}`); return v; };
const font = (face) => { const f = path.join(APP, 'tools/seo/fonts', `${face}.woff2`); if (!fs.existsSync(f)) throw new Error(`store-shots: no font ${f}`); return f; };
const esc = (s) => s.replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[ch]);

function page(t, shot, img) {
  const u = Math.min(t.w, t.h * 0.5625) / 100;           // one unit = 1 % of a phone-shaped width: type scales with the image
  return `<!doctype html><meta charset="utf-8"><style>
@font-face { font-family: Head; src: url("file://${font('BricolageGrotesque_800ExtraBold')}") format("woff2"); }
@font-face { font-family: Body; src: url("file://${font('Archivo_500Medium')}") format("woff2"); }
html, body { margin: 0; width: ${t.w}px; height: ${t.h}px; overflow: hidden; background: ${c('wash')}; }
.wrap { box-sizing: border-box; height: 100%; display: flex; flex-direction: column; align-items: center; padding: ${7 * u}px ${6 * u}px 0; }
h1 { font: ${7.4 * u}px/1.08 Head; color: ${c('ink')}; margin: 0; text-align: center; letter-spacing: -0.01em; }
p { font: ${3.9 * u}px/1.3 Body; color: ${c('sub')}; margin: ${2.2 * u}px 0 ${5.5 * u}px; text-align: center; max-width: ${80 * u}px; }
.card { flex: 1; min-height: 0; display: flex; align-items: flex-start; }
/* the app's own look: square corners (theme radii are 0) and the hard offset shadow */
img { display: block; max-width: ${86 * u}px; max-height: 100%; width: auto; height: auto; border-radius: 0;
      border: ${0.4 * u}px solid ${c('ink')}; border-bottom: 0; box-shadow: ${1.4 * u}px ${1.4 * u}px 0 ${c('ink')}; }
</style><div class="wrap"><h1>${esc(shot.title)}</h1><p>${esc(shot.line)}</p><div class="card"><img src="file://${img}"></div></div>`;
}

// the composed raws: the parts stacked at the raw's own pixel size, written where the framing step looks for the shot's raw
const pngSize = (f) => { const b = fs.readFileSync(f); return [b.readUInt32BE(16), b.readUInt32BE(20)]; };
async function composeRaw(raw, shot) {
  const cs = shot.compose;
  const files = cs.map((c) => rawFile(raw, c.scene)), [W, H] = pngSize(files[0]);
  for (const f of files) { const [w, h] = pngSize(f); if (w !== W || h !== H) throw new Error(`store-shots: ${shot.scene}: ${f} is ${w}x${h}, the first part ${W}x${H}`); }
  const u = W / 100, ink = c('ink'), wash = c('wash');
  // stacked top to bottom, each part under a label band (`label`); crops are fractions of a part's height, so the crops plus
  // the bands (`band`, a fraction of the height each) must sum to 1 and nothing is rescaled
  const band = shot.band ?? 0.05, bandPx = Math.round(band * H);
  // (T319) a part's crop may differ per raw size (`at: { phone: [from, to], ... }`): the layouts differ between a phone and a tablet
  const fr = (p) => p.at?.[raw] ?? [p.from, p.to];
  if (Math.abs(cs.reduce((a, c) => a + (fr(c)[1] - fr(c)[0]), 0) + band * cs.length - 1) > 0.001) throw new Error(`store-shots: ${shot.scene}: the compose crops plus the bands must sum to 1`);
  let y = 0;
  const divs = cs.map((p, i) => { const last = i === cs.length - 1, n = last ? H - y - bandPx : Math.round((fr(p)[1] - fr(p)[0]) * H), top = y + bandPx; y = top + n;
    return `<div class="band" style="top:${top - bandPx}px">${esc(p.label ?? '')}</div><div style="position:absolute;left:0;top:${top}px;width:${W}px;height:${n}px;overflow:hidden"><img src="file://${files[i]}" style="position:absolute;left:0;top:${-Math.round(fr(p)[0] * H)}px;width:${W}px;height:${H}px"></div>`; });
  const html = `<!doctype html><meta charset="utf-8"><style>
@font-face { font-family: Body; src: url("file://${font('Archivo_500Medium')}") format("woff2"); }
html, body { margin: 0; width: ${W}px; height: ${H}px; overflow: hidden; background: ${wash}; position: relative; }
.band { position: absolute; left: 0; width: ${W}px; height: ${bandPx}px; background: ${ink}; color: ${wash}; font: ${3.8 * u}px/${bandPx}px Body; text-align: center; white-space: nowrap; }
</style>${divs.join('')}`;
  const tmpf = path.join(tmp, `compose-${raw}-${shot.scene}.html`); fs.writeFileSync(tmpf, html);
  const b = await open(`file://${tmpf}`, { width: W, height: H, dpr: 1 });
  try {
    await b.evaluate(`document.fonts.ready.then(() => Promise.all([...document.images].map((i) => i.decode())))`);
    if (!(await b.evaluate(`[...document.fonts].filter((f) => f.status === 'loaded').length === 1 && [...document.images].every((i) => i.naturalWidth > 0)`))) throw new Error(`store-shots: ${shot.scene}: a font or a part did not load`);
    const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
    fs.writeFileSync(rawFile(raw, shot.scene), Buffer.from(data, 'base64'));
  } finally { await b.close(); }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'store-shots-'));
let n = 0;
try {
  for (const raw of new Set(use.map(([, t]) => t.raw))) for (const shot of onRaw(raw)) if (shot.compose) await composeRaw(raw, shot);
  for (const [name, t] of use) {
    const out = path.join(STORE, 'store', THEME, name); fs.mkdirSync(out, { recursive: true });
    fs.rmSync(out, { recursive: true, force: true }); fs.mkdirSync(out, { recursive: true });   // a shot left out of a set must not linger
    for (const [i, shot] of listFor(name).entries()) {
      const html = path.join(tmp, `${name}-${i}.html`); fs.writeFileSync(html, page(t, shot, rawFile(t.raw, shot.scene)));
      const b = await open(`file://${html}`, { width: t.w, height: t.h, dpr: 1 });
      try {
        await b.evaluate(`document.fonts.ready.then(() => Promise.all([...document.images].map((i) => i.decode())))`);
        const ok = await b.evaluate(`[...document.fonts].filter((f) => f.status === 'loaded').length === 2 && [...document.images].every((i) => i.naturalWidth > 0)`);
        if (!ok) throw new Error(`store-shots: ${name} ${shot.scene}: a font or the screenshot did not load`);
        const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
        const f = path.join(out, `${String(i + 1).padStart(2, '0')}-${shot.scene}.png`);
        fs.writeFileSync(f, Buffer.from(data, 'base64')); n++; console.log(f);
      } finally { await b.close(); }
    }
  }
} finally { fs.rmSync(tmp, { recursive: true, force: true }); }
console.log(`store-shots: ${n} images (${THEME}) in ${path.join(STORE, 'store', THEME)}`);
