#!/usr/bin/env node
// APP STORE CREATIVE ASSETS (T1134) — the product page HEADER and SEARCH RESULTS pictures, as stills (PNG) and short loops (MP4).
//   scripts/heavy.sh creative 30 -- node app/tools/store-shots/creative.mjs [--stills-only] [--reuse-map] [--web URL]
//   (on a lane another job holds: HEAVY_LOCK=$HOME/.claude/heavy-job-3.lock scripts/heavy.sh creative 30 -- node app/tools/store-shots/creative.mjs)
// Writes handoff/PLAN/app-integration/store/graphics/store/light/appstore-creative/:
//   header-3840x1646.png  header-3840x1646.mp4   (21:9)    search-3840x2560.png  search-3840x2560.mp4   (3:2)
// THE DESIGN (owner 2026-11-25: "i like the gif coho with the map behind cream. search result will use the same"): the app's COHO LOADER FISH,
// swimming, centred over the app's coloured MAP, faded behind a CREAM veil; the name and one line under the fish. Apple takes no GIF, so the
// animation is video; each slot also gets a still. THE PARTS, none drawn by hand:
//   - the fish: the owner's generator (tools/fish-loader/fish_loader.py, untouched), 120 frames over its own 4 s loop = 30 fps, in the
//     `color.ui.coho.ink` / `.red` tokens of the light theme; rendered at the size it is shown (no scaling);
//   - the map: ONE headless-Chrome capture of the DEPLOYED web app (default staging.canifishthis.ca; --web) at 2400x1700 CSS px, dpr 2, then the
//     centre 3840x2560 of it: the app's own chrome (search bar, day control, legend bar, credits) sits in the margin that is cut off. The map
//     credit is never hidden in the app: it lies outside the crop. (The header is the centre 3840x1646 band of the same crop.)
//   - the veil + words: one Chrome page per slot (fonts self-hosted in tools/seo/fonts), the cream of the icon ground (the coho ground token is
//     the white card colour, not cream), the name and `creativeAssetLine` from app/store-listing/listing.json;
//   - the stills and videos: ffmpeg overlays the fish frames on that one background. Video: H.264 High, yuv420p, bt709, 30 fps, 8 s (two loops, seamless), no audio.
//     (Apple's codec/bitrate/audio rules were not readable: this is a conservative choice, UNCONFIRMED.)
// APPLE'S SPEC, read 2026-11-25 via a summarised fetch (confirm on the App Store Connect upload screen):
//   https://developer.apple.com/help/app-store-connect/reference/app-information/creative-assets-specifications/
//   https://developer.apple.com/app-store/asset-best-practices/
//   header 3840x1646 PNG, art safe area 1646x661 at (1097,493); search 3:2, 1920x1280..3840x2560, art safe area 2168x1030 at (836,765) (at 3840x2560);
//   videos 30/60 fps, 5-30 s, .mov/.m4v/.mp4; no alpha; no prices, URLs, other platforms' names, awards.
// IT CHECKS ITS OWN OUTPUTS and fails by name: exact pixel sizes, no alpha in a PNG, ffprobe on a video (codec, pix_fmt, fps, duration, size, no audio),
// and that the fish frame + words lie inside the slot's art safe area (measured in the rendered page, not assumed).
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { open } from '../regs-web/cdp.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '../..'), ROOT = path.resolve(APP, '..');
const args = process.argv.slice(2), flag = (k) => args.includes(k), val = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
for (const a of args) if (a.startsWith('--') && !['--stills-only', '--reuse-map', '--web', '--check'].includes(a)) throw new Error(`creative: unknown flag ${a}`);
const WEB = val('--web', process.env.STORE_SHOTS_WEB ?? 'https://staging.canifishthis.ca').replace(/\/$/, '');
const GRAPHICS = path.join(ROOT, 'handoff/PLAN/app-integration/store/graphics');
export const OUT = path.join(GRAPHICS, 'store/light/appstore-creative');
const RAW = path.join(GRAPHICS, 'raw');   // ignored by git
const FFMPEG = process.env.FFMPEG ?? '/opt/homebrew/bin/ffmpeg', FFPROBE = process.env.FFPROBE ?? '/opt/homebrew/bin/ffprobe';
const PY = path.join(ROOT, '.venv/bin/python'), GENERATOR = path.join(APP, 'tools/fish-loader/fish_loader.py');

/** The slots: pixel size, Apple's art safe area [x, y, w, h], the fish frame's width, the type sizes (px), the still's frame. */
export const SLOTS = {
  header: { W: 3840, H: 1646, safe: [1097, 493, 1646, 661], fish: 960, name: 112, line: 54, gap: [14, 8] },
  search: { W: 3840, H: 2560, safe: [836, 765, 2168, 1030], fish: 1500, name: 150, line: 70, gap: [20, 12] },
};
const CAP_W = 4800, CAP_H = 3400;   // the map capture: 2400x1700 CSS px at dpr 2
const FPS = 30, LOOP_FRAMES = 120, SECONDS = 8, STILL_FRAME = 24;
export const FILES = { header: ['header-3840x1646.png', 'header-3840x1646.mp4'], search: ['search-3840x2560.png', 'search-3840x2560.mp4'] };

const theme = JSON.parse(fs.readFileSync(path.join(APP, 'packages/map/style/themes/light.json'), 'utf8')).values;
const INK = theme['color.ui.coho.ink'], RED = theme['color.ui.coho.red'];
if (!INK || !RED) throw new Error('creative: the light theme has no color.ui.coho.ink / .red');
const CREAM = fs.readFileSync(path.join(APP, 'apps/mobile/assets/icon/icon-ground.txt'), 'utf8').trim();
const listing = JSON.parse(fs.readFileSync(path.join(APP, 'store-listing/listing.json'), 'utf8'));
const NAME = listing.name, LINE = listing.creativeAssetLine;
if (!NAME || !LINE) throw new Error('creative: listing.json needs name and creativeAssetLine');
const font = (f) => `file://${path.join(APP, 'tools/seo/fonts', f + '.woff2')}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const run = (cmd, a, o = {}) => execFileSync(cmd, a, { stdio: ['ignore', 'inherit', 'inherit'], ...o });
const problems = [];
const fail = (m) => problems.push(m);

// ---- 1. the map: one capture of the deployed app, cropped to 3840x2560 ------------------------------------------------------------------
async function captureMap(file) {
  const rd = (f, re) => re.exec(fs.readFileSync(path.join(APP, f), 'utf8'))?.[1];
  const SEEN = `try { localStorage.setItem('canifishthis.firstRun.v1', ${JSON.stringify(rd('packages/core/src/menu/legal.ts', /FIRST_RUN_VERSION = "([^"]+)"/))});
    localStorage.setItem('canifishthis.walkthrough.v1', ${JSON.stringify(rd('packages/core/src/help.ts', /WALKTHROUGH_VERSION = "([^"]+)"/))});
    localStorage.setItem('canifishthis.inseasonSeen.v1', '["*"]'); } catch {}`;
  const b = await open(WEB + '/', { width: 2400, height: 1700, dpr: 2, init: SEEN });
  try {
    let ready = false;
    for (let t = 0; t < 120000 && !ready; t += 250) { ready = await b.evaluate(`!!document.querySelector('[data-testid=map-day]')`).catch(() => false); if (!ready) await sleep(250); }
    console.log(`creative: map page ${ready ? 'ready' : 'NOT ready after 120 s'}`);
    const shot = (o) => Promise.race([b.send('Page.captureScreenshot', o), new Promise((_, no) => setTimeout(() => no(new Error('creative: Chrome did not answer a capture in 30 s')), 30000))]);
    const settle = async () => { let prev = null, same = 0; const t0 = Date.now();   // three successive small captures identical: tiles and labels have stopped arriving
      while (same < 2 || Date.now() - t0 < 6000) { const { data } = await shot({ format: 'jpeg', quality: 25 }); same = data === prev ? same + 1 : 0; prev = data; if (Date.now() - t0 > 45000) break; await sleep(500); } };
    await settle();
    // the camera moves NORTH (the map is dragged down 420 CSS px, in small steps, released without momentum) so the grey out-of-coverage band south of the border lies below the crop
    const mv = (type, y) => b.send('Input.dispatchMouseEvent', { type, x: 1300, y, button: type === 'mouseMoved' ? 'none' : 'left', buttons: type === 'mouseReleased' ? 0 : 1, clickCount: 1 });
    await mv('mouseMoved', 300); await mv('mousePressed', 300); for (let i = 1; i <= 14; i++) { await mv('mouseMoved', 300 + 30 * i); await sleep(30); } await sleep(300); await mv('mouseReleased', 720);
    console.log('creative: map dragged'); await sleep(1500); await settle();
    console.log('creative: map settled');
    const text = await b.evaluate('document.body.innerText');
    if (/slow connection/i.test(text)) throw new Error('creative: the page shows the "Slow connection" notice; no capture');
    // JPEG q80 (q96 and PNG hang after a drag): a PNG capture at this size (4800x3400 device px) hangs headless Chrome (measured 2026-11-25); the veil hides any JPEG softness
    let data = null;   // Chrome sometimes never answers a full-size capture: ask again (a fresh ask has always been answered)
    for (let i = 0; i < 4 && !data; i++) data = (await shot({ format: 'jpeg', quality: 80 }).catch((e) => { console.log(e.message); return null; }))?.data ?? null;
    if (!data) throw new Error('creative: no full-size capture of the map after 4 tries');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, Buffer.from(data, 'base64'));
  } finally { await b.close(); }
}

// ---- 2. the fish: frames at the size shown ----------------------------------------------------------------------------------------------
function fishFrames(width, dir) {
  fs.mkdirSync(dir, { recursive: true });
  run(PY, ['-I', GENERATOR, '--sheet-only', '--format', 'png', '--width', String(width), '--cols', '12', '--frames', String(LOOP_FRAMES), '--fps', String(FPS),
    '--ink', INK, '--accent', RED, '--name', 'fish', '--out', dir]);
  const meta = JSON.parse(fs.readFileSync(path.join(dir, 'fish.json'), 'utf8'));
  if (meta.frames !== LOOP_FRAMES || meta.loop_ms !== 4000) throw new Error(`creative: the generator made ${JSON.stringify(meta)}, not a 4 s loop of ${LOOP_FRAMES} frames`);
  run(PY, ['-I', '-c', `import sys, json
from PIL import Image
d = sys.argv[1]; m = json.load(open(d + '/fish.json')); s = Image.open(d + '/fish_sheet.png'); fw, fh, c = m['frame_width'], m['frame_height'], m['columns']
for i in range(m['frames']):
    s.crop(((i % c) * fw, (i // c) * fh, (i % c + 1) * fw, (i // c + 1) * fh)).save('%s/f%03d.png' % (d, i))
`, dir]);
  fs.rmSync(path.join(dir, 'fish_sheet.png'));
  return { w: meta.frame_width, h: meta.frame_height };
}

// ---- 3. the words: two small transparent PNGs from Chrome (the page is small: Chrome hangs on a 3840-px screenshot of a big page) -----------
async function textPng(text, face, weight, size, opacity, file) {
  const html = `<!doctype html><meta charset="utf-8"><style>
@font-face { font-family: F; font-weight: ${weight}; src: url("${font(face)}") format("woff2"); }
html, body { margin: 0; background: transparent; } #t { display: inline-block; font: ${weight} ${size}px/1.15 F; padding: 0.08em 0.06em; white-space: nowrap; color: ${INK}; opacity: ${opacity}; ${weight === 800 ? 'letter-spacing: -0.015em;' : ''} }
</style><span id="t">${text}</span>`;
  const f = file.replace(/\.png$/, '.html'); fs.writeFileSync(f, html);
  const b = await open(`file://${f}`, { width: 3000, height: 400, dpr: 1 });
  try {
    await b.send('Emulation.setDefaultBackgroundColorOverride', { color: { r: 0, g: 0, b: 0, a: 0 } });
    await b.evaluate(`document.fonts.load('${weight} 20px F').then(() => document.fonts.ready)`);
    if (!(await b.evaluate(`document.fonts.check('${weight} 20px F')`))) fail(`the font ${face} did not load`);
    const r = await b.evaluate(`(() => { const e = document.getElementById('t').getBoundingClientRect(); return [e.left, e.top, e.width, e.height]; })()`);
    const w = Math.ceil(r[2]), h = Math.ceil(r[3]);
    const { data } = await b.send('Page.captureScreenshot', { format: 'png', clip: { x: r[0], y: r[1], width: w, height: h, scale: 1 }, fromSurface: true });
    fs.writeFileSync(file, Buffer.from(data, 'base64'));
    return { w, h };
  } finally { await b.close(); }
}

// ---- 3b. the background (map crop, desaturated, cream veil, words) per slot, by ffmpeg; the boxes are the layout's own numbers ---------------
async function background(slot, mapFile, fishFrame, dir, file) {
  const S = SLOTS[slot], [sx, sy, sw, sh] = S.safe;
  const nm = await textPng(NAME, 'BricolageGrotesque_800ExtraBold', 800, S.name, 1, path.join(dir, `name-${slot}.png`));
  const ln = await textPng(LINE, 'Archivo_500Medium', 500, S.line, 0.85, path.join(dir, `line-${slot}.png`));
  const total = fishFrame.h + S.gap[0] + nm.h + S.gap[1] + ln.h, y0 = sy + (sh - total) / 2, cx = sx + sw / 2;
  const fish = [cx - fishFrame.w / 2, y0], name = [cx - nm.w / 2, y0 + fishFrame.h + S.gap[0]], line = [cx - ln.w / 2, name[1] + nm.h + S.gap[1]];
  const box = { fish: [fish[0], fish[1], fish[0] + fishFrame.w, fish[1] + fishFrame.h], name: [name[0], name[1], name[0] + nm.w, name[1] + nm.h], line: [line[0], line[1], line[0] + ln.w, line[1] + ln.h] };
  const cropY = Math.round((CAP_H - S.H) / 2), cream = '0x' + CREAM.replace('#', '');
  run(FFMPEG, ['-y', '-loglevel', 'error', '-i', mapFile, '-i', path.join(dir, `name-${slot}.png`), '-i', path.join(dir, `line-${slot}.png`), '-filter_complex',
    `[0:v]crop=${S.W}:${S.H}:${(CAP_W - S.W) / 2}:${cropY},eq=saturation=0.75:contrast=0.9,drawbox=x=0:y=0:w=iw:h=ih:color=${cream}@0.70:t=fill[m];` +
    `[m][1:v]overlay=${Math.round(name[0])}:${Math.round(name[1])}[a];[a][2:v]overlay=${Math.round(line[0])}:${Math.round(line[1])},format=rgb24`, '-frames:v', '1', file]);
  return box;
}

// ---- 4. ffmpeg: still and video ---------------------------------------------------------------------------------------------------------
const OVER = (x, y) => `[0:v][1:v]overlay=${Math.round(x)}:${Math.round(y)}:format=auto`;
function stillPng(bg, fishDir, box, file) {
  run(FFMPEG, ['-y', '-loglevel', 'error', '-i', bg, '-i', path.join(fishDir, `f${String(STILL_FRAME).padStart(3, '0')}.png`), '-filter_complex', `${OVER(box.fish[0], box.fish[1])},format=rgb24`, '-frames:v', '1', file]);
}
function video(bg, fishDir, box, file) {
  run(FFMPEG, ['-y', '-loglevel', 'error', '-loop', '1', '-framerate', String(FPS), '-i', bg, '-stream_loop', '1', '-framerate', String(FPS), '-i', path.join(fishDir, 'f%03d.png'),
    '-filter_complex', `${OVER(box.fish[0], box.fish[1])}:shortest=1,scale=out_color_matrix=bt709:out_range=tv,format=yuv420p`,
    '-c:v', 'libx264', '-profile:v', 'high', '-preset', 'slow', '-crf', '19', '-r', String(FPS), '-t', String(SECONDS), '-colorspace', 'bt709', '-color_primaries', 'bt709', '-color_trc', 'bt709',
    '-movflags', '+faststart', '-an', file]);
}

// ---- 5. the checks ----------------------------------------------------------------------------------------------------------------------
const pngSize = (f) => { const b = fs.readFileSync(f); return { w: b.readUInt32BE(16), h: b.readUInt32BE(20), colorType: b[25] }; };
function checkPng(slot, file) {
  const S = SLOTS[slot]; if (!fs.existsSync(file)) return fail(`${slot}: ${path.basename(file)} was not written`);
  const p = pngSize(file);
  if (p.w !== S.W || p.h !== S.H) fail(`${slot} still: ${p.w}x${p.h}, not ${S.W}x${S.H}`);
  if (p.colorType === 4 || p.colorType === 6) fail(`${slot} still: has an alpha channel (PNG colour type ${p.colorType})`);
}
function checkVideo(slot, file) {
  const S = SLOTS[slot]; if (!fs.existsSync(file)) return fail(`${slot}: ${path.basename(file)} was not written`);
  const j = JSON.parse(execFileSync(FFPROBE, ['-v', 'error', '-print_format', 'json', '-show_streams', '-show_format', file], { encoding: 'utf8' }));
  const v = j.streams.find((s) => s.codec_type === 'video');
  if (!v) return fail(`${slot} video: no video stream`);
  if (v.codec_name !== 'h264') fail(`${slot} video: codec ${v.codec_name}, not h264`);
  if (v.pix_fmt !== 'yuv420p') fail(`${slot} video: pixel format ${v.pix_fmt} (alpha or not yuv420p)`);
  if (v.width !== S.W || v.height !== S.H) fail(`${slot} video: ${v.width}x${v.height}, not ${S.W}x${S.H}`);
  const [n, d] = v.avg_frame_rate.split('/').map(Number); const fps = n / d;
  if (![30, 60].includes(Math.round(fps * 1000) / 1000)) fail(`${slot} video: ${v.avg_frame_rate} fps, not 30 or 60`);
  const dur = Number(j.format.duration); if (!(dur >= 5 && dur <= 30)) fail(`${slot} video: ${dur} s, outside 5-30 s`);
  if (j.streams.some((s) => s.codec_type === 'audio')) fail(`${slot} video: has an audio track`);
  if (!/\.(mp4|m4v|mov)$/.test(file)) fail(`${slot} video: extension`);
  return { codec: `${v.codec_name} ${v.profile}`, pix: v.pix_fmt, size: `${v.width}x${v.height}`, fps: v.avg_frame_rate, dur, frames: v.nb_frames };
}
function checkSafe(slot, box) {
  const [sx, sy, sw, sh] = SLOTS[slot].safe;
  for (const k of ['fish', 'name', 'line']) { const [l, t, r, bt] = box[k];
    if (l < sx - 0.5 || t < sy - 0.5 || r > sx + sw + 0.5 || bt > sy + sh + 0.5) fail(`${slot}: the ${k} box ${[l, t, r, bt].map(Math.round)} is outside the art safe area ${[sx, sy, sx + sw, sy + sh]}`); }
}

// ---- main -------------------------------------------------------------------------------------------------------------------------------
export async function main() {
  fs.mkdirSync(OUT, { recursive: true }); fs.mkdirSync(RAW, { recursive: true });
  const mapFile = path.join(RAW, 'creative-map.jpg');
  if (!flag('--reuse-map') || !fs.existsSync(mapFile)) { console.log(`creative: capturing the map from ${WEB}`); await captureMap(mapFile); }
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'creative-'));
  const report = {};
  try {
    for (const slot of Object.keys(SLOTS)) {
      const fishDir = path.join(tmp, `fish-${slot}`), bg = path.join(tmp, `bg-${slot}.png`);
      const frame = fishFrames(SLOTS[slot].fish, fishDir);
      const box = await background(slot, mapFile, frame, tmp, bg);
      checkSafe(slot, box);
      const [pngName, mp4Name] = FILES[slot];
      stillPng(bg, fishDir, box, path.join(OUT, pngName)); checkPng(slot, path.join(OUT, pngName));
      if (!flag('--stills-only')) { console.log(`creative: encoding ${mp4Name}`); video(bg, fishDir, box, path.join(OUT, mp4Name)); report[slot] = checkVideo(slot, path.join(OUT, mp4Name)); }
      report[`${slot}-box`] = box;
    }
  } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
  console.log(JSON.stringify(report, null, 1));
  if (problems.length) { console.error('creative: FAILED\n  ' + problems.join('\n  ')); process.exit(1); }
  console.log('creative: ok, written to ' + OUT);
}
if (import.meta.url === `file://${process.argv[1]}`) await main();
