#!/usr/bin/env node
// STORE SCREENSHOTS (T319) — the store listing's images from the web app, in one command:
//   node tools/store-shots.mjs [--theme light|dark] [--only play-phone,appstore-6.9] [--scenes 01,18] [--no-shoot] [--site URL] [--reuse] [--web URL]
// (T1076) THE SITE: with no --site / STORE_SHOTS_SITE this builds its OWN store-shots web export (EXPO_PUBLIC_STORE_SHOTS=1) into
// app/dist/web-store-shots and serves it on a local port for the run (store-shots/site.mjs; what is pinned and live is in its
// header); --reuse serves the export already there without rebuilding. An explicit site is used as given but REFUSED at the start
// unless it carries the store-shots marker (store-shots-build.json): a deployed site never does. Building is heavy: wrap the whole
// run, `scripts/heavy.sh store-shots 30 -- node app/tools/store-shots.mjs ...`.
// (T1072) SPEED, owner 2026-11-25 "pictures should be captured quicker": the raw sizes are shot in PARALLEL (one Chrome worker per size, at most
// 4, inside the one heavy.sh job); every fixed sleep that waited on a page is a wait on the page itself (play-shots.mjs `idle`: successive
// captures identical; `notch`: the scale bar steady); the content checks still run before every capture. FULL RUN: about 5 min per raw size
// before (20 min for four, one after the other), see the T1072 ticket for the measured time now. TO REDO ONE FRAME: `--reuse --scenes <nn> --only
// <targets>` (serves the export already built, shoots just that scene at the chosen targets' sizes, keeps the day's river pick, then reframes those targets; `--no-shoot`
// alone only reframes, e.g. after a crop change in store-shots.json).
// (T1072, owner 2026-11-25 "could use website too for most pictures much quicker") `--web <url>` (or STORE_SHOTS_WEB): shoot every scene that does NOT
// need the store-shots build from that DEPLOYED site (staging or production) and only the scenes that do (store-shots/scenes.mjs: 07-saved-spot, 20-help)
// from the local export (built, or --reuse). No scene is picked by name here: scenes.mjs is the one table. A scene that needs the build is still
// refused from a site without the marker (T1076). With --web and --only/--scenes asking for map/regs scenes alone, no export is built or served at
// all. A deployed site shows the RELEASED app, not this checkout: the console and report.json (`sources`) say which site each scene came from, with
// its entry bundle and Last-Modified, so a set shot that way is labelled as the release it shows.
// 0. store-shots/pick-river.mjs picks the day's river for the conditions split (owner 2026-11-19), into <raw>/river-pick.json.
// 1. play-shots.mjs takes the raw screenshots (headless Chrome at each store size's viewport and pixel ratio) for every raw
//    size the targets need, into <store>/graphics/raw/ (skipped with --no-shoot: the raw shots already there are framed again).
//    EVERY shot is the web app (owner 2026-11-19: "can all images be generated with the webapp?"): the site is a web export
//    built with EXPO_PUBLIC_STORE_SHOTS=1, which draws the phone's Spots and Help lines (`@app/ui-native` storeShots.ts); the
//    checks refuse the website's own "get the app" notice.
// 2. Each raw shot is framed at the store's exact pixel size: a title + one line in the app's own faces
//    (tools/seo/fonts, the self-hosted WOFF2) and colours (the theme files, no colour literal here), the screen below in a
//    rounded card. The page is drawn and captured by the same headless Chrome (a file:// page, dpr 1 = the store's pixels).
// A shot with `compose` is two (or more) captures of the same size stacked into one picture, each a crop (from..to, fractions of
// its height; the label bands share what is left, 3-7 % each, so nothing is rescaled) with a label pill on the join: the Conditions map in Change over the map in
// Forecast; a river's sections on the map over one section's rules. Each part is a real capture, none is drawn.
// A shot with `only` (target names) is in those targets' sets alone; T1072's set uses none: the SAME 7 frames, captions and order
// for every target.
// (T1072) THE RULES ARE ENFORCED HERE (store-shots/rules.mjs): the captions' words (no ranking, price, "free", download call, time-bound
// word, platform or store name), each target's exact size and image count, the caption block (title + line + the padding above the card)
// MEASURED in the rendered page at most 20 % of the height (Play's rule) in every target, and no alpha channel in any written PNG.
// The shot list (order, titles, lines) and the target sizes are tools/store-shots.json: the owner's to pick and reword.
// Output: <store>/graphics/store/<theme>/<target>/<nn>-<scene>.png. Refuses a missing raw shot (no silent gap).
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { open } from './regs-web/cdp.mjs';
import { resolveSite } from './store-shots/site.mjs';
import { needsBuild, NEEDS_STORE_BUILD } from './store-shots/scenes.mjs';
import { captionProblems, setProblems, hasAlpha, captionShare, captionProblem } from './store-shots/rules.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '..');
const STORE = process.env.STORE_SHOTS_DIR ? path.resolve(process.env.STORE_SHOTS_DIR) : path.resolve(APP, '../handoff/PLAN/app-integration/store/graphics');   // (T701) run.mjs points it at its own folder
const RAW = path.join(STORE, 'raw');
const args = process.argv.slice(2), flag = (k) => args.includes(k), val = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
const THEME = val('--theme', 'light');
// the server lives in this process, so a child that needs it must run async (spawnSync would block the server)
const runAsync = (cmd, argv, opts) => new Promise((ok, no) => { const c = spawn(cmd, argv, { stdio: 'inherit', ...opts }); c.once('error', no); c.once('exit', (code) => ok(code)); });
const ONLY = val('--only', null)?.split(',');
for (const a of args) if (a.startsWith('--') && !['--theme', '--only', '--scenes', '--no-shoot', '--site', '--reuse', '--web'].includes(a)) throw new Error(`store-shots: unknown flag ${a} (flags: --theme, --only, --scenes, --no-shoot, --site, --reuse, --web)`);
const WEB = val('--web', process.env.STORE_SHOTS_WEB ?? null);   // (T1072) a deployed site to shoot every scene that does not need the store-shots build from
if (WEB && !/^https?:\/\/[^/]+/.test(WEB)) throw new Error(`store-shots: --web ${WEB} is not an http(s) URL`);
const SCENES = val('--scenes', null)?.split(',');   // (T1072) shoot only these scenes (a name or its two-digit number); the other raw shots stay as they are
const { shots, targets } = JSON.parse(fs.readFileSync(path.join(HERE, 'store-shots.json'), 'utf8'));
const use = Object.entries(targets).filter(([k]) => !ONLY || ONLY.includes(k));
if (!use.length) throw new Error(`store-shots: no target named ${ONLY}; targets: ${Object.keys(targets).join(', ')}`);
for (const s of shots) for (const t of s.only ?? []) if (!targets[t]) throw new Error(`store-shots: ${s.scene}: only names no target ${t}`);
// each target's set, in the list's order; the shots any chosen target (or any target on one raw size) uses
const listFor = (name) => shots.filter((s) => !s.only || s.only.includes(name));
const usedBy = (pairs) => shots.filter((s) => pairs.some(([name]) => listFor(name).includes(s)));
const onRaw = (raw) => usedBy(use.filter(([, t]) => t.raw === raw));

const RAW_PRE = { phone: '', tablet7: 'tablet7-', tablet10: 'tablet10-', iphone69: 'iphone69-', iphone67: 'iphone67-', iphone55: 'iphone55-', ipad13: 'ipad13-' };
const rawFile = (raw, scene) => path.join(RAW, `${RAW_PRE[raw] ?? `${raw}-`}${scene}.png`);

// every capture the list needs: a shot's own scene, or each part of a composed one
const parts = (shot) => (shot.compose ?? [{ scene: shot.scene }]).map((p) => p.scene);
const allNeeded = [...new Set(usedBy(use).flatMap(parts))];
const needed = SCENES ? allNeeded.filter((sc) => SCENES.includes(sc) || SCENES.includes(sc.slice(0, 2))) : allNeeded;
if (SCENES && !needed.length) throw new Error(`store-shots: --scenes ${SCENES} matches no scene of the chosen targets (${allNeeded.join(', ')})`);
// 0. the day's river (the chart and the change map open it), 1. the raw shots, one play-shots run for every raw size wanted
if (!flag('--no-shoot')) {
  // (T1076) the site first: a site that is not a store-shots build is refused before anything else runs
  // (T1072) the export is built / served ONLY when a scene that needs it is asked for (store-shots/scenes.mjs); with --web every other scene comes from that deployed site
  const buildScenes = WEB ? needed.filter(needsBuild) : needed;
  const site = buildScenes.length ? await resolveSite({ site: val('--site', process.env.STORE_SHOTS_SITE ?? null), reuse: flag('--reuse') }) : { url: '', close: async () => {} };
  console.log(WEB ? `store-shots: ${needed.length - buildScenes.length} scenes from the deployed site ${WEB}; ${buildScenes.length ? `${buildScenes.join(', ')} from the store-shots export ${site.url}` : 'no export built or served'}` : `store-shots: every scene from the store-shots export ${site.url}`);
  try {
    // (T1072) the day's river: STORE_RIVER skips the picker (no network work); --reuse keeps the pick already in <raw> (the same river as the last run)
    const pickFile = path.join(RAW, 'river-pick.json');
    if (needed.some((sc) => /^1[123]-cond-/.test(sc)) && !process.env.STORE_RIVER && !(flag('--reuse') && fs.existsSync(pickFile))) {
      const p = spawnSync(process.execPath, [path.join(HERE, 'store-shots/pick-river.mjs'), RAW], { stdio: 'inherit', cwd: APP });
      if (p.status !== 0) throw new Error(`store-shots: pick-river failed (exit ${p.status}); nothing shot`);
    }
    // (T1072) ONE play-shots worker per raw size, in parallel (at most 4 Chrome workers; the sizes share nothing but the site and the
    // river pick), each writing its own report, merged below into play-shots-report.json. Each scene still runs its content check before capture.
    const sizes = [...new Set(use.map(([, t]) => t.raw))].slice(0, 4);
    const t0 = Date.now();
    const codes = await Promise.all(sizes.map(async (sz) => { const t1 = Date.now();
      const code = await runAsync(process.execPath, [path.join(HERE, 'play-shots.mjs'), RAW, '--only', needed.join(',')],
        { env: { ...process.env, SIZES: sz, STORE_SHOTS_SITE: site.url, STORE_SHOTS_WEB: WEB ?? '', PLAY_REPORT: `play-shots-report-${sz}.json` }, cwd: APP });
      console.log(`store-shots: raw size ${sz}: ${((Date.now() - t1) / 1000).toFixed(0)} s (exit ${code})`); return code; }));
    console.log(`store-shots: shooting ${needed.length} scenes x ${sizes.length} sizes took ${((Date.now() - t0) / 1000).toFixed(0)} s`);
    const merged = { scenes: {}, sources: {}, site: site.url || null, web: WEB };
    for (const sz of sizes) { const f = path.join(RAW, `play-shots-report-${sz}.json`); if (fs.existsSync(f)) { const r = JSON.parse(fs.readFileSync(f, 'utf8')); Object.assign(merged.scenes, r.scenes); Object.assign(merged.sources, r.sources); merged.pinAt = r.pinAt; fs.rmSync(f); } }
    fs.writeFileSync(path.join(RAW, 'play-shots-report.json'), JSON.stringify(merged, null, 2) + '\n');
    const bad = codes.filter((c) => c !== 0).length;
    if (bad) throw new Error(`store-shots: play-shots failed in ${bad} raw size(s) (see above); nothing framed`);
  } finally { await site.close(); }
}
// (T1072) the rules that need no browser: captions' words, each target's size and count
const ruleProblems = [...shots.flatMap(captionProblems), ...use.flatMap(([name, t]) => setProblems(name, t, listFor(name).length))];
if (ruleProblems.length) throw new Error(`store-shots: the set breaks store rules:\n  ${ruleProblems.join('\n  ')}`);
const missing = use.flatMap(([name, t]) => listFor(name).flatMap((s) => parts(s).map((sc) => rawFile(t.raw, sc)))).filter((f) => !fs.existsSync(f));
if (missing.length) throw new Error(`store-shots: raw shots missing (run without --no-shoot):\n  ${missing.join('\n  ')}`);

// 2. the frame: the app's colours and faces
const theme = JSON.parse(fs.readFileSync(path.join(APP, `packages/map/style/themes/${THEME}.json`), 'utf8')).values;
const c = (k) => { const v = theme[`color.ui.${k}`]; if (!v) throw new Error(`store-shots: theme ${THEME} has no color.ui.${k}`); return v; };
const font = (face) => { const f = path.join(APP, 'tools/seo/fonts', `${face}.woff2`); if (!fs.existsSync(f)) throw new Error(`store-shots: no font ${f}`); return f; };
const esc = (s) => s.replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[ch]);

// (T1072) the caption is kept inside Play's 20 % of the height: tight top padding and gaps, and a title size at which the longest
// title wraps to two lines on the narrowest target and still fits (measured in the render, never assumed: see `share` below)
function page(t, shot, img) {
  const u = Math.min(t.w, t.h * 0.5625) / 100;           // one unit = 1 % of a phone-shaped width: type scales with the image
  return `<!doctype html><meta charset="utf-8"><style>
@font-face { font-family: Head; src: url("file://${font('BricolageGrotesque_800ExtraBold')}") format("woff2"); }
@font-face { font-family: Body; src: url("file://${font('Archivo_500Medium')}") format("woff2"); }
html, body { margin: 0; width: ${t.w}px; height: ${t.h}px; overflow: hidden; background: ${c('wash')}; }
.wrap { box-sizing: border-box; height: 100%; display: flex; flex-direction: column; align-items: center; padding: ${5 * u}px ${6 * u}px 0; }
h1 { font: ${6.6 * u}px/1.06 Head; color: ${c('ink')}; margin: 0; text-align: center; letter-spacing: -0.01em; }
p { font: ${3.8 * u}px/1.25 Body; color: ${c('sub')}; margin: ${1.8 * u}px 0 ${3.6 * u}px; text-align: center; max-width: ${84 * u}px; }
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
  // stacked top to bottom, each part under a label band. (T1072) THE BAND IS WHAT IS LEFT: each part's crop (from..to, fractions of its height) is
  // chosen to start and end on a whole line, and the label bands share the rest of the height equally (3-7 % each, refused outside that), so
  // nothing is rescaled and no crop has to be bent to make the sum come out
  // (T319) a part's crop may differ per raw size (`at: { phone: [from, to], ... }`): the layouts differ between a phone and a tablet
  const fr = (p) => p.at?.[raw] ?? [p.from, p.to];
  const band = (1 - cs.reduce((a, c) => a + (fr(c)[1] - fr(c)[0]), 0)) / cs.length, bandPx = Math.round(band * H);
  if (band < 0.03 || band > 0.07) throw new Error(`store-shots: ${shot.scene} at ${raw}: the crops leave ${(band * 100).toFixed(1)} % of the height for each label band, it must be 3-7 %`);
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
let n = 0; const shares = {};   // target -> the widest caption block's share of the height
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
        // (T1072) Play's rule, measured on the render: the caption block's bottom edge is the card's top (title, line, and the padding above the card)
        const share = captionShare(await b.evaluate(`document.querySelector('.card').getBoundingClientRect().top`), t.h);
        shares[name] = Math.max(shares[name] ?? 0, share);
        const over = captionProblem(`${name} ${shot.scene}`, share); if (over) throw new Error(`store-shots: ${over}`);
        const { data } = await b.send('Page.captureScreenshot', { format: 'png' });
        if (hasAlpha(Buffer.from(data, 'base64'))) throw new Error(`store-shots: ${name} ${shot.scene}: the capture has an alpha channel (the App Store refuses it)`);
        const f = path.join(out, `${String(i + 1).padStart(2, '0')}-${shot.scene}.png`);
        fs.writeFileSync(f, Buffer.from(data, 'base64')); n++; console.log(f);
      } finally { await b.close(); }
    }
  }
} finally { fs.rmSync(tmp, { recursive: true, force: true }); }
for (const [name, v] of Object.entries(shares)) console.log(`store-shots: ${name}: widest caption block ${(v * 100).toFixed(1)} % of the height (Play allows 20 %)`);
console.log(`store-shots: ${n} images (${THEME}) in ${path.join(STORE, 'store', THEME)}`);
