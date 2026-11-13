// STORE VIDEO CHECKS (T709): the pure half of the store preview video — what each scene must show on the device before and
// after it is recorded (read from the screen's nodes, never guessed), how the recorded clips become one timeline, and the
// finished file's specs. Nothing here touches a device or a file; `drive.mjs` and `run.mjs` call it, `checks.test.mjs` pins it.

// THE COLOURS are the app's (tools/check-colours.mjs: the palette lives in the token files and nowhere else), read from
// the generated light theme as tools/build-spinner.mjs does: the caption pill is `color.ui.ink` with `color.ui.card`
// text; the end card is `color.ui.card` with the name in `color.ui.ink` and the line in `color.ui.sub`.
import { readFileSync } from 'node:fs';
const UI = JSON.parse(readFileSync(new URL('../../packages/map/style/style.meta.json', import.meta.url), 'utf8')).themes.light;
const INK = UI['color.ui.ink'], CARD = UI['color.ui.card'], SUB = UI['color.ui.sub'];
for (const [k, v] of Object.entries({ INK, CARD, SUB })) if (!v) throw new Error(`store-video: the light theme has no ${k} colour`);

// ---------- the storyboard (T709 "Proposed 30 s storyboard", as built) ----------
// Captions say only what the fact sheet (handoff/PLAN/play/listing.md) backs; the line each one rests on is beside it.
// `capY`: the caption's centre as a share of the frame's height, kept inside the stores' safe band (CAPTION_BAND) and off
// the content the scene is about. `lead`: seconds kept before the scene's first touch; `hold`/`minHold`: seconds kept after
// its end state was seen (the longest and the shortest the timeline may use).
export const SCENES = [
  { id: 'map-tap', caption: "Tap a river.\nSee today's rules.", capY: 0.82, lead: 1.2, hold: 3.0, minHold: 2.0, captionAfterDone: true,
    backs: 'Map: coloured by what the regulations say for the chosen day; Regulations: tapping a water shows the rules for it' },
  { id: 'day', caption: 'Pick a day,\nsee its rules', capY: 0.30, lead: 0.5, hold: 2.0, minHold: 1.4,   // capY above the day sheet: the month and the day tapped stay clear
    backs: 'Map: coloured for the chosen day; a legend and a day picker sit on the map' },
  { id: 'salmon', caption: 'Salmon openings\nfrom DFO', capY: 0.72, lead: 0.5, hold: 2.2, minHold: 1.6,
    backs: 'Salmon: DFO is the salmon feed (federal salmon limits and closures); a salmon colouring of the map' },
  { id: 'conditions', caption: 'River flow against\nother years', capY: 0.82, lead: 0.4, hold: 2.2, minHold: 1.6,
    backs: "Flows and conditions: flow from Water Survey of Canada gauges, with how the day compares with other years" },
  { id: 'spot', caption: 'Save spots\nwith your notes', capY: 0.70, lead: 0.4, hold: 2.0, minHold: 1.5,
    backs: 'Saved spots: save a spot with notes and photos; notes stay on the phone' },
];
export const END_CARD = { seconds: 2.5, name: 'Can I Fish This?', line: 'BC freshwater fishing regulations, on a map',
  backs: 'App name (store name "Can I Fish This?"); What the app is: a map of BC freshwater fishing regulations' };
/** The caption pill must sit inside this band of the frame's height: the stores lay their own controls over the edges. */
export const CAPTION_BAND = [0.12, 0.88];
export const SPEC = { w: 864, h: 1920, fps: 30, minS: 25, maxS: 30, maxBytes: 500 * 1024 * 1024 };

// ---------- the screen ----------
const text = (ns) => ns.map((n) => `${n.text}\n${n.desc}`).join('\n');
const shown = (n) => !!n && n.box.x1 > n.box.x0 && n.box.y1 > n.box.y0;
const byId = (ns, id) => ns.find((n) => n.id === id && shown(n)) ?? null;
const need = (cond, msg) => (cond ? [] : [msg]);
const BAD = /still loading|something went wrong|couldn.t load|could not load|could not be loaded|unable to load/i;

/** The map in trout mode on `day` ("today" or e.g. "Wednesday, June 10"), drawn, with its legend. */
export function mapProblems(ns, { day = 'today', mode = 'trout' } = {}) {
  const t = text(ns);
  const pick = ns.find((n) => n.id === 'map-day-pick');
  return [
    ...need(ns.some((n) => /^Showing a Map/.test(n.desc)), 'no map on screen'),
    ...need(pick && new RegExp(`Map coloured for ${day}\\b`, 'i').test(pick.desc), `the map is not coloured for ${day} (${pick?.desc ?? 'no day picker'})`),
    ...(mode === 'trout' ? need(/Own rules/.test(t) && !/Keep salmon/.test(t), 'the map is not in trout mode (legend)')
      : need(/Keep salmon/.test(t) && /DFO rules checked/.test(t), 'the map is not in salmon mode (no "Keep salmon" / "DFO rules checked" legend)')),
  ];
}

/** A water's Regulations page for `water`: its title, the salmon (DFO) block and the game-fish answer. */
export function regsProblems(ns, water) {
  const t = text(ns);
  return [
    ...need(byId(ns, 'water-title') && new RegExp(`^${water}$`, 'm').test(t), `the page is not ${water}'s`),
    ...need(byId(ns, 'regs-view'), 'the Regulations view is not showing'),
    ...need(/SALMON \(DFO\)/.test(t), 'no SALMON (DFO) block'),
    ...need(/GAME FISH/.test(t), 'no GAME FISH (B.C. RULES) block'),
    ...need(/(✓ ?Open|✕ ?Closed|^Open$|^Closed$)/m.test(t), 'no open/closed answer'),
  ];
}

/** The Conditions view with the gauge card's chart: the gauge, a flow reading, the other-years bands, the release. */
export function conditionsProblems(ns, { water, release, screenH }) {
  const t = text(ns);
  const legend = byId(ns, 'chart-legend');
  return [
    ...need(byId(ns, 'water-view-conditions') && new RegExp(`^${water}$`, 'm').test(t), `not ${water}'s Conditions`),
    ...need(byId(ns, 'gauge-card'), 'no gauge card'),
    ...need(byId(ns, 'gauge-title') && byId(ns, 'gauge-title').box.y1 - byId(ns, 'gauge-title').box.y0 > 20, "the gauge's name is not whole on screen (scrolled under the header)"),
    ...need(/m³\/s/.test(t), 'no flow reading (m³/s)'),
    ...need(/middle half/.test(t) && /median/.test(t), 'no other-years bands (middle half / median) on the chart'),
    ...need(legend && legend.box.y1 <= screenH * 0.9, `the chart's legend is not whole on screen (${legend ? `y ${legend.box.y0}-${legend.box.y1}` : 'absent'})`),
    ...need(new RegExp(`Data release ${release}`).test(t), `the card does not name the data release ${release}`),
  ];
}

/** A saved spot's page scrolled to its notes: the title is the spot's and the note is whole on screen. */
export function spotNotesProblems(ns, { title, note, screenH }) {
  const added = byId(ns, 'spot-added');
  const line = ns.find((n) => n.text?.length >= 15 && note.startsWith(n.text.replace(/…$/, '').slice(0, 20)));
  return [
    ...need(added, 'the spot\'s notes block (spot-added) is not on screen'),
    ...need(shown(line), `the note "${note.slice(0, 30)}…" is not on screen`),
    ...need(!line || (line.box.y0 >= screenH * 0.1 && line.box.y1 <= screenH * 0.62), `the note sits at y ${line?.box.y0}-${line?.box.y1}: not in the top part the caption leaves clear`),
  ];
}

/** Every scene: no loading or error words on the screen. */
export const badWords = (ns) => { const m = text(ns).match(BAD); return m ? [`the screen says "${m[0]}"`] : []; };

// ---------- the timeline ----------
/**
 * The clip's own clock: `screenrecord` writes a frame only when the screen changes, and starts a beat after the host asked.
 * Each scene waits with the screen still before its first touch, so the first frame after a still gap of at least `gap` s
 * near the host's estimate IS the touch (the touch dot, `show_touches`). Returns host-seconds minus clip-seconds (≥ 0), or
 * throws: a scene that cannot be placed on its clip's clock is not cut by guesswork.
 */
export function clipOffset(pts, hostTap, { gap = 0.45, before = 1.6, after = 0.3 } = {}) {
  const t0 = pts[0] ?? 0;
  const rel = pts.map((p) => p - t0);
  for (let i = 1; i < rel.length; i++) {
    if (rel[i] < hostTap - before || rel[i] > hostTap + after) continue;
    if (rel[i] - rel[i - 1] >= gap) return hostTap - rel[i];
  }
  throw new Error(`no still-then-touch frame in the clip within ${before} s before / ${after} s after the host's tap at ${hostTap.toFixed(2)} s (frames there: ${rel.filter((p) => p > hostTap - before - 1 && p < hostTap + after + 1).map((p) => p.toFixed(2)).join(', ') || 'none'})`);
}

/**
 * When the screen came to rest after the scene's last touch: the first frame at or after `fromV` (clip seconds) that is
 * followed by `gap` s of no change (or is the clip's last). That is the end state as the viewer sees it, not when the
 * host's slower screen check noticed it.
 */
export function settleAfter(pts, fromV, { gap = 0.6 } = {}) {
  const t0 = pts[0] ?? 0;
  const rel = pts.map((p) => p - t0);
  for (let i = 0; i < rel.length; i++) {
    if (rel[i] < fromV) continue;
    if (i === rel.length - 1 || rel[i + 1] - rel[i] >= gap) return rel[i];
  }
  throw new Error(`the screen never came to rest after ${fromV.toFixed(2)} s on the clip`);
}

/**
 * The cut: per scene [start, end] on its clip (seconds) from the touch and the end state's host times. Holds start at
 * `hold`; when the sum passes SPEC.maxS they shrink together towards `minHold`; a story that cannot fit even then fails.
 * Each scene's `avail` is how long its clip ran after the end state was seen (a hold never reaches past the recording).
 */
export function planTimeline(scenes, { endCard = END_CARD.seconds, maxS = SPEC.maxS - 0.5, minS = SPEC.minS } = {}) {
  const base = scenes.map((s) => ({ ...s, start: Math.max(0, s.tapV - s.lead), act: s.doneV - Math.max(0, s.tapV - s.lead) }));
  for (const s of base) if (s.avail < s.minHold) throw new Error(`${s.id}: the clip ran ${s.avail.toFixed(2)} s past its end state, under the shortest hold ${s.minHold} s`);
  const total = (k) => endCard + base.reduce((a, s) => a + s.act + Math.min(s.avail, s.minHold + (s.hold - s.minHold) * k), 0);
  let k = 1;
  if (total(1) < minS + 0.5) {
    // a short story: the holds grow together (never past what was recorded) until the cut reaches the stores' minimum
    let lo = 1, hi = 6;
    for (let i = 0; i < 30; i++) { const m = (lo + hi) / 2; if (total(m) < minS + 0.5) lo = m; else hi = m; }
    k = hi;
  } else if (total(1) > maxS) {
    if (total(0) > maxS) throw new Error(`the scenes take ${total(0).toFixed(1)} s at their shortest holds: over ${maxS} s, so a scene is slower than its slot (nothing is sped up)`);
    let lo = 0, hi = 1;
    for (let i = 0; i < 30; i++) { const m = (lo + hi) / 2; if (total(m) > maxS) hi = m; else lo = m; }
    k = lo;
  }
  const out = base.map((s) => { const hold = Math.min(s.avail, s.minHold + (s.hold - s.minHold) * k); return { id: s.id, start: s.start, end: s.start + s.act + hold, doneAt: s.act, hold }; });
  for (const p of out) if (![p.start, p.end, p.doneAt, p.hold].every(Number.isFinite)) throw new Error(`${p.id}: the cut is not a number (${JSON.stringify(p)})`);
  const sum = total(k);
  if (sum < minS) throw new Error(`the cut is ${sum.toFixed(1)} s: under the ${minS} s the stores want`);
  return { scenes: out, total: sum };
}

// ---------- captions ----------
const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
/** A full-frame transparent SVG holding one caption pill (white bold text on a dark pill), centred at `capY`. */
export function captionSvg(caption, capY, { w = SPEC.w, h = SPEC.h } = {}) {
  const lines = caption.split('\n');
  const size = Math.round(w * 0.066), lh = Math.round(size * 1.22);
  const estW = Math.max(...lines.map((l) => l.length)) * size * 0.56;
  const pw = Math.min(w * 0.9, estW + size * 1.4), ph = lines.length * lh + size * 0.9;
  const x = (w - pw) / 2, y = h * capY - ph / 2;
  if (y < h * CAPTION_BAND[0] || y + ph > h * CAPTION_BAND[1]) throw new Error(`caption "${caption}" at ${capY} leaves the safe band ${CAPTION_BAND.join('-')}`);
  const ty = y + size * 0.45 + lh * 0.8;
  return { box: { x, y, w: pw, h: ph }, svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${pw.toFixed(1)}" height="${ph.toFixed(1)}" rx="${(size * 0.45).toFixed(1)}" fill="${INK}" fill-opacity="0.95"/>
${lines.map((l, i) => `<text x="${w / 2}" y="${(ty + i * lh).toFixed(1)}" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-weight="700" font-size="${size}" fill="${CARD}">${esc(l)}</text>`).join('\n')}
</svg>` };
}

/** The end card: the icon, the name and one line, on white. */
export function endCardSvg(iconDataUri, { w = SPEC.w, h = SPEC.h } = {}) {
  const icon = Math.round(w * 0.36), iy = Math.round(h * 0.33);
  return `<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
<defs><clipPath id="r"><rect x="${(w - icon) / 2}" y="${iy}" width="${icon}" height="${icon}" rx="${Math.round(icon * 0.22)}"/></clipPath></defs>
<rect width="${w}" height="${h}" fill="${CARD}"/>
<image x="${(w - icon) / 2}" y="${iy}" width="${icon}" height="${icon}" clip-path="url(#r)" xlink:href="${iconDataUri}" href="${iconDataUri}"/>
<text x="${w / 2}" y="${iy + icon + Math.round(w * 0.14)}" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-weight="700" font-size="${Math.round(w * 0.088)}" fill="${INK}">${esc(END_CARD.name)}</text>
<text x="${w / 2}" y="${iy + icon + Math.round(w * 0.215)}" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-size="${Math.round(w * 0.04)}" fill="${SUB}">${esc(END_CARD.line)}</text>
</svg>`;
}

// ---------- the finished file ----------
/** `ffprobe -show_streams -show_format -of json` of the MP4 → the list of what breaks the spec (empty = ok). */
export function specProblems(probe, { bytes, poster } = {}) {
  const v = probe.streams?.find((s) => s.codec_type === 'video');
  const a = probe.streams?.find((s) => s.codec_type === 'audio');
  const dur = Number(probe.format?.duration);
  const rate = (r) => { const [n, d] = String(r ?? '0/1').split('/').map(Number); return d ? n / d : NaN; };
  return [
    ...need(v, 'no video stream'),
    ...need(v?.codec_name === 'h264', `video codec ${v?.codec_name}, not h264`),
    ...need(v?.profile === 'High', `H.264 profile ${v?.profile}, not High`),
    ...need(Number(v?.level) <= 40, `H.264 level ${v?.level}, over 4.0`),
    ...need(v?.width === SPEC.w && v?.height === SPEC.h, `size ${v?.width}x${v?.height}, not ${SPEC.w}x${SPEC.h} (portrait)`),
    ...need(v?.pix_fmt === 'yuv420p', `pixel format ${v?.pix_fmt}, not yuv420p`),
    ...need(Math.abs(rate(v?.r_frame_rate) - SPEC.fps) < 0.01 && Math.abs(rate(v?.avg_frame_rate) - SPEC.fps) < 0.05, `frame rate ${v?.r_frame_rate} / avg ${v?.avg_frame_rate}, not a constant ${SPEC.fps}`),
    ...need(dur >= SPEC.minS && dur <= SPEC.maxS, `duration ${dur} s, not ${SPEC.minS}-${SPEC.maxS} s`),
    ...need(a?.codec_name === 'aac' && a?.channels === 2, `audio ${a ? `${a.codec_name} ${a.channels} ch` : 'missing'}, not a stereo AAC track`),
    ...need(['44100', '48000'].includes(String(a?.sample_rate)), `audio sample rate ${a?.sample_rate}, not 44.1 or 48 kHz`),
    ...need(bytes > 0 && bytes <= SPEC.maxBytes, `file size ${bytes} bytes (over 500 MB or empty)`),
    ...need(poster && poster.w === SPEC.w && poster.h === SPEC.h, `poster PNG ${poster ? `${poster.w}x${poster.h}` : 'missing'}`),
  ];
}
