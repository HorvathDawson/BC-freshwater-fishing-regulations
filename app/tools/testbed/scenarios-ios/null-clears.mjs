// (T832) ON iOS A RIVER LETS GO OF ITS COLOUR: the simulator proof. React Native's iOS bridge drops a null-valued key, so
// a cleared standing (`{standing: null}`) reached MapLibre as `{}` and the river kept its old colour; and feature-state
// SURVIVES a style swap on the phone (T451), so before T832 nothing in a session took a standing colour back off. Now
// the engine writes the sentinel "" (`sections.ts` `unsetValue`) and the style reads it as `missing`.
//
// The Conditions map framed on a water (search → its page → back), then the horizon moved Now → +1 → +10 → Now (the
// corner's `map-horizons` strip), twice:
//   FRAMES[0] the Tulameen River (Princeton): rivers coloured now, every one forecast to +9 in the staging feed
//   FRAMES[1] the Okanagan River (Penticton to Oliver): its own gauges (08NM050, 08NM002, 08NM247, 08NM085) report now
//             and have NO forecast on any day, so it loses its colour already at +1 while forecast rivers keep theirs
// The staging feed forecasts no station 10 days out, so at +10 EVERY stream must show the missing grey. Not eyeballed:
// the screenshots' pixels are read against the standing ramp (`style.ts` `colorExpression("stream", "standing")`, light
// theme) and its `missing` grey, in the map's own area (under the corner strip, above the legend panel, left of the
// rail; the map's blue label ink left out — it sits on the ramp's navy end):
//   ramp     a pixel on the 0..100 ramp (within 22 of the polyline through its stops, saturated)
//   lost     a ramp pixel at Now that is the missing grey at the horizon (the clear reached the map)
//   stale    a ramp pixel at Now still within 12 of the SAME colour at the horizon (what the bug left)
// PASS per frame with enough colour at Now (≥ 1500 ramp px): at +10 stale ≤ 5% and lost ≥ 50% of Now's ramp pixels;
// back at Now the ramp is ≥ 80% of what it was. +1 is reported (lost / stale / recoloured, and the legend's own
// "N reaches coloured" and the watershed's day line), never judged by pixels: which rivers +1 leaves is the feed's.
// Shots `T832-<frame>-*.png` (and `*-mask.png`: lost magenta, stale yellow, over the horizon's shot), words `*.txt`,
// numbers `T832-pixels.json`.
//
//   node app/tools/testbed/run-ios.mjs t832
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { readPng } from '../lib/png.mjs';

// the search rows by their accessibility text (data names: kept out of the static check's literal scan, as regs-open's)
const FRAMES = [
  { id: 'tulameen', q: 'Tulameen River', row: 'Show Tulameen River,.*' },
  { id: 'okanagan', q: 'Okanagan River', row: 'Show Okanagan River,.*' },
];
// style.ts colorExpression("stream", "standing", resolveTheme("light")): the 0..100 stops, and `missing`
const RAMP = ['7A1608', 'E0552A', 'E08A2B', '12857C', '1E86B8', '1D4ED8', '122A8F']
  .map((h) => [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16)));
const MISSING = [0xd5, 0xd5, 0xd5];
// the corner's horizon strip (MapScreen `HorizonPicker testID="map-horizons"`): its ids are built twice over from that
// prop (`${testID}-${key}` in CornerStrip: -0 Now, -days the forecast segment; `${testID}-list`, `${testID}-day-${d}` in
// HorizonPicker), which the static id scan cannot follow; named here from the one prefix
const STRIP = 'map-horizons';
const NOW_SEG = `${STRIP}-0`, DAYS_SEG = `${STRIP}-days`, LIST = `${STRIP}-list`, DAY = (h) => `${STRIP}-day-${h}`;

/** Euclidean RGB distance from p to the polyline through the ramp's stops. */
function rampDist(r, g, b) {
  let best = Infinity;
  for (let k = 0; k + 1 < RAMP.length; k++) {
    const a = RAMP[k], c = RAMP[k + 1];
    const d = [c[0] - a[0], c[1] - a[1], c[2] - a[2]], v = [r - a[0], g - a[1], b - a[2]];
    const t = Math.max(0, Math.min(1, (v[0] * d[0] + v[1] * d[1] + v[2] * d[2]) / (d[0] ** 2 + d[1] ** 2 + d[2] ** 2)));
    const e = Math.hypot(v[0] - t * d[0], v[1] - t * d[1], v[2] - t * d[2]);
    if (e < best) best = e;
  }
  return best;
}
/** the map's label ink (place and water names, blue: about 0-16, 32, 160) and its anti-aliased edge */
const labelInk = (r, g, b) => b >= 140 && b - r >= 60 && b - g >= 50 && Math.abs(r - g) <= 45;
const onRamp = (r, g, b) => Math.max(r, g, b) - Math.min(r, g, b) >= 40 && !labelInk(r, g, b) && rampDist(r, g, b) <= 22;
const isMissing = (r, g, b) => Math.abs(r - MISSING[0]) + Math.abs(g - MISSING[1]) + Math.abs(b - MISSING[2]) <= 18;

/** The map's own area (iPhone 17): under the corner strip and search, above the legend chip and the panel, left of the rail. */
const area = (img) => ({ x0: 0, x1: Math.round(img.w * 0.84), y0: Math.round(img.h * 0.2), y1: Math.round(img.h * 0.6) });

function rampCount(img) {
  const a = area(img); let n = 0;
  for (let y = a.y0; y < a.y1; y++) for (let x = a.x0; x < a.x1; x++) {
    const i = (y * img.w + x) * img.ch;
    if (onRamp(img.px[i], img.px[i + 1], img.px[i + 2])) n++;
  }
  return n;
}

/**
 * Did the camera stay put? The map's label ink (names, drawn by the camera, never by the colouring) at the same pixels:
 * the share of `a`'s ink pixels that are ink in `b` too. Not a whole-area diff: the basemap's land shade can change
 * between two shots of the same camera (seen here: the land a shade lighter in one Now shot, every river identical).
 */
export function inkKept(a, b) {
  const r = area(a); let n = 0, k = 0;
  for (let y = r.y0; y < r.y1; y++) for (let x = r.x0; x < r.x1; x++) {
    const i = (y * a.w + x) * a.ch;
    if (!labelInk(a.px[i], a.px[i + 1], a.px[i + 2])) continue;
    n++;
    if (labelInk(b.px[i], b.px[i + 1], b.px[i + 2])) k++;
  }
  return n ? k / n : 0;
}

/** Now's ramp pixels against the horizon's shot: lost (now missing grey), stale (the same colour), other; and a mask. */
function compare(now, at, maskFile) {
  if (now.w !== at.w || now.h !== at.h) throw new Error(`shot sizes differ (${now.w}×${now.h} vs ${at.w}×${at.h})`);
  const a = area(now);
  const out = Buffer.alloc(at.w * at.h * 3);
  for (let p = 0, q = 0; p < at.w * at.h; p++, q += at.ch) { out[p * 3] = at.px[q]; out[p * 3 + 1] = at.px[q + 1]; out[p * 3 + 2] = at.px[q + 2]; }
  let ramp = 0, lost = 0, stale = 0;
  for (let y = a.y0; y < a.y1; y++) for (let x = a.x0; x < a.x1; x++) {
    const i = (y * now.w + x) * now.ch, j = (y * at.w + x) * at.ch;
    const [r, g, b] = [now.px[i], now.px[i + 1], now.px[i + 2]];
    if (!onRamp(r, g, b)) continue;
    ramp++;
    const [R, G, B] = [at.px[j], at.px[j + 1], at.px[j + 2]];
    const o = (y * at.w + x) * 3;
    if (isMissing(R, G, B)) { lost++; out[o] = 255; out[o + 1] = 0; out[o + 2] = 255; }
    else if (Math.abs(R - r) + Math.abs(G - g) + Math.abs(B - b) <= 12) { stale++; out[o] = 255; out[o + 1] = 220; out[o + 2] = 0; }
  }
  writePng(maskFile, at.w, at.h, out);
  return { ramp, lost, stale, other: ramp - lost - stale };
}

/** An 8-bit RGB PNG (filter 0 on every row). */
function writePng(file, w, h, rgb) {
  const raw = Buffer.alloc(h * (w * 3 + 1));
  for (let y = 0; y < h; y++) rgb.copy(raw, y * (w * 3 + 1) + 1, y * w * 3, (y + 1) * w * 3);
  const chunk = (type, data) => {
    const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
    const td = Buffer.concat([Buffer.from(type, 'ascii'), data]);
    const crc = Buffer.alloc(4); crc.writeUInt32BE(zlib.crc32(td) >>> 0);
    return Buffer.concat([len, td, crc]);
  };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  fs.writeFileSync(file, Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr),
    chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]));
}

export const nullClearsIos = {
  id: 't832',
  inAll: false,
  title: 'T832: a river that loses its standing at a horizon change shows none, not its old colour (iOS null clears)',
  async run(ctx) {
    const { coldStart, pageBack } = ctx.ios;
    const d = ctx.device, sleep = d.sleep;
    const nums = {};
    const save = () => fs.writeFileSync(path.join(ctx.dir, 'T832-pixels.json'), JSON.stringify(nums, null, 2));
    const shot = (name) => {
      const f = path.join(ctx.dir, `T832-${name}.png`);
      d.screenshot(f);
      const w = d.screenText();
      // (run 3) the app's offline banner pushes the whole screen down: a pixel comparison across it is void, and said so
      const offline = w.find((x) => /^Offline —/.test(x));
      if (offline) throw new Error(`the app went offline ("${offline}"): its banner moves the screen, the shots cannot be compared`);
      fs.writeFileSync(path.join(ctx.dir, `T832-${name}.txt`), [...w, '--- ids', ...(w.ids ?? [])].join('\n'));
      // the legend's own words: its count of reaches coloured in view, and the watershed's line for the day shown
      const legend = [w.find((x) => /reaches? coloured/i.test(x)), w.find((x) => / · /.test(x) && /normal|reading|high|low/i.test(x))]
        .filter(Boolean).join(' / ');
      return { file: f, img: readPng(f), words: w, legend };
    };
    /** The corner strip's middle segment as the screen reader hears it (the forecast day lit, or "Choose a forecast day"). */
    const daySaid = () => d.nodes().find((n) => n.id === DAYS_SEG)?.desc ?? '';
    /** the map repainted: two equal frames in a row, after a beat for the relabel to be sent */
    const repainted = async () => { await sleep(4000); await ctx.settle({ ms: 20_000, every: 1500, frame: true }); };
    const pick = async (h) => {
      if (h === 0) { ctx.flow('tap-id', { ID: NOW_SEG }); await repainted(); return daySaid(); }
      if (!d.hasId(LIST)) ctx.flow('tap-id', { ID: DAYS_SEG });
      await ctx.until(() => d.hasId(DAY(1)), { ms: 10_000, what: 'the forecast days open' });
      // the day list scrolls sideways under the strip, as wide as the map's corner (MapScreen `map-corner`: right 72 pt):
      // swiped inside the strip's own width (a swipe that starts off the list pans the map: run 1 moved the camera so)
      // until the day is whole inside the corner
      for (let i = 0; i < 10; i++) {
        const ns = d.nodes();
        const strip = ns.find((x) => x.id === STRIP), n = ns.find((x) => x.id === DAY(h)), list = ns.find((x) => x.id === LIST);
        const corner = ns.find((x) => x.id === 'map-corner');
        if (!strip || !list) throw new Error('the horizon strip or its day list is not in the tree');
        const right = corner ? corner.box.x1 : strip.box.x1;
        if (n && n.box.x0 >= strip.box.x0 - 1 && n.box.x1 <= right + 1) break;
        const y = Math.round((list.box.y0 + list.box.y1) / 2), l = strip.box.x0 + 12, r = strip.box.x1 - 12;
        const toLeft = !n || n.box.x1 > right;
        const go = () => ctx.swipe(toLeft ? [r, y] : [l, y], toLeft ? [l, y] : [r, y], 600);
        // (run 3) Maestro's iOS driver once timed out starting: one retry, said
        try { go(); } catch (e) { ctx.finding(`a swipe failed once (${e.message.slice(0, 120)}); retried`); await sleep(3000); go(); }
        await sleep(1000);
      }
      ctx.flow('tap-id', { ID: DAY(h) });
      await sleep(500);
      if (d.hasId(LIST)) ctx.flow('tap-id', { ID: DAYS_SEG });   // the list closed (it stays open after a pick)
      await repainted();
      return daySaid();
    };
    const frame = async (f) => {
      ctx.flow('search', { Q: f.q });
      await sleep(2500);
      ctx.flow('open-result', { LINE: f.row });
      await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: `the ${f.q} page (water-back)` });
      await sleep(3000);
      await pageBack(ctx);
      await sleep(3000);
    };

    await ctx.step('cold start; the Conditions tab (light appearance)', async () => {
      d.appearance('light');
      await coldStart(ctx, { ms: 600_000 });
      d.logcatClear();
      ctx.flow('tab', { TAB: 'Conditions' });
      await sleep(6000);
    });

    for (const f of FRAMES) {
      const n = (nums[f.id] = {});
      let A = null;
      await ctx.step(`${f.q}: the map framed on it; Now`, async () => {
        await pick(0);
        await frame(f);
        const said = await pick(0);
        // the feed in: the streams wear the ramp (polled: a cold start reads the feed after the map draws)
        const end = Date.now() + 60_000;
        do { A = shot(`${f.id}-1-now`); if (rampCount(A.img) >= 1500) break; await sleep(5000); } while (Date.now() < end);   // poll interval
        n.now = { said, legend: A.legend, ramp: rampCount(A.img) };
        save();
        return `strip: "${said}" · legend: ${A.legend} · ${n.now.ramp} ramp px in the map area`;
      });
      for (const [h, name] of [[1, '2-plus1'], [10, '3-plus10']]) {
        await ctx.step(`${f.q}: horizon +${h}; what lost its standing shows the missing grey, not its Now colour`, async () => {
          if (!A) ctx.skip('no Now shot');
          const said = await pick(h);
          const B = shot(`${f.id}-${name}`);
          const c = compare(A.img, B.img, path.join(ctx.dir, `T832-${f.id}-${name}-mask.png`));
          n[`plus${h}`] = { said, legend: B.legend, rampAtHorizon: rampCount(B.img), ...c };
          save();
          const pct = (x) => `${((100 * x) / Math.max(1, c.ramp)).toFixed(1)}%`;
          const line = `strip: "${said}" · legend: ${B.legend} · of Now's ${c.ramp} ramp px: lost ${c.lost} (${pct(c.lost)}), stale ${c.stale} (${pct(c.stale)}), recoloured ${c.other} · ramp px at +${h}: ${n[`plus${h}`].rampAtHorizon}`;
          if (!said) ctx.finding(`${f.id} +${h}: the strip's middle segment had no screen-reader name to read the day from`);
          else if (!new RegExp(`\\b${h} days? ahead`).test(said)) throw new Error(`the strip does not show +${h} (${line})`);
          if (h === 10) {
            if (c.ramp < 1500) ctx.skip(`too little colour at Now to judge (${line})`);
            if (c.stale > 0.05 * c.ramp) throw new Error(`rivers kept their Now colour at +10, where no gauge has a forecast (${line})`);
            if (c.lost < 0.5 * c.ramp) throw new Error(`too few rivers went to the missing grey at +10 (${line})`);
          }
          return line;
        });
      }
      await ctx.step(`${f.q}: back to Now; the colours come back, the camera did not move (label ink in place)`, async () => {
        if (!A) ctx.skip('no Now shot');
        const said = await pick(0);
        const D = shot(`${f.id}-4-now-again`);
        const c = compare(A.img, D.img, path.join(ctx.dir, `T832-${f.id}-4-now-again-mask.png`));
        const ink = inkKept(A.img, D.img);
        n.nowAgain = { said, legend: D.legend, ramp: rampCount(D.img), sameColour: c.stale, grey: c.lost, labelInkKept: ink };
        save();
        const line = `strip: "${said}" · legend: ${D.legend} · ramp px ${n.nowAgain.ramp} (Now was ${n.now.ramp}); of Now's: same colour ${c.stale}, grey ${c.lost} · label ink in place ${(ink * 100).toFixed(1)}%`;
        if (ink < 0.9) throw new Error(`the map is not where it was at Now: the comparison is void (${line})`);
        if (n.now.ramp >= 1500 && n.nowAgain.ramp < 0.8 * n.now.ramp) throw new Error(`the colours did not come back at Now (${line})`);
        return line;
      });
    }

    fs.writeFileSync(path.join(ctx.dir, 'T832-js.log'), d.logcatJs());
  },
};
