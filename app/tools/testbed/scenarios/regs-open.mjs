// (T286) A MAP TAP TO ITS REGULATIONS, TIMED: for each case (a small stream, the Fraser, a lake) search the water, let
// the map frame it (the result row's preview), close the search, then TAP THE WATER ON THE MAP where its highlight ring
// is drawn (found in the raw screen pixels) and wait for the page's words. The app's open probe (`@app/data`
// openProbe.ts, on in the test-bed build) logs one `[open] {…}` line per tap: each stage's arrival (ms since the press,
// SQLite calls by then) — hit test, route, water sheet, store, target, inputs, page, commit, first frame — and the
// SQLite calls' queue wait and run time. Each case is tapped OPEN_TAPS times (first = cold for that water, then warm).
// A benchmark: the lines go into the step note and `T286-open.txt` beside the report; a spinner seen fails the step.
//
//   node app/tools/testbed/run.mjs regs-open          (OPEN_TAPS=n, default 3; OPEN_CASES=small,fraser,lake)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';

const TAPS = Number(process.env.OPEN_TAPS ?? 3);
const CASES = [
  { id: 'small', q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*', title: /Norrish Creek/ },
  { id: 'fraser', q: 'Fraser River', row: 'Stream · .*', title: /Fraser River/ },
  { id: 'lake', q: 'Cultus', row: 'Lake · .*Lindell Beach.*', title: /Cultus Lake/ },
].filter((c) => !process.env.OPEN_CASES || process.env.OPEN_CASES.split(',').includes(c.id));
/** The selected-water ring (`color.highlight`, every theme: style.meta.json). */
const RING = [[0xf5, 0x5c, 0x00], [0xff, 0x77, 0xb8], [0xc2, 0x18, 0x7a]];

/** The raw screen (RGBA): `screencap` without -p — a 12- or 16-byte header, then the pixels. */
function screen(ctx) {
  const b = ctx.device.adb(['exec-out', 'screencap']);
  const w = b.readUInt32LE(0), h = b.readUInt32LE(4);
  return { w, h, px: b.subarray(b.length - w * h * 4) };
}

/** A point on the ring between the top bar and the bottom chrome: the ring pixel nearest the ring's median. */
function ringPoint(ctx) {
  const { w, h, px } = screen(ctx);
  const xs = [], ys = [];
  for (let y = Math.round(h * 0.14); y < Math.round(h * 0.8); y += 2) {
    for (let x = Math.round(w * 0.04); x < Math.round(w * 0.96); x += 2) {
      const i = (y * w + x) * 4;
      if (RING.some(([r, g, b]) => Math.abs(px[i] - r) + Math.abs(px[i + 1] - g) + Math.abs(px[i + 2] - b) < 40)) { xs.push(x); ys.push(y); }
    }
  }
  if (xs.length < 6) return null;
  const mid = (a) => [...a].sort((p, q) => p - q)[a.length >> 1];
  const mx = mid(xs), my = mid(ys);
  let best = 0, d = Infinity;
  for (let i = 0; i < xs.length; i++) { const e = (xs[i] - mx) ** 2 + (ys[i] - my) ** 2; if (e < d) { d = e; best = i; } }
  return { x: xs[best], y: ys[best], n: xs.length };
}

const opens = (ctx) => ctx.device.logcatJs().split('\n').filter((l) => l.includes('[open] '))
  .map((l) => { try { return JSON.parse(l.slice(l.indexOf('[open] ') + 7)); } catch { return null; } }).filter(Boolean);

/** One trace as a line: total to the first frame, then each stage. */
function said(o) {
  const st = Object.entries(o.marks).sort((a, b) => a[1][0] - b[1][0]).map(([k, [ms, n]]) => `${k} ${ms}${n ? `/${n}q` : ''}`);
  return `${o.marks.frame?.[0] ?? o.marks.timeout?.[0] ?? '?'} ms · ${st.join(', ')} · sql ${o.sql} (wait ${o.sqlWait}, run ${o.sqlRun}, max ${o.sqlMax})`
    + (Object.keys(o.spans).length ? ` · ${Object.entries(o.spans).map(([k, v]) => `${k} ${v} ms`).join(', ')}` : '');
}

export const regsOpen = {
  id: 'regs-open',
  title: `A map tap to its regulations, timed (${CASES.map((c) => c.id).join(', ')} × ${TAPS})`,
  async run(ctx) {
    const lines = [];
    await ctx.step('the map up (cold start, Wi-Fi)', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await ctx.device.sleep(4000);
    });
    for (const c of CASES) {
      let at = null;
      await ctx.step(`${c.id}: search "${c.q}", the map framed on it, the search closed`, async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: c.q });
        ctx.flow('tap-text', { TEXT: c.row });
        await ctx.device.sleep(3500);
        at = ringPoint(ctx);
        if (!at) {                                        // the results sheet over the ring: close the search, look again
          ctx.device.sh('input keyevent 4');
          await ctx.device.sleep(2500);
          at = ringPoint(ctx);
        }
        if (!at) throw new Error('no highlight ring found on screen');
        // the search closed (as a reader leaves it): the map keeps its framing, browse mode, the waters on screen read
        // ahead while it is still (T286 prefetch)
        if (ctx.device.screenText().some((x) => /NAMED WATERS/i.test(x))) { ctx.device.sh('input keyevent 4'); await ctx.device.sleep(3000); }
        return `ring at ${at.x},${at.y} (${at.n} px)`;
      });
      if (!at) continue;
      for (let i = 1; i <= TAPS; i++) {
        await ctx.step(`${c.id}: tap ${i}/${TAPS} on the map → its regulations`, async () => {
          // what the settle read ahead (`[prefetch]`, the waters on screen), before this tap
          const pre = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[prefetch] ')).map((l) => l.slice(l.indexOf('[prefetch] ') + 11)).at(-1);
          ctx.device.logcatClear();
          ctx.device.sh(`input tap ${at.x} ${at.y}`);
          const end = Date.now() + 20_000;
          let o = null;
          while (Date.now() < end && !o) { await ctx.device.sleep(700); o = opens(ctx).at(-1) ?? null; }
          const w = ctx.device.screenText();
          if (!o) throw new Error(`no [open] line in 20 s (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
          lines.push(`${c.id} #${i} ${JSON.stringify(o)}`);
          fs.writeFileSync(path.join(ctx.dir, 'T286-open.txt'), `${lines.join('\n')}\n`);
          const named = w.some((x) => c.title.test(x));
          const note = `${pre ? `(prefetch: ${pre}) ` : ''}${said(o)}${named ? '' : ` · (title ${c.title} not on screen: ${w.slice(0, 6).join(' | ').slice(0, 160)})`}`;
          if (o.marks.spinner) throw new Error(`a spinner was shown at ${o.marks.spinner[0]} ms — ${note}`);
          return note;
        });
        // back to the map at the same framing (the water's frame closed)
        ctx.device.sh('input keyevent 4');
        await ctx.device.sleep(2000);
      }
    }
  },
};
