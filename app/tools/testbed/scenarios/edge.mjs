// (T401) STREAMS POPPING IN AT THE EDGE, MEASURED. Owner 2026-11-05: panning, the streams just outside the view are not
// there yet and pop in. The map is framed on a water (search → result → back, as a reader arrives), then each swipe moves
// it EDGE_SHARE of its width; right after the swipe, screenshots back to back for EDGE_WATCH ms, and one more once
// settled. For the strip the swipe revealed: the share of its pixels still unlike the settled frame, shot by shot — so
// "how long until the new edge is drawn" in ms, and how much was missing at the first look. The app's `[pan]` lines
// (tiles taken, sections coloured) say whether the wait is tiles arriving or colours being sent. A benchmark, not a
// pass/fail: numbers in the step lines and T401-edge.txt beside the report.
//
//   node app/tools/testbed/run.mjs edge         (EDGE_Q water, EDGE_SWIPES n (default 8), EDGE_SHARE 0..1 (0.4))
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { changed, readPng } from '../lib/png.mjs';

const Q = process.env.EDGE_Q ?? 'Coquitlam River';
const ROW = process.env.EDGE_ROW ?? '.* · Region 2 · MU 2-8.*';
const SWIPES = Number(process.env.EDGE_SWIPES ?? 8);
const SHARE = Number(process.env.EDGE_SHARE ?? 0.4);
const WATCH = Number(process.env.EDGE_WATCH ?? 4000);
const DONE = 0.03;          // the strip counts as drawn when under 3 % of it still differs from the settled frame
// the swipe directions in turn: left, up, right, down (the map shows what lay right, below, left, above)
const DIRS = ['left', 'up', 'right', 'down'];

export const edge = {
  id: 'edge',
  title: `T401: what a pan reveals at the edge, and how long until it is drawn (${Q}, ${SWIPES} swipes of ${SHARE * 100} %)`,
  async run(ctx) {
    const d = ctx.device;
    let size;
    await ctx.step(`the map framed on ${Q} (cold start, search, back)`, async () => {
      await coldStart(ctx);
      await toMap(ctx);
      const m = /(\d+)x(\d+)/.exec(d.sh('wm size'));
      size = { w: Number(m?.[1] ?? 1080), h: Number(m?.[2] ?? 2400) };
      ctx.flow('search', { Q });
      ctx.flow('open-result', { LINE: ROW });
      await d.sleep(3500);
      d.sh('input keyevent 4');
      await d.sleep(8000);                                     // the frame's tiles and colours in
      return `${size.w}x${size.h}`;
    }, { stop: true });
    // the map's open area: under the search bar, over the bottom bar and its legend
    const area = { x: 0, y: Math.round(size.h * 0.16), w: size.w, h: Math.round(size.h * 0.56) };
    const rows = [];
    for (let i = 0; i < SWIPES; i++) {
      const dir = DIRS[i % DIRS.length];
      await ctx.step(`swipe ${i + 1}/${SWIPES} (${dir})`, async () => {
        const cx = area.x + area.w / 2, cy = area.y + area.h / 2;
        const dx = dir === 'left' ? -1 : dir === 'right' ? 1 : 0, dy = dir === 'up' ? -1 : dir === 'down' ? 1 : 0;
        const hx = Math.round(area.w * SHARE / 2) * dx, hy = Math.round(area.h * SHARE / 2) * dy;
        // the revealed strip: the side the content moved away from
        const strip = dx ? { x: dx < 0 ? area.x + Math.round(area.w * (1 - SHARE)) : area.x, y: area.y, w: Math.round(area.w * SHARE), h: area.h }
                         : { x: area.x, y: dy < 0 ? area.y + Math.round(area.h * (1 - SHARE)) : area.y, w: area.w, h: Math.round(area.h * SHARE) };
        d.logcatClear();
        // a slow, deliberate drag (no fling): the map stops where the finger lifts
        d.sh(`input swipe ${Math.round(cx - hx)} ${Math.round(cy - hy)} ${Math.round(cx + hx)} ${Math.round(cy + hy)} 900`);
        const t0 = Date.now(), shots = [];
        while (Date.now() - t0 < WATCH) {
          const f = path.join(ctx.dir, `T401-${i + 1}-${shots.length}.png`);
          d.screenshot(f);
          shots.push({ t: Date.now() - t0, f });
        }
        await d.sleep(4000);
        const settled = readPng(d.screenshot(path.join(ctx.dir, `T401-${i + 1}-settled.png`)));
        const curve = shots.map((s) => ({ t: s.t, left: changed(readPng(s.f), settled, strip) }));
        for (const s of shots.slice(1, -1)) fs.rmSync(s.f);   // keep the first and last look, and the settled frame
        const first = curve[0]?.left ?? 0;
        const drawnAt = curve.find((c) => c.left < DONE)?.t ?? null;
        const pans = d.logcatJs().split('\n').filter((l) => l.includes('[pan] ')).map((l) => {
          try { return JSON.parse(l.slice(l.indexOf('[pan] ') + 6)); } catch { return null; }
        }).filter(Boolean);
        const tiles = pans.reduce((n, p) => n + p.tiles, 0), labels = pans.reduce((n, p) => n + p.labels, 0);
        const sends = pans.reduce((n, p) => n + p.sends, 0), ackMax = Math.max(0, ...pans.map((p) => p.ackMax));
        const row = { swipe: i + 1, dir, firstMissing: +(first * 100).toFixed(1), drawnMs: drawnAt ?? `>${WATCH}`,
                      curve: curve.map((c) => `${c.t}:${(c.left * 100).toFixed(0)}`).join(' '), tiles, labels, sends, ackMax };
        rows.push(row);
        return `first look ${row.firstMissing} % of the new strip unlike settled; drawn at ${row.drawnMs} ms; ` +
               `tiles ${tiles}, sections coloured ${labels}, colour sends ${sends} (ack max ${ackMax} ms)`;
      });
    }
    await ctx.step('summary', async () => {
      const drawn = rows.map((r) => (typeof r.drawnMs === 'number' ? r.drawnMs : WATCH)).sort((a, b) => a - b);
      const firsts = rows.map((r) => r.firstMissing).sort((a, b) => a - b);
      const med = (xs) => xs[Math.floor(xs.length / 2)];
      fs.writeFileSync(path.join(ctx.dir, 'T401-edge.txt'), rows.map((r) => JSON.stringify(r)).join('\n') + '\n');
      return `median: ${med(firsts)} % missing at the first look, drawn at ${med(drawn)} ms ` +
             `(worst ${drawn.at(-1)} ms); ${rows.filter((r) => typeof r.drawnMs !== 'number').length}/${rows.length} not drawn within ${WATCH} ms`;
    });
  },
};
