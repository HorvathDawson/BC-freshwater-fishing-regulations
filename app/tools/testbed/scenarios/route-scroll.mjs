// (T603) THE CONDITIONS ROUTE MAP KEEPS ITS PICTURE WHILE THE PANE SCROLLS. Seen once on build 25: the route map (the
// small map on a water's Conditions, you-are-here and the gauges) drew blank dark green while scrolling. Here the pane is
// flung down and back up ten times; each round the map is photographed while the pane moves, the moment it comes back and
// again once the screen rests, and its box is measured (lib/screen.mjs `blankShare`: how much of it is one flat colour).
//   FAILS   the map is one flat colour after the screen has rested (it did not draw again)
//   FINDING it was flat only while moving or in the first frame back (a flash), with the shot
// Every round's two shots are kept (T603-route-NN-{moving,back,rest}.png): if it reproduces, they are the capture for its own
// ticket.
//
//   node app/tools/testbed/run.mjs route-scroll      ROUTE_Q / ROUTE_ROW: the water · ROUTE_ROUNDS (10)
import path from 'node:path';
import { coldStart, openWater, closeWaters, toMap } from './common.mjs';
import { BLANK_AT, blankShare, blankShareOf, look, screenSize, shown } from '../lib/screen.mjs';
import { changed, readPng } from '../lib/png.mjs';

const Q = process.env.ROUTE_Q ?? 'Coquitlam River';
const ROW = process.env.ROUTE_ROW ?? '.* · Region 2 · MU 2-8.*';
const ROUNDS = Number(process.env.ROUTE_ROUNDS ?? 10);

/** The route map's box: the pane's own MapLibre view (the browse map behind the pane is full screen; this one is not). */
async function routeMap(d, H) {
  const ns = await look(d);
  return ns.find((n) => /^Showing a Map created with MapLibre/.test(n.desc) && shown(n) && n.box.y1 - n.box.y0 < H * 0.6) ?? null;
}
/** the map's picture without its buttons' corners and its frame: the middle 80 % by 70 % */
const core = (b) => { const w = b.x1 - b.x0, h = b.y1 - b.y0; return { x0: Math.round(b.x0 + w * 0.1), x1: Math.round(b.x1 - w * 0.1), y0: Math.round(b.y0 + h * 0.2), y1: Math.round(b.y1 - h * 0.1) }; };

export const routeScroll = {
  id: 'route-scroll',
  title: `The Conditions route map still draws after ${ROUNDS} scrolls down and up (T603: seen blank once on build 25) — ${Q}`,
  async run(ctx) {
    const d = ctx.device;
    const { H } = screenSize(d);
    let home = null;
    await ctx.step(`${Q} → Conditions: the route map drawn`, async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx);
      await closeWaters(ctx);
      await toMap(ctx);
      await openWater(ctx, { q: Q, row: ROW, view: 'conditions' });
      await ctx.until(() => ctx.hasId('conditions-day-label'), { ms: 30_000, what: 'the Conditions view (conditions-day-label)' });
      const map = await ctx.until(() => routeMap(d, H), { ms: 30_000, what: 'the route map on the Conditions view' });
      await ctx.settle({ ms: 8000, frame: true });
      home = (await routeMap(d, H)).box;
      const file = path.join(ctx.dir, 'T603-route-00-first.png');
      d.screenshot(file);
      const m = blankShareOf(file, core(home));
      if (m.share >= BLANK_AT) throw new Error(`the route map is one flat colour before any scroll (${(m.share * 100).toFixed(1)} % ${m.colour})`);
      return `map box ${JSON.stringify(map.box)} · ${(m.share * 100).toFixed(0)} % its commonest colour (${m.colour}): drawn`;
    }, { stop: true });

    await ctx.step(`${ROUNDS} × (fling down the pane, fling back up): the map each time it returns`, async () => {
      // in the pane's left gutter: a drag that starts ON the map is the map's own (it pans), not the pane's scroll
      const x = 20;
      const rows = [];
      const blankAtRest = [], flashes = [];
      for (let i = 1; i <= ROUNDS; i++) {
        const n = String(i).padStart(2, '0');
        // 1. a slow drag: the map moves up the screen but stays on it — its picture WHILE the pane moves
        d.sh(`input swipe ${x} ${Math.round(H * 0.6)} ${x} ${Math.round(H * 0.45)} 500`);
        const mid = path.join(ctx.dir, `T603-route-${n}-moving.png`);
        d.screenshot(mid);
        const moved = await routeMap(d, H);
        if (!moved || Math.abs(moved.box.y0 - home.y0) < 40) throw new Error(`round ${i}: the pane did not scroll (the map is ${moved ? `still at y ${moved.box.y0}` : 'gone after a short drag'})`);
        const m = blankShareOf(mid, core(moved.box));
        // 2. down: three fast drags (flings), the map leaves the screen; 3. up: until the pane is at its top again
        for (let k = 0; k < 3; k++) d.sh(`input swipe ${x} ${Math.round(H * 0.62)} ${x} ${Math.round(H * 0.33)} 120`);
        const gone = await routeMap(d, H);
        if (gone && gone.box.y0 > H * 0.3) throw new Error(`round ${i}: three flings down left the route map on screen (y ${gone.box.y0}): the pane is not scrolling`);
        for (let k = 0; k < 4; k++) d.sh(`input swipe ${x} ${Math.round(H * 0.4)} ${x} ${Math.round(H * 0.8)} 120`);
        const back = path.join(ctx.dir, `T603-route-${n}-back.png`);
        d.screenshot(back);                                  // the first frame back, before anything settles
        await ctx.settle({ ms: 4000, frame: true });
        const map = await routeMap(d, H);
        if (!map) throw new Error(`round ${i}: no route map on screen after scrolling back up`);
        const rest = path.join(ctx.dir, `T603-route-${n}-rest.png`);
        d.screenshot(rest);
        const backImg = readPng(back), restImg = readPng(rest);
        const a = blankShare(backImg, core(home)), b = blankShare(restImg, core(map.box));
        // was the pane already home in the first frame? The strip just above the map (the estimate card's foot) is then
        // the same picture as at rest; while the pane is still gliding it is not, and that frame's "map box" is not the map
        const atHome = Math.abs(map.box.y0 - home.y0) < 8
          && changed(backImg, restImg, { x: home.x0, y: home.y0 - 160, w: home.x1 - home.x0, h: 150 }) < 0.02;
        rows.push(`${i}: moving ${(m.share * 100).toFixed(0)} % · back ${atHome ? `${(a.share * 100).toFixed(0)} %` : 'still gliding'} · rest ${(b.share * 100).toFixed(0)} %`);
        if (m.share >= BLANK_AT) flashes.push(`round ${i}, while the pane moved: ${(m.share * 100).toFixed(1)} % ${m.colour} (${path.basename(mid)})`);
        // the first frame is only judged when the pane was already home in it
        if (b.share >= BLANK_AT) blankAtRest.push(`round ${i}: ${(b.share * 100).toFixed(1)} % ${b.colour} (${path.basename(rest)})`);
        else if (a.share >= BLANK_AT && atHome) flashes.push(`round ${i}: ${(a.share * 100).toFixed(1)} % ${a.colour} (${path.basename(back)})`);
      }
      if (flashes.length) ctx.finding(`route map: one flat colour while the pane moved or in the first frame back, drawn again at rest — ${flashes.join('; ')}`);
      if (blankAtRest.length) {
        ctx.finding(`route map BLANK after the screen rested — ${blankAtRest.join('; ')}`);
        throw new Error(`the route map stayed one flat colour after scrolling: ${blankAtRest.join('; ')}`);
      }
      return `${ROUNDS} rounds, share of the map that is its commonest colour — ${rows.join(' | ')}${flashes.length ? ` · ${flashes.length} flash(es) while moving` : ''}`;
    });

    await ctx.step('clean up: the water closed', async () => { await closeWaters(ctx); await toMap(ctx); }, { shot: false });
  },
};
