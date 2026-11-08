// (T512) THE MAP TAB AFTER N SWITCHES. Owner 2026-11-08 (Pixel 9, Play 0.6.9 build 21): "laggy map … worst on the main
// map page; Conditions is very fast, and when switching between maps it gets worse". The same travelling pan on the Map
// tab (pan.mjs's path and frame stats: the map surface's SurfaceFlinger presents), measured fresh and again after every
// SP_STAGES switches — a switch is Map → Conditions → Map (two style swaps, the engine relabels every held tile), and
// with SP_WATER=1 also a water opened into its Map view by search (the Fraser: a second live map, built under the T508
// hold) and closed. Beside each pan, what is alive: MapLibre render threads (`RenderThread <n>`), map worker threads,
// the app's SurfaceView layers in SurfaceFlinger, total PSS — a map that is never torn down shows here as a count that
// grows with the switches.
// A benchmark, not a pass/fail: the numbers go in the step lines and `T512-switch-pan.json` beside the report.
//
//   node app/tools/testbed/run.mjs switch-pan   (SP_AT=lon,lat,zoom, default Coquitlam z13; SP_STAGES=0,4,8,16 switches
//                                                done before each pan, cumulative; SP_WATER=1; SP_TABS=Map,Conditions;
//                                                SP_SWIPES=16; SP_MS=120)
import fs from 'node:fs';
import path from 'node:path';
import { closeWaters, coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';
import { mapFrames, mapLayer, presents } from './pan.mjs';

const AT = (process.env.SP_AT ?? '-122.78,49.28,13').split(',').map(Number);
const STAGES = (process.env.SP_STAGES ?? '0,4,8,16').split(',').map(Number);
const WATER = process.env.SP_WATER === '1';
const SWIPES = Number(process.env.SP_SWIPES ?? 16);
const SWIPE_MS = Number(process.env.SP_MS ?? 120);
/** SP_TABS=Map,Conditions: the tabs panned at each stage (the Conditions pan, the owner's "very fast" one, is the control) */
const TABS = (process.env.SP_TABS ?? 'Map,Conditions').split(',');

/** What is alive in the app now: MapLibre render threads, map workers, SurfaceView layers, PSS (MB). */
export function alive(d) {
  const pid = d.pidOf();
  const threads = pid ? d.sh(`ps -T -p ${pid}`).split('\n') : [];
  const named = (re) => threads.filter((l) => re.test(l.trim().split(/\s+/).slice(9).join(' '))).length;
  const layers = d.sh('dumpsys SurfaceFlinger --list').split('\n')
    .filter((l) => /SurfaceView\[ca\.canifishthis[^\]]*\]\(BLAST\)#\d+/.test(l));
  const h = d.heap();
  return { pid, renderThreads: named(/^RenderThread \d+$/), workers: named(/^Worker \d+$/), threads: Math.max(0, threads.length - 1),
           surfaceViews: new Set(layers.map((l) => /\(BLAST\)#\d+/.exec(l)[0])).size,
           pssMb: h && Number.isFinite(h.total) ? Math.round(h.total / 1024) : null };
}

export const switchPan = {
  id: 'switch-pan',
  title: `The Map tab's pan after ${STAGES.join(' / ')} switches (Map → Conditions → Map${WATER ? ', + a water Map view' : ''}), at ${AT.join(',')} (T512)`,
  async run(ctx) {
    const d = ctx.device;
    const out = [];
    await ctx.step(`the map up (cold start, Wi-Fi), the camera at ${AT.join(',')}`, async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
      await d.sleep(6000);
      // every tab once, so a first visit's coach marks and feeds are out of the way before anything is timed
      for (const t of ['Conditions', 'Map']) {
        ctx.flow('tab', { TAB: t }); await d.sleep(3500);
        for (let k = 0; k < 3; k++) {
          const w = d.screenText(), b = ['Skip', 'Got it'].find((x) => w.includes(x));
          if (!b) break;
          ctx.flow('tap-text', { TEXT: b }); await d.sleep(1500);
        }
      }
      return JSON.stringify(alive(d));
    });
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.35), dy = Math.round(h * 0.18);
    const travel = [[-dx, 0], [-dx, 0], [-dx, 0], [-dx, 0], [0, dy], [0, dy], [0, dy], [0, dy]];
    let done = 0;
    for (const n of STAGES) {
      if (n > done) {
        await ctx.step(`switches ${done + 1}–${n}: Map → Conditions → Map${WATER ? ' + the Fraser opened into its Map view and closed' : ''}`, async () => {
          for (; done < n; done++) {
            ctx.flow('tab', { TAB: 'Conditions' }); await d.sleep(2600);
            ctx.flow('tab', { TAB: 'Map' }); await d.sleep(2600);
            if (WATER) {
              ctx.flow('search', { Q: 'Fraser River' });
              ctx.flow('open-result', { LINE: 'Stream · .*' });
              await d.sleep(4000);
              await closeWaters(ctx);
              await toMap(ctx);
              await d.sleep(1500);
            }
          }
          return JSON.stringify(alive(d));
        });
      }
      for (const tab of TABS) await ctx.step(`${tab} tab, pan after ${n} switches: ${SWIPES} travelling flings (${SWIPE_MS} ms)`, async () => {
        ctx.flow('tab', { TAB: tab }); await d.sleep(2600);
        cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
        await d.sleep(5000);
        const layer = mapLayer(ctx), times = [];
        for (let i = 0; i < SWIPES; i++) {
          const [mx, my] = travel[i % travel.length];
          d.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} ${SWIPE_MS}`);
          await d.sleep(900);
          if (layer) times.push(...presents(ctx, layer));
        }
        await d.sleep(2000);
        const mf = mapFrames(times), after = alive(d);
        out.push({ switches: n, tab, frames: mf, after });
        fs.writeFileSync(path.join(ctx.dir, 'T512-switch-pan.json'), JSON.stringify({ at: AT, water: WATER, stages: out }, null, 1));
        if (tab !== 'Map') { ctx.flow('tab', { TAB: 'Map' }); await d.sleep(2600); }
        return `MAP SURFACE ${layer ? `${mf.frames} frames, >50 ms ${mf.over50}, p50 ${mf.p50} / p95 ${mf.p95} / max ${mf.max} ms` : 'not found'}` +
          ` · alive: render threads ${after.renderThreads}, workers ${after.workers}, SurfaceViews ${after.surfaceViews}, threads ${after.threads}, PSS ${after.pssMb} MB`;
      });
    }
  },
};
