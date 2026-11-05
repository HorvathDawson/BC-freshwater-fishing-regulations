// (T245) LOW-END ANDROID PERFORMANCE, AGAINST BUDGETS: cold start to the drawn map, memory peak, the map's frame rate
// while panning (the start view, ~z10), on the test-bed emulator (`n1_pixel`: 2 GB RAM, the low-end profile). The numbers
// go into the step lines and `T245-perf.json` beside the report; a number over its budget FAILS its step, so the run
// is the repeatable budget check. The regs page's open time is T286's `regs-open` scenario (same run: `perf regs-open`).
//
//   node app/tools/testbed/run.mjs perf            (PERF_COLD=n cold starts, default 3; PERF_SWIPES=n, default 8)
import fs from 'node:fs';
import path from 'node:path';
import { appUp, toMap } from './common.mjs';
import { mapFrames, mapLayer, presents } from './pan.mjs';

/** The budgets (T245; owner's proposal P11 defaults). */
export const BUDGET = { coldStartMs: 4000, memoryMb: 400, panFps: 45 };
const COLD = Number(process.env.PERF_COLD ?? 3);
const SWIPES = Number(process.env.PERF_SWIPES ?? 8);
/** PERF_ZOOMS: the zooms the pan is measured at besides the start view (default 8,12; empty: none) */
const ZOOMS = (process.env.PERF_ZOOMS ?? '8,12').split(',').filter(Boolean).map(Number);
/** where: the Fraser Valley (dense streams, lakes, the Fraser) */
const PLACE = { lon: -122.05, lat: 49.25 };

/** (T245) The test bed's camera link: the map on screen goes there at once (`@app/map` native/camProbe.ts). */
export function cameraTo(ctx, c) {
  ctx.device.sh(`am start -a android.intent.action.VIEW -d 'canifishthis://testbed/camera?lon=${c.lon}&lat=${c.lat}&zoom=${c.zoom}' ${ctx.device.APP_ID} >/dev/null`);
}

/** TOTAL PSS of the app, MB (null when it is not running). */
function pssMb(ctx) {
  const h = ctx.device.heap();
  return h && Number.isFinite(h.total) ? Math.round(h.total / 1024) : null;
}

/**
 * The camera's zoom, read off the scale bar: its label (a distance) over its width on screen. MapLibre's zoom is in
 * 512 px tiles: metres per logical px = 40075016.686·cos(lat)/(512·2^z). The latitude is taken as 51° (BC's middle:
 * ±0.1 z over the province).
 */
function zoomNow(ctx) {
  ctx.device.sh('uiautomator dump /sdcard/tb-ui.xml >/dev/null 2>&1');
  const x = ctx.device.sh('cat /sdcard/tb-ui.xml');
  const node = /content-desc="scale"[^>]*bounds="\[(\d+),\d+\]\[(\d+),\d+\]"/.exec(x);
  const label = /text="([\d.,]+)\s*(km|m)"/.exec(x);
  if (!node || !label) return null;
  const density = Number(/(\d+)/.exec(ctx.device.sh('wm density').split('\n').filter(Boolean).pop())?.[1] ?? 160) / 160;
  const px = (Number(node[2]) - Number(node[1])) / density;
  const metres = Number(label[1].replace(',', '')) * (label[2] === 'km' ? 1000 : 1);
  if (!(px > 0) || !(metres > 0)) return null;
  return Math.round(Math.log2((40075016.686 * Math.cos((51 * Math.PI) / 180) * px) / (512 * metres)) * 10) / 10;
}

/** A travelling pan (new ground every swipe), measured on the map's own surface (pan.mjs). */
async function travel(ctx, layer) {
  const [w, h] = (/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
  const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.35), dy = Math.round(h * 0.18);
  const moves = [[-dx, 0], [-dx, 0], [0, dy], [0, dy], [dx, 0], [dx, 0], [0, -dy], [0, -dy]];
  const times = [];
  let mem = 0;
  for (let i = 0; i < SWIPES; i++) {
    const [mx, my] = moves[i % moves.length];
    ctx.device.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 350`);
    await ctx.device.sleep(900);
    if (layer) times.push(...presents(ctx, layer));
    mem = Math.max(mem, pssMb(ctx) ?? 0);
  }
  const mf = mapFrames(times);
  // frames per second while the map moves: the frames over the time they took (gaps over 250 ms are rest, not frames)
  const t = [...new Set(times)].sort((a, b) => a - b);
  let ms = 0;
  for (let i = 1; i < t.length; i++) { const g = (t[i] - t[i - 1]) / 1e6; if (g < 250) ms += g; }
  return { ...mf, fps: ms ? Math.round((mf.frames / ms) * 1000 * 10) / 10 : 0, mem };
}

export const perf = {
  id: 'perf',
  title: `Low-end budgets: cold start ×${COLD} (< ${BUDGET.coldStartMs / 1000} s), memory (< ${BUDGET.memoryMb} MB), pan fps (≥ ${BUDGET.panFps})`,
  async run(ctx) {
    const out = { budget: BUDGET, device: {}, cold: [], pan: {}, memPeakMb: 0 };
    const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T245-perf.json'), `${JSON.stringify(out, null, 1)}\n`);
    const mem = () => { const m = pssMb(ctx); if (m) out.memPeakMb = Math.max(out.memPeakMb, m); return m; };
    out.device = { model: ctx.device.sh('getprop ro.product.model').trim(), sdk: ctx.device.sh('getprop ro.build.version.sdk').trim(),
                   ramMb: Math.round(Number(/MemTotal:\s+(\d+)/.exec(ctx.device.sh('cat /proc/meminfo'))?.[1] ?? 0) / 1024) };
    await ctx.device.net('wifi'); ctx.device.link('full', 'none');
    for (let i = 1; i <= COLD; i++) {
      await ctx.step(`cold start ${i}/${COLD}: launch → the map drawn`, async () => {
        ctx.device.stop();
        await ctx.device.sleep(1500);
        const t0 = Date.now();
        const launched = /TotalTime:\s*(\d+)/.exec(ctx.device.launch())?.[1];
        await appUp(ctx, { launch: false, ms: 60_000 });
        const up = Date.now() - t0;
        await ctx.device.sleep(3000);
        const m = mem();
        out.cold.push({ activityMs: Number(launched ?? NaN), mapUpMs: up, pssMb: m });
        keep();
        const note = `activity drawn ${launched ?? '?'} ms (am start -W) · map up ${up} ms (tab bar on screen, ±1 s: a screen read) · PSS ${m} MB`;
        if (up > BUDGET.coldStartMs) throw new Error(`over the ${BUDGET.coldStartMs} ms budget — ${note}`);
        return note;
      });
    }
    await ctx.step('ground: the drawn map (satellite off if a run left it on)', async () => {
      await toMap(ctx);
      if (ctx.device.screenText().some((w) => /Powered by Esri/.test(w))) {
        ctx.flow('layers-satellite');
        ctx.flow('tap-text', { TEXT: 'Done' });
        await ctx.device.sleep(3000);
      }
      if (ctx.device.screenText().some((w) => /Powered by Esri/.test(w))) throw new Error('still on satellite');
    }, { shot: false });
    const layer = mapLayer(ctx);
    // a pan at the start view (the camera the app opens on; its zoom read off the scale bar), then at z8 and z12
    await ctx.step(`pan at the start view: ${SWIPES} travelling swipes`, async () => {
      await toMap(ctx);
      const z = zoomNow(ctx);
      ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID} reset`);
      const r = await travel(ctx, layer);
      out.memPeakMb = Math.max(out.memPeakMb, r.mem);
      out.pan.start = { zoom: z, ...r };
      keep();
      const note = `at z ${z ?? '?'} · ${r.fps} fps while moving · ${r.frames} frames, >50 ms ${r.over50}, p50 ${r.p50} / p95 ${r.p95} / p99 ${r.p99} ms · PSS peak ${r.mem} MB`;
      if (!layer || !r.frames) throw new Error(`no map frames read — ${note}`);
      if (r.fps < BUDGET.panFps) throw new Error(`under ${BUDGET.panFps} fps — ${note}`);
      return note;
    });
    // (T245) and at z8 and z12, put there by the test bed's camera link (`@app/map` native/camProbe.ts): adb has no pinch
    for (const z of ZOOMS) {
      await ctx.step(`pan at z${z} (camera link, ${PLACE.lon},${PLACE.lat}): ${SWIPES} travelling swipes`, async () => {
        await toMap(ctx);
        cameraTo(ctx, { ...PLACE, zoom: z });
        await ctx.device.sleep(6000);                                // that view's tiles and colours in
        ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID} reset`);
        const r = await travel(ctx, layer);
        out.memPeakMb = Math.max(out.memPeakMb, r.mem);
        out.pan[`z${z}`] = { zoom: z, ...r };
        keep();
        const note = `${r.fps} fps while moving · ${r.frames} frames, >50 ms ${r.over50}, p50 ${r.p50} / p95 ${r.p95} / p99 ${r.p99} ms · PSS peak ${r.mem} MB`;
        if (!layer || !r.frames) throw new Error(`no map frames read — ${note}`);
        if (r.fps < BUDGET.panFps) throw new Error(`under ${BUDGET.panFps} fps — ${note}`);
        return note;
      });
    }
    await ctx.step(`memory peak (TOTAL PSS) < ${BUDGET.memoryMb} MB`, async () => {
      mem();
      keep();
      const note = `${out.memPeakMb} MB peak over the run · device ${out.device.model}, API ${out.device.sdk}, ${out.device.ramMb} MB RAM`;
      if (out.memPeakMb > BUDGET.memoryMb) throw new Error(`over budget — ${note}`);
      return note;
    }, { shot: false });
  },
};
