// (T312, T400) THE MAP'S FEEL, MEASURED: where the camera comes to rest after a gesture, read from the app's own
// `[cam]` lines (`@app/map` native/camProbe.ts, the test-bed build) — so a drag's travel against the finger's (1:1?), a
// fling's glide per release speed, and (T400) how many camera moves a search arrival makes before it rests. A benchmark,
// not a pass/fail: numbers in the step lines and T312-feel.json beside the report.
//
//   node app/tools/testbed/run.mjs feel      (FEEL_ARRIVALS=Fraser River,Cultus,… ; FEEL_FLINGS=50,80,120,200 ms)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';

const FLINGS = (process.env.FEEL_FLINGS ?? '50,80,120,200').split(',').map(Number);
const ARRIVALS = [
  { q: 'Fraser River', row: 'Stream · .*' },
  { q: 'Coquitlam River', row: '.* · Region 2 · MU 2-8.*' },
  { q: 'Cultus', row: 'Lake · .*Lindell Beach.*' },
  { q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*' },
].filter((a) => !process.env.FEEL_ARRIVALS || process.env.FEEL_ARRIVALS.split(',').includes(a.q));
const PLACE = { lon: -122.05, lat: 49.25, zoom: 11 };

/** The `[cam]` / `[move]` lines logged since the last clear. */
function lines(ctx, tag) {
  return ctx.device.logcatJs().split('\n').filter((l) => l.includes(`${tag} `))
    .map((l) => { try { return JSON.parse(l.slice(l.indexOf(`${tag} `) + tag.length + 1)); } catch { return null; } }).filter(Boolean);
}
/** Web Mercator, in dp at zoom z (512 dp tiles, MapLibre's). */
const mx = (lon, z) => ((lon + 180) / 360) * 512 * 2 ** z;
const my = (lat, z) => { const p = (lat * Math.PI) / 180; return ((1 - Math.log(Math.tan(p) + 1 / Math.cos(p)) / Math.PI) / 2) * 512 * 2 ** z; };
/** The screen distance (dp) between two camera centres, at the first one's zoom. */
const travelDp = (a, b) => Math.hypot(mx(b.lon, a.zoom) - mx(a.lon, a.zoom), my(b.lat, a.zoom) - my(a.lat, a.zoom));
/**
 * MapLibre Android 13.6's fling (`MapGestureDetector.onFling`, javap, T312): released at v dp/s (≥ the threshold), it
 * glides for v / 7 / 1.5 + base ms and moves v × time × 0.28 / 1000 dp. The defaults (base 150, threshold 1000) and the
 * T312 patch (450, 500), for the same release speed.
 */
const glide = (v, base, threshold) => (v < threshold ? 0 : (v * (v / 7 / 1.5 + base) * 0.28) / 1000);

export const feel = {
  id: 'feel',
  title: `The map's feel: drag 1:1, fling glide (${FLINGS.join('/')} ms flicks), search arrivals' camera moves (T312, T400)`,
  async run(ctx) {
    const d = ctx.device;
    const out = { drag: [], fling: [], arrivals: [] };
    const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T312-feel.json'), `${JSON.stringify(out, null, 1)}\n`);
    const k = Number(/(\d+)\s*$/.exec(d.sh('wm density').trim().split('\n').pop() ?? '')?.[1] ?? 420) / 160;
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45);
    let at = null;
    /** The camera at rest now: the last `[cam]` line (the log is cleared before each gesture, so it is kept). */
    const rest = () => lines(ctx, '[cam]').at(-1) ?? null;
    await ctx.step(`the map up (cold start, Wi-Fi), the camera at ${PLACE.lon},${PLACE.lat} z${PLACE.zoom} (camera link)`, async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      d.logcatClear();
      cameraTo(ctx, PLACE);
      await d.sleep(6000);
      at = rest();
      if (!at) throw new Error('no [cam] line after the camera link (build without EXPO_PUBLIC_PAN_PROBE, or the link not taken)');
      return `density ${k} · at ${JSON.stringify(at)}`;
    }, { stop: true });

    /** One gesture: the swipe, then the camera's travel (dp) and the finger's. */
    const gesture = async (dx, ms) => {
      d.logcatClear();
      d.sh(`input swipe ${cx - dx / 2} ${cy} ${cx + dx / 2} ${cy} ${ms}`);
      await d.sleep(3000);
      const now = rest();
      if (!now) throw new Error('no [cam] line after the gesture');
      const r = { fingerDp: Math.abs(dx) / k, mapDp: travelDp(at, now), user: now.user };
      at = now;
      return r;
    };
    await ctx.step('drag: 3 slow drags of 400 px (1500 ms, no fling): the map moves with the finger', async () => {
      for (const dx of [400, -400, 400]) out.drag.push(await gesture(dx, 1500));
      keep();
      return out.drag.map((r) => `finger ${r.fingerDp.toFixed(0)} dp → map ${r.mapDp.toFixed(0)} dp (×${(r.mapDp / r.fingerDp).toFixed(2)})`).join(' · ');
    });
    await ctx.step(`fling: 300 px flicks in ${FLINGS.join(', ')} ms, the glide past the finger`, async () => {
      let dir = 1;
      for (const ms of FLINGS) {
        const r = await gesture(300 * dir, ms);
        dir = -dir;
        const v = r.fingerDp / (ms / 1000);
        out.fling.push({ ms, vDp: Math.round(v), ...r, glideDp: Math.round(r.mapDp - r.fingerDp),
                         was: Math.round(glide(v, 150, 1000)), patched: Math.round(glide(v, 450, 500)) });
      }
      keep();
      return out.fling.map((f) => `${f.vDp} dp/s: map ${f.mapDp.toFixed(0)} dp, glide ${f.glideDp} dp (formula: defaults ${f.was}, T312 ${f.patched})`).join(' · ');
    });
    for (const a of ARRIVALS) {
      await ctx.step(`arrival: "${a.q}" opened from search, back → the camera moves until it rests`, async () => {
        await toMap(ctx);
        // the whole way: the search preview's framing (the browse map), the water's page and its map, then back
        d.logcatClear();
        ctx.flow('search', { Q: a.q });
        await d.sleep(4000);
        const preview = { moves: lines(ctx, '[move]'), cams: lines(ctx, '[cam]') };
        d.logcatClear();
        ctx.flow('open-result', { LINE: a.row });
        await d.sleep(6000);
        const page = { moves: lines(ctx, '[move]'), cams: lines(ctx, '[cam]') };
        d.logcatClear();
        d.sh('input keyevent 4');
        await d.sleep(8000);
        const back = { moves: lines(ctx, '[move]'), cams: lines(ctx, '[cam]') };
        const said = (x) => {
          const t0 = x.moves[0]?.t ?? x.cams[0]?.t, last = x.cams.at(-1);
          return { moves: x.moves.map((m) => `${m.why}${m.round}@${m.h ?? "?"}`), settles: x.cams.length,
                   restMs: t0 && last ? last.t - t0 : null, zoom: last ? Math.round(last.zoom * 100) / 100 : null };
        };
        const r = { q: a.q, preview: said(preview), page: said(page), back: said(back) };
        out.arrivals.push(r);
        keep();
        const one = (k) => `${k}: ${r[k].moves.length} moves (${r[k].moves.join(' ') || '-'}), ${r[k].settles} settles over ${r[k].restMs ?? '?'} ms, z ${r[k].zoom ?? '?'}`;
        return ['preview', 'page', 'back'].map(one).join(' · ');
      });
    }
  },
};
