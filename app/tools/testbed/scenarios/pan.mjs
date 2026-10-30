// (T273) A SCRIPTED PAN, MEASURED: the map up on Wi-Fi, then the same swipes every run (adb `input swipe`, so no
// Maestro in the loop), with the frame stats Android keeps for the app (`dumpsys gfxinfo`: janky frames, frame-time
// percentiles) and the app's own `[pan]` / `T74PROBE` lines from logcat. A benchmark, not a pass/fail: the numbers go
// into the step line and `T273-pan-<n>.txt` beside the report.
//
//   node app/tools/testbed/run.mjs pan          (PAN_SWIPES=n, default 16; PAN_ROUNDS=n, default 2)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, openLog, toMap } from './common.mjs';
import { capture, LAKE } from './spots.mjs';

const SWIPES = Number(process.env.PAN_SWIPES ?? 16);
const ROUNDS = Number(process.env.PAN_ROUNDS ?? 2);
/** PAN_GROUND=satellite: the same pan over Esri's imagery instead of the drawn basemap (splits the basemap's share). */
const GROUND = process.env.PAN_GROUND ?? 'map';
/** PAN_UPDATE=1 (T280): first take the live staging release (test channel reset → staged → applied), e.g. a new basemap. */
const UPDATE = process.env.PAN_UPDATE === '1';
/** PAN_SPOTS=n (T316): first save n spots round Cultus Lake (offline capture: instant) and pan from "Show on map" over
 *  them — the spot marks are what lagged a pan (view annotations); deleted again after the rounds. */
const SPOTS = Number(process.env.PAN_SPOTS ?? 0);

/** `dumpsys gfxinfo <app>`'s summary: frames, janky share, the 50/90/95/99th percentile frame times (ms). */
function gfx(t) {
  const n = (re) => Number(re.exec(t)?.[1] ?? NaN);
  return { frames: n(/Total frames rendered:\s*(\d+)/), janky: n(/Janky frames:\s*(\d+)/),
           p50: n(/50th percentile:\s*(\d+)ms/), p90: n(/90th percentile:\s*(\d+)ms/),
           p95: n(/95th percentile:\s*(\d+)ms/), p99: n(/99th percentile:\s*(\d+)ms/),
           slowUi: n(/Number Slow UI thread:\s*(\d+)/), slowDraw: n(/Number Slow issue draw commands:\s*(\d+)/) };
}

/** The map's own surface (MapLibre draws into a SurfaceView, which gfxinfo does not see): its SurfaceFlinger layer. */
export function mapLayer(ctx) {
  // Android 15 lists `RequestedLayerState{<name> parentId=…}`; the BLAST layer is the one frames are queued to
  const m = /(SurfaceView\[ca\.canifishthis[^\]]*\]\(BLAST\)#\d+)/.exec(ctx.device.sh('dumpsys SurfaceFlinger --list'));
  return m ? m[1] : null;
}
/** The present times (ns) SurfaceFlinger holds for that layer (its last ~128 frames). */
export function presents(ctx, layer) {
  const out = ctx.device.sh(`dumpsys SurfaceFlinger --latency '${layer}'`).split('\n').slice(1);
  return out.map((l) => l.trim().split(/\s+/).map(Number)).filter((c) => c.length === 3 && c[1] > 0 && c[1] < 9e18).map((c) => c[1]);
}
/** The map's frame intervals while it moves (gaps over 250 ms are the map at rest, not a frame). */
export function mapFrames(times) {
  const t = [...new Set(times)].sort((a, b) => a - b);
  const gaps = [];
  for (let i = 1; i < t.length; i++) { const ms = (t[i] - t[i - 1]) / 1e6; if (ms < 250) gaps.push(ms); }
  gaps.sort((a, b) => a - b);
  const q = (p) => (gaps.length ? Math.round(gaps[Math.min(gaps.length - 1, Math.floor(p * gaps.length))] * 10) / 10 : 0);
  return { frames: gaps.length, over20: gaps.filter((g) => g > 20).length, over50: gaps.filter((g) => g > 50).length,
           p50: q(0.5), p95: q(0.95), p99: q(0.99), max: Math.round((gaps.at(-1) ?? 0) * 10) / 10 };
}

export const pan = {
  id: 'pan',
  title: `A scripted pan: ${SWIPES} swipes × ${ROUNDS} rounds, frame times and the app's pan lines`,
  async run(ctx) {
    await ctx.step('the map up on Wi-Fi (cold start)', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await ctx.device.sleep(5000);                                  // the first screen's tiles and colours in
    });
    if (UPDATE) {
      await ctx.step('the live staging release taken (PAN_UPDATE): staged, applied at the next pause', async () => {
        const target = ctx.updates('reset').release?.data_release;
        openLog(ctx);
        ctx.flow('updates-log-action', { ACTION: 'check' });
        const row = await ctx.waitText(/Release, (staged|unchanged)/, 15 * 60_000);
        if (/staged/.test(row)) {
          await toMap(ctx);
          ctx.device.background(); await ctx.device.sleep(3000); ctx.device.launch();
          openLog(ctx);
          await ctx.waitText(/Release, applied/, 30_000);
        }
        const head = ctx.device.screenText().find((w) => /release r-/.test(w)) ?? '';
        if (target && !head.includes(target)) throw new Error(`in use: ${head} (want ${target})`);
        await coldStart(ctx, { ms: 90_000 });
        await ctx.device.sleep(5000);
        return head;
      });
    }
    if (GROUND === 'satellite') {
      await ctx.step('ground: satellite (PAN_GROUND)', async () => {
        ctx.flow('layers-satellite');
        ctx.flow('tap-text', { TEXT: 'Done' });
        await ctx.device.sleep(6000);
      });
    }
    const made = [];
    if (SPOTS > 0) {
      await ctx.step(`${SPOTS} spots saved round Cultus Lake, the map shown on the last (PAN_SPOTS)`, async () => {
        const d = ctx.device;
        await d.net('offline');
        for (let i = 0; i < SPOTS; i++) {
          const title = `Pan ${i + 1} ${Date.now().toString(36).slice(-4)}`;
          d.geo(LAKE.lon + ((i % 3) - 1) * 0.006, LAKE.lat + (Math.floor(i / 3) - 0.5) * 0.004);
          await d.sleep(1500);
          ctx.flow('tab', { TAB: 'Spots' });
          ctx.flow('tap-text', { TEXT: 'At my location' });
          await d.sleep(3000);
          await capture(ctx);
          ctx.flow('spot-name-save', { TITLE: title });
          await d.sleep(2500);
          made.push(title);
        }
        await d.net('wifi');
        ctx.flow('tab', { TAB: 'Spots' });
        ctx.flow('tap-text', { TEXT: made.at(-1) });
        await d.sleep(2000);
        ctx.flow('scroll-to', { TEXT: 'Show on map' });
        ctx.flow('tap-text', { TEXT: 'Show on map' });
        await d.sleep(6000);
        return made.join(', ');
      });
    }
    const [w, h] = (/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.35), dy = Math.round(h * 0.18);
    // the same path every run: east, north, west, south, … so the camera ends near where it started
    const loop = [[-dx, 0], [0, dy], [dx, 0], [0, -dy]];
    // round 1 circles one place (its tiles warm after the first lap); round 2 TRAVELS (east, then north: new tiles every
    // swipe), which is what a reader exploring does
    const travel = [[-dx, 0], [-dx, 0], [-dx, 0], [-dx, 0], [0, dy], [0, dy], [0, dy], [0, dy]];
    for (let r = 1; r <= ROUNDS; r++) {
      const moves = r % 2 ? loop : travel;
      await ctx.step(`pan round ${r}/${ROUNDS} (${r % 2 ? 'circling' : 'travelling'}): ${SWIPES} swipes`, async () => {
        ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID} reset`);
        ctx.device.logcatClear();
        const t0 = Date.now();
        const layer = mapLayer(ctx), times = [];
        for (let i = 0; i < SWIPES; i++) {
          const [mx, my] = moves[i % moves.length];
          ctx.device.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 350`);
          await ctx.device.sleep(900);
          if (layer) times.push(...presents(ctx, layer));            // each swipe's frames (the layer keeps ~128)
        }
        await ctx.device.sleep(2500);                                // the last settle's colours
        const mf = mapFrames(times);
        const g = gfx(ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID}`));
        const js = ctx.device.logcatJs().split('\n').filter((l) => /\[pan\]|T74PROBE/.test(l));
        fs.writeFileSync(path.join(ctx.dir, `T273-pan-${r}.txt`), `${JSON.stringify(g)}\n${JSON.stringify(mf)}\n${js.join('\n')}\n`);
        // the pan probe's busy seconds (T273, `native/panProbe.ts`): JS lag, tiles labelled, feature-state round trips
        const ps = js.map((l) => { try { return JSON.parse(l.slice(l.indexOf('[pan] ') + 6)); } catch { return null; } })
          .filter(Boolean);
        const sum = (k) => ps.reduce((n, p) => n + (p[k] || 0), 0), max = (k) => ps.reduce((n, p) => Math.max(n, p[k] || 0), 0);
        const acks = ps.filter((p) => p.ackMean > 0);
        const probe = ps.length ? ` · JS lag max ${max('lagMax')} ms, ${sum('late')} ticks >100 ms late, tiles ${sum('tiles')}, ` +
          `labels ${sum('labels')}, sends ${sum('sends')} (ack max ${max('ackMax')} ms, mean ${acks.length ? Math.round(acks.reduce((n, p) => n + p.ackMean, 0) / acks.length) : 0} ms), ` +
          `map renders ${sum('renders')}` : ' · no pan probe lines (build without EXPO_PUBLIC_PAN_PROBE)';
        const jank = g.frames ? `${Math.round((g.janky / g.frames) * 1000) / 10} %` : '?';
        return `${Math.round((Date.now() - t0) / 1000)} s · MAP SURFACE ${layer ? `${mf.frames} frames, >20 ms ${mf.over20}, >50 ms ${mf.over50}, ` +
          `p50 ${mf.p50} / p95 ${mf.p95} / p99 ${mf.p99} / max ${mf.max} ms` : 'not found'} · UI (gfxinfo) frames ${g.frames}, janky ${g.janky} (${jank}), ` +
          `p50 ${g.p50} / p90 ${g.p90} / p95 ${g.p95} / p99 ${g.p99} ms, slow UI ${g.slowUi}, slow draw ${g.slowDraw}` +
          probe;
      });
    }
    if (made.length) {
      await ctx.step('clean up: the PAN_SPOTS spots deleted', async () => {
        for (const title of made) {
          ctx.flow('tab', { TAB: 'Spots' });
          ctx.flow('tap-text', { TEXT: title });
          await ctx.device.sleep(1500);
          ctx.flow('spot-delete');
          await ctx.device.sleep(1500);
        }
      }, { shot: false });
    }
    if (GROUND === 'satellite') {
      await ctx.step('ground back to the map', async () => {
        ctx.flow('layers-satellite');
        ctx.flow('tap-text', { TEXT: 'Done' });
      }, { shot: false });
    }
  },
};
