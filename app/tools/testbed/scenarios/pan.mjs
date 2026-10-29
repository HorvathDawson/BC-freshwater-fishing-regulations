// (T273) A SCRIPTED PAN, MEASURED: the map up on Wi-Fi, then the same swipes every run (adb `input swipe`, so no
// Maestro in the loop), with the frame stats Android keeps for the app (`dumpsys gfxinfo`: janky frames, frame-time
// percentiles) and the app's own `[pan]` / `T74PROBE` lines from logcat. A benchmark, not a pass/fail: the numbers go
// into the step line and `T273-pan-<n>.txt` beside the report.
//
//   node app/tools/testbed/run.mjs pan          (PAN_SWIPES=n, default 16; PAN_ROUNDS=n, default 2)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart } from './common.mjs';

const SWIPES = Number(process.env.PAN_SWIPES ?? 16);
const ROUNDS = Number(process.env.PAN_ROUNDS ?? 2);

/** `dumpsys gfxinfo <app>`'s summary: frames, janky share, the 50/90/95/99th percentile frame times (ms). */
function gfx(t) {
  const n = (re) => Number(re.exec(t)?.[1] ?? NaN);
  return { frames: n(/Total frames rendered:\s*(\d+)/), janky: n(/Janky frames:\s*(\d+)/),
           p50: n(/50th percentile:\s*(\d+)ms/), p90: n(/90th percentile:\s*(\d+)ms/),
           p95: n(/95th percentile:\s*(\d+)ms/), p99: n(/99th percentile:\s*(\d+)ms/),
           slowUi: n(/Number Slow UI thread:\s*(\d+)/), slowDraw: n(/Number Slow issue draw commands:\s*(\d+)/) };
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
    const [w, h] = (/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.35), dy = Math.round(h * 0.18);
    // the same path every run: east, north, west, south, … so the camera ends near where it started
    const moves = [[-dx, 0], [0, dy], [dx, 0], [0, -dy]];
    for (let r = 1; r <= ROUNDS; r++) {
      await ctx.step(`pan round ${r}/${ROUNDS}: ${SWIPES} swipes`, async () => {
        ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID} reset`);
        ctx.device.logcatClear();
        const t0 = Date.now();
        for (let i = 0; i < SWIPES; i++) {
          const [mx, my] = moves[i % moves.length];
          ctx.device.sh(`input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 350`);
          await ctx.device.sleep(900);
        }
        await ctx.device.sleep(2500);                                // the last settle's colours
        const g = gfx(ctx.device.sh(`dumpsys gfxinfo ${ctx.device.APP_ID}`));
        const js = ctx.device.logcatJs().split('\n').filter((l) => /\[pan\]|T74PROBE/.test(l));
        fs.writeFileSync(path.join(ctx.dir, `T273-pan-${r}.txt`), `${JSON.stringify(g)}\n${js.join('\n')}\n`);
        const jank = g.frames ? `${Math.round((g.janky / g.frames) * 1000) / 10} %` : '?';
        return `${Math.round((Date.now() - t0) / 1000)} s · frames ${g.frames}, janky ${g.janky} (${jank}), ` +
          `p50 ${g.p50} / p90 ${g.p90} / p95 ${g.p95} / p99 ${g.p99} ms, slow UI ${g.slowUi}, slow draw ${g.slowDraw}` +
          ` · app lines ${js.length}`;
      });
    }
  },
};
