// (T716, owner 2026-11-13: "The salmon build seems way to long?") NO LONG STALL OF THE JS THREAD where the DFO salmon is
// first asked: the cold start, the map's Salmon colouring, the Run timing tab and the bell (the in-season inbox). The app's
// test-bed build logs every JS-thread stall over 50 ms (`[stall]`, apps/mobile index.js) and every salmon composition
// that held the thread 20 ms or more (`[work] dfo pick`); each step lists what it saw and FAILS on a stall of
// STALL_MAX ms or more (default 300; before T716 the first DFO composition held the thread ~1.0 s). The first Fraser
// open is `regs-open`'s.
//
//   node app/tools/testbed/run.mjs tap-stalls
import { coldStart, toMap } from './common.mjs';

const STALL_MAX = Number(process.env.STALL_MAX ?? 300);

/** The `[stall]` and `[work]` lines since the log was last cleared. */
function seen(ctx) {
  const log = ctx.device.logcatJs().split('\n');
  const stalls = log.filter((l) => l.includes('[stall] ')).map((l) => JSON.parse(l.slice(l.indexOf('[stall] ') + 8)).ms);
  const work = log.filter((l) => l.includes('[work] ')).map((l) => l.slice(l.indexOf('[work] ') + 7));
  return { stalls, work };
}

async function watch(ctx, what, act, ms) {
  ctx.device.logcatClear();
  await act();
  await ctx.device.sleep(ms);
  const { stalls, work } = seen(ctx);
  const worst = Math.max(0, ...stalls);
  const note = `${what}: stalls ${stalls.length ? stalls.join(', ') : 'none'} ms (worst ${worst}); work ${work.join('; ') || 'none'}`;
  if (worst >= STALL_MAX) throw new Error(`a ${worst} ms stall (limit ${STALL_MAX}) — ${note}`);
  return note;
}

export const tapStalls = {
  id: 'tap-stalls',
  title: `No JS stall of ${STALL_MAX} ms or more: cold start, Salmon colouring, Run timing, the bell`,
  async run(ctx) {
    await ctx.step('cold start (Wi-Fi), 20 s watched', async () => {
      await ctx.device.net('wifi');
      ctx.device.stop();
      return watch(ctx, 'cold start', async () => { await coldStart(ctx, { ms: 90_000 }); }, 20_000);
    });
    await ctx.step('the map coloured for Salmon', async () => {
      await toMap(ctx);
      return watch(ctx, 'Salmon', async () => { ctx.flow('tap-id', { ID: 'regs-paint-salmon' }); }, 8000);
    });
    await ctx.step('the Run timing tab', async () =>
      watch(ctx, 'Run timing', async () => { ctx.flow('tab', { TAB: 'Runs' }); }, 8000));
    await ctx.step('back on the map, the bell opened (the in-season inbox)', async () => {
      ctx.flow('tab', { TAB: 'Map' });
      await ctx.device.sleep(3000);
      const note = await watch(ctx, 'bell', async () => { ctx.flow('open-bell'); }, 4000);
      ctx.device.sh('input keyevent 4');
      await ctx.device.sleep(1500);
      return note;
    });
    await ctx.step('the map back to Standard', async () => {
      await toMap(ctx);
      return watch(ctx, 'Trout', async () => { ctx.flow('tap-id', { ID: 'regs-paint-standard' }); }, 4000);
    });
  },
};
