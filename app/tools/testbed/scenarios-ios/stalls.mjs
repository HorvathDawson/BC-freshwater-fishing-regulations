// (T720, IOS-3) THE iOS TWIN OF scenarios/stalls.mjs (T716 `tap-stalls`): no JS-thread stall of STALL_MAX ms or more at
// the cold start, the map's Salmon colouring, the Run timing tab and the bell, on the simulator. The probe lines are the
// app's own, the same on both platforms: apps/mobile index.js logs `[stall] {"at","ms"}` for every JS-thread hold over
// 50 ms and openProbe `[work] <label> <ms>` for work that held it 20 ms or more, both on EXPO_PUBLIC_PAN_PROBE=1, which
// `build-ios-sim.sh --testbed` and `--probes` both set. On iOS they reach the unified log
// (`[com.facebook.react.log:javascript]`; ios-t313g had 357 `[stall]` and 210 `[work]` lines), streamed by run-ios.mjs to
// app-os.log and read through lib/device-ios.mjs's `logcatClear` / `logcatJs` (a mark in that file, as logcat -c).
// What differs from Android: Back (no system back: the screens' own back / Done, run-ios.mjs `toMap`) and no `net`.
//
//   node app/tools/testbed/run-ios.mjs tap-stalls        (STALL_MAX=ms, default 300)
//
// Unify with scenarios/stalls.mjs once the Android tap/back work lands (only Back and the cold start differ).
const STALL_MAX = Number(process.env.STALL_MAX ?? 300);

/** The `[stall]` and `[work]` lines since the mark. */
function seen(ctx) {
  const log = ctx.device.logcatJs().split('\n');
  const stalls = log.filter((l) => l.includes('[stall] ')).map((l) => { try { return JSON.parse(l.slice(l.indexOf('[stall] ') + 8)).ms; } catch { return null; } })
    .filter((x) => Number.isFinite(x));
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

export const tapStallsIos = {
  id: 'tap-stalls',
  inAll: false,                                    // a bench, run by name: not part of the T718 baseline's `all`
  title: `No JS stall of ${STALL_MAX} ms or more (iOS): cold start, Salmon colouring, Run timing, the bell`,
  async run(ctx) {
    const { appUp, toMap } = ctx.ios;
    await ctx.step('cold start, 20 s watched', async () => {
      ctx.device.stop();
      await ctx.device.sleep(1000);
      const note = await watch(ctx, 'cold start', async () => { await appUp(ctx); }, 20_000);
      // a probe build logs [heap] every 2 s: none at all means the lines are not reaching the log (not a pass)
      if (!ctx.device.logcatJs().includes('[heap] ')) throw new Error(`no probe line in the app log in 20 s: a build without --testbed/--probes, or the log stream is not running — ${note}`);
      return note;
    });
    await ctx.step('the map coloured for Salmon', async () => {
      await toMap(ctx);
      return watch(ctx, 'Salmon', async () => { ctx.flow('tap-id', { ID: 'regs-paint-salmon' }); }, 8000);
    });
    await ctx.step('the Run timing tab', async () =>
      watch(ctx, 'Run timing', async () => { ctx.flow('tab', { TAB: 'Runs' }); }, 8000));
    await ctx.step('back on the map, the bell opened (the in-season inbox)', async () => {
      await toMap(ctx);
      await ctx.device.sleep(3000);
      const note = await watch(ctx, 'bell', async () => { ctx.flow('open-bell'); }, 4000);
      // the inbox is a sheet: its Done, else the screens' own way back
      if (ctx.device.screenText().includes('Done')) ctx.flow('tap-text', { TEXT: 'Done' });
      await ctx.device.sleep(1500);
      return note;
    });
    await ctx.step('the map back to Standard', async () => {
      await toMap(ctx);
      return watch(ctx, 'Trout', async () => { ctx.flow('tap-id', { ID: 'regs-paint-standard' }); }, 4000);
    });
  },
};
