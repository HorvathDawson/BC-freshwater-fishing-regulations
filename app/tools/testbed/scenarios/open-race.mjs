// (T734) A WATER OPENED BEFORE ITS MAP IS READY. Build 32's verify lane saw the app close twice with
// `NullPointerException at MLRNMapView.project(MLRNMapView.kt:1653)`: a `project()` call (the boundary marks' label
// shift) reached a MapLibre view whose map did not exist yet (`mapLibreMap!!`), thrown on the native main thread where
// no JS `catch` can take it. Once in the pane-open hold (6th Fraser open), once by hand seconds after a cold start
// (search "Chilliwack River" → Open).
//
// Each cycle opens a water by search → Open as fast as the flows go and reads the crash buffer and the app's pid:
//   cold  OPEN_RACE_COLD cycles (default 30): force-stop, launch, the map up, then at once search → Open
//   warm  OPEN_RACE_WARM cycles (default 30): the water closed, then search → Open again on the same process
// The waters alternate (OPEN_RACE_WATERS, default Chilliwack River and Fraser River: the two seen). The first crash
// stops the step, its logcat saved beside the report (`open-race-crash-<n>.txt`).
//
//   node app/tools/testbed/run.mjs open-race      (OPEN_RACE_COLD=n OPEN_RACE_WARM=n)
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { closeWaters, coldStart, toMap } from './common.mjs';

const ADB = process.env.ADB ?? 'adb';
const COLD = Number(process.env.OPEN_RACE_COLD ?? 30);
const WARM = Number(process.env.OPEN_RACE_WARM ?? 30);
const WATERS = [
  { q: 'Chilliwack River', row: 'Stream · .*' },
  { q: 'Fraser River', row: 'Stream · .*' },
];

/** The app's death in the log since the last clear (a Java FATAL EXCEPTION or a native signal), or null. */
function death(d) {
  const log = execFileSync(ADB, ['-s', d.serial(), 'logcat', '-d', '-b', 'main,system,crash']).toString();
  const hit = log.split('\n').filter((l) => /FATAL EXCEPTION|Fatal signal|SIGSEGV|SIGABRT/.test(l));
  return hit.length ? { log, first: hit[0] } : null;
}

/** One search → Open; throws with the crash (its logcat saved) when the app died under it. */
async function openOnce(ctx, n, w) {
  const d = ctx.device;
  ctx.flow('search', { Q: w.q });
  ctx.flow('open-result', { LINE: w.row });
  await d.sleep(2500);
  const dead = death(d);
  const pid = d.pidOf();
  if (dead || !pid) {
    const file = path.join(ctx.dir, `open-race-crash-${n}.txt`);
    fs.writeFileSync(file, dead?.log ?? execFileSync(ADB, ['-s', d.serial(), 'logcat', '-d', '-b', 'all']).toString());
    const why = dead ? dead.log.split('\n').filter((l) => /AndroidRuntime|Fatal signal|backtrace/.test(l)).slice(0, 6).join(' / ') : 'no pid';
    throw new Error(`cycle ${n} (${w.q}): the app died — ${why.slice(0, 700)} (log: ${path.basename(file)})`);
  }
}

export const openRace = {
  id: 'open-race',
  title: `A water opened by search at once, ${COLD} cold + ${WARM} warm, no crash (T734)`,
  async run(ctx) {
    const d = ctx.device;
    await ctx.step('the map up (Wi-Fi)', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
    }, { stop: true });
    if (COLD > 0) await ctx.step(`cold start → search → Open at once × ${COLD}, no crash`, async () => {
      const t0 = Date.now();
      for (let k = 0; k < COLD; k++) {
        await coldStart(ctx, { ms: 90_000 });
        execFileSync(ADB, ['-s', d.serial(), 'logcat', '-b', 'all', '-c']);
        await openOnce(ctx, k + 1, WATERS[k % WATERS.length]);
      }
      return `${COLD} cold opens, no FATAL EXCEPTION / Fatal signal (${Math.round((Date.now() - t0) / 1000)} s)`;
    });
    if (WARM > 0) await ctx.step(`search → Open → close × ${WARM} on one process, no crash`, async () => {
      await closeWaters(ctx);
      await toMap(ctx);
      execFileSync(ADB, ['-s', d.serial(), 'logcat', '-b', 'all', '-c']);
      const pid = d.pidOf(), t0 = Date.now();
      for (let k = 0; k < WARM; k++) {
        await openOnce(ctx, k + 1, WATERS[k % WATERS.length]);
        await closeWaters(ctx);
        await d.sleep(800);
        if (d.pidOf() !== pid) throw new Error(`warm cycle ${k + 1}: the app's pid moved (${pid} → ${d.pidOf() ?? 'none'})`);
      }
      return `${WARM} warm opens on pid ${pid}, no FATAL EXCEPTION / Fatal signal (${Math.round((Date.now() - t0) / 1000)} s)`;
    });
  },
};
