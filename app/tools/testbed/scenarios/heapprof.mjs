// (T360) WHERE CONDITIONS' NATIVE MEMORY GOES: Perfetto's heapprofd attached to the running app on the Map tab, then
// Conditions opened and left to settle; at the trace's end heapprofd dumps every sampled allocation still held since it
// attached, with its call stack. `heapprof-conditions.pftrace` lands beside the report; `heapprof.py` (this folder's
// ../heapprof.py) groups what is held by library and by function. Needs a profileable APK (session.sh patches every
// test-bed build). A measurement, no budget.
//
//   node app/tools/testbed/run.mjs heapprof          (two traces: Conditions from the Map tab; the rule book opened + closed twice)
import path from 'node:path';
import { closeWaters, coldStart, toMap } from './common.mjs';

const SECONDS = Number(process.env.HEAPPROF_S ?? 45);

const config = (app) => `
buffers { size_kb: 65536 fill_policy: DISCARD }
data_sources {
  config {
    name: "android.heapprofd"
    heapprofd_config {
      sampling_interval_bytes: 4096
      process_cmdline: "${app}"
      shmem_size_bytes: 16777216
      block_client: true
    }
  }
}
duration_ms: ${SECONDS * 1000}
`;

export const heapprof = {
  id: 'heapprof',
  title: 'Native memory held after opening Conditions, by call stack (T360, heapprofd)',
  async run(ctx) {
    const remote = '/data/misc/perfetto-traces/tb-heapprof.pftrace';
    await ctx.step('the map drawn (no clean install: the release already held)', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 600_000 });
      await closeWaters(ctx); await toMap(ctx);
      await ctx.device.sleep(8000);
      return `pid ${ctx.device.pidOf()}`;
    }, { stop: true });
    /** heapprofd attached now, `act` done, the trace held until it ends; pulled as heapprof-<name>.pftrace. */
    const traced = async (name, act) => {
      ctx.device.sh(`rm -f ${remote}`);
      ctx.device.sh(`echo '${config(ctx.device.APP_ID)}' | perfetto --txt -c - -o ${remote} --background`);
      await ctx.device.sleep(4000);                              // attached (block_client: nothing is missed after this)
      const end = Date.now() + SECONDS * 1000 + 10_000;
      await act();
      // `pidof` exits 1 once perfetto is gone
      while (Date.now() < end && ctx.device.sh('pidof perfetto || true').trim()) await ctx.device.sleep(2000);
      const local = path.join(ctx.dir, `heapprof-${name}.pftrace`);
      ctx.device.adb(['pull', remote, local]);
      return `${local} (python app/tools/testbed/heapprof.py ${local})`;
    };
    await ctx.step(`Conditions from the Map tab, ${SECONDS} s traced`, () => traced('conditions', async () => {
      ctx.flow('tab', { TAB: 'Conditions' });
      await ctx.device.sleep(3000);
      if (ctx.device.screenText().includes('Skip')) ctx.flow('tap-text', { TEXT: 'Skip' });   // a first-use tour
    }));
    // (T360) the rule book opened and closed twice on a water's page: what each viewer leaves behind
    await ctx.step(`the rule book opened and closed twice, ${SECONDS} s traced`, async () => {
      await toMap(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('open-result', { LINE: 'Lake · .*Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 30_000);
      const LINK = 'Rule book p\\.[2-9][0-9] ↗.*';
      ctx.flow('scroll-to', { TEXT: LINK });
      ctx.device.adb(['logcat', '-c']);
      const got = await traced('rulebook', async () => {
        for (let i = 0; i < 2; i++) {
          ctx.flow('tap-text', { TEXT: LINK });
          await ctx.waitText(/^Page \d+ of \d+$/, 30_000);
          await ctx.device.sleep(2000);
          ctx.device.sh('input keyevent KEYCODE_BACK');
          await ctx.device.sleep(3000);
        }
      });
      // the viewer's close (the react-native-pdf patch): did it run, and how many pages did it find open
      const log = ctx.device.adb(['logcat', '-d', '-s', 'PdfView:*']).toString().split('\n').filter((l) => /T360|recycle/.test(l));
      return `${got} · PdfView: ${log.length ? log.map((l) => l.replace(/^.*PdfView\s*:\s*/, '')).join(' | ') : 'no PdfView line'}`;
    });
  },
};
