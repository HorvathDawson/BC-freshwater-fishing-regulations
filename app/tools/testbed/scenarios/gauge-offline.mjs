// (T315) A GAUGE VIEWED ONLINE STILL DRAWS ITS MEASURED LINE OFFLINE: a gauged water's Conditions opened on Wi-Fi (the
// station file is kept on the phone, `gaugeHistory`), then airplane mode and a COLD START (no session copy left): the
// same Conditions draws its chart from the phone's kept week, saying "Offline — measured line as of …".
//
// (T603) THE WAY IN IS `openWater` (common.mjs), as the other Conditions scenarios': the result row by its line, then the
// view's own tab by id. The old way searched "Vedder River" (which now lists as "see Chilliwack / Vedder Rivers", so no
// water opened) and tapped the text "Conditions", which then landed on the Conditions TAB. The default water is the one
// the recorded T603 run opens (Coquitlam River, 08MH002); `GAUGE_WATER` + `GAUGE_ROW` name another.
import { coldStart, openWater } from './common.mjs';

const WATER = process.env.GAUGE_WATER ?? 'Coquitlam River';
const ROW = process.env.GAUGE_ROW ?? '.* · Region 2 · MU 2-8.*';
const AS_OF = /^Offline — measured line (as of .*|saved on this phone.*)/;

async function conditions(ctx) {
  await openWater(ctx, { q: WATER, row: ROW, view: 'conditions' });
  await ctx.device.sleep(10_000);                     // the reading, the chart and its envelope
  return ctx.device.screenText();
}

export const gaugeOffline = {
  id: 'gauge-offline',
  title: `T315: a gauge viewed online (${WATER}) still draws its measured line offline after a cold start`,
  async run(ctx) {
    await ctx.step(`online: ${WATER} → Conditions, the chart drawn live (no "as of")`, async () => {
      await ctx.device.net('wifi');
      await coldStart(ctx);
      const w = await conditions(ctx);
      if (w.some((x) => AS_OF.test(x))) throw new Error('online, the chart says it is the kept copy');
      if (!w.some((x) => /m³\/s|\bm\b/.test(x))) throw new Error(`no reading or chart (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      await ctx.device.sleep(3000);                    // the kept week is written a moment after the read
      return w.find((x) => /m³\/s/.test(x)) ?? 'chart';
    });
    await ctx.step('airplane mode + cold start: the same Conditions draws the measured line, "as of" its age', async () => {
      await ctx.device.net('offline');
      await coldStart(ctx, { ms: 60_000 });
      await conditions(ctx);
      // the note sits under the chart, below the fold
      try { ctx.flow('scroll-to', { TEXT: 'Offline — measured line.*' }); } catch { /* said below */ }
      const w = ctx.device.screenText();
      if (w.some((x) => /^NO ESTIMATE$|The live readings did not arrive/.test(x))) ctx.finding('offline: the estimate said "The live readings did not arrive" (the kept index was not used)');
      const note = w.find((x) => AS_OF.test(x));
      if (!note) throw new Error(`no "as of" note: the chart was not drawn from the phone (saw: ${w.slice(0, 12).join(' | ').slice(0, 400)})`);
      if (w.some((x) => /Loading the record/.test(x))) throw new Error('the chart is still loading offline');
      return note;
    });
    await ctx.step('back online', async () => { await ctx.device.net('wifi'); }, { shot: false });
  },
};
