// (T315) A GAUGE VIEWED ONLINE STILL DRAWS ITS MEASURED LINE OFFLINE: Vedder River's Conditions opened on Wi-Fi (the
// station file is kept on the phone, `gaugeHistory`), then airplane mode and a COLD START (no session copy left): the
// same Conditions draws its chart from the phone's kept week, saying "Offline — measured line as of …".
import { coldStart, toMap } from './common.mjs';

const WATER = process.env.GAUGE_WATER ?? 'Vedder River';
const AS_OF = /^Offline — measured line (as of .*|saved on this phone.*)/;

async function conditions(ctx) {
  await toMap(ctx);
  ctx.flow('search', { Q: WATER });
  ctx.flow('open-result', { LINE: WATER });
  await ctx.waitText(/Regulations/, 30_000);
  ctx.flow('tap-text', { TEXT: 'Conditions' });
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
      const w = await conditions(ctx);
      const note = w.find((x) => AS_OF.test(x));
      if (!note) throw new Error(`no "as of" note: the chart was not drawn from the phone (saw: ${w.slice(0, 12).join(' | ').slice(0, 400)})`);
      if (w.some((x) => /Loading the record/.test(x))) throw new Error('the chart is still loading offline');
      return note;
    });
    await ctx.step('back online', async () => { await ctx.device.net('wifi'); }, { shot: false });
  },
};
