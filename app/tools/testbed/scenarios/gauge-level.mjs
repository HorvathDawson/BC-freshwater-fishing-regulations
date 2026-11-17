// (T830) A GAUGE THAT READS A LEVEL LIVE WITH NO LEVEL RECORD: Capilano River at Canyon (08GA031) has two years of HYDAT
// level, so no level band. Its card under Flow is ranked as before; where the gauge index says the station reports a level
// (`reports`, the publisher's), the card offers Level and shows the level in metres with the one Python line in place of a
// standing ("Not ranked: no long record of its level to compare with."). Never "Reports flow only today". A feed from
// before `reports` offers no Level (a finding, not a failure: the worker that writes it is not deployed yet).
// `GAUGE_LEVEL_WATER` / `GAUGE_LEVEL_ROW` name another water.
import { coldStart, openWater } from './common.mjs';

const WATER = process.env.GAUGE_LEVEL_WATER ?? 'Capilano River';
const ROW = process.env.GAUGE_LEVEL_ROW ?? '.*Region 2.*';
const NO_RECORD = 'Not ranked: no long record of its level to compare with.';
const FLOW_ONLY = /Reports flow only today, not water level/;

export const gaugeLevel = {
  id: 'gauge-level',
  title: `T830: ${WATER}'s gauge reads a level with no record: Level offered unranked, never "flow only"`,
  async run(ctx) {
    await ctx.step(`${WATER} → Conditions, the gauge card under Flow`, async () => {
      await ctx.device.net('wifi');
      await coldStart(ctx);
      await openWater(ctx, { q: WATER, row: ROW, view: 'conditions' });
      await ctx.device.sleep(12_000);                          // the reading, the chart and its envelope
      try { ctx.flow('scroll-to', { TEXT: '72 h' }); } catch { /* said below */ }
      const w = ctx.device.screenText();
      if (w.some((x) => FLOW_ONLY.test(x))) throw new Error('the card says the gauge reports flow only');
      if (!w.some((x) => /m³\/s/.test(x))) throw new Error(`no flow reading (saw: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
      return w.find((x) => /m³\/s/.test(x)) ?? 'flow';
    });
    await ctx.step('Level on the card: the level in metres, unranked, in Python\'s words', async () => {
      const w0 = ctx.device.screenText();
      if (!w0.includes('Level')) {
        ctx.finding('no Level choice on the card: the gauge index carries no `reports` (a feed from before T830)');
        return 'no Level choice';
      }
      ctx.flow('tap-text', { TEXT: 'Level' });
      await ctx.device.sleep(6_000);
      const w = ctx.device.screenText();
      if (w.some((x) => FLOW_ONLY.test(x))) throw new Error('under Level the card says the gauge reports flow only');
      if (!w.includes(NO_RECORD)) throw new Error(`no "${NO_RECORD}" (saw: ${w.slice(0, 14).join(' | ').slice(0, 400)})`);
      return NO_RECORD;
    });
  },
};
