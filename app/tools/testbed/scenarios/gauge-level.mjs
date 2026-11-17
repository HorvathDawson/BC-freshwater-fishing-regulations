// (T830) A GAUGE CARD UNDER LEVEL. Default: Capilano River at Canyon (08GA031), which reads a level live with two years of
// HYDAT level, so no level band. Its card under Flow is ranked as before; where the gauge index says the station reports a
// level (`reports`, the publisher's), the card offers Level and shows the level in metres with Python's line in place of a
// standing (`record.NO_RECORD_LINE`). Never "Reports flow only today". A feed from before `reports` offers no Level (a
// finding, not a failure: the worker that writes it is not deployed yet).
//   GAUGE_LEVEL_WATER / GAUGE_LEVEL_ROW   another water (e.g. Harrison Lake, its own level gauge 08MG012)
//   GAUGE_LEVEL_UNRANKED=0                the card's level is ranked there: Python's no-record line is not expected
//   GAUGE_LEVEL_DP=2                      (owner 2026-11-17 09:57) the level figure must carry exactly that many decimals
import { coldStart, openWater } from './common.mjs';

const WATER = process.env.GAUGE_LEVEL_WATER ?? 'Capilano River';
const ROW = process.env.GAUGE_LEVEL_ROW ?? '.*Region 2.*';
const UNRANKED = process.env.GAUGE_LEVEL_UNRANKED !== '0';
const DP = process.env.GAUGE_LEVEL_DP ? Number(process.env.GAUGE_LEVEL_DP) : null;
const NO_RECORD = 'Too new to say if this is high or low.';
const FLOW_ONLY = /Reports flow only today, not water level/;

/** The card's big figure: the number on its own line just before the unit "m". */
const levelFigure = (w) => { const i = w.findIndex((x, k) => x === 'm' && /^-?\d+(\.\d+)?$/.test(w[k - 1] ?? '')); return i > 0 ? w[i - 1] : null; };

export const gaugeLevel = {
  id: 'gauge-level',
  title: `T830: ${WATER}'s gauge card under Flow and Level: never "flow only"${UNRANKED ? ', an unranked level in Python\'s words' : ''}`,
  async run(ctx) {
    await ctx.step(`${WATER} → Conditions, the gauge card as it opens (Flow)`, async () => {
      await ctx.device.net('wifi');
      await coldStart(ctx);
      await openWater(ctx, { q: WATER, row: ROW, view: 'conditions' });
      await ctx.device.sleep(12_000);                          // the reading, the chart and its envelope
      try { ctx.flow('scroll-to', { TEXT: '72 h' }); } catch { /* said below */ }
      const w = ctx.device.screenText();
      if (w.some((x) => FLOW_ONLY.test(x))) throw new Error('the card says the gauge reports flow only');
      return w.find((x) => /m³\/s|level only/.test(x)) ?? w.slice(0, 6).join(' | ');
    });
    await ctx.step('Level on the card: the level in metres', async () => {
      const w0 = ctx.device.screenText();
      if (!w0.includes('Level')) {
        ctx.finding('no Level choice on the card: the gauge index carries no `reports` (a feed from before T830)');
        return 'no Level choice';
      }
      ctx.flow('tap-text', { TEXT: 'Level' });
      await ctx.device.sleep(6_000);
      const w = ctx.device.screenText();
      if (w.some((x) => FLOW_ONLY.test(x))) throw new Error('under Level the card says the gauge reports flow only');
      if (UNRANKED && !w.includes(NO_RECORD)) throw new Error(`no "${NO_RECORD}" (saw: ${w.slice(0, 14).join(' | ').slice(0, 400)})`);
      const fig = levelFigure(w);
      if (DP !== null && !(fig && new RegExp(`^-?\\d+\\.\\d{${DP}}$`).test(fig)))
        throw new Error(`the level figure is ${JSON.stringify(fig)}, not ${DP} decimals`);
      return `${fig ?? '?'} m${UNRANKED ? ` · ${NO_RECORD}` : ''}`;
    });
  },
};
