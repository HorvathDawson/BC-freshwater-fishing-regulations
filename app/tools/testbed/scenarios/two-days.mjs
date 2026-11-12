// (T603, T582) THE TWO-DAYS LINE ON THE PHONE: the regulations day (the map's day strip) set to a day inside the forecast
// window, then to one past it; each time a gauged water's Conditions must say how that day sits against its own:
//   today   no line, the chart marks Today only
//   inside  "Regulations day: <day>, marked on the chart." and the chart carries the regulations-day mark
//   past    "<day> is past the forecast." and no regulations-day mark
// The whole composed sentence is compared (AGENTS rule 33), the day named as B.C.'s (lib/screen.mjs `bcDay`). FAILS when
// the line is missing, reads otherwise, is cut off, or the mark is missing / still drawn.
//
//   node app/tools/testbed/run.mjs two-days        TWO_DAYS_Q / TWO_DAYS_ROW: the gauged water
import path from 'node:path';
import { coldStart, openWater, closeWaters, toMap } from './common.mjs';
import { bcDay, find, inside, labelProblem, look, screenSize, scrollTo, shown, tap } from '../lib/screen.mjs';

const Q = process.env.TWO_DAYS_Q ?? 'Coquitlam River';
const ROW = process.env.TWO_DAYS_ROW ?? '.* · Region 2 · MU 2-8.*';
const PANE_BAND = { top: 720, bottom: 2300 };
/** core `FORECAST_WINDOW` (gauge-policy FORECAST_DAYS: 1..10): the first day past it */
const PAST = 11;

/** The map's day strip pressed until it is `want` days from today (it starts wherever it was left). */
async function setDay(ctx, want) {
  const d = ctx.device;
  await closeWaters(ctx);
  await toMap(ctx);
  const said = async () => find(await look(d), 'resource-id="map-day"', { ends: '-pick' })?.desc ?? '';
  // back to today first: the strip names it, so the count below starts from a known day
  for (let i = 0; i < 40 && !/coloured for today/i.test(await said()); i++) {
    tap(d, find(await look(d), 'resource-id="map-day"', { ends: '-earlier' }));
    await d.sleep(500);   // poll interval: the strip redraws
  }
  if (!/coloured for today/i.test(await said())) throw new Error(`the map's day would not go back to today (it says "${await said()}")`);
  for (let i = 0; i < want; i++) {
    const before = await said();
    tap(d, find(await look(d), 'resource-id="map-day"', { ends: '-later' }));
    await ctx.until(async () => (await said()) !== before, { ms: 5000, what: `the map's day moving on from "${before}"` });
  }
  return said();
}

/** The water's Conditions for the day set, the day block read; then the chart brought on screen and its marks read. */
async function conditions(ctx) {
  const d = ctx.device;
  await openWater(ctx, { q: Q, row: ROW, view: 'conditions' });
  await ctx.until(() => ctx.hasId('conditions-day-label'), { ms: 30_000, what: 'the Conditions view (conditions-day-label)' });
  const top = await look(d);
  const label = find(top, 'resource-id="conditions-day-label"');
  const link = find(top, 'resource-id="conditions-day-link"');
  // the chart: its legend on screen means the plot above it is too
  await scrollTo(d, 'resource-id="chart-legend"', { ...PANE_BAND, top: 1250 });
  await ctx.until(() => ctx.hasId('chart-mark-today'), { ms: 20_000, what: 'the chart drawn (chart-mark-today)', soft: true });
  const ns = await look(d);
  const legend = find(ns, 'resource-id="chart-legend"');
  // the plot: the drawing just above the legend
  const plot = ns.filter((n) => n.cls === 'SvgView' && shown(n) && n.box.y1 <= legend.box.y0 && n.box.x1 - n.box.x0 > 400).at(-1) ?? null;
  return { label, link, plot, today: find(ns, 'resource-id="chart-mark-today"'), regs: find(ns, 'resource-id="chart-mark-regs"') };
}

export const twoDays = {
  id: 'two-days',
  title: `The two-days line (T603, T582): ${Q}'s Conditions with the regulations day today, inside the forecast, past it`,
  async run(ctx) {
    const d = ctx.device;
    const { W } = screenSize(d);
    await ctx.step('setup: Wi-Fi, a cold start', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx);
    }, { shot: false, stop: true });

    await ctx.step('regulations day = today: no line, the chart marks Today only', async () => {
      await setDay(ctx, 0);
      const c = await conditions(ctx);
      if (c.label?.text !== 'Conditions: today') throw new Error(`the day label reads "${c.label?.text}", not "Conditions: today"`);
      if (c.link) throw new Error(`a two-days line is up with both days on today: "${c.link.text}"`);
      if (!c.plot) throw new Error('no chart on the gauge card');
      if (!shown(c.today)) throw new Error('the chart does not mark Today');
      if (c.regs) throw new Error('the chart marks a regulations day while it is today');
      return `${c.label.text} · chart: Today marked, no regulations-day mark`;
    });

    await ctx.step('regulations day inside the forecast (tomorrow): the line, and the chart marks it', async () => {
      const name = bcDay(1);
      const strip = await setDay(ctx, 1);
      const c = await conditions(ctx);
      const want = `Regulations day: ${name}, marked on the chart.`;
      if (!c.link) throw new Error(`no two-days line (want "${want}"; the map's day strip says "${strip}")`);
      if (c.link.text !== want) throw new Error(`the line reads "${c.link.text}", want "${want}"`);
      const cut = labelProblem(c.link, { screenW: W });
      if (cut) throw new Error(cut);
      if (c.label?.text !== 'Conditions: today') throw new Error(`the regulations day moved Conditions' own day: "${c.label?.text}"`);
      if (!c.plot) throw new Error('no chart on the gauge card');
      if (!shown(c.regs)) throw new Error(`the chart does not mark the regulations day (${name})`);
      if (!inside(c.regs, c.plot, 4)) throw new Error(`the regulations-day mark is outside the plot (mark ${JSON.stringify(c.regs.box)}, plot ${JSON.stringify(c.plot.box)})`);
      d.screenshot(path.join(ctx.dir, 'T603-two-days-inside-chart.png'));
      return `"${c.link.text}" · mark at x ${c.regs.box.x0}–${c.regs.box.x1} in the plot ${c.plot.box.x0}–${c.plot.box.x1}`;
    });

    await ctx.step(`regulations day past the forecast (${PAST} days on): the line, and no mark`, async () => {
      const name = bcDay(PAST);
      const strip = await setDay(ctx, PAST);
      const c = await conditions(ctx);
      const want = `${name} is past the forecast.`;
      if (!c.link) throw new Error(`no two-days line (want "${want}"; the map's day strip says "${strip}")`);
      if (c.link.text !== want) throw new Error(`the line reads "${c.link.text}", want "${want}"`);
      const cut = labelProblem(c.link, { screenW: W });
      if (cut) throw new Error(cut);
      if (c.label?.text !== 'Conditions: today') throw new Error(`the regulations day moved Conditions' own day: "${c.label?.text}"`);
      if (!shown(c.today)) throw new Error('the chart does not mark Today');
      if (c.regs) throw new Error(`the chart still marks a regulations day past the forecast (${JSON.stringify(c.regs.box)})`);
      d.screenshot(path.join(ctx.dir, 'T603-two-days-past-chart.png'));
      return `"${c.link.text}" · chart: Today marked, no regulations-day mark`;
    });

    await ctx.step('clean up: the map back on today', async () => {
      const said = await setDay(ctx, 0);
      return said;
    });
  },
};
