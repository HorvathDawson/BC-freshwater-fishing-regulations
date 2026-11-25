// T704: the store-shot content checks refuse a page that lacks the shot's content (each case fails before the content is there).
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs tool, no types
import { judge } from './checks.mjs';

const page = (o: Record<string, unknown> = {}) => ({ text: 'GAME FISH (B.C. RULES) Open Salmon (DFO) 34.7 m³/s', vis: { 'map-day': true, 'keep-strip': true, 'gauge-card': true },
  keepTop: 900, salmonHeadTop: 300, clever: true, canvas: true, scale: '20 km', ...o });

describe('store shot checks', () => {
  it('shot 2 needs salmon above the game-fish calendar', () => {
    expect(judge('19-regs-salmon', page())).toEqual([]);
    expect(judge('19-regs-salmon', page({ salmonHeadTop: 969, keepTop: 281 }))[0]).toMatch(/salmon is not above/);
    expect(judge('19-regs-salmon', page({ salmonHeadTop: null }))[0]).toMatch(/no "Salmon \(DFO\)" block/);
  });
  it('(T1072) shot 18 shows the limits table and not the season calendar', () => {
    const ok = { tableHeadInView: true, tableRowsInView: ['Brook trout', 'Brown trout'], stripInView: false };
    expect(judge('18-regs-trout', page(ok))).toEqual([]);
    expect(judge('18-regs-trout', page({ ...ok, stripInView: true }))[0]).toMatch(/season calendar/);
    expect(judge('18-regs-trout', page({ ...ok, tableHeadInView: false }))[0]).toMatch(/heading/);
    expect(judge('18-regs-trout', page({ ...ok, tableRowsInView: ['Brook trout'] }))[0]).toMatch(/species rows/);
    expect(judge('18-regs-trout', page({ tableHeadInView: false, tableRowsInView: [], stripInView: true })).length).toBe(3);
  });
  it('(T1072) map shots refuse the out-of-coverage band, and an unmeasured view', () => {
    expect(judge('01-map', page(), { band: null })).toEqual([]);
    expect(judge('01-map', page(), { band: [400, 700] })[0]).toMatch(/grey out-of-coverage band/);
    expect(judge('14-split-map', page(), {})[0]).toMatch(/not measured/);
    for (const sc of ['04-salmon', '05-runs']) expect(judge(sc, page({ text: 'Runs' }), { band: [1, 2] })[0]).toMatch(/band/);
  });
  it('(T1072) a page that says "Slow connection" is not shot', () => {
    expect(judge('01-map', page({ text: 'Slow connection — showing what I have' }), { band: null }).join()).toMatch(/Slow connection/);
  });
  it('the conditions chart needs the forecast switched on', () => {
    expect(judge('11-cond-chart', page())).toEqual([]);
    expect(judge('11-cond-chart', page({ clever: false }))[0]).toMatch(/forecast/);
  });
  it('the sections shot needs more than one part', () => {
    expect(judge('15-split-rules', page(), { parts: 4 })).toEqual([]);
    expect(judge('15-split-rules', page(), { parts: 1 })[0]).toMatch(/more than one/);
    expect(judge('15-split-rules', page(), {})[0]).toMatch(/more than one/);
  });
  it('(T1072) the Change panel must name the river\'s own watershed (Lower Fraser for Kanaka Creek), never the neighbour in view', () => {
    const t = (w: string) => ({ text: `Change 3 reaches coloured\n${w} watershed\nNow`, ids: ['map-horizons-change'] });
    expect(judge('12-cond-change', page(t('Lillooet')))[0]).toMatch(/names the "Lillooet" watershed, not "Lower Fraser"/);
    expect(judge('12-cond-change', page({ text: 'Change 3 reaches coloured', ids: ['map-horizons-change'] }))[0]).toMatch(/names the "null" watershed/);
    expect(judge('12-cond-change', page(t('Lillooet')), { watershedSeen: ['Lillooet', 'Seton Lake'] })[0]).toMatch(/tried: Lillooet, Seton Lake/);
  });
  it('the Conditions maps need the 20 km camera and coloured reaches', () => {
    const ok = { text: 'Change 3 reaches coloured\nLower Fraser watershed\nNow', ids: ['map-horizons-change'] };
    expect(judge('12-cond-change', page(ok))).toEqual([]);
    expect(judge('12-cond-change', page({ ...ok, scale: '10 km' }))[0]).toMatch(/20 km/);
    expect(judge('13-cond-forecast', page({ text: 'Forecast still loading', ids: [] })).join()).toMatch(/still loading/);
  });
});
