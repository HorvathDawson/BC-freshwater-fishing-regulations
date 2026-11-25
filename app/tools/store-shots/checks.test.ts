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
  it('the conditions chart needs the forecast switched on', () => {
    expect(judge('11-cond-chart', page())).toEqual([]);
    expect(judge('11-cond-chart', page({ clever: false }))[0]).toMatch(/forecast/);
  });
  it('the sections shot needs more than one part', () => {
    expect(judge('15-split-rules', page(), { parts: 4 })).toEqual([]);
    expect(judge('15-split-rules', page(), { parts: 1 })[0]).toMatch(/more than one/);
    expect(judge('15-split-rules', page(), {})[0]).toMatch(/more than one/);
  });
  it('the Conditions maps need the 20 km camera and coloured reaches', () => {
    expect(judge('12-cond-change', page({ text: 'Change 3 reaches coloured', ids: ['map-horizons-change'] }))).toEqual([]);
    expect(judge('12-cond-change', page({ text: 'Change 3 reaches coloured', ids: ['map-horizons-change'], scale: '10 km' }))[0]).toMatch(/20 km/);
    expect(judge('13-cond-forecast', page({ text: 'Forecast still loading', ids: [] })).join()).toMatch(/still loading/);
  });
});
