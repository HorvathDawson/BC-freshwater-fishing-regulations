/**
 * Q161 (owner 2026-10-24): "it should save the date that you set as the time of trip in the add a spot".
 *
 * A spot added for an EARLIER day records that day's gauges — its lead gauge's daily mean (`dayFlow`) and the
 * section's panel with every member's daily mean that day (`panelDay: day`) — through the one history read, never the
 * live feed. A spot added for today is read live, exactly as before. Offline: saved pending, filled by a refresh. A
 * spot saved before this, holding the save day's panel for an earlier trip, is refilled for its trip day. Whole values.
 */
import { describe, expect, it, vi } from "vitest";
import { percentileInBand, type Band } from "@app/core";
import { captureSpot } from "./capture";
import { needsDayFlow, needsDayPanel, panelOfAnswer, savedGauges, type HydatDaily, type HydatDay } from "./dayFlow";
import { needsRefresh, refreshReason, refreshSpot } from "./refresh";
import { DAY_PANEL_WORDS } from "./words";
import { answerFrom, type PanelAnswer } from "../panel";
import type { Aged, Panel, WaterSource, SectionId, Series, StationId } from "../index";
import type { Spot } from "./model";

const BAND: Band = [10, 20, 40, 60, 100];
const NOW = new Date(2026, 9, 23, 12).getTime();               // today, local; the feed was written today
const THREE_DAYS_AGO = new Date(2026, 9, 20, 9, 30).getTime();
const LONG_AGO = new Date(2024, 7, 15, 9, 20).getTime();       // older than the feed's record: HYDAT's

/** Each station's "year" series: this year's daily means on the days given; the same envelope every day. */
function yearOf(values: Record<string, number>): Aged<Series> {
  const doy = (d: string) => Math.round((Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10))
    - Date.UTC(+d.slice(0, 4), 0, 1)) / 86_400_000);
  const v: (number | null)[] = Array.from({ length: 366 }, () => null);
  for (const [d, x] of Object.entries(values)) v[doy(d)] = x;
  return { fetchedAt: NOW, value: { step: "1d", from: "2026-01-01", parameter: "discharge", at: [], values: v,
                                    band: v.map(() => BAND), now: null, forecast: null, forecasts: [],
                                    priorYears: [] } as unknown as Series };
}
const DAYS: Record<string, Record<string, number>> = {
  "08MH001": { "2026-10-20": 30 },        // 0.375
  "08MH999": { "2026-10-20": 60 },        // 0.75
};
const PANEL: Panel = { areaKm2: 40, members: ["08MH001", "08MH999"].map((station) => (
  { station: station as StationId, role: "up" as const, areaKm2: 60, years: { discharge: 40, level: 40 },
    regulated: false, sameRiver: true })) };
/** The live feed, which a trip on an earlier day must never read: today both gauges sit in the bottom tenth. */
const LIVE = { stations: { "08MH001": { percentile: 0.05, discharge: 0.05, parameter: "discharge" as const },
                           "08MH999": { percentile: 0.05, discharge: 0.05, parameter: "discharge" as const } } };

function source(gaugeNow = vi.fn(async () => ({ fetchedAt: NOW, value: { discharge: 11.2, level: 1.38,
  parameter: "discharge", percentile: 0.05, at: new Date(2026, 9, 23, 11, 20).toISOString() } }) as never)) {
  return { gaugeNow, src: {
    panelsFor: async () => new Map([[1 as SectionId, PANEL]]),
    lakeStationsFor: async () => new Map(),
    gaugeForSection: async () => null,
    traceToGauge: async () => [],
    gaugeParameters: async () => ["discharge", "level"],
    gaugeSeries: async (st: string) => yearOf(DAYS[st] ?? {}),
    gaugeNow,
  } as unknown as WaterSource };
}
const hydat = (a: HydatDay): HydatDaily & { asked: string[] } => {
  const asked: string[] = [];
  return { asked, day: async (st, d) => { asked.push(`${st} ${d}`); return a; } };
};
const sky = { at: async () => ({ tempC: 9 }) as never, days: async () => null };
const input = (src: WaterSource, visitedAt: number, history?: { hydat?: HydatDaily; online?: () => boolean }) => ({
  source: src, weather: sky, at: { lat: 49.1, lon: -121.96 }, item: null, section: 1 as SectionId,
  waterName: "Chilliwack River", title: "", now: NOW, visitedAt, feed: { index: async () => LIVE },
  id: () => "s1", ...(history ? { history } : {}) });

/** The panel `answerFrom` gives for these members ranked as given — the one function, asked directly. */
const expected = (ps: Record<string, number>): PanelAnswer => answerFrom(PANEL, { stations: Object.fromEntries(
  Object.entries(ps).map(([s, p]) => [s, { percentile: p, discharge: p, parameter: "discharge" as const }])) }, "discharge");

describe("a spot added for an earlier day records that day (Q161)", () => {
  it("3 days ago: the lead gauge's daily mean and the panel of that day, never the live feed", async () => {
    const { src } = source();
    const s = await captureSpot(input(src, THREE_DAYS_AGO));
    // (`gaugeNow` is still asked by the lead-up, for the quantity the station declares — never frozen as a reading)
    expect([percentileInBand(30, BAND), percentileInBand(60, BAND)]).toEqual([0.375, 0.75]);
    expect({ reading: s.reading, dayFlow: s.dayFlow, panelDay: s.panelDay, panel: s.panel }).toEqual({
      reading: { station: "08MH001", discharge: null, level: null, percentile: null, at: null },
      dayFlow: { state: "filled", station: "08MH001", date: "2026-10-20", discharge: 30, percentile: 0.375,
                 place: 0.375, source: "feed", filledAt: NOW },
      panelDay: { state: "day", date: "2026-10-20" },
      panel: expected({ "08MH001": 0.375, "08MH999": 0.75 }),
    });
    // each member's own value that day, not today's 0.05 (the rows sort by weight; equal members keep panel order)
    expect(Object.fromEntries(s.panel!.rows.map((r) => [r.station, r.percentile]))).toEqual(
      { "08MH001": 0.375, "08MH999": 0.75 });
    expect([savedGauges(s), needsDayPanel(s), needsDayFlow(s)]).toEqual(["day", false, false]);
    expect(DAY_PANEL_WORDS.ofTheDay(s.panelDay!.date)).toBe("Each gauge's daily mean on 20 Oct 2026, the day of the visit.");
  });

  it("today: read live, exactly as before — no day panel, no day flow", async () => {
    const { src, gaugeNow } = source();
    const s = await captureSpot(input(src, NOW - 3_600_000));
    expect(gaugeNow).toHaveBeenCalled();
    expect({ reading: s.reading, dayFlow: "dayFlow" in s, panelDay: "panelDay" in s, panel: s.panel }).toEqual({
      reading: { station: "08MH001", discharge: 11.2, level: 1.38, parameter: "discharge", percentile: 0.05,
                 at: new Date(2026, 9, 23, 11, 20).toISOString() },
      dayFlow: false, panelDay: false, panel: expected({ "08MH001": 0.05, "08MH999": 0.05 }),
    });
    expect(savedGauges(s)).toBe("live");
  });

  it("offline, for a day older than the feed: saved pending; a refresh online fills the day and the panel", async () => {
    const { src } = source();
    const off = await captureSpot(input(src, LONG_AGO, { hydat: hydat({ kind: "none" }), online: () => false }));
    // (`gaugeNow` is still asked by the lead-up, for the quantity the station declares — never frozen as a reading)
    expect({ reading: off.reading, dayFlow: off.dayFlow, panelDay: off.panelDay, panel: off.panel }).toEqual({
      reading: { station: "08MH001", discharge: null, level: null, percentile: null, at: null },
      dayFlow: undefined,
      panelDay: { state: "pending", date: "2024-08-15", members: PANEL },
      panel: null,
    });
    expect([savedGauges(off), needsDayFlow(off), needsDayPanel(off), needsRefresh(off)]).toEqual(["pending", true, true, true]);
    const h = hydat({ kind: "day", discharge: 19.6, level: 2.05 });
    const got = await refreshSpot(off, sky, src, undefined, { hydat: h });
    const p = percentileInBand(19.6, BAND)!;
    expect({ dayFlow: got.dayFlow?.state, panelDay: got.panelDay, panel: got.panel }).toEqual({
      dayFlow: "filled", panelDay: { state: "day", date: "2024-08-15" }, panel: expected({ "08MH001": p, "08MH999": p }) });
    expect([savedGauges(got), needsDayFlow(got), needsDayPanel(got), h.asked.sort()]).toEqual(["day", false, false,
      ["08MH001 2024-08-15", "08MH001 2024-08-15", "08MH999 2024-08-15"]]);
  });
});

describe("a spot saved before Q161 for an earlier day (migration)", () => {
  it("its save-day panel is not shown, it is due a refresh, and the refresh refills it for the trip day", async () => {
    const { src } = source();
    const saveDay = expected({ "08MH001": 0.05, "08MH999": 0.05 });     // what such a spot froze: today's river
    const old: Spot = { id: "s1", createdAt: NOW, updatedAt: NOW, visitedAt: THREE_DAYS_AGO, lat: 49.1, lon: -121.96,
      item: null, waterName: "Chilliwack River", title: "", notes: "", photos: [], trace: null, weather: { tempC: 9 } as never,
      leadUp: { weather: [], station: "08MH001", gauge: [] },
      reading: { station: "08MH001", discharge: 11.2, level: 1.38, parameter: "discharge", percentile: 0.05,
                 at: new Date(2026, 9, 23, 11, 20).toISOString() },
      panel: saveDay,
      dayFlow: { state: "filled", station: "08MH001", date: "2026-10-20", discharge: 30, percentile: 0.375, place: 0.375,
                 source: "feed", filledAt: NOW } };
    expect([savedGauges(old), needsDayPanel(old), needsRefresh(old), refreshReason(old)]).toEqual(["pending", true, true,
      "The gauges for the day of this visit have not been read yet. They can be filled in from the day's record."]);
    const got = await refreshSpot(old, sky, src);
    expect({ panelDay: got.panelDay, panel: got.panel, reading: got.reading, dayFlow: got.dayFlow }).toEqual({
      panelDay: { state: "day", date: "2026-10-20" }, panel: expected({ "08MH001": 0.375, "08MH999": 0.75 }),
      reading: old.reading, dayFlow: old.dayFlow });                     // a recorded value is never rewritten
    expect([savedGauges(got), needsRefresh(got)]).toEqual(["day", false]);
  });

  it("a reading taken on the trip day is never touched, nor its panel", async () => {
    const { src } = source();
    const day = new Date(2026, 9, 20, 9).getTime();
    const s: Spot = { id: "s2", createdAt: day + 86_400_000, updatedAt: day, visitedAt: day, lat: 49.1, lon: -121.96,
      item: null, waterName: null, title: "", notes: "", photos: [], trace: null, weather: { tempC: 9 } as never,
      leadUp: { weather: [], station: "08MH001", gauge: [] },
      reading: { station: "08MH001", discharge: 30, level: 1, percentile: 0.375, at: new Date(2026, 9, 20, 8, 50).toISOString() },
      panel: expected({ "08MH001": 0.375 }) };
    expect([needsDayPanel(s), savedGauges(s)]).toEqual([false, "live"]);
    expect(await refreshSpot(s, sky, src)).toBe(s);
  });

  it("panelOfAnswer gives back the members, and the one that led keeps leading", () => {
    const led: Panel = { ...PANEL, members: PANEL.members.map((m, i) => (i === 1 ? { ...m, areaKm2: 4000, lead: 1 } : m)) };
    const frozen = answerFrom(led, { stations: { "08MH001": { percentile: 0.3, discharge: 0.3, parameter: "discharge" },
                                                 "08MH999": { percentile: 0.6, discharge: 0.6, parameter: "discharge" } } });
    expect(panelOfAnswer(frozen)).toEqual({ areaKm2: 40, members: frozen.rows.map((r) => ({
      station: r.station, role: "up", areaKm2: r.areaKm2, years: { discharge: 40, level: 0 }, regulated: false,
      sameRiver: true, ...(r.station === "08MH999" ? { lead: 1 } : {}) })) });
    // and answered again with the same readings it gives the same answer
    expect(answerFrom(panelOfAnswer(frozen), { stations: { "08MH001": { percentile: 0.3, discharge: 0.3, parameter: "discharge" },
      "08MH999": { percentile: 0.6, discharge: 0.6, parameter: "discharge" } } }).answer).toEqual(frozen.answer);
  });
});
