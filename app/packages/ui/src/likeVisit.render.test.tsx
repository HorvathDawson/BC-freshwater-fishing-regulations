/** T51: what the "Like your visit?" strip draws (whole), and the hook: last fetched at once, fresh when online. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { LikeFetched, LikeSources, Spot } from "@app/data/spots";
import { likeVisitView, useLikeVisit, type LikeVisitView } from "./likeVisit";
import type { Connectivity } from "./imagery";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

const VISIT = new Date(2026, 7, 30, 9, 20).getTime();
const NOW = new Date(2026, 9, 22, 10).getTime();
const spot: Spot = {
  id: "s1", createdAt: VISIT, updatedAt: VISIT, visitedAt: VISIT, lat: 49.1, lon: -121.96, item: null,
  waterName: null, title: "", notes: "", photos: [], trace: null, panel: null,
  reading: { station: "08MH016", discharge: 41.2, level: null, percentile: 0.42, at: "x" },
  weather: { tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3, at: "x",
             window: { before: null, after: null,
                       at: { at: "2026-08-30T09:00", tempC: 14, cloudPct: 40, pressureHpa: 1014, humidityPct: 70 } } },
  leadUp: { station: "08MH016", gauge: null,
            weather: [23, 24, 25, 26, 27, 28, 29].map((d) => ({ date: `2026-08-${d}`, maxC: 20, minC: 9,
                                                              precipMm: d >= 27 ? 1 : 0, windMaxKph: 10, code: 1 })) },
};
// the visit's rain window: 1 + 1 + 1 = 3 mm
const days = ["2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25", "2026-10-26"];
const FETCHED: LikeFetched = {
  weatherAt: NOW - 2 * 3_600_000, flowAt: NOW - 3_600_000,
  weather: { days: days.map((date) => ({ date, precipMm: 1 })),
             hours: days.map((date) => ({ at: `${date}T09:00`, tempC: date === "2026-10-23" ? 8 : 14, windKph: 8,
                                          cloudPct: 40, pressureHpa: 1014 })) },
  flow: { "2026-10-22": 0.42, "2026-10-23": 0.67, "2026-10-25": 0.42 }, flowNow: "2026-10-22",
};

describe("likeVisitView", () => {
  it("draws the visit and five days whole: the tier order, the carried flow said, the closest day marked", () => {
    const v = likeVisitView(spot, FETCHED, false, NOW);
    expect([v.heading, v.intro, v.visit, v.age, v.note]).toEqual([
      "Like your visit?",
      "How alike each of the next five days looks to the day you were here. The river's flow and the rain in the "
        + "three days before count most, then cloud and air pressure, then air temperature and wind.",
      { top: "Your visit", sub: "30 Aug", label: "Your visit, 30 Aug: the day the others are compared with." },
      "Forecast checked 2h ago.", null]);
    const ALL = ["Flow: the same", "Rain in the 3 days before: the same", "Cloud: the same", "Pressure: the same",
                 "Air temperature: the same", "Wind: the same"];
    const CARRIED = "No river forecast reaches this day, so its flow is today's reading carried forward: less certain.";
    expect(v.days!.map((d) => [d.date, d.score, d.read, d.basis, d.best, d.reasons])).toEqual([
      ["2026-10-22", "100%", "Very similar", null, true, ALL],
      // flow 25 points up and the air 6° down, each half-way to its span: 1 − 0.25×0.5 − 0.1×0.5 = 82.5 → 83
      ["2026-10-23", "83%", "Similar", null, false,
        ["Flow: 25 points higher", "Rain in the 3 days before: the same", "Cloud: the same", "Pressure: the same",
         "Air temperature: 6° cooler", "Wind: the same"]],
      // no forecast reaches the 24th and 26th: today's reading carried forward, said, never "weather only"
      ["2026-10-24", "100%", "Very similar", CARRIED, false, ALL],
      ["2026-10-25", "100%", "Very similar", null, false, ALL],
      ["2026-10-26", "100%", "Very similar", CARRIED, false, ALL]]);
    expect(v.days![2]!.why[0]).toBe(
      "Flow: 6 in 10 days higher then, 6 in 10 days higher from today's reading. The same: 100% alike, 14% of the score.");
    expect(v.days![2]!.why.at(-1)).toBe(CARRIED);
  });

  it("a station with no reading at all falls back to the weather only, said", () => {
    const none: LikeFetched = { ...FETCHED, flow: null, flowNow: null };
    const d = likeVisitView(spot, none, false, NOW).days![2]!;
    expect([d.score, d.basis, d.reasons.includes("Flow: the same")]).toEqual(
      ["100%", "No flow forecast for this day: scored on the weather only.", false]);
  });

  it("T171: every forecast day carries flow; a day ELF forecast says 'low-water model' and gives the caveat", () => {
    const every: LikeFetched = { ...FETCHED,
      flow: { "2026-10-22": 0.42, "2026-10-23": 0.67, "2026-10-24": 0.42, "2026-10-25": 0.42, "2026-10-26": 0.17 },
      flowModels: { "2026-10-23": { model: "CLEVER" }, "2026-10-24": { model: "CLEVER" },
                    "2026-10-25": { model: "COFFEE" }, "2026-10-26": { model: "ELF", lowWater: true } } };
    const v = likeVisitView(spot, every, false, NOW);
    expect(v.days!.map((d) => [d.date, d.score, d.basis])).toEqual([
      ["2026-10-22", "100%", null], ["2026-10-23", "83%", null], ["2026-10-24", "100%", null],
      ["2026-10-25", "100%", null], ["2026-10-26", "88%", "low-water model"]]);
    expect(v.days![4]!.why).toEqual([
      "Flow: 6 in 10 days higher then, 8 in 10 days higher forecast by ELF. 25 points lower: 50% alike, 25% of the score.",
      "Rain in the 3 days before: 3 mm then, 3 mm forecast. The same: 100% alike, 25% of the score.",
      "Cloud: 40% then, 40% forecast at 09:00. The same: 100% alike, 15% of the score.",
      "Pressure: 1014 hPa then, 1014 hPa forecast at 09:00. The same: 100% alike, 15% of the score.",
      "Air temperature: 14°C then, 14°C forecast at 09:00. The same: 100% alike, 10% of the score.",
      "Wind: 8 km/h then, 8 km/h forecast at 09:00. The same: 100% alike, 10% of the score.",
      "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it "
        + "cannot see a rise coming.",
    ]);
  });

  it("today's flow is called NOW only when it is the gauge's reading for today, not yesterday's forecast for it", () => {
    // fetched yesterday: yesterday's reading and its +1 forecast, which is today's value
    const stale: LikeFetched = { ...FETCHED, flow: { "2026-10-21": 0.42, "2026-10-22": 0.42 }, flowNow: "2026-10-21" };
    expect(likeVisitView(spot, stale, true, NOW).days![0]!.why[0])
      .toBe("Flow: 6 in 10 days higher then, 6 in 10 days higher forecast. The same: 100% alike, 25% of the score.");
  });

  it("a visit with nothing recorded says so, and draws no days", () => {
    const bare = { ...spot, reading: null, weather: null, leadUp: null };
    expect(likeVisitView(bare, FETCHED, true, NOW)).toEqual(expect.objectContaining({
      days: null, note: "Nothing was recorded at this visit to compare with: no flow, no weather.",
      age: "Offline: showing the forecast checked 2h ago." }));
  });
});

function Probe({ sources, store, out, connectivity }: {
  sources: LikeSources; store: { read(): string | null; write(t: string): void }; out: { v?: LikeVisitView | null };
  connectivity?: Connectivity }) {
  out.v = useLikeVisit(spot, sources, store, () => NOW, connectivity);
  return <p>{out.v?.age ?? "none"}|{out.v?.days?.[0]?.score ?? ""}</p>;
}

describe("useLikeVisit", () => {
  it("opened offline, it fetches again when the device comes back online", async () => {
    let up = false;
    let tell: (online: boolean) => void = () => {};
    const connectivity = { watches: true, probeWhileUp: false, online: () => up,
                           subscribe(on: (online: boolean) => void) { tell = on; return () => { tell = () => {}; }; } };
    let text: string | null = JSON.stringify({ s1: FETCHED });
    const store = { read: () => text, write: (t: string) => { text = t; } };
    const out: { v?: LikeVisitView | null } = {};
    const sources: LikeSources = {
      weather: { ahead: async () => (up ? FETCHED.weather : null) },
      feed: { index: async () => (up ? { fetchedAt: new Date(NOW).toISOString(),
                                         stations: { "08MH016": { percentile: 0.3, discharge: 0.3 } } } : null) } };
    const { container } = render(<Probe out={out} store={store} sources={sources} connectivity={connectivity} />);
    await settle();
    expect(container.textContent).toBe("Offline: showing the forecast checked 2h ago.|100%");
    up = true;
    await act(async () => { tell(true); });
    await settle();
    expect(container.textContent).toBe("Forecast checked just now.|94%");
  });

  it("offline: the last fetched, at once, and says offline with its age", async () => {
    let text: string | null = JSON.stringify({ s1: FETCHED });
    const store = { read: () => text, write: (t: string) => { text = t; } };
    const out: { v?: LikeVisitView | null } = {};
    const { container } = render(<Probe out={out} store={store} sources={{
      weather: { ahead: async () => null }, feed: { index: async () => null } }} />);
    await settle();
    expect(container.textContent).toBe("Offline: showing the forecast checked 2h ago.|100%");
  });

  it("online: fetched fresh, kept for next time", async () => {
    let text: string | null = null;
    const store = { read: () => text, write: (t: string) => { text = t; } };
    const out: { v?: LikeVisitView | null } = {};
    const { container } = render(<Probe out={out} store={store} sources={{
      weather: { ahead: async () => FETCHED.weather },
      feed: { index: async () => ({ fetchedAt: new Date(NOW).toISOString(),
                                     stations: { "08MH016": { percentile: 0.3, discharge: 0.3 } } }) } }} />);
    await settle();
    // flow 12 points lower now: 1 − 0.25 × 0.24 = 0.94
    expect(container.textContent).toBe("Forecast checked just now.|94%");
    expect(JSON.parse(text!).s1.flow["2026-10-22"]).toBe(0.3);
  });
});
