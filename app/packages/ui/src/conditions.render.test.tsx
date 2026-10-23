/**
 * The conditions view models (P12a): `donorView`, the pure builder the donor panel draws,
 * and `useConditionsView`, the hook the conditions panel draws.
 *
 * The WORDS are pinned in core (`conditions.test.ts`). This pins the CHOICES the
 * components used to make inline — which row is the lead station, which sentences appear,
 * the chosen day rather than the clock — and that every string arrives finished.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import { donorBecause, donorFacts, donorLastReport, donorKeyLabel, donorPinTitle, donorReads, donorReason,
         donorRowLabel, donorWeightLine, estimateHeadline, estimateRange, estimateSentence,
         freshness, HORIZON_LABELS, horizonChipLabel, localStamp, noGaugeSentence,
         readingAgeLine, readingHeading,
         readingStandingLine, stageLine, unplacedNote } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache, donorView, horizonChips, useConditionsView, type DonorRow,
         type PanelAnswer } from "./panel";

const row = (station: string, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12,
  weight: 0.6, areaRatio: 2.4, areaKm2: 120, trust: "close", years: 40,
  regulated: false, sameRiver: true,
  factors: { share: 0.42, role: 1, record: 1 }, ...over,
});

const value = (over: Partial<PanelAnswer> = {}): PanelAnswer => ({
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close",
                               spread: 6, donors: 2 } },
  rows: [row("08MH001"), row("08MH024", { weight: 0.4, role: "down" })],
  areaKm2: 50, routesReady: true, ...over,
});

const DAY = new Date(2026, 8, 4, 12);
const opts = { day: DAY, horizon: 0 as const, from: null, size: { width: 360, height: 210 },
               selected: null, pickable: false };

describe("donorView", () => {
  it("refuses with the reason, per NoEstimate", () => {
    const vm = donorView({ answer: { ok: false, why: "offline" }, rows: [], areaKm2: null,
                           routesReady: true }, opts);
    expect(vm).toEqual({ ok: false, heading: "NO ESTIMATE", reason: donorReason("offline"),
                         horizons: null, silent: null });
  });

  it("keeps the horizons on a refusal that is about the chosen day, so the reader can step back", () => {
    // +10 is past every run for these gauges. Without the chips the panel would strand the
    // reader on a day nothing can answer, with no way back to Now from the panel itself.
    const vm = donorView({ answer: { ok: false, why: "no-forecast" }, rows: [], areaKm2: 50,
                           routesReady: true }, { ...opts, horizon: 10 });
    expect(vm).toEqual({
      ok: false, heading: "NO ESTIMATE",
      reason: "No forecast for the gauges here reaches this far ahead.",
      horizons: [
        { day: 0, chip: "Now", label: "Conditions now", on: false, note: null, needs: null },
        { day: 1, chip: "+1d", label: "Forecast 1 day ahead", on: false, note: null, needs: null },
        { day: 3, chip: "+3d", label: "Forecast 3 days ahead", on: false, note: null, needs: null },
        { day: 5, chip: "+5d", label: "Forecast 5 days ahead", on: false, note: null, needs: null },
        { day: 10, chip: "+10d", label: "Forecast 10 days ahead, showing", on: true, note: null, needs: null },
        { day: "change", chip: "Change", label: "Expected change over the next 10 days", on: false, note: null, needs: null },
      ],
      silent: null,
    });
  });

  it("finishes every line of the estimate for the CHOSEN day", () => {
    const v = value();
    const vm = donorView(v, { ...opts, horizon: 3 });
    if (!vm.ok) throw new Error("expected an estimate");
    const e = v.answer.ok ? v.answer.value : null!;
    expect(vm.heading).toBe(HORIZON_LABELS[3].heading);
    expect(vm.headline).toBe(estimateHeadline(e));
    expect(vm.headlineSuffix).toBe("by then");
    expect(vm.sentence).toBe(estimateSentence(e, 3));
    expect(vm.range).toBe(estimateRange(e, DAY, 3, 50));
    // The day, not the clock: the same inputs give the same range whenever this runs.
    expect(vm.range).toBe("3rd–27th percentile for early September · this spot drains 50 km²");
  });

  it("has no suffix now, and offers every horizon with its own label", () => {
    const vm = donorView(value(), opts);
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.headlineSuffix).toBeNull();
    expect(vm.horizons).toEqual([
      { day: 0, chip: "Now", label: horizonChipLabel(0, true), on: true, note: null, needs: null },
      { day: 1, chip: "+1d", label: horizonChipLabel(1, false), on: false, note: null, needs: null },
      { day: 3, chip: "+3d", label: horizonChipLabel(3, false), on: false, note: null, needs: null },
      { day: 5, chip: "+5d", label: horizonChipLabel(5, false), on: false, note: null, needs: null },
      { day: 10, chip: "+10d", label: horizonChipLabel(10, false), on: false, note: null, needs: null },
      { day: "change", chip: "Change", label: horizonChipLabel("change", false), on: false, note: null, needs: null },
    ]);
  });

  it("builds the map's chips and the panel's from one function, so they cannot disagree", () => {
    for (const h of [0, 1, 3, 5, 10] as const) {
      const vm = donorView(value(), { ...opts, horizon: h });
      if (!vm.ok) throw new Error("expected an estimate");
      expect(horizonChips(h)).toEqual(vm.horizons);
    }
  });

  it("gives each row its finished lines and its bar", () => {
    // 08MH024 reported in the small hours of the chosen day with nothing to rank — quiet, but it speaks, so a row.
    // 08MH099 is silent and is not a row (owner 2026-10-22); the line names it instead.
    const early = new Date(2026, 8, 4, 2).toISOString();
    const v = value({ rows: [row("08MH001", { regulated: true }),
                             row("08MH024", { percentile: null, weight: 0, observedAt: early }),
                             row("08MH099", { percentile: null, weight: 0 })] });
    const vm = donorView(v, { ...opts, selected: "08MH024" as never, pickable: true });
    if (!vm.ok) throw new Error("expected an estimate");
    const [a, b] = [v.rows[0]!, v.rows[1]!];
    expect(vm.rows).toEqual([
      { station: "08MH001", row: 0, name: null, quiet: false, on: false, bar: "60%",
        weightLine: donorWeightLine(a), reads: donorReads(a), lastReport: null, facts: donorFacts(a),
        because: donorBecause(a), label: donorRowLabel(a, true, false) },
      // A quiet gauge says when it last reported (owner 2026-10-21).
      { station: "08MH024", row: 1, name: null, quiet: true, on: true, bar: "1%",
        weightLine: donorWeightLine(b), reads: "Reporting, but its reading cannot be ranked today.",
        lastReport: donorLastReport(early), facts: donorFacts(b),
        because: null,
        label: "08MH024, upstream, 2.4× apart, reporting but not ranked today, so it counts for nothing. "
          + "Showing this gauge's chart" },
    ]);
    expect(vm.silent).toBe("Not reporting: 08MH099 (last report not known)");
    expect(vm.regulated).not.toBeNull();
  });

  it("says the tide in the Level view only, once a tidal gauge's route is in (T78)", () => {
    // 08MH035, the Pitt near Port Coquitlam: the release flags it tidal (owner 2026-10-22).
    const pitt = row("08MH035", { route: { station: "08MH035" as never, name: "PITT RIVER NEAR PORT COQUITLAM",
                                           lat: 49.3, lon: -122.6, role: "up", path: [], tidal: true } });
    const v = value({ rows: [pitt, row("08MH006", { weight: 0.3 })] });
    const level = donorView(v, { ...opts, quantity: "level" });
    const flow = donorView(v, { ...opts, quantity: "discharge" });
    const unsaid = donorView(v, opts);
    const plain = donorView(value(), { ...opts, quantity: "level" });
    if (!level.ok || !flow.ok || !unsaid.ok || !plain.ok) throw new Error("expected an estimate");
    expect([level.tidal, flow.tidal, unsaid.tidal, plain.tidal]).toEqual([
      "One of these gauges is tidal: its level rises and falls with the tide and with water backed up from "
        + "downstream, not only with what this river carries.",
      null, null, null]);
  });

  it("places the donors, and marks the spot with its own pin even at a gauge", () => {
    // Owner 2026-10-21: the spot is a different SHAPE from a gauge, so a gauge at the spot no
    // longer absorbs it — the gauge keeps the "you are here" words on its own title.
    const route = (station: string, lat: number | null) => ({
      station: station as never, name: "VEDDER", lat, lon: lat === null ? null : -122,
      role: "up" as const, path: ["a", "b"] as never });
    const v = value({ rows: [row("08MH001", { route: route("08MH001", 49) }),
                             row("08MH024", { route: route("08MH024", null) })] });
    const vm = donorView(v, { ...opts, from: { lat: 49, lon: -122 } });
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.pins).toEqual([
      { lat: 49, lon: -122, row: 0, kind: "gauge", title: donorPinTitle(v.rows[0]!, true),
        label: "60%", rank: 0 },
      { lat: 49, lon: -122, row: null, kind: "here", title: "you are here",
        label: "you are here", rank: -1 },
    ]);
    expect(vm.map.keys).toEqual([{ row: null, label: "you are here" },
                                 { row: 0, label: "08MH001 · 60%" }]);
    expect(vm.map.unplaced).toBe(unplacedNote(1));
    expect(vm.map.chain).toEqual(["a", "b"]);
  });

  it("hands the overlay each placed donor's route, its role, and both ends", () => {
    const route = (station: string, lat: number, role: "up" | "down", path: string[]) => ({
      station: station as never, name: null, lat, lon: -121.5, role, path: path as never });
    const v = value({ rows: [
      row("08MH001", { route: route("08MH001", 49.2, "up", ["s", "a"]) }),
      row("08MH024", { weight: 0.4, role: "down", percentile: null, observedAt: new Date(2026, 8, 4, 2).toISOString(),
                       route: route("08MH024", 49.0, "down", ["s", "b", "c"]) }),
      // Placed but with no walkable chain: a dot, no line, no key line for its role.
      row("08MF005", { weight: 0, route: { ...route("08MF005", 49.1, "up", []) } }),
    ] });
    const vm = donorView(v, { ...opts, from: { lat: 49.1, lon: -122 } });
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.routes).toEqual([
      { row: 0, role: "up", path: ["s", "a"], from: { lat: 49.1, lon: -122 },
        to: { lat: 49.2, lon: -121.5 } },
      { row: 1, role: "down", path: ["s", "b", "c"], from: { lat: 49.1, lon: -122 },
        to: { lat: 49.0, lon: -121.5 } },
    ]);
    expect(vm.map.legend).toEqual([
      { role: "down", label: "gauge downstream: your water flows on to it" },
      { role: "up", label: "gauge upstream: its water flows down to you" },
    ]);
    // The share on the map is the panel's: a quiet gauge says so, it never reads "0%".
    expect(vm.map.pins.map((p) => p.label))
      .toEqual(["60%", "reporting, not ranked", "0%", "you are here"]);
    expect(vm.map.bbox).toEqual([-122, 49.0, -121.5, 49.2]);
    expect(vm.map.words).toEqual({ fit: "Fit", fitLabel: "Fit the map to you and the gauges",
                                   larger: "Larger map", smaller: "Smaller map" });
  });

  it("has no map until the routes are in", () => {
    const vm = donorView(value({ routesReady: false }), opts);
    if (!vm.ok) throw new Error("expected an estimate");
    expect(vm.map).toBeNull();
  });
});

/* ------------------------------------------------------------- the hook ---- */

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false,
    sameRiver: true },
] };

const source = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49,
                              lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(),
  gaugePoints: async () => [{ station: "LAKE", name: "Lake at Outlet", lon: -122, lat: 49, mag: null }],
  gaugeForSection: async () => ({ station: "MATCH", name: "Matched River", trust: "close",
                                  reachMagnitude: 1, gaugeMagnitude: 2, lon: -122, lat: 49 }),
  traceToGauge: async () => [SEC],
  gaugeSeries: async () => null,
  gaugeNow: async (station: string) => ({
    fetchedAt: Date.now() - 5 * 60_000,
    value: station === "LEAD"
      ? { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
          at: "2026-09-04T06:00:00Z", parameter: "discharge" }
      : { discharge: null, level: 3.4, percentile: null, standing: null, at: "", parameter: "level" },
  }),
} as unknown as RegsSource;

const feed = { index: async () => ({ stations: {
  LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };

beforeEach(clearPanelCache);

describe("useConditionsView", () => {
  it("leads with the heaviest reporting donor and finishes its reading", async () => {
    const { result } = renderHook(() =>
      useConditionsView(source, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBeDefined());
    const vm = result.current;
    expect(vm.loading).toBe(false);
    expect(vm.day).toBe(DAY);
    expect(vm.lead).toBe("LEAD");
    expect(vm.reading).toMatchObject({
      heading: readingHeading("Lead Creek near Town"),
      figure: 12.5, unit: "m³/s", quantity: "discharge",
      stage: stageLine(1.2),
      standing: readingStandingLine(0.15, "below"),
    });
    // LOCAL AND LABELLED (P16): the same instant, in the reader's clock with its zone named.
    expect(vm.reading?.age).toBe(readingAgeLine(freshness(Date.now() - 5 * 60_000, Date.now()),
                                                "2026-09-04T06:00:00Z"));
    expect(vm.reading?.age).toContain(`observed ${localStamp(Date.parse("2026-09-04T06:00:00Z"))} · `);
    expect(vm.noGauge).toBe(noGaugeSentence());
    expect(vm.trace).toBeNull();          // the panel answered, so no single-station fallback
  });

  it("reads no series for the reading: the chart asks for its own (P16, the unused fetch)", async () => {
    // `useConditions` and `useStationReading` each fetched the station's 72-hour series for a
    // `forecast` field nothing read — the chart has `useSeries`. A reading is a reading.
    const gaugeSeries = vi.fn(async () => null);
    const { result } = renderHook(() =>
      useConditionsView({ ...source, gaugeSeries } as RegsSource, SEC, DAY, 0,
                        { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBeDefined());
    expect(gaugeSeries).not.toHaveBeenCalled();
  });

  it("moves to a donor the reader picks, and never to a station the speaker does not hold (T105)", async () => {
    const two: Panel = { areaKm2: 50, members: [...PANEL.members,
      { station: "OTHER" as never, role: "down", areaKm2: 70, years: 30, regulated: false, sameRiver: true }] };
    const src = { ...source, panelsFor: async () => new Map([[SEC, two]]),
                  panelRoutes: async () => [...await source.panelRoutes(SEC),
                    { station: "OTHER", name: "Other River at Bridge", lat: 49, lon: -122, role: "down", path: [SEC] }],
                } as unknown as RegsSource;
    const { result } = renderHook(() =>
      useConditionsView(src, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBe(readingHeading("Lead Creek near Town")));
    // the reach's `section_gauge` match is not one of the panel's: picking it changes nothing
    act(() => result.current.select("MATCH" as never));
    expect(result.current.lead).toBe("LEAD");
    act(() => result.current.select("OTHER" as never));
    await waitFor(() => expect(result.current.reading?.figure).toBe(3.4));
    expect(result.current.lead).toBe("OTHER");
    expect(result.current.reading).toMatchObject({
      heading: readingHeading("Other River at Bridge"), unit: "m", quantity: "level", stage: null,
      standing: readingStandingLine(null, null) });
  });

  it("no panel and no lake: the matched station is never quoted — the sheet refuses, as the map leaves it uncoloured (T105)", async () => {
    // one of the 54,386 sections with a `section_gauge` row and no `section_panel` row
    const gaugeNow = vi.fn(source.gaugeNow);
    const { result } = renderHook(() =>
      useConditionsView({ ...source, panelsFor: async () => new Map(), gaugeNow } as unknown as RegsSource,
                        SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect({ lead: result.current.lead, reading: result.current.reading, quiet: result.current.quiet,
             refuse: result.current.refuse, trace: result.current.trace })
      .toEqual({ lead: null, reading: null, quiet: null, refuse: true, trace: null });
    expect(gaugeNow).not.toHaveBeenCalled();
  });

  it("a gauged lake reads its own station, as the map colours it (T105)", async () => {
    const lake = { ...source, panelsFor: async () => new Map(), gaugeForSection: async () => null,
                   lakeStationsFor: async () => new Map([[SEC, "LAKE"]]),
                   gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000,
                     value: { discharge: null, level: 2.31, percentile: 0.6, standing: "normal",
                              at: "2026-09-04T06:00:00Z", parameter: "level" } }) } as unknown as RegsSource;
    const { result } = renderHook(() => useConditionsView(lake, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBeDefined());
    const { age: _age, ...shown } = result.current.reading!;
    expect([result.current.lead, shown, result.current.refuse]).toEqual(["LAKE", {
      heading: readingHeading("Lake at Outlet"), figure: 2.31, unit: "m", quantity: "level", stage: null,
      standing: readingStandingLine(0.6, "normal") }, false]);
  });

  it("shows the quantity the station DECLARES: 08ME030's level beside its level percentile, never its discharge (T106)", async () => {
    // `data/generated/gauges/feeds/08ME030.json` `now` as the ticket recorded it: both quantities, declared level.
    const now08ME030 = { discharge: 5.46, level: 4.162, percentile: 0.432, standing: "normal",
                         at: "2026-10-22T18:40:00-08:00", parameter: "level" };
    const src = { ...source, gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000, value: now08ME030 }) } as
      unknown as RegsSource;
    const { result } = renderHook(() => useConditionsView(src, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBeDefined());
    const { age: _age, ...shown } = result.current.reading!;
    expect(shown).toEqual({
      heading: readingHeading("Lead Creek near Town"),
      figure: 4.162, unit: "m", quantity: "level",
      stage: null,                                        // the stage line is said beside a discharge only
      standing: readingStandingLine(0.432, "normal"),
    });
    // and declared discharge, the same two values: the discharge, with its stage beside it
    const src2 = { ...source, gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000,
                                                       value: { ...now08ME030, parameter: "discharge" } }) } as
      unknown as RegsSource;
    const two = renderHook(() => useConditionsView(src2, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(two.result.current.reading?.heading).toBeDefined());
    expect(two.result.current.reading).toMatchObject({ figure: 5.46, unit: "m³/s", quantity: "discharge",
                                                       stage: stageLine(4.162) });
  });

  it("a quiet donor picked to chart says when it last reported — never the no-gauge refusal (review 2026-10-21)", async () => {
    const two: Panel = { areaKm2: 50, members: [...PANEL.members,
      { station: "QUIET" as never, role: "down", areaKm2: 70, years: { discharge: 30, level: 30 }, regulated: false, sameRiver: true }] };
    const src = { ...source, panelsFor: async () => new Map([[SEC, two]]),
                  gaugeNow: async (st: string) => (st === "QUIET" ? null : source.gaugeNow(st as never)) } as RegsSource;
    const quietFeed = { index: async () => ({ stations: { ...(await feed.index()).stations,
      QUIET: { percentile: null, parameter: "discharge" as const, observedAt: "2026-08-30T10:00:00-08:00" } } }) };
    const { result } = renderHook(() =>
      useConditionsView(src, SEC, DAY, 0, { feed: quietFeed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.reading?.heading).toBeDefined());
    expect(result.current.quiet).toBeNull();
    expect(result.current.refuse).toBe(false);
    act(() => result.current.select("QUIET" as never));
    await waitFor(() => expect(result.current.quiet).not.toBeNull());
    expect(result.current.reading).toBeNull();
    expect(result.current.refuse).toBe(false);
    expect(result.current.quiet).toEqual({
      heading: readingHeading("QUIET"),
      reads: donorReads({ percentile: null, observedAt: "2026-08-30T10:00:00-08:00" } as never,
                        { ref: DAY.getTime() }),
      lastReport: donorLastReport("2026-08-30T10:00:00-08:00"),
    });
  });

  it("refuses only where no donor leads and nothing reads", async () => {
    const { result } = renderHook(() =>
      useConditionsView({ ...source, panelsFor: async () => new Map(), gaugeForSection: async () => null,
                          gaugeNow: async () => null } as unknown as RegsSource,
                        SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.reading).toBeNull();
    expect(result.current.quiet).toBeNull();
    expect(result.current.refuse).toBe(true);
  });

  it("where the panel cannot answer (offline) it leads with its first member, and traces only a donor's match (T105)", async () => {
    const own = { ...source, gaugeForSection: async () => ({ ...(await source.gaugeForSection(SEC))!, station: "LEAD" }) } as
      unknown as RegsSource;
    const { result } = renderHook(() =>
      useConditionsView(own, SEC, DAY, 0, { feed: undefined, quantity: "discharge" }));
    await waitFor(() => expect(result.current.trace?.station).toBe("LEAD"));
    expect(result.current.lead).toBe("LEAD");
    // matched to a station the panel does not hold: no trace to it
    const asked = vi.fn(source.gaugeForSection);
    const other = renderHook(() =>
      useConditionsView({ ...source, gaugeForSection: asked } as unknown as RegsSource, SEC, DAY, 0,
                        { feed: undefined, quantity: "discharge" }));
    await waitFor(() => expect(other.result.current.lead).toBe("LEAD"));
    await waitFor(() => expect(asked).toHaveBeenCalled());
    await act(async () => { await new Promise((r) => setTimeout(r, 30)); });   // the match has landed
    expect(other.result.current.trace).toBeNull();
  });
});

/*
 * THE ROUTE MAP, ONE PATHWAY (engine polish 2026-10-21): on a map that labels its own tiles the route's colours go
 * through the engine's standing channel, with the route as its classes (`routeColour.ts`); pushed `data` is the phone's
 * alone. Both carry the same number for the same reach.
 */
describe("useConditionsView: the route map's colours", () => {
  it("per-tile: the engine's standing channel over the route, no pushed data; pushed: the same value as data", async () => {
    const web = renderHook(({ h }: { h: 0 | 1 }) =>
      useConditionsView(source, SEC, DAY, h, { feed, quantity: "discharge", perTile: true }), { initialProps: { h: 0 } });
    const phone = renderHook(() => useConditionsView(source, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(web.result.current.route.standing?.values).not.toBeNull());
    await waitFor(() => expect(Object.keys(phone.result.current.route.data.stream ?? {})).toEqual([String(SEC)]));
    const pushed = phone.result.current.route.data as Record<string, Record<string, { standing: number }>>;
    const value = pushed.stream![SEC]!.standing;
    expect(pushed).toEqual({ stream: { [SEC]: { standing: value } }, lake: { [SEC]: { standing: value } } });
    expect(phone.result.current.route.standing).toBeNull();
    const r = web.result.current.route;
    expect(r.data).toEqual({});
    const { labels, values } = r.standing!;
    // the route IS the labelling: the chain's sections have a class, nothing else does
    expect([labels.classOf(SEC), labels.classOf(8), values!.section(0), values!.basins])
      .toEqual([0, null, value, {}]);
    // a horizon change: new values over the SAME labels (the engine relabels; it never clears the route)
    web.rerender({ h: 1 });
    await waitFor(() => expect(web.result.current.route.standing?.values).not.toBe(values));
    expect(web.result.current.route.standing!.labels).toBe(labels);
  });
});
