/**
 * The conditions view models (P12a): `donorView`, the pure builder the donor panel draws,
 * and `useConditionsView`, the hook the conditions panel draws.
 *
 * The WORDS are pinned in core (`conditions.test.ts`). This pins the CHOICES the
 * components used to make inline — which row is the lead station, which sentences appear,
 * the chosen day rather than the clock — and that every string arrives finished.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import { donorBecause, donorFacts, donorKeyLabel, donorPinTitle, donorReads, donorReason,
         donorRowLabel, donorWeightLine, estimateHeadline, estimateRange, estimateSentence,
         HORIZON_LABELS, horizonChipLabel, noGaugeSentence, readingHeading,
         readingStandingLine, stageLine, unplacedNote } from "@app/core";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache, donorView, useConditionsView, type DonorRow,
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
    expect(vm).toEqual({ ok: false, heading: "NO ESTIMATE", reason: donorReason("offline") });
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
      { day: 0, chip: "Now", label: horizonChipLabel(0, true), on: true },
      { day: 1, chip: "+1d", label: horizonChipLabel(1, false), on: false },
      { day: 3, chip: "+3d", label: horizonChipLabel(3, false), on: false },
      { day: 5, chip: "+5d", label: horizonChipLabel(5, false), on: false },
    ]);
  });

  it("gives each row its finished lines and its bar", () => {
    const v = value({ rows: [row("08MH001", { regulated: true }),
                             row("08MH024", { percentile: null, weight: 0 })] });
    const vm = donorView(v, { ...opts, selected: "08MH024" as never, pickable: true });
    if (!vm.ok) throw new Error("expected an estimate");
    const [a, b] = [v.rows[0]!, v.rows[1]!];
    expect(vm.rows).toEqual([
      { station: "08MH001", name: null, quiet: false, on: false, bar: "60%",
        weightLine: donorWeightLine(a), reads: donorReads(a), facts: donorFacts(a),
        because: donorBecause(a), label: donorRowLabel(a, true, false) },
      { station: "08MH024", name: null, quiet: true, on: true, bar: "1%",
        weightLine: donorWeightLine(b), reads: donorReads(b), facts: donorFacts(b),
        because: null, label: donorRowLabel(b, true, true) },
    ]);
    expect(vm.regulated).not.toBeNull();
  });

  it("places the donors, absorbing 'you are here' into a gauge at the spot", () => {
    const route = (station: string, lat: number | null) => ({
      station: station as never, name: "VEDDER", lat, lon: lat === null ? null : -122,
      role: "up" as const, path: ["a", "b"] as never });
    const v = value({ rows: [row("08MH001", { route: route("08MH001", 49) }),
                             row("08MH024", { route: route("08MH024", null) })] });
    const vm = donorView(v, { ...opts, from: { lat: 49, lon: -122 } });
    if (!vm.ok || !vm.map) throw new Error("expected a map");
    expect(vm.map.pins).toEqual([
      { lat: 49, lon: -122, row: 0, title: donorPinTitle(v.rows[0]!, true) },
    ]);
    expect(vm.map.keys).toEqual([{ row: 0, label: donorKeyLabel(v.rows[0]!, true) }]);
    expect(vm.map.unplaced).toBe(unplacedNote(1));
    expect(vm.map.chain).toEqual(["a", "b"]);
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
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: 40, regulated: false,
    sameRiver: true },
] };

const source = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49,
                              lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => ({ station: "MATCH", name: "Matched River", trust: "close",
                                  reachMagnitude: 1, gaugeMagnitude: 2, lon: -122, lat: 49 }),
  traceToGauge: async () => [SEC],
  gaugeSeries: async () => null,
  gaugeNow: async (station: string) => ({
    fetchedAt: Date.now() - 5 * 60_000,
    value: station === "LEAD"
      ? { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below",
          at: "2026-09-04T06:00:00Z" }
      : { discharge: null, level: 3.4, percentile: null, standing: null, at: "" },
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
    expect(vm.reading?.age).toMatch(/^This gauge's own reading · observed 2026-09-04 06:00 UTC · checked \d+ min ago$/);
    expect(vm.noGauge).toBe(noGaugeSentence());
    expect(vm.trace).toBeNull();          // the panel answered, so no single-station fallback
  });

  it("moves to a station the reader picks", async () => {
    const { result } = renderHook(() =>
      useConditionsView(source, SEC, DAY, 0, { feed, quantity: "discharge" }));
    await waitFor(() => expect(result.current.lead).toBe("LEAD"));
    act(() => result.current.select("MATCH" as never));
    await waitFor(() => expect(result.current.reading?.figure).toBe(3.4));
    expect(result.current.lead).toBe("MATCH");
    expect(result.current.reading).toMatchObject({
      heading: readingHeading("Matched River"), unit: "m", quantity: "level", stage: null,
      standing: readingStandingLine(null, null) });
  });

  it("falls back to the matched trace where no panel answers", async () => {
    const { result } = renderHook(() =>
      useConditionsView(source, SEC, DAY, 0, { feed: undefined, quantity: "discharge" }));
    await waitFor(() => expect(result.current.trace?.station).toBe("MATCH"));
  });
});
