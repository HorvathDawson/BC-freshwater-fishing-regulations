/**
 * T143 — THE HORIZONS, OVER THE FEED THE OWNER'S APP ACTUALLY READ. Not a hand-made index: `fixtures/live/` holds the
 * dev server's `/feeds/live/index.json` byte for byte (published 2026-10-23T03:50:35Z, 432 stations) and the release's
 * own panel for the Chilliwack reach the sheet opened on (`panelsFor([1235781])`, r-012fb43aa175). A fixture written to
 * match the code cannot catch the code disagreeing with the publisher; this one was written by the publisher.
 *
 * What it found (owner 2026-10-23: "+1, +3, +5, +10 says no forecast for gauges here even tho there definitely is"):
 * under FLOW every horizon answers on this feed — 107k of 239k release sections at +1 as at Now. Under LEVEL, +1/+3/+5
 * answered "No forecast for the gauges here" on almost every water, because CLEVER (which answers those days) forecasts
 * discharge and no stage; the Layers choice is kept, so a reader who once picked Level saw it everywhere. The refusal
 * now says the flow is ranked (`silentWhy`), and a flow still never stands in for a level. The adversarial review found
 * the publisher's half: ELF DOES forecast stage on those days; `forecast_percentiles` stopped at CLEVER's flow. Fixed per
 * quantity in the publisher (feed republish); the "per-quantity feed" block below reads its output.
 *
 * MUTATION PINS (each fails the named test; checked by hand 2026-10-23):
 *   - `reading` returning null for every horizon past 0 (the regression's shape)  → "answers every day ahead in Flow"
 *   - `reading` reading `row.ahead?.[q]` instead of by the day's key             → "answers every day ahead in Flow"
 *   - `silentWhy` always "no-forecast" (the shipped refusal)                     → "says the forecast is in flow under Level"
 *   - `silentWhy` dropping its `typeof horizon === "number"` guard                → "Change under Level is no forecast"
 *   - `donorView` keeping chips only for "no-forecast"                           → DonorPanel "under Level at +3 …"
 *   - `donorView` keeping the silent line under a flow-only refusal               → DonorPanel "under Level at +3 …"
 *   - `silentWhy` answering flow-forecast-only from the LEVEL contributions       → "says the forecast is in flow under Level"
 *   - `silentWhy` ignoring its quantity (the first T143 build)                    → "is never flow-forecast-only under Flow …"
 *   - `reading` reading `levelModel`'s day as absent / dropping a level beside `levelModel` → "answers Level at +1/+3/+5 …"
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { donorMapLabel, donorReads, gaugeChoiceLabel } from "@app/core";
import { answerFrom, reading, silentWhy, type Panel } from "@app/data";

const dir = join(__dirname, "fixtures/live");
const INDEX = JSON.parse(readFileSync(join(dir, "feed-index-2026-10-23T0350Z.json"), "utf8"));
const PANEL: Panel = JSON.parse(readFileSync(join(dir, "panel-chilliwack-r-012fb43aa175-1235781.json"), "utf8")).panel;

const r4 = (x: number) => Number(x.toFixed(4));
const said = (q: "discharge" | "level", h: 0 | 1 | 3 | 5 | 10 | "change") => {
  const a = answerFrom(PANEL, INDEX, q, h).answer;
  return a.ok ? { ok: true, percentile: r4(a.value.percentile), donors: a.value.donors } : a;
};

describe("the Chilliwack panel on the owner's feed", () => {
  it("answers every day ahead in Flow, from the forecast, not today", () => {
    expect({ 0: said("discharge", 0), 1: said("discharge", 1), 3: said("discharge", 3), 5: said("discharge", 5),
             10: said("discharge", 10), change: said("discharge", "change") }).toEqual(EXPECT_FLOW);
  });

  it("says the forecast is in flow under Level at +1/+3/+5, and reads ELF's level at +10", () => {
    expect({ 0: said("level", 0), 1: said("level", 1), 3: said("level", 3), 5: said("level", 5),
             10: said("level", 10) }).toEqual(EXPECT_LEVEL);
  });

  /*
   * T157: under that refusal the chart's gauge switcher called every member "quiet". Each member now says what the sheet
   * says — the switcher's word and label are the map dot's (`donorMapLabel`, `gaugeChoiceLabel`, @app/ui `gaugeChoices`).
   * Mutation: `donorState` ignoring `aheadWhy` (every day-ahead silence "no-forecast") -> this fails ("no forecast").
   */
  it("each member says 'flow forecast only' under Level at +3, as the sheet does", () => {
    const rows = answerFrom(PANEL, INDEX, "level", 3).rows;
    const ask = { horizon: 3 as const };
    // the four that report (T143's "4 gauges") have a flow forecast; the four ECCC does not list stay not real-time
    const flow = (s: string) => [s, "flow-only", "flow forecast only", `Chart ${s}, flow forecast only`];
    const off = (s: string) => [s, null, "not a real-time gauge", `Chart ${s}, not a real-time gauge`];
    expect(rows.map((r) => [r.station, r.aheadWhy ?? null, donorMapLabel(r, ask), gaugeChoiceLabel(r, false, ask)]))
      .toEqual([flow("08MH016"), flow("08MH103"), flow("08MH001"), off("08MH055"), flow("08MH056"), off("08MH047"),
                off("08MH094"), off("08MH131")]);
    expect(donorReads(rows[0]!, ask)).toBe(
      "Has a flow forecast this far ahead, but no level forecast ranked against its record.");
  });
});

describe("every station of the owner's feed, as a one-gauge panel", () => {
  const one = (station: string): Panel => ({ areaKm2: 100, members: [{ station: station as never, role: "up",
    areaKm2: 100, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] });
  const stations = Object.keys(INDEX.stations).sort();
  const tally = (q: "discharge" | "level", h: 1 | 3 | 5 | 10) => {
    const t: Record<string, number> = {};
    for (const s of stations) {
      const a = answerFrom(one(s), INDEX, q, h).answer;
      const k = a.ok ? "ok" : a.why;
      t[k] = (t[k] ?? 0) + 1;
    }
    return t;
  };

  it("answers in Flow at every station whose own row forecasts a flow that day", () => {
    const flowAhead = (h: number) => stations.filter((s) => INDEX.stations[s].ahead?.[String(h)]?.discharge != null);
    for (const h of [1, 3, 5, 10] as const)
      expect(stations.filter((s) => reading(INDEX.stations[s], "discharge", h) !== null)).toEqual(flowAhead(h));
    expect({ 1: tally("discharge", 1), 10: tally("discharge", 10) }).toEqual(EXPECT_FLOW_TALLY);
  });

  it("reads each day's own forecast, never today's: the Fraser at Hope (08MF005)", () => {
    const days = ([0, 1, 3, 5, 10] as const).map((h) => {
      const a = answerFrom(one("08MF005"), INDEX, "discharge", h).answer;
      return a.ok ? r4(a.value.percentile) : a.why;
    });
    expect(days).toEqual([0.36, 0.3921, 0.3165, 0.2422, 0.2212]);
  });

  it("a day nobody forecasts is still no forecast; one forecast in flow only says so", () => {
    expect({ 1: tally("level", 1), 3: tally("level", 3), 5: tally("level", 5), 10: tally("level", 10) })
      .toEqual(EXPECT_LEVEL_TALLY);
  });

  /*
   * T157: a forecast with no record to rank it against says so — the two new roster stations whose runs give a Change
   * but whose days ahead, ranked against no HYDAT envelope, are absent. Mutations: `aheadWhy` never "no-record" -> this
   * and both tallies fail; `aheadWhy` dropping its `row.change` guard -> 08MH098 (a run held, stale, no change) joins
   * them and this fails.
   */
  it("says 'no record to compare with' where a station's runs forecast the day and nothing ranks it", () => {
    const unranked = (q: "discharge" | "level", h: 1 | 3 | 5 | 10) =>
      stations.filter((s) => { const a = answerFrom(one(s), INDEX, q, h).answer; return !a.ok && a.why === "forecast-unranked"; });
    for (const h of [1, 3, 5, 10] as const)
      expect([h, unranked("discharge", h), unranked("level", h)]).toEqual([h, ["07FC008", "08FB012"], ["07FC008", "08FB012"]]);
    // Change answers from the same runs (the ratio needs no record), as it did
    expect(["07FC008", "08FB012"].map((s) => answerFrom(one(s), INDEX, "discharge", "change").answer.ok)).toEqual([true, true]);
    // and each member's own words, at +3: its row, its dot and its switcher entry say the same as the sheet
    const row = answerFrom(one("07FC008"), INDEX, "discharge", 3).rows[0]!;
    expect([row.aheadWhy, donorReads(row, { horizon: 3 }), donorMapLabel(row, { horizon: 3 })]).toEqual(
      ["no-record", "Has a forecast this far ahead, but no record to compare it with.", "no record to compare with"]);
  });
});

describe("silentWhy", () => {
  it("today is no-record", () => {
    expect(silentWhy(PANEL, INDEX, "level", 0)).toBe("no-record");
  });
  it("Change under Level is no forecast (the Shell answers it as Now; Change is flow only, T52)", () => {
    expect(silentWhy(PANEL, INDEX, "level", "change")).toBe("no-forecast");
  });
  it("is never flow-forecast-only under Flow, whatever the members forecast (T143 review)", () => {
    // the exported function used to ignore the quantity and lean on its one caller's precondition
    expect(([1, 3, 5, 10] as const).map((h) => silentWhy(PANEL, INDEX, "discharge", h)))
      .toEqual(["no-forecast", "no-forecast", "no-forecast", "no-forecast"]);
    expect(silentWhy(PANEL, INDEX, "level", 3)).toBe("flow-forecast-only");
  });
});

/*
 * THE REPUBLISHED FEED (T143 review). The publisher let the first model to answer ANYTHING win the whole day, so CLEVER's
 * flow ended the search and ELF's stage forecast for +1/+3/+5 was never ranked. `forecast_percentiles` now takes the
 * first model per quantity. `ahead-per-quantity-*.json` is that function's output over the SAME snapshot (each station
 * file's runs, the feed's clim.json, now = fetchedAt) for the panel's members and the Fraser at Hope, laid over the real
 * index here: the app reads it with no change (both shapes), and Level answers those days from ELF's level.
 */
const PER_Q = JSON.parse(readFileSync(join(dir, "ahead-per-quantity-2026-10-23T0350Z.json"), "utf8")).ahead;
const REPUBLISHED = { ...INDEX, stations: Object.fromEntries(Object.entries(INDEX.stations as Record<string, object>)
  .map(([s, row]) => [s, s in PER_Q ? { ...row, ...(PER_Q[s] ? { ahead: PER_Q[s] } : {}) } : row])) };

describe("the Chilliwack panel on the per-quantity feed", () => {
  const on = (q: "discharge" | "level", h: 1 | 3 | 5 | 10) => {
    const a = answerFrom(PANEL, REPUBLISHED, q, h).answer;
    return a.ok ? { ok: true, percentile: r4(a.value.percentile), donors: a.value.donors } : a;
  };
  it("answers Level at +1/+3/+5 from ELF's stage, and Flow as before", () => {
    expect({ 1: on("level", 1), 3: on("level", 3), 5: on("level", 5), 10: on("level", 10) })
      .toEqual({ 1: ok(0.05, 3), 3: ok(0.05, 3), 5: ok(0.05, 3), 10: ok(0.05, 3) });
    expect({ 1: on("discharge", 1), 3: on("discharge", 3), 5: on("discharge", 5), 10: on("discharge", 10) })
      .toEqual({ 1: ok(0.05, 4), 3: ok(0.05, 4), 5: ok(0.05, 4), 10: ok(0.05, 3) });
  });
  it("ranks the Fraser at Hope's level day by day, each its own", () => {
    const one: Panel = { areaKm2: 100, members: [{ station: "08MF005" as never, role: "up", areaKm2: 100,
      years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
    expect(([1, 3, 5, 10] as const).map((h) => {
      const a = answerFrom(one, REPUBLISHED, "level", h).answer;
      return a.ok ? r4(a.value.percentile) : a.why;
    })).toEqual([0.152, 0.1112, 0.05, 0.05]);
  });
});

/* The whole answers, as the 2026-10-23 feed gives them. The Chilliwack sits at the 5th percentile every day (a dry
 * October; +10 is ELF's, which 08MH016 has no run of — three donors). */
const ok = (percentile: number, donors: number) => ({ ok: true, percentile, donors });
const EXPECT_FLOW = { 0: ok(0.05, 4), 1: ok(0.05, 4), 3: ok(0.05, 4), 5: ok(0.05, 4), 10: ok(0.05, 3),
                      change: ok(0.4856, 4) };
const FLOW_ONLY = { ok: false, why: "flow-forecast-only" };
const EXPECT_LEVEL = { 0: ok(0.05, 4), 1: FLOW_ONLY, 3: FLOW_ONLY, 5: FLOW_ONLY, 10: ok(0.05, 3) };
/* T157: the two stations with a fresh run and no record (07FC008, 08FB012) left "no-forecast" for "forecast-unranked". */
const EXPECT_FLOW_TALLY = { 1: { ok: 367, "forecast-unranked": 2, "no-forecast": 63 },
                            10: { ok: 359, "forecast-unranked": 2, "no-forecast": 71 } };
/* Before T143 the 310 (and +10's 5) were "no-forecast": the owner's "no forecast … for every gauge". */
const EXPECT_LEVEL_TALLY = {
  1: { ok: 62, "flow-forecast-only": 310, "forecast-unranked": 2, "no-forecast": 58 },
  3: { ok: 62, "flow-forecast-only": 310, "forecast-unranked": 2, "no-forecast": 58 },
  5: { ok: 62, "flow-forecast-only": 310, "forecast-unranked": 2, "no-forecast": 58 },
  10: { ok: 359, "flow-forecast-only": 5, "forecast-unranked": 2, "no-forecast": 66 },
};
