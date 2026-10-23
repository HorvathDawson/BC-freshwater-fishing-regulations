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
 * now says the forecast is there, in flow (`silentWhy`), and a flow still never stands in for a level.
 *
 * MUTATION PINS (each fails the named test; checked by hand 2026-10-23):
 *   - `reading` returning null for every horizon past 0 (the regression's shape)  → "answers every day ahead in Flow"
 *   - `reading` reading `row.ahead?.[q]` instead of by the day's key             → "answers every day ahead in Flow"
 *   - `silentWhy` always "no-forecast" (the shipped refusal)                     → "says the forecast is in flow under Level"
 *   - `silentWhy` dropping its `typeof horizon === "number"` guard                → "Change under Level is no forecast"
 *   - `donorView` keeping chips only for "no-forecast"                           → DonorPanel "under Level at +3 …"
 *   - `donorView` keeping the silent line under a flow-only refusal               → DonorPanel "under Level at +3 …"
 *   - `silentWhy` answering flow-forecast-only from the LEVEL contributions       → "says the forecast is in flow under Level"
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { Panel } from "./index";
import { answerFrom, reading, silentWhy } from "./panel";

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
});

describe("silentWhy", () => {
  it("today is no-record", () => {
    expect(silentWhy(PANEL, INDEX, 0)).toBe("no-record");
  });
  it("Change under Level is no forecast (the Shell answers it as Now; Change is flow only, T52)", () => {
    expect(silentWhy(PANEL, INDEX, "change")).toBe("no-forecast");
  });
});

/* The whole answers, as the 2026-10-23 feed gives them. The Chilliwack sits at the 5th percentile every day (a dry
 * October; +10 is ELF's, which 08MH016 has no run of — three donors). */
const ok = (percentile: number, donors: number) => ({ ok: true, percentile, donors });
const EXPECT_FLOW = { 0: ok(0.05, 4), 1: ok(0.05, 4), 3: ok(0.05, 4), 5: ok(0.05, 4), 10: ok(0.05, 3),
                      change: ok(0.4856, 4) };
const FLOW_ONLY = { ok: false, why: "flow-forecast-only" };
const EXPECT_LEVEL = { 0: ok(0.05, 4), 1: FLOW_ONLY, 3: FLOW_ONLY, 5: FLOW_ONLY, 10: ok(0.05, 3) };
const EXPECT_FLOW_TALLY = { 1: { ok: 367, "no-forecast": 65 }, 10: { ok: 359, "no-forecast": 73 } };
/* Before T143 the 310 (and +10's 5) were "no-forecast": the owner's "no forecast … for every gauge". */
const EXPECT_LEVEL_TALLY = {
  1: { ok: 62, "flow-forecast-only": 310, "no-forecast": 60 },
  3: { ok: 62, "flow-forecast-only": 310, "no-forecast": 60 },
  5: { ok: 62, "flow-forecast-only": 310, "no-forecast": 60 },
  10: { ok: 359, "flow-forecast-only": 5, "no-forecast": 68 },
};
