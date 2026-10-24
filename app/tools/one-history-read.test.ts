/**
 * ONE HISTORY READ (T170): a spot's visit-day flow is read in ONE place.
 *
 * `flowOnDay` in `@app/data/spots` `dayFlow.ts` is the only function that asks for a past day's flow — the feed's daily
 * series first, then HYDAT's daily means through ECCC GeoMet. The refresh and the spot's "Get the flow for that day"
 * both reach it through `fillDayFlow`; nothing writes a spot's `dayFlow` but `fillDayFlow` (and the refresh, copying
 * its answer onto the spot it is filling). A second reader would be a second answer to "what was the river doing that
 * day" — the drift T51's backdated-spot bug already was (capture reading TODAY's feed for an earlier day).
 *
 * Q161 (owner 2026-10-24, "it should save the date that you set as the time of trip"): a spot ADDED for an earlier
 * day is read through the same one read — its lead gauge by `fillDayFlow`, its panel members by `panelOnDay` (both in
 * `dayFlow.ts`, both calling `flowOnDay`) — so capture is a third caller of the fill, and never a reader of its own.
 *
 * Over every app module (tests excluded):
 *   1. the GeoMet collection is named in `dayFlow.ts` only;
 *   2. `flowOnDay(` is called only inside `dayFlow.ts` (by `fillDayFlow` and `panelOnDay`);
 *   3. `fillDayFlow(` is called by the refresh (`refresh.ts`), capture (`capture.ts`) and the app's one-spot action
 *      (`App.tsx`) only; the trip day's panel (`capturePanelOnDay(` / `fillDayPanel(`) by capture and the refresh only;
 *   4. `dayFlow:` is written only in `dayFlow.ts` and `refresh.ts`; `panelDay:` only in `dayFlow.ts`.
 * Each pattern is proven able to fail on a spelling it forbids (`SPELLINGS`).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["apps", "packages"];

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx|mjs)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}

const FILES = ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => ({ path: relative(APP, p), text: readFileSync(p, "utf8") }));
const where = (re: RegExp) => FILES.filter((f) => re.test(f.text)).map((f) => f.path).sort();

const GEOMET = /hydrometric-daily-mean/;
const CALLS_READ = /(?<!function )\bflowOnDay\(/;
const CALLS_FILL = /(?<!function )\bfillDayFlow\(/;
const WRITES = /\bdayFlow: (?!DayFlow)/;
const CALLS_PANEL = /(?<!function )\b(capturePanelOnDay|fillDayPanel)\(/;
const WRITES_PANEL = /\bpanelDay: (?!PanelDay)/;

const SPELLINGS: [RegExp, string][] = [
  [GEOMET, 'fetch("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items?STATION_NUMBER=08MH001")'],
  [CALLS_READ, "const a = await flowOnDay(station, date, ports);"],
  [CALLS_FILL, "await fillDayFlow(spot, { source });"],
  [WRITES, "return { ...spot, dayFlow: rec };"],
  [CALLS_PANEL, "next = await fillDayPanel(next, ports);"],
  [WRITES_PANEL, "return { ...spot, panelDay: { state: \"day\", date } };"],
];

describe("one history read (T170)", () => {
  it("each pattern can fail on the spelling it forbids", () => {
    expect(SPELLINGS.map(([re, s]) => re.test(s))).toEqual([true, true, true, true, true, true]);
    // and the declarations themselves are not calls
    expect([CALLS_READ.test("export async function flowOnDay(station"),
            CALLS_FILL.test("export async function fillDayFlow(spot"),
            WRITES.test("  dayFlow?: DayFlow | null;"), CALLS_PANEL.test("export async function fillDayPanel(spot"),
            WRITES_PANEL.test("  panelDay?: PanelDay | null;")]).toEqual([false, false, false, false, false]);
  });

  it("GeoMet is named in dayFlow.ts only", () => {
    expect(where(GEOMET)).toEqual(["packages/data/src/spots/dayFlow.ts"]);
  });

  it("flowOnDay is called in dayFlow.ts only; the fills by capture, the refresh and the app's action only", () => {
    expect(where(CALLS_READ)).toEqual(["packages/data/src/spots/dayFlow.ts"]);
    expect(where(CALLS_FILL)).toEqual(["apps/mobile/src/App.tsx", "packages/data/src/spots/capture.ts",
                                       "packages/data/src/spots/refresh.ts"]);
    expect(where(CALLS_PANEL)).toEqual(["packages/data/src/spots/capture.ts", "packages/data/src/spots/refresh.ts"]);
  });

  it("capture reads no live reading for an earlier trip day: gaugeNow only on the today path", () => {
    const cap = FILES.find((f) => f.path === "packages/data/src/spots/capture.ts")!.text;
    // the one call of the live reader is the `readingFor` of the today branch (`earlier ? … : readingFor(…)`)
    expect(cap.match(/gaugeNow\(/g)?.length).toBe(1);
    expect(/earlier \? Promise\.resolve\([^)]*\)\s*:\s*readingFor\(source, lead\)/.test(cap.replace(/\s+/g, " "))
      || /earlier\s*\?[\s\S]{0,200}?:\s*readingFor\(source, lead\)/.test(cap)).toBe(true);
  });

  it("a spot's dayFlow is written by the fill and the refresh only", () => {
    expect(where(WRITES)).toEqual(["packages/data/src/spots/dayFlow.ts", "packages/data/src/spots/refresh.ts"]);
    expect(where(WRITES_PANEL)).toEqual(["packages/data/src/spots/dayFlow.ts"]);
  });
});
