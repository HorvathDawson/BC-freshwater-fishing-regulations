/**
 * ONE HISTORY READ (T170): a spot's visit-day flow is read in ONE place.
 *
 * `flowOnDay` in `@app/data/spots` `dayFlow.ts` is the only function that asks for a past day's flow — the feed's daily
 * series first, then HYDAT's daily means through ECCC GeoMet. The refresh and the spot's "Get the flow for that day"
 * both reach it through `fillDayFlow`; nothing writes a spot's `dayFlow` but `fillDayFlow` (and the refresh, copying
 * its answer onto the spot it is filling). A second reader would be a second answer to "what was the river doing that
 * day" — the drift T51's backdated-spot bug already was (capture reading TODAY's feed for an earlier day).
 *
 * Over every app module (tests excluded):
 *   1. the GeoMet collection is named in `dayFlow.ts` only;
 *   2. `flowOnDay(` is called only inside `dayFlow.ts` (by `fillDayFlow`);
 *   3. `fillDayFlow(` is called by the refresh (`refresh.ts`) and the app's one-spot action (`App.tsx`) only;
 *   4. `dayFlow:` is written only in `dayFlow.ts` and `refresh.ts`.
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

const SPELLINGS: [RegExp, string][] = [
  [GEOMET, 'fetch("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items?STATION_NUMBER=08MH001")'],
  [CALLS_READ, "const a = await flowOnDay(station, date, ports);"],
  [CALLS_FILL, "await fillDayFlow(spot, { source });"],
  [WRITES, "return { ...spot, dayFlow: rec };"],
];

describe("one history read (T170)", () => {
  it("each pattern can fail on the spelling it forbids", () => {
    expect(SPELLINGS.map(([re, s]) => re.test(s))).toEqual([true, true, true, true]);
    // and the declarations themselves are not calls
    expect([CALLS_READ.test("export async function flowOnDay(station"),
            CALLS_FILL.test("export async function fillDayFlow(spot"),
            WRITES.test("  dayFlow?: DayFlow | null;")]).toEqual([false, false, false]);
  });

  it("GeoMet is named in dayFlow.ts only", () => {
    expect(where(GEOMET)).toEqual(["packages/data/src/spots/dayFlow.ts"]);
  });

  it("flowOnDay is called by fillDayFlow only; fillDayFlow by the refresh and the app's action only", () => {
    expect(where(CALLS_READ)).toEqual(["packages/data/src/spots/dayFlow.ts"]);
    expect(where(CALLS_FILL)).toEqual(["apps/mobile/src/App.tsx", "packages/data/src/spots/refresh.ts"]);
  });

  it("a spot's dayFlow is written by the fill and the refresh only", () => {
    expect(where(WRITES)).toEqual(["packages/data/src/spots/dayFlow.ts", "packages/data/src/spots/refresh.ts"]);
  });
});
