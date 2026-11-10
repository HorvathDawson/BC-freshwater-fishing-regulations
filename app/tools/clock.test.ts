/**
 * (T382, owner 2026-11-04: "all get the date from a single source so it is consistent and has no possibility of drift")
 * ONE CLOCK — the net. App source (core, data, ui, ui-native, map, the app) reads the device clock in ONE file,
 * `packages/core/src/clock.ts`: `now()` for an instant, `bcNow()` / `bcToday()` for the day (the B.C. clock). A
 * `new Date()` with no arguments or a `Date.now()` anywhere else FAILS HERE — take `now()` (an age, a timeout, a stamp)
 * or `bcNow()` (anything deciding which day it is) from `@app/core`.
 *
 * `performance.now()` is a frame timer, not a clock (it says nothing about the date): it is allowed only where listed
 * in FRAME_TIMERS, so a new one is a decision too. Tests are not scanned (they inject their own `now`).
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["packages/core/src", "packages/data/src", "packages/ui/src", "packages/ui-native/src",
  "packages/map/src", "apps/mobile/src"];
const CLOCK = "packages/core/src/clock.ts";
/** (T551) where a SECOND B.C. clock is also banned: the static site's tooling and the feeds worker (card, page, share). */
const ALSO_ROOTS = ["tools/seo", "workers/feeds/src"];

/** The frame timers (`performance.now()`), per file: how many, and why. */
const FRAME_TIMERS: Record<string, { n: number; why: string }> = {
  "packages/data/src/openProbe.ts": { n: 1, why: "the open probe's spans (dev timing)" },
  "packages/map/src/Map.native.tsx": { n: 1, why: "the open probe's press stamp, the same timer as openProbe" },
  "packages/map/src/native/useColouring.ts": { n: 2, why: "(T420 lab) the gap between colour batches, a duration" },
  "packages/map/src/web/useChannels.ts": { n: 2, why: "a channel push's cost (dev timing)" },
  "packages/map/src/web/useMapInstance.ts": { n: 4, why: "a style and a tile request's cost (dev timing)" },
  "packages/ui/src/regs.ts": { n: 2, why: "the open probe's build span" },
};

function sources(dir: string, out: string[] = []): string[] {
  let names: string[];
  try { names = readdirSync(dir); } catch { return out; }
  for (const f of names) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !/\.d\.ts$/.test(f)) out.push(p);
  }
  return out;
}
const files = (): string[] => ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p)).sort();
/** Comments out (a doc comment may name `Date.now()`); strings kept. */
const code = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[\s;{}),])\/\/.*$/gm, "$1");

/** A device-clock read: `Date.now()`, or `new Date()` / `new Date` with no arguments. */
const CLOCK_READ = /\bDate\s*\.\s*now\s*\(|\bnew\s+Date\s*(\(\s*\)|(?![\s(]))/g;
const FRAME_READ = /\bperformance\s*(\?\.)?\s*\.?\s*now\s*(\?\.)?\s*\(/g;

/**
 * (T551, T580 F15) A SECOND B.C. CLOCK: a device-zone reading of an instant -- `getHours()` / `getMinutes()` /
 * `getTimezoneOffset()` -- or a zone named for `Intl` (`timeZone:`). Only `day.ts` (the stamp formatters, whose `zone`
 * parameter is a test seam) and `clock.ts` may; everything else says an instant through `bcFields` / `localStamp` /
 * `bcClockTime`, or reads the day's own fields from `bcNow()` (`getDate()`, `getMonth()`: a wall-clock Date).
 */
const SECOND_CLOCK = /\.\s*(getHours|getMinutes|getTimezoneOffset)\s*\(|\btimeZone\s*:/g;
const CLOCK_HOMES = new Set([CLOCK, "packages/core/src/day.ts"]);
/** Known second clocks, as many as listed (fix, then delete the line). The gauge page's script ships as one plain script that may import only core's `figure`, so the B.C. words must be inlined by `clientScript` first. */
const KNOWN_SECOND_CLOCKS: Record<string, number> = { "tools/seo/gaugeClient.ts": 3 };
function secondClocks(src: string): number { return (code(src).match(SECOND_CLOCK) ?? []).length; }

function clockReads(src: string): number { return (code(src).match(CLOCK_READ) ?? []).length; }
function frameReads(src: string): number { return (code(src).match(FRAME_READ) ?? []).length; }

describe("one clock (T382)", () => {
  const all = files();
  it("scans the app's source", () => {
    expect(all.length).toBeGreaterThan(100);
    expect(all).toContain(CLOCK);
  });

  it("no file but clock.ts reads the device clock", () => {
    const bad = all.filter((p) => p !== CLOCK)
      .map((p) => [p, clockReads(readFileSync(join(APP, p), "utf8"))] as const)
      .filter(([, n]) => n > 0).map(([p, n]) => `${p}: ${n}`);
    expect(bad, "take now() (an instant) or bcNow() (the day) from @app/core").toEqual([]);
  });

  it("no file but clock.ts and day.ts says an instant in the device's zone or names a zone", () => {
    const wider = [...all, ...ALSO_ROOTS.flatMap((r) => sources(join(APP, r))).map((p) => relative(APP, p))];
    expect(wider.some((p) => p.startsWith("tools/seo/")) && wider.some((p) => p.startsWith("workers/feeds/src/"))).toBe(true);
    const seen = Object.fromEntries(wider.filter((p) => !CLOCK_HOMES.has(p))
      .map((p) => [p, secondClocks(readFileSync(join(APP, p), "utf8"))] as const).filter(([, n]) => n > 0));
    const bad = Object.entries(seen).filter(([p, n]) => KNOWN_SECOND_CLOCKS[p] !== n).map(([p, n]) => `${p}: ${n}`);
    for (const p of Object.keys(KNOWN_SECOND_CLOCKS)) if (!(p in seen)) bad.push(`${p}: listed, but now clean -- delete it from KNOWN_SECOND_CLOCKS`);
    expect(bad, "say an instant with bcFields / localStamp / bcClockTime from @app/core").toEqual([]);
  });

  it("clock.ts is where the clock is read", () => {
    expect(clockReads(readFileSync(join(APP, CLOCK), "utf8"))).toBe(1);
  });

  it("frame timers only where listed, as many as listed", () => {
    const seen: Record<string, number> = {};
    for (const p of all) {
      const n = frameReads(readFileSync(join(APP, p), "utf8"));
      if (n) seen[p] = n;
    }
    expect(seen).toEqual(Object.fromEntries(Object.entries(FRAME_TIMERS).map(([p, v]) => [p, v.n])));
  });
});

describe("the scan itself (mutation pins)", () => {
  it("catches each spelling of a clock read", () => {
    expect(clockReads("const t = Date.now();")).toBe(1);
    expect(clockReads("const d = new Date();")).toBe(1);
    expect(clockReads("const d = new Date;")).toBe(1);
    expect(clockReads("f(new Date( ), Date . now())")).toBe(2);
    expect(clockReads("const d = mapDay ?? new Date();")).toBe(1);
  });
  it("lets a date built from a value, a comment and a frame timer through", () => {
    expect(clockReads("new Date(ms); new Date(y, m, d); new Date(iso)")).toBe(0);
    expect(clockReads("/** an instant is `Date.now()` */ const x = 1; // new Date()")).toBe(0);
    expect(clockReads("const t = performance.now();")).toBe(0);
    expect(clockReads("const t = now(); const d = bcNow();")).toBe(0);
  });
  it("catches a second clock: device-zone getters and a named zone", () => {
    expect(secondClocks("const h = new Date(ms).getHours();")).toBe(1);
    expect(secondClocks("d.getMinutes(); d . getTimezoneOffset ()")).toBe(2);
    expect(secondClocks("Intl.DateTimeFormat('en', { timeZone: 'America/Vancouver' })")).toBe(1);
    expect(secondClocks("bcNow().getDate(); w.getUTCHours(); // d.getHours()")).toBe(0);
  });
  it("counts frame timers", () => {
    expect(frameReads("globalThis.performance?.now?.() ?? 0; performance.now()")).toBe(2);
  });
});
