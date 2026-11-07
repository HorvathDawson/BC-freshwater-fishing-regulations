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
  it("counts frame timers", () => {
    expect(frameReads("globalThis.performance?.now?.() ?? 0; performance.now()")).toBe(2);
  });
});
