/**
 * (T768) The part-end proof's MUTANT is a release the store reads whole: one made-up end on the Chilliwack (at
 * Chilliwack Lake), every other water as committed. So whatever fails under `tools/part-end-proof.mjs` fails on the new
 * end, never on a damaged copy. Also: the verdict counts only the content pin, and the driver honours the mutant's
 * variable only for a folder the tool wrote.
 */
import { mkdtempSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { markLabel, navSections, partName, type WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixture, openReleaseDir, regsFixtureDir } from "@app/data/release/node";
// @ts-expect-error a .mjs tool, no types
import { PIN, PROVENANCE, WATER, verdict, writeMutant } from "./part-end-proof.mjs";

const KEY = "REGS_FIXTURE_MUTANT";
/** this file compares the committed fixture with a mutant of it, so it reads the committed one even under the proof */
const SAVED = process.env[KEY];
afterAll(() => { if (SAVED === undefined) delete process.env[KEY]; else process.env[KEY] = SAVED; });

let was: RegsStore, now: RegsStore, done: { part: number; parts: number; outlet: string; inlet: string; moved: number[] };
beforeAll(async () => {
  delete process.env[KEY];
  const out = mkdtempSync(join(tmpdir(), "part-end-test-"));
  done = writeMutant(out);
  [was, now] = await Promise.all([openRegsStore(openRegsFixture()), openRegsStore(openReleaseDir(regsFixtureDir(out)))]);
});

const words = (g: WaterGroup) => ({ names: g.parts.map((_, i) => partName(g, i)),
  marks: g.marks.map((m) => `${markLabel(g, m) ?? "—"}: ${m.down} → ${m.up.join(",")}`), exit: g.exit });

describe("the part-end proof's mutant", () => {
  it("the Chilliwack has one more part, cut at its lake: the lower piece ends at the outlet, the upper starts at the inlet", async () => {
    const [a, b] = await Promise.all([loadWaterGroup(was, WATER), loadWaterGroup(now, WATER)]);
    const k = done.part;
    expect([b.parts.length, done.parts]).toEqual([a.parts.length + 1, a.parts.length + 1]);
    expect([b.parts[k]!.lo, b.parts[k]!.hi, b.parts[k + 1]!.lo, b.parts[k + 1]!.hi])
      .toEqual([a.parts[k]!.lo, done.outlet, done.inlet, a.parts[k]!.hi]);
    // the parts below the cut are as committed; the ones above it are the committed ones, one place higher
    expect(words(b).names.slice(0, k)).toEqual(words(a).names.slice(0, k));
    expect(words(b).names.slice(k + 2)).toEqual(words(a).names.slice(k + 1));
    // one mark more, at the new end, between the two pieces
    expect(b.marks.length).toBe(a.marks.length + 1);
    expect(b.marks.filter((m) => m.token === done.outlet).map((m) => [m.down, m.up])).toEqual([[k, [k + 1]]]);
    expect(b.exit).toEqual(a.exit);
    // the moved section is the upper piece's, every other section where it was
    const sel = (g: WaterGroup, i: number) => [...navSections(g, i).selected];
    expect(sel(b, k + 1)).toEqual(done.moved);
    expect([...sel(b, k), ...sel(b, k + 1)].sort()).toEqual(sel(a, k).sort());
  });

  it("every other fixture water reads as committed", async () => {
    for (const item of ["gnis:3062", "gnis:12227", "gnis:3137", "gnis:2936", "gnis:19986"]) {
      const [a, b] = await Promise.all([loadWaterGroup(was, item), loadWaterGroup(now, item)]);
      expect(words(b), item).toEqual(words(a));
    }
  });
});

describe("the proof's verdict and its switch", () => {
  const report = (failed: string[]) => ({ testResults: [...failed, "packages/ui/src/ok.test.ts"].map((f) =>
    ({ name: join(__dirname, "..", f), status: failed.includes(f) ? "failed" : "passed" })) });
  it("met only when the content pin is the one counted failure; provenance is listed, never counted", () => {
    const prov = Object.keys(PROVENANCE)[0]!;
    expect(verdict(report([PIN])).ok).toBe(true);
    expect(verdict(report([PIN, prov]))).toMatchObject({ ok: true, provenance: [prov], unexpected: [] });
    expect(verdict(report([PIN, "packages/ui/src/waterMap.test.ts"]))).toMatchObject({ ok: false, unexpected: ["packages/ui/src/waterMap.test.ts"] });
    expect(verdict(report([]))).toMatchObject({ ok: false, pinFailed: false });
  });

  afterEach(() => { delete process.env[KEY]; });
  it("the driver takes the mutant root only from a folder holding the tool's MUTANT.json", () => {
    const empty = mkdtempSync(join(tmpdir(), "not-a-mutant-"));
    mkdirSync(join(empty, "r-000000000001"));
    process.env[KEY] = empty;
    expect(() => regsFixtureDir()).toThrow(/holds no MUTANT\.json/);
  });
});
