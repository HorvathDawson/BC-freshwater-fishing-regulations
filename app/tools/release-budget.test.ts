/**
 * T129: THE WEB REQUEST BUDGET — what a web visitor fetches, gated per moment, not only the release total (a phone's).
 *
 *   the pin (Tier 1)        the browser walk's pinned request list (`request-walk.pin.json`, written by
 *                           `tools/request-walk.mjs --pin` on the running app) fetches, at each gated moment, exactly
 *                           the files `WEB_BUDGET` prices (pipeline/deliver/release/contract.py, generated into
 *                           pack.generated.ts) — so the gate prices what the app really loads; and no moment before the
 *                           first question loads a SQLite file
 *   the live release        every gated moment, priced from the manifest the way the release step prices it
 *                           (`build.web_moments`), is under its gate; and through the loader the app runs, a cold open of
 *                           a water in each region fetches core + water + that region's file and nothing else
 *
 * The live half runs on the release this machine holds (`liveRelease`, a worktree reads the main checkout's) and skips
 * without one, unless REGS_STORE_REQUIRED=1.
 */
import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ItemId, Manifest } from "@app/data";
import { bootRelease, type ReleasePlatform } from "@app/data/release";
import { WEB_BUDGET } from "../packages/data/src/pack.generated";
import { releaseCache } from "../packages/data/src/release/drivers/cache.web";
import { releaseSource } from "../packages/data/src/release/drivers/source.web";
import { liveRelease, nodeOpener, releaseFetch } from "../conformance/src/fixtureRelease";

const PIN = JSON.parse(readFileSync(join(__dirname, "request-walk.pin.json"), "utf8")) as Record<string, string[]>;
/** The release's own files (and the tile set's mask) in a pinned moment: the feed, the wasm and other hosts are not
 *  release files. A region file reads as `regs-{region}`, the budget's word for "the region tapped". */
const releaseFiles = (moment: string): string[] => {
  const files = PIN[moment];
  if (!files) throw new Error(`the walk pins no moment "${moment}" (node tools/request-walk.mjs --pin)`);
  return files.filter((f) => !/^(feed:|wasm$)/.test(f)).map((f) => (/^regs-(\d|7[ab])/.test(f) ? "regs-{region}" : f));
};
const sorted = (xs: readonly string[]) => [...xs].sort();

/** Which pinned moment each gate prices. */
const GATED: Record<string, string> = {
  first_paint: "visit | first paint",
  first_tap: "tap named | map tap: a named water (R2)",
  first_tap_unnamed: "tap unnamed | map tap: an unnamed stream (R2)",
  conditions_open: "conditions | open Conditions",
};

describe("the pinned walk fetches what the budget prices", () => {
  it("every gate names a pinned moment, and that moment fetches exactly the gate's files, once each", () => {
    expect(sorted(Object.keys(WEB_BUDGET))).toEqual(sorted(Object.keys(GATED)));
    for (const [gate, moment] of Object.entries(GATED))
      expect([gate, sorted(releaseFiles(moment))]).toEqual([gate, sorted(WEB_BUDGET[gate]!.files)]);
  });

  it("before the first question nothing but the manifest, paint and the mask (and the feed's index) — every walk", () => {
    const firsts = Object.entries(PIN).filter(([k]) => k.endsWith("| first paint"));
    expect(firsts.length).toBeGreaterThan(3);
    for (const [k, files] of firsts) expect([k, files]).toEqual([k, ["feed:index", "manifest", "mask", "paint"]]);
  });

  it("the Change horizon, the Spots tab and a spot fetch no release file", () => {
    for (const k of ["conditions | the Change horizon", "spot | the Spots tab", "spot | open a spot"])
      expect([k, PIN[k]]).toEqual([k, []]);
  });
});

/** A moment's bytes, priced as the release step prices them (`pipeline/deliver/release/build.py` `web_moments`). */
function price(dir: string, files: readonly string[]): { bytes: number; files: string[] } {
  const m = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as Manifest;
  const region = Object.keys(m.files).filter((k) => k.startsWith("regs-") && k !== "regs-sid")
    .sort((a, b) => m.files[b]!.gz - m.files[a]!.gz)[0]!;
  const priced = files.map((f): [string, number] =>
    f === "manifest" ? [f, statSync(join(dir, "manifest.json")).size]
    : f === "mask" ? [f, m.tiles.mask?.bytes ?? 0]
    : [f === "regs-{region}" ? region : f, m.files[f === "regs-{region}" ? region : f]?.gz ?? 0]);
  return { bytes: priced.reduce((a, [, n]) => a + n, 0), files: priced.map(([f]) => f) };
}

const LIVE = liveRelease();
const BASE = "https://r2.test/release/";
const web: ReleasePlatform = {
  releaseOpener: () => nodeOpener(),
  releaseCache: () => releaseCache(),
  releaseSource: (m, at) => releaseSource(m, at),
  device: () => null,
};
/** A water per region whose every part's key is in that region alone (request-walk.mjs's REGION_WATERS). */
const REGION_WATERS: Record<string, string> = {
  1: "gnis:21141", 2: "gnis:15333", 3: "gnis:39492", 4: "gnis:37414", 5: "gnis:13744", 6: "gnis:15430",
  "7a": "gnis:8018", "7b": "gnis:14619", 8: "gnis:32069",
};

describe.skipIf(!LIVE)("the live release is under every web gate", () => {
  it("each gated moment, priced from the manifest, is under its gate", () => {
    const over = Object.entries(WEB_BUDGET).map(([k, g]) => ({ k, ...price(LIVE!, g.files), gate: g.gate }))
      .filter((r) => r.bytes > r.gate);
    expect(over).toEqual([]);
  });

  it("mutation: the SMALLEST region file grown past the tap headroom becomes the priced region and fails the tap", () => {
    const tap = price(LIVE!, WEB_BUDGET.first_tap!.files);
    const m = JSON.parse(readFileSync(join(LIVE!, "manifest.json"), "utf8")) as Manifest;
    const regions = Object.keys(m.files).filter((k) => /^regs-(\d|7[ab])$/.test(k));
    const small = regions.sort((a, b) => m.files[a]!.gz - m.files[b]!.gz)[0]!;
    const largest = tap.files.find((f) => f.startsWith("regs-"))!;
    m.files[small]!.gz = m.files[largest]!.gz + (WEB_BUDGET.first_tap!.gate - tap.bytes) + 1;
    const dir = mkdtempSync(join(tmpdir(), "budget-"));
    writeFileSync(join(dir, "manifest.json"), JSON.stringify(m));
    const grown = price(dir, WEB_BUDGET.first_tap!.files);
    expect([grown.files.includes(small), grown.bytes > WEB_BUDGET.first_tap!.gate]).toEqual([true, true]);
  });

  it("a cold open of a water in each region fetches core + water + that region's file, nothing more", async () => {
    for (const [region, item] of Object.entries(REGION_WATERS)) {
      const log: string[] = [];
      const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                    platform: web, fetch: releaseFetch(BASE, LIVE!, log) });
      expect((await b.source.regulationKeys(item as ItemId)).length).toBeGreaterThan(0);
      expect([region, b.loader.opened()]).toEqual([region, ["paint", "core", "water", `regs-${region}`]]);
      expect(log.length).toBe(5);
    }
  }, 120_000);
});
