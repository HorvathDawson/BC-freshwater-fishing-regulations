/**
 * (T738) THE FIRST INSTALL'S FIGURE COUNTS EVERY FILE: the system downloads the four big files at once, from before the
 * install starts, so the bar's "N of M MB downloaded" is the bytes had of ALL of them — the data files fetched, and each
 * big file's bytes so far (what the device holds of it before its turn, its own transfer's count after) — never above
 * the total, never back within one install, and right at the first moment after a relaunch. T722's proof saw "38.8 of
 * 479.3 MB" with 116.3 MB on disk: the big files counted only from their turn, after the data files.
 *
 * The sum (`tally.ts`) is pinned alone, then through `install` itself with a fake device, and the bar's words
 * (`installLine`, what App.tsx shows under the bar) as whole sentences (AGENTS 33).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { installLine, setFailureSink } from "@app/core";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import type { BgJob } from "./bgDownload";
import { install, type DownloadTo, type LookBytes, type Progress, type ReleaseFs } from "./install";
import { bytesHad, downloadTally, had, lookHad, type Look } from "./tally";

const MB = 1_000_000;

describe("the sum (tally.ts)", () => {
  it("adds every file, parallel ones too, each held to its size and the whole to the total", () => {
    expect(bytesHad([{ n: 2 * MB, size: 2 * MB }, { n: 60 * MB, size: 300 * MB }, { n: 44 * MB, size: 180 * MB }], 482 * MB))
      .toBe(106 * MB);
    expect(bytesHad([{ n: 400 * MB, size: 300 * MB }, { n: 180 * MB, size: 180 * MB }], 482 * MB)).toBe(480 * MB);
    expect(bytesHad([{ n: 300 * MB, size: 300 * MB }, { n: 200 * MB, size: 200 * MB }], 482 * MB)).toBe(482 * MB);
  });

  it("a count not known yet (-1) is none had; a BigInt (iOS's Int64) is its number; NaN is thrown, never shown", () => {
    expect(had(-1, 300 * MB)).toBe(0);
    expect(had(BigInt(116_300_000), 300 * MB)).toBe(116_300_000);
    expect(() => had(Number.NaN, 300 * MB)).toThrow("download progress: NaN is not a byte count");
  });

  it("a look: the finished file is all of it, a live job its own count, a failed or emptied job nothing, else the .part", () => {
    const job = (o: Partial<BgJob>): BgJob => ({ id: 1, url: "u", status: "running", reason: 0, bytes: 0, total: -1, metered: true, ...o });
    expect(lookHad({ file: 300 * MB, part: 0, job: null }, 300 * MB)).toBe(300 * MB);
    expect(lookHad({ file: 0, part: 7 * MB, job: job({ bytes: 60 * MB }) }, 300 * MB)).toBe(60 * MB);
    expect(lookHad({ file: 0, part: 0, job: job({ status: "pending", bytes: -1, total: -1 }) }, 300 * MB)).toBe(-1);
    expect(lookHad({ file: 0, part: 0, job: job({ status: "failed", bytes: 90 * MB }) }, 300 * MB)).toBe(0);
    expect(lookHad({ file: 0, part: 0, job: job({ status: "done", bytes: 300 * MB, present: false }) }, 300 * MB)).toBe(0);
    expect(lookHad({ file: 0, part: 0, job: job({ status: "done", bytes: 300 * MB }) }, 300 * MB)).toBe(300 * MB);
    expect(lookHad({ file: 12, part: 41 * MB, job: null }, 300 * MB)).toBe(41 * MB);
  });

  describe("the running figure", () => {
    let reported: unknown[] = [];
    let unsink = () => {};
    beforeEach(() => { reported = []; unsink = setFailureSink((e) => reported.push(e)); });
    afterEach(() => unsink());

    it("never goes back within a run; the sum as it is (`real`) does", () => {
      const t = downloadTally(482 * MB);
      t.set("tiles", 60 * MB, 300 * MB);
      t.set("basemap", 44 * MB, 180 * MB);
      expect(t.shown()).toBe(104 * MB);
      t.set("tiles", 0, 300 * MB);                         // its transfer started it again
      expect([t.real(), t.shown()]).toEqual([44 * MB, 104 * MB]);
      t.set("tiles", 70 * MB, 300 * MB);
      expect(t.shown()).toBe(114 * MB);
    });

    it("a count that is not one is reported and the file keeps its last count", () => {
      const t = downloadTally(482 * MB);
      t.set("tiles", 60 * MB, 300 * MB);
      t.set("tiles", Number.NaN, 300 * MB);
      expect(t.shown()).toBe(60 * MB);
      expect(reported.map((e) => (e as Error).message)).toEqual(["download progress: NaN is not a byte count"]);
    });
  });
});

// ── through `install`: a data file (2 MB) and two big files (tiles 300 MB, basemap 180 MB), 482 MB in all ──────────

const H = "d768d6e135e62b99";
const CORE = new Uint8Array(2 * MB);
const BASE = "https://r2.test/";
const M: Manifest = parseManifest({
  format: "app-manifest/2", data_release: "r-1",
  tiles: { compatible: [`${H}.*`], current: { id: "v1", url: "tiles/v1/atlas.pmtiles", bytes: 300 * MB, sha256: "t".repeat(64) },
           assets: null, mask: null },
  basemap: { id: "b1", url: "tiles/basemap-b1.pmtiles", bytes: 180 * MB, sha256: "m".repeat(64) },
  files: { core: { url: "r-1/core-x.sqlite.gz", bytes: CORE.length, gz: 0, sha256: sha256Hex(CORE), format: "core/1",
                   section_handles: H } },
  regulations: { valid_until: null, versions: null },
});
const TILES = `${BASE}tiles/v1/atlas.pmtiles`, BASEMAP = `${BASE}tiles/basemap-b1.pmtiles`;

function memFs(): ReleaseFs {
  const files = new Map<string, Uint8Array>();
  return {
    exists: async (p) => files.has(p),
    read: async (p) => files.get(p)!,
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { files.delete(p); },
  };
}

/** A gate a fake transfer waits at, opened by the test. */
function gate() {
  let open!: () => void;
  const p = new Promise<void>((r) => { open = r; });
  return { p, open };
}
const until = async (ok: () => boolean) => { for (let i = 0; i < 400 && !ok(); i++) await new Promise((r) => setTimeout(r, 5)); expect(ok()).toBe(true); };

/** The phone's device, faked: the data file waits at `core`; each big file's transfer reports what `steps` say, in turn. */
function phone(o: { looks: Look[][] | ((files: readonly { url: string }[]) => Look[]); steps: Record<string, number[]> }) {
  const core = gate(), bigs = gate();
  let looked = 0;
  const look: LookBytes = async (files) => {
    if (typeof o.looks === "function") return o.looks(files);
    expect(files.map((f) => f.url)).toEqual([TILES, BASEMAP]);
    return o.looks[Math.min(looked++, o.looks.length - 1)]!;
  };
  const dl = async () => { await core.p; return CORE; };
  const toDisk: DownloadTo = async (url, _path, e, onBytes) => {
    await bigs.p;
    for (const n of o.steps[url] ?? []) { onBytes?.(n); await new Promise((r) => setTimeout(r, 1)); }
    onBytes?.(e.bytes);
  };
  return { look, dl, toDisk, core, bigs };
}

const job = (url: string, bytes: number | bigint, o: Partial<BgJob> = {}): BgJob =>
  ({ id: url === TILES ? 1 : 2, url, status: "running", reason: 0, bytes: bytes as number, total: -1, metered: true, ...o });
const lines = (seen: Progress[]) => seen.filter((p) => !p.setup).map(installLine);

describe("install: the bar counts every file being downloaded (T738)", () => {
  beforeEach(() => { vi.spyOn(console, "info").mockImplementation(() => {}); });
  afterEach(() => vi.restoreAllMocks());

  it("after a relaunch the first figure is what the big files already hold, before any data file is fetched", async () => {
    // relaunched mid-download: the system's jobs hold 60 MB of the tiles and 44 MB of the basemap
    const d = phone({ looks: [[{ file: 0, part: 0, job: job(TILES, 60 * MB) }, { file: 0, part: 0, job: job(BASEMAP, 44 * MB) }]],
                      steps: { [TILES]: [70 * MB, 150 * MB], [BASEMAP]: [50 * MB] } });
    const seen: Progress[] = [];
    const run = install(M, memFs(), d.dl, BASE, null, (p) => seen.push(p), d.toDisk, undefined, undefined, d.look);
    await until(() => seen.length > 0);
    expect(installLine(seen[0]!)).toBe("104.0 of 482.0 MB downloaded");           // was "2.0 of 482.0" after the data file
    d.core.open();
    await until(() => seen.some((p) => p.file === "core"));
    expect(installLine(seen.find((p) => p.file === "core")!)).toBe("106.0 of 482.0 MB downloaded");
    d.bigs.open();
    await run;
    const said = lines(seen);
    // in their turn: the tiles at 70 MB while the basemap still has its 44 MB (counting one job at a time read 72.0 here)
    expect(said).toContain("116.0 of 482.0 MB downloaded");
    expect(said).toContain("122.0 of 482.0 MB downloaded");                    // tiles 70 + basemap 50 + core 2
    expect(said).toContain("202.0 of 482.0 MB downloaded");                    // tiles 150 + basemap 50 + core 2
    const shown = seen.filter((p) => !p.setup).map((p) => p.bytes);
    expect(shown).toEqual([...shown].sort((a, b) => a - b));                   // never back
    expect(Math.max(...seen.map((p) => p.bytes))).toBe(482 * MB);              // never above the total
    expect(seen.at(-1)!.setup?.step).toBe("verify");                           // the second bar, as before
  });

  it("the big files' bytes move the bar while the data files are fetched (looked at again and again)", async () => {
    const d = phone({ looks: [
      [{ file: 0, part: 0, job: job(TILES, 10 * MB) }, { file: 0, part: 0, job: job(BASEMAP, 5 * MB) }],
      [{ file: 0, part: 0, job: job(TILES, 40 * MB) }, { file: 0, part: 0, job: job(BASEMAP, 25 * MB) }],
    ], steps: {} });
    const seen: Progress[] = [];
    const run = install(M, memFs(), d.dl, BASE, null, (p) => seen.push(p), d.toDisk, undefined, undefined, d.look);
    await until(() => lines(seen).includes("65.0 of 482.0 MB downloaded"));
    expect(lines(seen)[0]).toBe("15.0 of 482.0 MB downloaded");
    d.core.open(); d.bigs.open();
    await run;
  });

  it("a big file already finished, one not known yet (-1), one resumed from its .part, one reported as a BigInt", async () => {
    const BOOK = new TextEncoder().encode("%PDF the rule book");
    const m4: Manifest = parseManifest({ ...JSON.parse(JSON.stringify(M)),
      synopsis: { id: "s1", url: "r-1/synopsis-s1.pdf", bytes: 20 * MB, sha256: sha256Hex(BOOK) },
      tiles: { compatible: [`${H}.*`], current: { id: "v1", url: "tiles/v1/atlas.pmtiles", bytes: 300 * MB, sha256: "t".repeat(64) },
               assets: null, mask: { url: "tiles/v1/mask.pmtiles", bytes: 10 * MB, sha256: "k".repeat(64) } } });
    const d = phone({ looks: (files) => files.map((f): Look => {
      if (f.url.endsWith("atlas.pmtiles")) return { file: 0, part: 0, job: job(f.url, BigInt(116_300_000)) };
      if (f.url.endsWith("mask.pmtiles")) return { file: 10 * MB, part: 0, job: null };                 // in place
      if (f.url.endsWith("basemap-b1.pmtiles")) return { file: 0, part: 0, job: job(f.url, -1, { status: "pending" }) };
      return { file: 0, part: 3 * MB, job: null };                                                      // an older app's .part
    }), steps: {} });
    const seen: Progress[] = [];
    const run = install(m4, memFs(), d.dl, BASE, null, (p) => seen.push(p), d.toDisk, undefined, undefined, d.look);
    await until(() => seen.length > 0);
    expect(installLine(seen[0]!)).toBe("129.3 of 512.0 MB downloaded");        // 116.3 + 10 + 0 + 3
    d.core.open(); d.bigs.open();
    await run;
  });

  it("a look that fails is reported once; the figure holds and the install finishes", async () => {
    const reported: unknown[] = [];
    const unsink = setFailureSink((e) => reported.push(e));
    try {
      let n = 0;
      const d = phone({ looks: () => { if (n++ > 0) throw new Error("jobs(): the downloader did not answer"); return [
        { file: 0, part: 0, job: job(TILES, 30 * MB) }, { file: 0, part: 0, job: job(BASEMAP, 0) }]; }, steps: {} });
      const seen: Progress[] = [];
      const run = install(M, memFs(), d.dl, BASE, null, (p) => seen.push(p), d.toDisk, undefined, undefined, d.look);
      await until(() => reported.length > 0);
      expect(lines(seen)).toEqual(["30.0 of 482.0 MB downloaded"]);
      d.core.open(); d.bigs.open();
      await run;
      expect(reported.map((e) => (e as Error).message)).toEqual(["jobs(): the downloader did not answer"]);
    } finally { unsink(); }
  });

  it("without a look (a client with no device facts) the big files count from their turn, as before", async () => {
    const d = phone({ looks: [], steps: { [TILES]: [70 * MB] } });
    const seen: Progress[] = [];
    const run = install(M, memFs(), d.dl, BASE, null, (p) => seen.push(p), d.toDisk);
    d.core.open(); d.bigs.open();
    await run;
    expect(lines(seen).slice(0, 2)).toEqual(["2.0 of 482.0 MB downloaded", "72.0 of 482.0 MB downloaded"]);
  });
});
