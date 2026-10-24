/**
 * (T186, Q164) THE WEB PREFETCH: the decision (region in view always; core + sql-wasm only when idle, not Save-Data,
 * not a slow connection), the tile's region word, and that a prefetched file is the SAME cached fetch the later open
 * reads — through the real web source over its cache, never a second download.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import type { Db } from "../sql/db";
import { releaseLoader, type DbName } from "./loader";
import { prefetchPlan, regionOfTile, runPrefetch, PREFETCH_MARK, type PrefetchSignals } from "./prefetch";
import { releaseCache } from "./drivers/cache.web";
import { releaseSource } from "./drivers/source.web";

describe("prefetchPlan: what to fetch ahead", () => {
  const base: PrefetchSignals = { region: "2", idle: true, saveData: false, effectiveType: "4g" };
  const cases: [string, Partial<PrefetchSignals>, { regs: string | null; core: boolean }][] = [
    ["idle, fast: the region's file and core", {}, { regs: "2", core: true }],
    ["not idle yet: the region's file only", { idle: false }, { regs: "2", core: false }],
    ["Save-Data: the region's file only", { saveData: true }, { regs: "2", core: false }],
    ["slow-2g", { effectiveType: "slow-2g" }, { regs: "2", core: false }],
    ["2g", { effectiveType: "2g" }, { regs: "2", core: false }],
    ["3g", { effectiveType: "3g" }, { regs: "2", core: false }],
    ["the browser does not say its speed: not slow", { effectiveType: null }, { regs: "2", core: true }],
    ["over no region: core alone (idle)", { region: null }, { regs: null, core: true }],
    ["another region in view: that region's file", { region: "7a" }, { regs: "7a", core: true }],
    ["over no region, Save-Data, 3g, not idle: nothing", { region: null, saveData: true, effectiveType: "3g", idle: false },
     { regs: null, core: false }],
  ];
  for (const [what, s, want] of cases)
    it(what, () => expect(prefetchPlan({ ...base, ...s })).toEqual(want));
});

describe("regionOfTile: a tile's region_id as the release's region word", () => {
  it("lower-cases 7A/7B, trims, and refuses what is no region", () => {
    expect(["1", "7A", " 7b ", 8, "9", "", null, undefined, "regs-2"].map(regionOfTile))
      .toEqual(["1", "7a", "7b", "8", null, null, null, null, null]);
  });
});

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
function manifest(files: Record<string, string>): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(Object.entries(files).map(([k, body]) => [k, {
      url: `r-1/${k}-x.sqlite.gz`, bytes: enc(body).length, gz: 0, sha256: sha256Hex(enc(body)),
      format: k.startsWith("regs-") ? "regs-region/1" : `${k}/1`, section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}

describe("runPrefetch through the loader: one download per file, the later open reads the prefetched one", () => {
  const body: Record<string, string> = { core: "c", "regs-2": "r2", "regs-5": "r5" };
  const setup = async () => {
    const m = manifest(body);
    const fetched: string[] = [], opened: string[] = [], marks: string[] = [];
    const fetchImpl = (async (url: string) => {
      fetched.push(url.replace(/^.*\/r-1\//, "").replace(/-x\.sqlite\.gz$/, ""));
      return new Response(enc(body[url.replace(/^.*\/r-1\//, "").replace(/-x\.sqlite\.gz$/, "")]!));
    }) as typeof fetch;
    const source = releaseSource(m, { base: "https://r2.test/release/", cache: await releaseCache(), held: null,
                                      fetch: fetchImpl });
    const open = async (name: DbName, bytes: () => Promise<Uint8Array>): Promise<Db> => {
      opened.push(name);
      await bytes();
      return { all: async () => [{ k: "format", v: name.startsWith("regs-") ? "regs-region/1" : `${name}/1` },
                                 { k: "section_handles", v: H }], get: async () => undefined };
    };
    const L = releaseLoader(m, source, open);
    return { L, fetched, opened, marks, mark: (n: string) => marks.push(n) };
  };
  const settle = () => new Promise((r) => setTimeout(r, 0));

  it("the region's file is fetched, NOT opened (no sql.js for it); its open later fetches nothing", async () => {
    const { L, fetched, opened, marks, mark } = await setup();
    runPrefetch(L, prefetchPlan({ region: "2", idle: false, saveData: false, effectiveType: null }), mark);
    await settle(); await settle();
    expect([fetched, opened, L.opened(), marks]).toEqual([["regs-2"], [], [], [`${PREFETCH_MARK}:regs-2`]]);
    await L.db("regs-2");                                       // the tap
    expect([fetched, opened]).toEqual([["regs-2"], ["regs-2"]]);
  });

  it("idle: core is opened (with the engine); a second plan for the same files asks for nothing", async () => {
    const { L, fetched, marks, mark } = await setup();
    const plan = prefetchPlan({ region: "2", idle: true, saveData: false, effectiveType: "4g" });
    runPrefetch(L, plan, mark);
    runPrefetch(L, plan, mark);
    await L.db("core"); await settle();
    expect([fetched.sort(), L.opened(), marks]).toEqual([["core", "regs-2"], ["core"],
                                                         [`${PREFETCH_MARK}:regs-2`, `${PREFETCH_MARK}:core`]]);
  });

  it("the view moves to another region: that region's file too, each once", async () => {
    const { L, fetched, mark } = await setup();
    for (const region of ["2", "5", "2"] as const)
      runPrefetch(L, prefetchPlan({ region, idle: false, saveData: false, effectiveType: null }), mark);
    await settle(); await settle();
    expect(fetched).toEqual(["regs-2", "regs-5"]);
  });

  it("a failed prefetch is not kept: the open fetches again", async () => {
    const m = manifest(body);
    let fail = true; const fetched: string[] = [];
    const source = releaseSource(m, { base: "https://r2.test/release/", cache: await releaseCache(), held: null,
      fetch: (async (url: string) => { fetched.push(url); return fail ? new Response("", { status: 503 })
                                                                       : new Response(enc("r2")); }) as typeof fetch });
    const L = releaseLoader(m, source, async (name, bytes) => { await bytes(); return {
      all: async () => [{ k: "format", v: "regs-region/1" }, { k: "section_handles", v: H }], get: async () => undefined }; });
    await expect(L.prefetch("regs-2")).rejects.toThrow("HTTP 503");
    expect(L.prefetched("regs-2")).toBe(false);
    fail = false;
    await L.db("regs-2");
    expect(fetched.length).toBe(2);
  });
});
