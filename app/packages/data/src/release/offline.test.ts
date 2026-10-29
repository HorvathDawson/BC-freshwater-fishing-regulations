/**
 * (T238) THE OFFLINE MAPS STORE over the real web source and a Cache Storage stand-in: what a region costs, what the
 * cache keeps of it, saving a region (shared files with the first, progress, a failure keeps what was saved and Resume
 * fetches only the rest), removing one (the shared files stay while another region is kept), removing everything.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { releaseLoader } from "./loader";
import { createOfflineStore, offlineSizes, regionFiles, sharedFiles } from "./offline";
import { releaseCache } from "./drivers/cache.web";
import { releaseSource } from "./drivers/source.web";

const enc = (s: string) => new TextEncoder().encode(s);
const H = "d768d6e135e62b99";
const BODY: Record<string, string> = { core: "cccc", water: "wwwwww", "regs-2": "r2r2r2r2", "salmon-2": "s2", "regs-5": "r5r5r5" };
function manifest(): Manifest {
  return parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: [`${H}.*`], current: { id: `${H}.1`, url: "tiles/a.pmtiles", bytes: 1000, sha256: "x" },
             assets: null, mask: { url: "tiles/m.bin", bytes: 10, sha256: "y" } },
    basemap: { id: "b", url: "basemap/b.pmtiles", bytes: 2000, sha256: "z" },
    files: Object.fromEntries(Object.entries(BODY).map(([k, body]) => [k, {
      url: `r-1/${k}-x.bin.gz`, bytes: enc(body).length, gz: 0, sha256: sha256Hex(enc(body)), format: `${k}/1`,
      section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
}

const store = new Map<string, Response>();
const g = globalThis as { caches?: unknown };
let had: unknown;
beforeEach(() => {
  store.clear();
  had = g.caches;
  g.caches = { open: async () => ({
    match: async (u: string) => store.get(u)?.clone(),
    put: async (u: string, r: Response) => { store.set(u, r); },
    delete: async (u: string) => store.delete(u),
  }) };
});
afterEach(() => { g.caches = had; });

const nameOf = (url: string) => url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "");
async function setup(fail: (name: string) => boolean = () => false) {
  const m = manifest();
  const fetched: string[] = [];
  const fetchImpl = (async (url: string) => {
    const n = nameOf(url);
    fetched.push(n);
    return fail(n) ? new Response("", { status: 503 }) : new Response(enc(BODY[n]!));
  }) as typeof fetch;
  const L = releaseLoader(m, releaseSource(m, { base: "https://r2.test/", cache: await releaseCache(), held: null, fetch: fetchImpl }),
                          async () => { throw new Error("not opened here"); });
  return { m, L, fetched, S: createOfflineStore(L) };
}

describe("sizes: a region is its own slices; everything else is the province's", () => {
  it("regions, shared and the province-wide map", () => {
    const m = manifest();
    expect([regionFiles(m, "2"), regionFiles(m, "5"), sharedFiles(m)])
      .toEqual([["regs-2", "salmon-2"], ["regs-5"], ["core", "water"]]);
    expect(offlineSizes(m)).toEqual({ regions: [{ region: "2", bytes: 10 }, { region: "5", bytes: 6 }], shared: 10,
                                      map: { tiles: 1010, basemap: 2000 } });
  });
});

describe("the web store over the release cache", () => {
  it("nothing kept: every region none; saving Region 2 fetches its files and the shared ones, each once, with progress", async () => {
    const { S, fetched } = await setup();
    await S.scan();
    expect(S.snapshot().regions.map((r) => r.state)).toEqual(["none", "none"]);
    const seen: number[] = [];
    S.subscribe(() => { const j = S.snapshot().job; if (j) seen.push(j.bytes); });
    await S.save("2");
    expect(fetched.sort()).toEqual(["core", "regs-2", "salmon-2", "water"]);
    expect(seen.at(-1)).toBe(20);
    const s = S.snapshot();
    expect([s.job, s.regions.map((r) => [r.region, r.state, r.held]), s.shared]).toEqual(
      [null, [["2", "kept", 10], ["5", "none", 0]], { bytes: 10, held: 10 }]);
    // Region 5 now costs only its own file
    await S.save("5");
    expect(fetched.filter((f) => f === "regs-5")).toEqual(["regs-5"]);
    expect(fetched.length).toBe(5);
  });

  it("a failed file stops the save with what was saved kept; Resume fetches only the rest", async () => {
    let down = true;
    const { S, fetched } = await setup((n) => down && n === "water");
    await S.scan();
    await S.save("2");
    const j = S.snapshot().job!;
    expect([j.phase, j.region, j.error]).toEqual(["failed", "2", expect.stringContaining("503")]);
    // its own slices came first and are kept; the province's files are the part that is short
    expect([S.snapshot().regions[0]!.state, S.snapshot().shared]).toEqual(["kept", { bytes: 10, held: 4 }]);
    down = false;
    const before = fetched.length;
    await S.save("2");
    expect(fetched.slice(before)).toEqual(["water"]);
    expect([S.snapshot().job, S.snapshot().regions[0]!.state]).toEqual([null, "kept"]);
  });

  it("remove: the region's own files go; the shared stay while another region is kept, and go with the last", async () => {
    const { S } = await setup();
    await S.save("2"); await S.save("5");
    await S.remove("2");
    expect([S.snapshot().regions.map((r) => r.state), S.snapshot().shared.held]).toEqual([["none", "kept"], 10]);
    expect([...store.keys()].map(nameOf).sort()).toEqual(["core", "regs-5", "water"]);
    await S.remove("5");
    expect([...store.keys()]).toEqual([]);
  });

  it("a removed file is fetched again on the next save (the loader forgot it was prefetched)", async () => {
    const { S, fetched } = await setup();
    await S.save("5"); await S.remove("5");
    await S.save("5");
    expect(fetched.filter((f) => f === "regs-5").length).toBe(2);
  });

  it("save(\"shared\"): the province's files alone", async () => {
    const { S, fetched } = await setup();
    await S.save("shared");
    expect([fetched.sort(), S.snapshot().shared.held, S.snapshot().regions.map((r) => r.state)])
      .toEqual([["core", "water"], 10, ["none", "none"]]);
  });

  it("clear drops everything kept; a later scan agrees", async () => {
    const { S, L } = await setup();
    await S.save("2");
    await S.clear();
    expect(store.size).toBe(0);
    const again = createOfflineStore(L);
    await again.scan();
    expect(again.snapshot().regions.map((r) => r.state)).toEqual(["none", "none"]);
  });

  it("a scan sees what the background prefetch (or a tap) put in the cache", async () => {
    const { S, L } = await setup();
    for (const f of ["core", "water", "regs-5"] as const) await L.prefetch(f);
    await S.scan();
    expect(S.snapshot().regions.map((r) => r.state)).toEqual(["none", "kept"]);
  });

  it("a loader that cannot say what it keeps (a phone, a fake) is not storable and offers nothing", async () => {
    const { L } = await setup();
    const S = createOfflineStore({ ...L, stored: undefined });
    await S.scan();
    await S.save("2");
    expect([S.snapshot().storable, S.snapshot().job]).toEqual([false, null]);
  });
});
