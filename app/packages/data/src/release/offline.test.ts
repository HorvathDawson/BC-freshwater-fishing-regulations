/**
 * (T238, owner 2026-10-29: regulations are never per region) THE OFFLINE MAPS FACTS over the real web source and a
 * Cache Storage stand-in: the regulations are one figure for all of B.C. (every data file), the map one province-wide
 * row; the web reads how much of the regulations its release cache keeps, whatever put it there.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { parseManifest, type Manifest } from "../manifest";
import { releaseLoader } from "./loader";
import { offlineSizes, regsKept } from "./offline";
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
  g.caches = { open: async () => ({ match: async (u: string) => store.get(u)?.clone(),
                                    put: async (u: string, r: Response) => { store.set(u, r); } }) };
});
afterEach(() => { g.caches = had; });

async function loader() {
  const m = manifest();
  const fetchImpl = (async (url: string) => new Response(enc(BODY[url.replace(/^.*\/r-1\//, "").replace(/-x\.bin\.gz$/, "")]!))) as typeof fetch;
  return releaseLoader(m, releaseSource(m, { base: "https://r2.test/", cache: await releaseCache(), held: null, fetch: fetchImpl }),
                       async () => { throw new Error("not opened here"); });
}

describe("the regulations are all of B.C.; the map is one province-wide file", () => {
  it("regs = every data file (every region's slices among them); map = tiles + mask, basemap", () => {
    expect(offlineSizes(manifest())).toEqual({ regs: 26, map: { tiles: 1010, basemap: 2000 } });
  });

  it("the web reads what its cache keeps of them, file by file, whatever fetched it", async () => {
    const L = await loader();
    expect(await regsKept(L)).toBe(0);
    for (const f of ["core", "regs-5"] as const) await L.prefetch(f);
    expect(await regsKept(L)).toBe(10);
  });

  it("a loader that cannot say (a phone, a test fake) says null", async () => {
    const L = await loader();
    expect(await regsKept({ ...L, stored: undefined })).toBeNull();
  });
});
