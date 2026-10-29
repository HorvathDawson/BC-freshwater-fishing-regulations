/**
 * (T231) THE UPDATE SERVICE OVER THE REAL BOOT: the service of `@app/data` driving `bootRelease`'s `stage` over the
 * fixture releases, as App.tsx wires it.
 *
 *   phone   a launch with `deferUpdate` installs nothing; the service sees the newer manifest (seeded by the boot's own
 *           read: no second request), stages it — only the changed files fetched, verified, the set in use untouched
 *           and still answering — and the next boot (the apply) opens it and prunes the old set. A big set on a metered
 *           connection waits for Wi-Fi. A download whose bytes are not the manifest's is refused: nothing swapped.
 *   web     the files this session opened that changed are warmed in the release cache; the boot that applies them
 *           reads them from there without the network.
 */
import { describe, expect, it } from "vitest";
import { createUpdateService, fetchVerified, type ItemId, type Manifest } from "@app/data";
import { bootRelease, heldPath, POINTER, type ReleaseFs, type ReleasePlatform } from "@app/data/release";
import { releaseCache } from "../packages/data/src/release/drivers/cache.web";
import { releaseSource } from "../packages/data/src/release/drivers/source.web";
import { fixtureRelease, nodeOpener, releaseFetch } from "../conformance/src/fixtureRelease";

const BASE = "https://r2.test/release/";
const M = `${BASE}manifest.json`;

function memFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(p + "/")),
    read: async (p) => { const b = files.get(p); if (!b) throw new Error(`no ${p}`); return b; },
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(p + "/")) files.delete(k); },
  };
}

function phone(fs: ReturnType<typeof memFs>): ReleasePlatform {
  return {
    releaseOpener: () => nodeOpener(),
    releaseCache: async () => ({ kind: "none", get: async () => null, put: async () => {} }),
    releaseSource: (m, at) => ({ bytes: async (name) => fs.read(heldPath(at.held!, m.files[name]!.url)) }),
    device: (f) => ({
      fs, download: (url, e) => fetchVerified(url, e, undefined, f),
      toDisk: async (url, path, e, onBytes) => { const b = await fetchVerified(url, e, undefined, f); await fs.write(path, b); onBytes?.(b.length); },
      dirUri: (p) => `file:///doc/${p}`, fileUri: (p) => `file:///doc/${p}`,
    }),
  };
}
const offline = (async () => { throw new TypeError("Network request failed"); }) as unknown as typeof fetch;
const manifestOf = async (dir: string) => JSON.parse(await (await releaseFetch(BASE, dir, [])(M)).text()) as Manifest;

/** The release spec App.tsx builds over a booted set. */
const releaseSpec = (b: () => Awaited<ReturnType<typeof bootRelease>>) => ({
  manifestUrl: M, held: () => b().held,
  stage: async (m: Manifest, on: (bytes: number, of: number) => void) => { await b().stage(m, (p) => on(p.bytes, p.of)); },
});
const settle = async (s: ReturnType<typeof createUpdateService>) => {
  for (let i = 0; i < 50 && ["downloading"].includes(s.snapshot().release!.phase); i++) await new Promise((r) => setTimeout(r, 5));
};

describe("phone: staged by the service, applied by the next boot", () => {
  it("deferUpdate: the launch installs nothing; the service stages only the changed files; the apply opens them", async () => {
    const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
    const fs = memFs();
    const boot = (f: typeof fetch) => bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: phone(fs),
                                                    fetch: f, manifestTimeoutMs: 1000, deferUpdate: true });
    const first = await boot(releaseFetch(BASE, r1.dir, []));
    const log: string[] = [];
    const f2 = releaseFetch(BASE, r2.dir, log);
    const b = await boot(f2);
    expect(await b.update).toBeNull();                                      // nothing installed by the launch
    expect(b.latest!.data_release).not.toBe(b.held.release);
    const m2 = await manifestOf(r2.dir);

    const s = createUpdateService({ feeds: [], fetch: f2, release: releaseSpec(() => b) });
    s.seedRelease(b.latest!);
    log.length = 0;
    s.start();
    await settle(s);
    expect(s.snapshot().release).toMatchObject({ phase: "ready", target: m2.data_release });
    // only the changed files were fetched: no manifest again (the seed), nothing unchanged
    const changed = Object.keys(m2.files).filter((k) => m2.files[k]!.sha256 !== first.manifest.files[k]!.sha256).sort();
    expect(log.sort()).toEqual(changed.map((k) => `${BASE}${m2.files[k]!.url}`).sort());
    // the set in use still answers while the next one waits
    expect(b.held.release).toBe(first.held.release);
    expect(await b.source.water("gnis:8634" as ItemId)).not.toBeNull();
    s.stop();

    // THE APPLY: the next boot opens the staged set and prunes the old one
    const applied = await boot(offline);
    expect(applied.held.release).toBe(m2.data_release);
    expect([...fs.files.keys()].some((k) => k.startsWith(`release/${first.held.release}/`))).toBe(false);
    s.applied(applied.manifest.data_release);
    expect(s.snapshot().release).toMatchObject({ phase: "current", target: null });
  });

  it("a large set on a metered connection waits for Wi-Fi; nothing is fetched until then", async () => {
    const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
    const fs = memFs();
    await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: phone(fs),
                        fetch: releaseFetch(BASE, r1.dir, []), deferUpdate: true });
    const log: string[] = [];
    const f2 = releaseFetch(BASE, r2.dir, log);
    const b = await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f2,
                                  deferUpdate: true });
    let metered = true;
    const subs: ((on: boolean) => void)[] = [];
    const s = createUpdateService({ feeds: [], fetch: f2, release: releaseSpec(() => b), policy: { bigBytes: 10 },
      network: { online: () => true, metered: () => metered, subscribe: (on) => { subs.push(on); return () => {}; } } });
    s.seedRelease(b.latest!);
    log.length = 0;
    s.start();
    await settle(s);
    expect(s.snapshot().release!.phase).toBe("waiting-wifi");
    expect(log).toEqual([]);
    metered = false;
    subs.forEach((on) => on(true));
    await new Promise((r) => setTimeout(r, 0));
    await settle(s);
    expect(s.snapshot().release!.phase).toBe("ready");
    s.stop();
  });

  it("SHA MISMATCH: a download that is not the manifest's bytes is refused; the pointer never moves", async () => {
    const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
    const fs = memFs();
    await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: phone(fs),
                        fetch: releaseFetch(BASE, r1.dir, []), deferUpdate: true });
    const pointer = fs.files.get(POINTER);
    const good = releaseFetch(BASE, r2.dir, []);
    // the origin serves core's bytes corrupted
    const bad = (async (u: RequestInfo | URL, i?: RequestInit) => {
      const r = await good(u, i);
      return String(u).includes("core") ? new Response(new Uint8Array([1, 2, 3]), { status: 200 }) : r;
    }) as typeof fetch;
    const b = await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: bad,
                                  deferUpdate: true });
    const s = createUpdateService({ feeds: [], fetch: bad, release: releaseSpec(() => b) });
    s.seedRelease(b.latest!);
    s.start();
    await settle(s);
    expect(s.snapshot().release).toMatchObject({ phase: "current", target: null });
    expect(s.snapshot().release!.error).toMatch(/bytes|sha256/);
    expect(fs.files.get(POINTER)).toEqual(pointer);
    s.stop();
  });
});

describe("web: warmed in the release cache, read from there by the apply", () => {
  // ONE cache across boots, as the browser's Cache Storage is (node has none: `releaseCache` falls back to memory per call)
  const shared = releaseCache();
  const web: ReleasePlatform = {
    releaseOpener: () => nodeOpener(),
    releaseCache: () => shared,
    releaseSource: (m, at) => releaseSource(m, at),
    device: () => null,
  };

  it("stages the changed files this session opened; the applying boot fetches them from the cache", async () => {
    const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
    let dir = r1.dir;
    const log: string[] = [];
    const f = ((u: RequestInfo | URL, i?: RequestInit) => releaseFetch(BASE, dir, log)(u as string, i)) as typeof fetch;
    const b = await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: web, fetch: f });
    await b.source.itemForSection(0 as never);                            // opens core
    const m1 = b.manifest;
    dir = r2.dir;
    const m2 = await manifestOf(r2.dir);
    const s = createUpdateService({ feeds: [], fetch: f, release: releaseSpec(() => b) });
    s.seedRelease(m1);
    log.length = 0;
    await s.check({ manual: true });
    await settle(s);
    expect(s.snapshot().release).toMatchObject({ phase: "ready", target: m2.data_release });
    // core changed and was opened: warmed. conditions changed but was never opened: left for first need
    expect(log.filter((u) => u !== M)).toEqual([`${BASE}${m2.files.core!.url}`]);
    log.length = 0;
    const applied = await bootRelease({ manifestUrl: M, releaseBase: BASE, wasmBase: "", platform: web, fetch: f });
    expect(applied.manifest.data_release).toBe(m2.data_release);
    await applied.source.itemForSection(0 as never);
    expect(log.filter((u) => u.includes(m2.files.core!.url))).toEqual([]);  // from the cache
  });
});
