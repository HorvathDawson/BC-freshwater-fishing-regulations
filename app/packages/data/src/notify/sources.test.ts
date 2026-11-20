/**
 * (review 2026-11-20) What the background check reads (`createCheckSources`): a NEW feed gate for each run (T676 item 2:
 * a gate made once at module level would carry one run's refusal into every later run), and each file by conditional
 * GET with ITS OWN kept copy (T741: dropping one costs a whole read every run, and nobody noticed).
 */
import { describe, expect, it, vi } from "vitest";
import { createFeedGate, GAUGE_FEED_FORMAT, resolveAppVersion } from "@app/core";
import { createCheckSources } from "./sources";

const BASE = "http://x/feeds/live";
const OBS = { format: GAUGE_FEED_FORMAT, stations: { "08MH001": { t: "2026-11-14T12:00:00Z", d: 15.7 } } };
const MANIFEST = {
  format: "app-manifest/2", data_release: "r-2",
  tiles: { compatible: ["d768d6e135e62b99.*"], current: { id: "d768d6e135e62b99.aaaaaaaaaaaa", url: "t" },
           assets: { id: "map-assets-1", url: "a" }, mask: null },
  basemap: { id: "protomaps-2025-09-25", url: "b" },
  files: { core: { url: "c", bytes: 1, gz: 1, sha256: "c2", format: "core/1", section_handles: "d768d6e135e62b99" },
           paint: { url: "p", bytes: 1, gz: 1, sha256: "p1", format: "paint/2", section_handles: "d768d6e135e62b99" } },
  regulations: { valid_until: null, versions: null },
};
const VERSION = { version: "0.9.0" };

function memHeld() {
  const h = { text: null as string | null, read: () => h.text, write: (t: string) => { h.text = t; } };
  return h;
}
/** an origin answering each file with its ETag, 304 to the strong validator; records what each url was asked with */
function origin() {
  const asked: [string, string | null][] = [];
  const body = (u: string) => (u.endsWith("/obs.json") ? OBS : u.endsWith("manifest.json") ? MANIFEST : VERSION);
  const f = (async (url: string, init?: { headers?: Record<string, string> }) => {
    const inm = init?.headers?.["If-None-Match"] ?? null;
    asked.push([url, inm]);
    const etag = `"${url.split("/").pop()}"`;
    if (inm === etag) return { ok: false, status: 304, headers: new Headers(), json: async () => { throw new Error("no body"); } };
    return { ok: true, status: 200, headers: new Headers({ ETag: etag }), json: async () => body(url) };
  }) as unknown as typeof fetch;
  return { f, asked };
}

describe("the background check's sources (createCheckSources)", () => {
  it("makes a NEW feed gate for every run of obs (never one for the module)", async () => {
    const srv = origin();
    const gates: ReturnType<typeof createFeedGate>[] = [];
    const gate = vi.fn(() => { const g = createFeedGate(null); gates.push(g); return g; });
    const s = createCheckSources({ fetch: srv.f, alertFeed: BASE, manifestUrl: "http://x/release/manifest.json", versionFile: null,
                                   appVersion: null, held: { obs: memHeld(), manifest: memHeld(), version: memHeld() }, gate });
    await s.obs();
    await s.obs();
    await s.obs();
    expect(gate).toHaveBeenCalledTimes(3);
    expect(new Set(gates).size).toBe(3);
    expect(s.latestApp).toBe(null);   // no version file: no new-version read
  });

  it("each file keeps its own copy and asks with its validator on the next run (obs, manifest, version)", async () => {
    const srv = origin();
    const held = { obs: memHeld(), manifest: memHeld(), version: memHeld() };
    const s = createCheckSources({ fetch: srv.f, alertFeed: BASE, manifestUrl: "http://x/release/manifest.json",
                                   versionFile: "http://x/app/latest.json", appVersion: null, held });
    expect(await s.obs()).toEqual(OBS);
    expect((await s.manifest()).data_release).toBe("r-2");
    expect(await s.latestApp!()).toEqual(VERSION);
    expect(JSON.parse(held.obs.text!)).toMatchObject({ url: `${BASE}/obs.json`, etag: '"obs.json"' });
    expect(JSON.parse(held.manifest.text!)).toMatchObject({ url: "http://x/release/manifest.json", etag: '"manifest.json"' });
    expect(JSON.parse(held.version.text!)).toMatchObject({ url: "http://x/app/latest.json", etag: '"latest.json"' });
    // the next run: every file asked with its validator, answered 304, the kept copy returned
    expect(await s.obs()).toEqual(OBS);
    expect((await s.manifest()).data_release).toBe("r-2");
    expect(await s.latestApp!()).toEqual(VERSION);
    expect(srv.asked.slice(3)).toEqual([[`${BASE}/obs.json`, '"obs.json"'], ["http://x/release/manifest.json", '"manifest.json"'],
                                        ["http://x/app/latest.json", '"latest.json"']]);
  });

  it("the default gate holds obs to this app's version, run by run", async () => {
    const newer = { ...OBS, minAppVersion: "99.0.0" };
    const f = (async () => ({ ok: true, status: 200, headers: new Headers(), json: async () => newer })) as unknown as typeof fetch;
    vi.spyOn(console, "info").mockImplementation(() => {});
    const s = createCheckSources({ fetch: f, alertFeed: BASE, manifestUrl: "http://x/m", versionFile: null,
                                   appVersion: resolveAppVersion({ version: "0.9.0" }), held: { obs: memHeld(), manifest: memHeld(), version: memHeld() } });
    await expect(s.obs()).rejects.toThrow(/99\.0\.0/);
  });
});
