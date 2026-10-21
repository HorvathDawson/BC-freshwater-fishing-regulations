/** P11: the map's status comes from paint, a failure is never cached, and colours wait for a PROVEN pair. */
import { describe, expect, it } from "vitest";
import { parseManifest, type Held, type Manifest } from "@app/data";
import type { StatusIndex } from "@app/core";
import { loadStatusIndex } from "./hooks";
import { pairingView, releaseStatus, statusOf, type StatusSource } from "./release";

const H = "d768d6e135e62b99";
const index = { handles: H } as unknown as StatusIndex;

describe("statusOf: one decode per key, and a failure is not kept", () => {
  it("retries after a failure, then holds the answer", async () => {
    let calls = 0;
    const src: StatusSource = { key: "k1", load: async () => { if (++calls === 1) throw new Error("offline"); return index; } };
    await expect(statusOf(src)).rejects.toThrow("offline");
    expect(await statusOf(src)).toBe(index);
    expect(await statusOf(src)).toBe(index);
    expect(calls).toBe(2);
  });
  it("the release's status is paint's status half, keyed by the release", async () => {
    let paints = 0;
    const loader = { release: "r-1", paint: async () => { paints++; return { status: index } as never; } };
    const s = releaseStatus(loader);
    expect(s.key).toBe("paint:r-1");
    expect(await statusOf(s)).toBe(index);
    expect(paints).toBe(1);
  });
});

describe("the dev path's status_index.bin (behind the flag) no longer caches a 404 forever", () => {
  it("fetches again after a failure", async () => {
    let calls = 0;
    const fetcher = (async () => { calls++; return new Response("no", { status: 404 }); }) as unknown as typeof fetch;
    expect(await loadStatusIndex("p11-u1", H, fetcher)).toBeNull();
    expect(await loadStatusIndex("p11-u1", H, fetcher)).toBeNull();
    expect(calls).toBe(2);
  });
});

const M = (tiles: string | null): Manifest => parseManifest({
  format: "app-manifest/2", data_release: "r-2",
  tiles: { compatible: [`${H}.*`], assets: null, mask: null,
           current: tiles ? { id: tiles, url: `tiles/${tiles}/atlas-${tiles}.pmtiles`, bytes: 1, sha256: "x" } : null },
  basemap: null, files: {}, regulations: { valid_until: null, versions: null },
});
const held = (tiles: string | null): Held => ({ tiles, basemap: null, release: "r-1", files: {} });

describe("pairingView: colour only on a proven pair", () => {
  it("the manifest's tiles in its own sid space: paired", () => {
    expect(pairingView({ manifest: M(`${H}.aaa`), held: held(null) }))
      .toEqual({ ok: true, data: "r-2", tiles: `${H}.aaa` });
  });
  it("tiles held from another atlas: refused, and both named", () => {
    expect(pairingView({ manifest: M(null), held: held("0123456789abcdef.bbb") }))
      .toEqual({ ok: false, data: "r-2", tiles: "0123456789abcdef.bbb" });
  });
  it("no tile version known anywhere (a dev server before the tile rebuild): not established — no colour, no alarm",
    () => {
      expect(pairingView({ manifest: M(null), held: held(null) }))
        .toEqual({ ok: null, data: "r-2", tiles: null });
    });
});
