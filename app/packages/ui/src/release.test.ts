/** P11: the map's status comes from paint, a failure is never cached, and colours wait for a PROVEN pair. */
import { describe, expect, it } from "vitest";
import { parseManifest, type Held, type Manifest } from "@app/data";
import type { StatusIndex } from "@app/core";
import { pairingOf, pairingView, releaseMapInput, releaseStatus, statusOf, type StatusSource } from "./release";

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
      .toEqual({ ok: true, data: "r-2", tiles: `${H}.aaa`, unproven: false });
  });
  it("the drawn tiles from another atlas: refused, and both named", () => {
    expect(pairingView({ manifest: M(null), held: held(null) }, "0123456789abcdef.bbb"))
      .toEqual({ ok: false, data: "r-2", tiles: "0123456789abcdef.bbb", unproven: false });
  });
  it("no tile version known anywhere (a dev server before the tile rebuild): not established — no colour, no alarm",
    () => {
      expect(pairingView({ manifest: M(null), held: held(null) }))
        .toEqual({ ok: null, data: "r-2", tiles: null, unproven: true });
    });
});

/** A server holding `meta` as the drawn atlas's sidecar (null: none), recording every url asked. */
function sidecar(meta: object | null) {
  const asked: string[] = [];
  const fetcher = (async (u: string | URL) => {
    asked.push(String(u));
    return meta ? new Response(JSON.stringify(meta), { status: 200 }) : new Response("no", { status: 404 });
  }) as unknown as typeof fetch;
  return { asked, fetcher };
}
const DRAWN = "http://dev.test/atlas.pmtiles";
const STAMP = `${H}.0123456789ab`;

describe("the release path with no tile set named proves the pair against the tiles it DRAWS (their tile_version)", () => {
  const input = (tiles: string | null = null) =>
    ({ kind: "release" as const, manifest: M(tiles), held: held(null), atlasUrl: DRAWN });
  it("drawn tiles stamped in the manifest's sid space: paired — status and flow may colour", async () => {
    const s = sidecar({ section_handles: H, build: "full", registry: "8c77ba1cd2dc3929", tile_version: STAMP });
    expect(await pairingOf(input(), s.fetcher)).toEqual({ ok: true, data: "r-2", tiles: STAMP, unproven: false });
    expect(s.asked).toEqual(["http://dev.test/atlas.meta.json"]);
  });
  it("drawn tiles that are not stamped (the sidecar as it is today: section_handles only): not proven", async () => {
    const s = sidecar({ section_handles: H, build: "full", registry: "8c77ba1cd2dc3929" });
    expect(await pairingOf(input(), s.fetcher)).toEqual({ ok: null, data: "r-2", tiles: null, unproven: true });
  });
  it("drawn tiles stamped in another sid space: refused, and both named", async () => {
    const s = sidecar({ section_handles: "0123456789abcdef", tile_version: "0123456789abcdef.0123456789ab" });
    expect(await pairingOf(input(), s.fetcher))
      .toEqual({ ok: false, data: "r-2", tiles: "0123456789abcdef.0123456789ab", unproven: false });
  });
  it("no sidecar beside the drawn tiles: not proven", async () => {
    expect(await pairingOf(input(), sidecar(null).fetcher)).toEqual({ ok: null, data: "r-2", tiles: null, unproven: true });
  });
  it("a manifest that names its tiles is the version drawn: the sidecar is never read", async () => {
    const s = sidecar({ tile_version: "0123456789abcdef.0123456789ab" });
    expect(await pairingOf(input(`${H}.aaa`), s.fetcher)).toEqual({ ok: true, data: "r-2", tiles: `${H}.aaa`, unproven: false });
    expect(s.asked).toEqual([]);
  });
});

describe("an unproven pair is said, a pair not yet checked is not", () => {
  it("nothing to check (no input, or still loading) is not 'unproven': no line is drawn for it", async () => {
    expect(await pairingOf(null)).toEqual({ ok: null, data: null, tiles: null, unproven: false });
  });
});

describe("releaseMapInput: what App hands the Shell on the release path (the tiles it draws, and the pair they prove)", () => {
  const DEV = { atlas: "http://dev.test/atlas.pmtiles", basemap: "http://dev.test/basemap.pmtiles",
                outside: "http://dev.test/bc_outside.geojson" };
  const locate = (path: string) => `https://r2.test/release/${path}`;
  it("a manifest that names no tiles draws the dev atlas, and the pairing is proven against THAT atlas's stamp",
    async () => {
      const b = { manifest: M(null), held: held(null), locate };
      const got = releaseMapInput(b, DEV);
      expect(got.tiles).toEqual(DEV);
      expect(got.pairing).toEqual({ kind: "release", manifest: b.manifest, held: b.held, atlasUrl: DEV.atlas });
      const s = sidecar({ section_handles: H, tile_version: STAMP });
      expect(await pairingOf(got.pairing, s.fetcher)).toEqual({ ok: true, data: "r-2", tiles: STAMP, unproven: false });
      expect(s.asked).toEqual(["http://dev.test/atlas.meta.json"]);
    });
  it("a manifest that names its tiles draws them from the release, and pairs them without a sidecar", async () => {
    const b = { manifest: M(`${H}.aaa`), held: held(null), locate };
    const got = releaseMapInput(b, DEV);
    expect(got.tiles.atlas).toBe(`https://r2.test/release/tiles/${H}.aaa/atlas-${H}.aaa.pmtiles`);
    expect(got.pairing).toEqual({ kind: "release", manifest: b.manifest, held: b.held, atlasUrl: got.tiles.atlas });
    const s = sidecar(null);
    expect(await pairingOf(got.pairing, s.fetcher)).toEqual({ ok: true, data: "r-2", tiles: `${H}.aaa`, unproven: false });
    expect(s.asked).toEqual([]);
  });
});
