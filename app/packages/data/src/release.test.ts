/** P10: fetchVerified, the manifest's diff and pairing, and the hash they all rest on. */
import { describe, expect, it } from "vitest";
import { gzipSync } from "fflate";
import { Sha256, sha256Hex } from "./sha256";
import { fetchVerified, VerifyError } from "./fetchVerified";
import { diff, pair, parseManifest, type Held, type Manifest } from "./manifest";

const enc = (s: string) => new TextEncoder().encode(s);

describe("sha256", () => {
  it("matches the FIPS 180-4 vectors", () => {
    expect(sha256Hex(enc(""))).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    expect(sha256Hex(enc("abc"))).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    expect(sha256Hex(enc("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")))
      .toBe("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    expect(sha256Hex(new Uint8Array(1_000_000).fill(0x61)))
      .toBe("cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
  });
  it("hashes in chunks exactly as in one piece (P11: a 531 MB tile file is verified off disk, never held whole)", () => {
    const data = new Uint8Array(1_000_000).fill(0x61);
    for (const step of [1, 63, 64, 65, 4096, 333_333]) {
      const h = new Sha256();
      for (let i = 0; i < data.length; i += step) h.update(data.subarray(i, i + step));
      expect([step, h.hex()]).toEqual([step, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"]);
    }
    expect(new Sha256().hex()).toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  });
});

describe("fetchVerified", () => {
  const raw = enc("SQLite format 3\u0000 a release file");
  const want = { bytes: raw.length, sha256: sha256Hex(raw) };
  const server = (body: Uint8Array, status = 200) =>
    (async () => ({ ok: status < 400, status, arrayBuffer: async () => body.slice().buffer })) as unknown as typeof fetch;

  it("accepts a gzip body on its DECODED bytes and sha", async () => {
    expect(await fetchVerified("x", want, undefined, server(gzipSync(raw)))).toEqual(raw);
    expect(await fetchVerified("x", want, undefined, server(raw))).toEqual(raw);     // decoded by the transport
  });
  it("rejects a wrong sha, a wrong length and an HTTP error", async () => {
    const other = enc("SQLite format 3\u0000 a release fil!");
    await expect(fetchVerified("x", want, undefined, server(other))).rejects.toThrow(/sha256/);
    await expect(fetchVerified("x", want, undefined, server(raw.subarray(1)))).rejects.toThrow(/bytes/);
    await expect(fetchVerified("x", want, undefined, server(raw, 404))).rejects.toBeInstanceOf(VerifyError);
  });
});

const M = (over: Partial<Manifest> = {}): Manifest => parseManifest({
  format: "app-manifest/2", data_release: "r-2",
  tiles: { compatible: ["d768d6e135e62b99.*"], current: { id: "d768d6e135e62b99.aaaaaaaaaaaa", url: "t" },
           assets: { id: "map-assets-1", url: "a" }, mask: null },
  basemap: { id: "protomaps-2025-09-25", url: "b" },
  files: { core: { url: "c", bytes: 1, gz: 1, sha256: "c2", format: "core/1", section_handles: "d768d6e135e62b99" },
           paint: { url: "p", bytes: 1, gz: 1, sha256: "p1", format: "paint/2", section_handles: "d768d6e135e62b99" } },
  regulations: { valid_until: null, versions: null }, ...over,
});
const HELD: Held = { tiles: "d768d6e135e62b99.aaaaaaaaaaaa", basemap: "protomaps-2025-09-25", assets: "map-assets-1",
                     release: "r-1", files: { core: "c1", paint: "p1" } };

describe("manifest", () => {
  it("refuses a format it does not know", () => {
    expect(() => parseManifest({ format: "app-manifest/1" })).toThrow(/format/);
  });
  it("a data-only release lists only the changed files and no tiles", () => {
    expect(diff(HELD, M())).toEqual({ files: ["core"], tiles: false, basemap: false, assets: false });
  });
  it("a geometry release lists the tiles with the data; the basemap only when its id moves", () => {
    const geo = M({ tiles: { compatible: ["ffffffffffffffff.*"], current: { id: "ffffffffffffffff.bbbbbbbbbbbb", url: "t" },
                             assets: { id: "map-assets-1", url: "a" }, mask: null } });
    expect(diff(HELD, geo)).toEqual({ files: ["core"], tiles: true, basemap: false, assets: false });
    expect(diff(HELD, M({ basemap: { id: "protomaps-2026-01-01", url: "b" } })).basemap).toBe(true);
  });
  it("same tiles + new data pairs; incompatible tiles keep the last good pair", () => {
    expect(pair(M(), HELD)).toEqual({ ok: true, tiles: HELD.tiles, release: "r-2" });
    const away = M({ tiles: { compatible: ["ffffffffffffffff.*"], current: null, assets: null, mask: null } });
    expect(pair(away, HELD)).toEqual({ ok: false, reason: "tiles-incompatible",
                                       keep: { tiles: HELD.tiles, release: "r-1" } });
    expect(pair(null, HELD)).toEqual({ ok: false, reason: "offline", keep: { tiles: HELD.tiles, release: "r-1" } });
  });
});
