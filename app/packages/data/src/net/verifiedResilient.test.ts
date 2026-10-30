/**
 * (T256 item 4) A phone's first install fetches its data files through `fetchVerifiedResilient`: a dropped request is
 * tried again (no app `Net` needed), the bytes are held to the manifest, and bytes that never verify are refused.
 */
import { gzipSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import { VerifyError } from "../fetchVerified";
import { sha256Hex } from "../sha256";
import { fetchVerifiedResilient } from "./verified";

const raw = new TextEncoder().encode("core file bytes ".repeat(50));
const want = { bytes: raw.length, sha256: sha256Hex(raw) };
const gz = gzipSync(raw);
const ok = (b: Uint8Array) => new Response(new Uint8Array(b) as unknown as BodyInit, { status: 200 });
const fast = { sleep: async () => {}, rand: () => 0.5 };

describe("fetchVerifiedResilient (a first install's data files)", () => {
  it("a dropped request is tried again, and the stored gzip verifies as the manifest's raw bytes", async () => {
    let n = 0;
    const f = (async () => { if (n++ === 0) throw new TypeError("network dropped"); return ok(gz); }) as unknown as typeof fetch;
    expect(await fetchVerifiedResilient("u", want, { fetchImpl: f, ...fast })).toEqual(raw);
    expect(n).toBe(2);
  });
  it("bytes that are not the manifest's are fetched once more whole, then refused", async () => {
    let n = 0;
    const bad = gzipSync(new TextEncoder().encode("another file"));
    const f = (async () => { n++; return ok(bad); }) as unknown as typeof fetch;
    await expect(fetchVerifiedResilient("u", want, { fetchImpl: f, ...fast })).rejects.toBeInstanceOf(VerifyError);
    expect(n).toBe(2);
  });
});
