/**
 * (T741) THE APP'S SHA-256 HELD TO NODE'S OWN (OpenSSL), not to itself. `packages/data/src/sha256.ts` runs its rounds
 * unrolled eight at a time with renamed variables (an interpreter's cost, Hermes has no JIT); every release file and the
 * iPhone's whole first install are checked by it. The package's own tests pin the FIPS vectors; this one holds it to an
 * independent implementation on megabytes of random bytes, at chunk boundaries that are not multiples of 64, and on the
 * install's resume path (T272: a `.part` left by a dropped line, followed from its first byte). Here in tools/ because
 * `packages/data` may not import `node:` modules (layers.json).
 */
import { describe, expect, it } from "vitest";
import { createHash, randomBytes } from "node:crypto";
import { Sha256, sha256Hex, sha256HexAsync } from "../packages/data/src/sha256";
import { partDownload, type PartFs } from "../packages/data/src/release/install";

const node = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("(T741) sha256 against node's crypto", () => {
  const data = new Uint8Array(randomBytes(5 * (1 << 20) + 37));
  const want = node(data);

  it("one piece, odd chunk runs, every length around a block's end", async () => {
    expect(sha256Hex(data)).toBe(want);
    expect(await sha256HexAsync(data)).toBe(want);
    for (const steps of [[1 << 20], [65, 1, 127, 1_000_003], [63, 64, 4097, 333_333, 7]]) {
      const h = new Sha256();
      for (let i = 0, k = 0; i < data.length; k++) { const n = steps[k % steps.length]!; h.update(data.subarray(i, i + n)); i += n; }
      expect([steps, h.hex()]).toEqual([steps, want]);
    }
    for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 128, 1000]) {
      const d = data.subarray(0, n);
      expect([n, sha256Hex(d)]).toEqual([n, node(d)]);
    }
  });

  it("refuses an update after the digest", () => {
    const h = new Sha256().update(data.subarray(0, 3));
    h.hex();
    expect(() => h.update(data.subarray(0, 1))).toThrow(/after hex/);
  });

  /** an in-memory `.part` file system with `readAt`, counting reads from the first byte (the follower's is one) */
  function memParts() {
    const files = new Map<string, Uint8Array>();
    let fromStart = 0;
    const fs: PartFs = {
      size: async (p) => files.get(p)?.length ?? 0,
      append: async (p, b) => {
        const had = files.get(p) ?? new Uint8Array();
        const out = new Uint8Array(had.length + b.length);
        out.set(had); out.set(b, had.length);
        files.set(p, out);
      },
      chunks: async (p, each) => { each(files.get(p)!); },
      readAt: async (p, at, n) => { if (at === 0) fromStart++; return files.get(p)!.slice(at, at + n); },
      rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
      remove: async (p) => { files.delete(p); },
    };
    return { fs, files, wholeReads: () => fromStart - 1 };
  }
  const tick = () => new Promise<void>((r) => setTimeout(r, 2));

  it("a .part resumed from an odd length is followed from its first byte and passes; a wrong sha still fails", async () => {
    const big = data.subarray(0, 3 * (1 << 20) + 4099);
    const ref = { bytes: big.length, sha256: node(big) };
    const m = memParts();
    await m.fs.append("t.part", big.subarray(0, (1 << 20) + 13));              // left by a dropped line
    await partDownload(m.fs, async (_u, part, have, total) => {
      for (let at = have; at < total; at += 777_777) { await m.fs.append(part, big.subarray(at, Math.min(total, at + 777_777))); await tick(); }
    })("u", "t", ref);
    expect(m.wholeReads()).toBe(0);                                             // the follower's hash decided
    expect(m.files.get("t")).toEqual(big);

    const bad = { ...ref, sha256: ref.sha256.replace(/^./, (c) => (c === "0" ? "1" : "0")) };
    const m2 = memParts();
    await m2.fs.append("t.part", big.subarray(0, 4097));
    await expect(partDownload(m2.fs, async (_u, part, have) => { await m2.fs.append(part, big.subarray(have)); })("u", "t", bad))
      .rejects.toThrow(/sha256/);
    expect([...m2.files.keys()]).toEqual([]);                                   // refused and dropped
  });
});
