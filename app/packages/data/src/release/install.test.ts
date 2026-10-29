/**
 * P11 (MINIMAL-PACKAGING §5): a file too big to hold — the tiles, 531 MB — downloads by HTTP Range into a `.part` file
 * that survives a crash, resumes from its own length, and is verified off disk before it is renamed into place.
 */
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { partDownload, rangedDownload, type PartFs } from "./install";

function memParts(): PartFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    size: async (p) => files.get(p)?.length ?? 0,
    append: async (p, b) => {
      const had = files.get(p) ?? new Uint8Array();
      const out = new Uint8Array(had.length + b.length);
      out.set(had); out.set(b, had.length);
      files.set(p, out);
    },
    chunks: async (p, each) => { const b = files.get(p)!; for (let i = 0; i < b.length; i += 3) each(b.subarray(i, i + 3)); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { files.delete(p); },
  };
}

/** A server that honours Range, logs what it was asked, and can fail the nth request. */
function rangeServer(body: Uint8Array, log: string[], failOn = -1, ignoreRange = false) {
  let n = 0;
  return (async (_url: string, init?: { headers?: Record<string, string> }) => {
    const range = init?.headers?.Range ?? "";
    log.push(range);
    if (n++ === failOn) throw new Error("connection dropped");
    const m = /^bytes=(\d+)-(\d+)$/.exec(range);
    if (!m || ignoreRange) return { status: 200, ok: true, arrayBuffer: async () => body.slice().buffer };
    const part = body.slice(Number(m[1]), Number(m[2]) + 1);
    return { status: 206, ok: true, arrayBuffer: async () => part.buffer };
  }) as unknown as typeof fetch;
}

describe("rangedDownload (native tiles: resumable, verified off disk)", () => {
  const body = new TextEncoder().encode("0123456789abcdefghij");      // 20 bytes, fetched 8 at a time
  const want = { bytes: body.length, sha256: sha256Hex(body) };

  it("fetches in ranges into .part, verifies, then renames into place", async () => {
    const fs = memParts(), log: string[] = [], seen: number[] = [];
    await rangedDownload(fs, rangeServer(body, log), 8)("u", "tiles/a.pmtiles", want, (n) => seen.push(n));
    expect(log).toEqual(["bytes=0-7", "bytes=8-15", "bytes=16-19"]);
    expect(seen).toEqual([8, 16, 20]);
    expect([...fs.files.keys()]).toEqual(["tiles/a.pmtiles"]);
    expect(fs.files.get("tiles/a.pmtiles")).toEqual(body);
  });

  it("a dropped connection leaves the .part, and the next attempt resumes from its length", async () => {
    const fs = memParts(), log: string[] = [];
    await expect(rangedDownload(fs, rangeServer(body, log, 1), 8)("u", "t", want)).rejects.toThrow("connection dropped");
    expect(fs.files.get("t.part")!.length).toBe(8);
    log.length = 0;
    await rangedDownload(fs, rangeServer(body, log), 8)("u", "t", want);
    expect(log).toEqual(["bytes=8-15", "bytes=16-19"]);
    expect(fs.files.get("t")).toEqual(body);
  });

  it("refuses bytes that are not the manifest's and drops the .part; refuses a server that ignores Range", async () => {
    const fs = memParts();
    const wrong = new TextEncoder().encode("0123456789abcdefghiX");
    await expect(rangedDownload(fs, rangeServer(wrong, []), 8)("u", "t", want)).rejects.toThrow(/sha256/);
    expect([...fs.files.keys()]).toEqual([]);
    await expect(rangedDownload(fs, rangeServer(body, [], -1, true), 8)("u", "t", want)).rejects.toThrow(/206/);
    expect([...fs.files.keys()]).toEqual([]);
  });
});

describe("partDownload (T262: any transport, e.g. Android's native one, held to the manifest off disk)", () => {
  const body = new TextEncoder().encode("0123456789abcdefghij");
  const want = { bytes: body.length, sha256: sha256Hex(body) };

  it("hands the transport what the .part already holds, and refuses a .part left short", async () => {
    const fs = memParts(), asked: number[] = [];
    await fs.append("t.part", body.subarray(0, 5));
    const short = partDownload(fs, async (_u, part, have) => { asked.push(have); await fs.append(part, body.subarray(have, 12)); });
    await expect(short("u", "t", want)).rejects.toThrow(/12 bytes on disk/);
    expect(fs.files.get("t.part")!.length).toBe(12);                       // kept: the next attempt resumes it
    const whole = partDownload(fs, async (_u, part, have) => { asked.push(have); await fs.append(part, body.subarray(have)); });
    await whole("u", "t", want);
    expect(asked).toEqual([5, 12]);
    expect(fs.files.get("t")).toEqual(body);
  });

  it("a file already in place with the manifest's bytes is kept (a stage that failed after it): nothing fetched", async () => {
    const fs = memParts(), asked: number[] = [];
    await fs.append("t", body);
    await partDownload(fs, async (_u, _p, have) => { asked.push(have); })("u", "t", want);
    expect(asked).toEqual([]);
    expect(fs.files.get("t")).toEqual(body);
  });
});
