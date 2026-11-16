// @vitest-environment node
// (T446) The phone's install root over expo-file-system as SDK 58 has it: `write` and `move` are PROMISES whose work runs
// natively after the call returns. Unawaited, `place` (write `.part`, rename) moved the empty file `create` made onto the
// release path, and a first install's re-verify read 0 bytes ("does not match the manifest (0 % of its size)").
import { describe, expect, it, vi } from "vitest";

const disk = new Map<string, Uint8Array>();

vi.mock("expo-file-system", () => {
  const later = (f: () => void) => new Promise<void>((r) => setTimeout(() => { f(); r(); }, 0));
  const join = (parts: unknown[]) => parts.map((p) => (typeof p === "string" ? p : (p as { uri: string }).uri)).join("/");
  class Directory {
    uri: string; constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return [...disk.keys()].some((k) => k.startsWith(`${this.uri}/`)); }
    create() {}
    delete() { for (const k of [...disk.keys()]) if (k.startsWith(`${this.uri}/`)) disk.delete(k); }
  }
  class File {
    uri: string; constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return disk.has(this.uri); }
    get size() { return disk.get(this.uri)?.length ?? 0; }
    create() { if (disk.has(this.uri)) throw new Error("exists"); disk.set(this.uri, new Uint8Array()); }
    delete() { disk.delete(this.uri); }
    write(b: Uint8Array) { return later(() => { disk.set(this.uri, new Uint8Array(b)); }); }
    move(to: File) {
      return later(() => {
        if (disk.has(to.uri)) throw new Error("DestinationAlreadyExists");
        disk.set(to.uri, disk.get(this.uri)!); disk.delete(this.uri);
      });
    }
    async bytes() { const b = disk.get(this.uri); if (!b) throw new Error(`no ${this.uri}`); return b; }
  }
  return { Directory, File, DownloadTask: class {}, FileMode: {}, Paths: { document: "doc" } };
});

import { sha256Hex } from "../../sha256";
import { device } from "./install.native";

describe("install.native (T446): writes and renames are whole before the next step", () => {
  it("a file written to .part and renamed reads back whole (the install's `place`)", async () => {
    const fs = device(fetch)!.fs;
    const b = new Uint8Array([1, 2, 3, 4, 5]);
    await fs.write("release/r-x/core.sqlite.part", b);
    await fs.rename("release/r-x/core.sqlite.part", "release/r-x/core.sqlite");
    expect([...(await fs.read("release/r-x/core.sqlite"))]).toEqual([...b]);
    expect(await fs.exists("release/r-x/core.sqlite.part")).toBe(false);
  });

  it("a rename replaces what is there (the pointer swap)", async () => {
    const fs = device(fetch)!.fs;
    await fs.write("release/current.part", new Uint8Array([1]));
    await fs.rename("release/current.part", "release/current");
    await fs.write("release/current.part", new Uint8Array([2, 2]));
    await fs.rename("release/current.part", "release/current");
    expect([...(await fs.read("release/current"))]).toEqual([2, 2]);
  });
});

describe("install.native (T760): a data file's cut try is said", () => {
  it("one line per failed try: which, why, and when the next one goes", async () => {
    const raw = new TextEncoder().encode("core file bytes ".repeat(50));     // served as it is: no gzip magic, so not decoded
    let n = 0;
    const f = (async () => {
      if (n++ === 0) throw new TypeError("network dropped");
      return new Response(new Uint8Array(raw) as unknown as BodyInit, { status: 200 });
    }) as unknown as typeof fetch;
    const said: string[] = [];
    const warn = vi.spyOn(console, "warn").mockImplementation((m: string) => { said.push(m); });
    const rand = vi.spyOn(Math, "random").mockReturnValue(0.5);              // the backoff's jitter: 1000 ms exactly
    try {
      const got = await device(f)!.download("https://x.test/r/core.sqlite.gz", { bytes: raw.length, sha256: sha256Hex(raw) });
      expect(got).toEqual(raw);
    } finally { warn.mockRestore(); rand.mockRestore(); }
    expect(said).toEqual(["[download] try 1 failed (https://x.test/r/core.sqlite.gz: network dropped); trying again in 1000 ms"]);
  });
});
