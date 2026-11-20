// @vitest-environment node
// (T831) The phone's atlas reader reads the asked range of the installed file and no more: from `offset`, `length` bytes,
// fewer at the file's end, none past it; the file is opened read-only by its `file://` uri and closed every time.
// expo-file-system's handle (the API `install.native.ts` `readAt` uses) is faked over bytes held in memory; the device
// run is the test bed's `route-scroll` T831 step.
import { describe, expect, it, vi } from "vitest";

const FILE = "file:///data/user/0/app/files/release/tiles/v/atlas.pmtiles";
const disk = new Map<string, Uint8Array>([[FILE, Uint8Array.from({ length: 100 }, (_, i) => i)]]);
const handles = vi.hoisted(() => ({ opened: [] as string[], closed: 0 }));

vi.mock("expo-file-system", () => ({
  FileMode: { ReadOnly: "r", ReadWrite: "rw" },
  File: class {
    constructor(private uri: string) {}
    open(mode: string) {
      handles.opened.push(`${this.uri} ${mode}`);
      const bytes = disk.get(this.uri)!;
      return {
        offset: 0,
        size: bytes.length,
        readBytesSync(n: number) { const b = bytes.slice(this.offset, this.offset + n); this.offset += b.length; return b; },
        close() { handles.closed++; },
      };
    }
  },
}));

const { atlasRange } = await import("./range.native");

describe("atlasRange (native)", () => {
  it("reads the asked range of the installed file, fewer bytes at its end, none past it, closing it each time", async () => {
    const read = atlasRange!;
    const got = [await read(FILE, 10, 4), await read(FILE, 97, 16), await read(FILE, 100, 8), await read(FILE, 0, 0)];
    expect(got.map((b) => [...b])).toEqual([[10, 11, 12, 13], [97, 98, 99], [], []]);
    expect(handles).toEqual({ opened: Array(4).fill(`${FILE} r`), closed: 4 });
  });
});
