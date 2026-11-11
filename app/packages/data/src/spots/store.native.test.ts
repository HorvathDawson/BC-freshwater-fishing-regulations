// @vitest-environment node
// (T446) Spots on a phone over expo-file-system as SDK 58 has it: `write` / `move` are promises whose work lands after
// the call returns. `core.ts`'s save is write-then-rename in one synchronous turn, so the adapter must use the SYNC calls
// (`writeSync` / `moveSync`): with the async ones a save moved a file before its bytes were in it.
import { describe, expect, it, vi } from "vitest";
import type { Spot } from "./model";

const disk = new Map<string, string>();

vi.mock("expo-file-system", () => {
  const later = (f: () => void) => new Promise<void>((r) => setTimeout(() => { f(); r(); }, 0));
  const join = (parts: unknown[]) => parts.map((p) => (typeof p === "string" ? p : (p as { uri: string }).uri)).join("/");
  class Directory {
    uri: string; constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return true; }
    create() {}
  }
  class File {
    uri: string; constructor(...parts: unknown[]) { this.uri = join(parts); }
    get exists() { return disk.has(this.uri); }
    delete() { disk.delete(this.uri); }
    textSync() { const t = disk.get(this.uri); if (t === undefined) throw new Error(`no ${this.uri}`); return t; }
    writeSync(t: string) { disk.set(this.uri, t); }
    write(t: string) { return later(() => this.writeSync(t)); }
    moveSync(to: File) { disk.set(to.uri, disk.get(this.uri)!); disk.delete(this.uri); }
    move(to: File) { return later(() => this.moveSync(to)); }
  }
  return { Directory, File, Paths: { document: "doc" } };
});

import { openSpots } from "./store.native";

describe("spots on a phone (T446): a save is on disk when it returns", () => {
  it("a spot put is read back by a fresh store", async () => {
    await openSpots().put({ id: "a", title: "a" } as unknown as Spot);
    expect((await openSpots().list()).map((s) => s.id)).toEqual(["a"]);
    expect([...disk.keys()].filter((k) => k.endsWith("spots.spots"))).toHaveLength(1);
  });
});

describe("a small store on a phone (T554 item 2)", () => {
  it("writes through a .part file moved over the old one; an unreadable notify.json is kept aside", async () => {
    const { openNotifyStore } = await import("./store.native");
    const s = openNotifyStore();
    s.write("{\"v\":3}");
    s.write("{\"v\":3,\"on\":true}");
    expect(s.read()).toBe("{\"v\":3,\"on\":true}");
    expect([...disk.keys()].filter((k) => k.includes("notify.json"))).toEqual(["doc/spots/notify.json"]);
    s.keepAside("torn");
    expect([...disk.entries()].filter(([k]) => k.includes("notify.json.corrupt-")).map(([, v]) => v)).toEqual(["torn"]);
  });
});
