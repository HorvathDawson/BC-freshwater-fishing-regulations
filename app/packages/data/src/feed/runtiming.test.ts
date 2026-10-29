import { describe, expect, it } from "vitest";
import { sha256Hex } from "../sha256";
import { httpRunTiming, type RunTimingStore } from "./runtiming";

const enc = new TextEncoder();
const runs = enc.encode(JSON.stringify({ format: "runtiming-runs/1", slots: ["Steelhead"], default: 0,
  relations: ["spawning", "passage", "observed"], relation_words: {}, sources: {}, runs: [
    { id: "psf:1", src: "psf", sp: "Steelhead", slot: 0, name: "X", months: 1, pentads: "1".padStart(19, "0") }] }));
const paintOf = (runsSha: string) => enc.encode(JSON.stringify({ format: "runtiming-paint/1", section_handles: "h",
  tile_version: null, runs_sha256: runsSha, profiles: [{ r: [0], s: 1, m: [[0, 1, 0]] }], sid: [7], p: [0], km: [0] }));
const paint = paintOf(sha256Hex(runs));
const index = { format: "runtiming-index/1", built_at: "t", section_handles: "h", tile_version: null, sources: {},
  files: { runs: { url: "runs-a.json.gz", bytes: runs.length, sha256: sha256Hex(runs) },
           paint: { url: "paint-b.json.gz", bytes: paint.length, sha256: sha256Hex(paint) } } };

function memStore(): RunTimingStore & { text: string | null } {
  const s = { text: null as string | null, async read() { return s.text; }, async write(t: string) { s.text = t; } };
  return s;
}
const serve = (files: Record<string, Uint8Array | string>, calls: string[] = []) => (async (url: string) => {
  const name = String(url).split("/").pop()!;
  calls.push(name);
  const f = files[name];
  if (f === undefined) return new Response("no", { status: 404 });
  return new Response(typeof f === "string" ? f : new Blob([f as BlobPart]));
}) as unknown as typeof fetch;

describe("httpRunTiming", () => {
  it("fetches the index and its two files verified, and keeps the capture", async () => {
    const store = memStore();
    const got = await httpRunTiming("https://o/runtiming", store, serve({ "index.json": JSON.stringify(index),
      "runs-a.json.gz": runs, "paint-b.json.gz": paint })).latest();
    expect(got?.from).toBe("live");
    expect(got?.facts.state(7, 0, 0)).toBe("spawn");
    expect(store.text).not.toBeNull();
  });

  it("offline, the last capture answers; an unchanged index fetches no file again", async () => {
    const store = memStore();
    await httpRunTiming("o", store, serve({ "index.json": JSON.stringify(index), "runs-a.json.gz": runs,
                                            "paint-b.json.gz": paint })).latest();
    const off = await httpRunTiming("o", store, serve({})).latest();
    expect(off?.from).toBe("capture");
    expect(off?.facts.has(7)).toBe(true);
    const calls: string[] = [];
    await httpRunTiming("o", store, serve({ "index.json": JSON.stringify(index) }, calls)).latest();
    expect(calls).toEqual(["index.json"]);
  });

  it("a file whose bytes are not the index's is refused: nothing, or the capture", async () => {
    const bad = await httpRunTiming("o", memStore(), serve({ "index.json": JSON.stringify(index),
      "runs-a.json.gz": runs, "paint-b.json.gz": paintOf("x") })).latest();
    expect(bad).toBeNull();
  });
});
