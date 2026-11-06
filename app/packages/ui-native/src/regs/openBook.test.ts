// @vitest-environment node
// (T215) "Rule book p.N" opens our copy of the synopsis; one load at a time (owner 2026-10-28).
import { describe, expect, it, vi } from "vitest";
import { openBook } from "./openBook";
import { setSynopsisSource } from "./synopsisSource";
import { cancelSynopsisLoad, dismissSynopsisError, getSynopsisLoad, percentOf, type LoadHooks } from "./synopsisLoad";
import { Linking } from "react-native";

describe("openBook with a box (T215)", () => {
  const boxes = [{ pdf: 23, box: [1, 2, 3, 4] as [number, number, number, number], size: [576, 756] as [number, number] }];
  it("opens our highlighted copy when the release names one and the link has a box", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 1 });
    const open = vi.fn().mockResolvedValue(undefined);
    await openBook(23, boxes, "web", open);
    expect(open).toHaveBeenCalledWith(expect.objectContaining({ url: "http://x/s.pdf" }), 23, boxes, expect.objectContaining({ signal: expect.anything() }));
  });
  it("no box: still OUR copy, at its page; no copy in the release: the plain link", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 1 });
    const link = vi.spyOn(Linking, "openURL").mockResolvedValue(undefined);
    const open = vi.fn().mockResolvedValue(undefined);
    await openBook(23, undefined, "web", open);
    expect(open).toHaveBeenCalledWith(expect.anything(), 23, [], expect.anything());
    setSynopsisSource(null);
    await openBook(23, boxes, "web", open);
    expect(open).toHaveBeenCalledTimes(1);
    expect(link).toHaveBeenCalledTimes(1);
  });
});

describe("the synopsis load (owner 2026-10-28)", () => {
  const boxes = [{ pdf: 23, box: [1, 2, 3, 4] as [number, number, number, number], size: [576, 756] as [number, number] }];
  it("one download at a time, progress, cancel, and a failure that keeps Retry's inputs", async () => {
    setSynopsisSource({ url: "http://x/s.pdf", sha256: "a".repeat(64), bytes: 100 });
    let release!: () => void, hooks!: LoadHooks;
    const open = vi.fn((_s, _p, _b, h: LoadHooks) => { hooks = h; return new Promise<void>((r) => { release = r; }); });
    const first = openBook(23, boxes, "web", open as never);
    expect(getSynopsisLoad()).toMatchObject({ phase: "loading", pdf: 23 });
    await openBook(23, boxes, "web", open as never);                 // a second press while loading
    expect(open).toHaveBeenCalledTimes(1);
    hooks.onProgress(25, 100);
    expect(percentOf(getSynopsisLoad() as never)).toBe(25);
    release(); await first;
    expect(getSynopsisLoad().phase).toBe("idle");

    const pending = openBook(23, boxes, "web", open as never);
    cancelSynopsisLoad();
    expect(hooks.signal.aborted).toBe(true);
    release(); await pending;
    expect(getSynopsisLoad().phase).toBe("idle");

    vi.spyOn(console, "warn").mockImplementation(() => {});
    await openBook(23, boxes, "web", vi.fn().mockRejectedValue(new Error("boom")));
    expect(getSynopsisLoad()).toMatchObject({ phase: "error", pdf: 23, boxes });
    dismissSynopsisError();
    expect(getSynopsisLoad().phase).toBe("idle");
    setSynopsisSource(null);
  });
  it("web loadSynopsis streams: progress against Content-Length", async () => {
    const { loadSynopsis } = await import("./synopsisPdf.web");
    const chunks = [new Uint8Array([1, 2]), new Uint8Array([3])];
    const res = { ok: true, status: 200, headers: new Headers({ "content-length": "3" }),
                  body: { getReader: () => ({ read: async () => (chunks.length ? { done: false, value: chunks.shift()! } : { done: true, value: undefined }) }) } };
    const seen: [number, number | null][] = [];
    const out = await loadSynopsis({ url: "u", sha256: "a", bytes: 3 }, (async () => res) as never, { onProgress: (l, t) => seen.push([l, t]) });
    expect([...out]).toEqual([1, 2, 3]);
    expect(seen).toEqual([[2, 3], [3, 3]]);
  });
});
