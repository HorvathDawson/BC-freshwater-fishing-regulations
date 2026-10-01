import { describe, expect, it } from "vitest";
import { loadStatusIndex, statusData, statusIndexUrl } from "./hooks";

/** A status index by hand: sections 5 and 6 and water "gnis:1" closed Jan 1 - Mar 31. */
function tiny(handles = "147b20dce7d8576c"): Uint8Array {
  const v = (n: number): number[] => {
    const out: number[] = [];
    do { out.push((n & 0x7f) | (n > 0x7f ? 0x80 : 0)); n >>>= 7; } while (n);
    return out;
  };
  const h = handles.match(/../g)!.map((x) => parseInt(x, 16));
  return Uint8Array.from([
    0x42, 0x43, 0x53, 0x49, 1, ...h,
    ...v(1), ...v(2), ...v((91 << 3) | 2), ...v((275 << 3) | 1),   // one profile
    ...v(1), ...v(5), ...v(2), ...v(0),                              // sections 5..6
    ...v(1), ...v(0), ...v(6), ..."gnis:1".split("").map((c) => c.charCodeAt(0)), ...v(0),
  ]);
}

const serve = (bytes: Uint8Array | null) => {
  let calls = 0;
  const fetcher = (async () => {
    calls++;
    return bytes ? new Response(bytes.slice().buffer as ArrayBuffer)
                 : new Response("no", { status: 404 });
  }) as unknown as typeof fetch;
  return { fetcher, calls: () => calls };
};

describe("loading the status index", () => {
  it("sits beside the atlas", () => {
    expect(statusIndexUrl("http://x:1/tiles/atlas.pmtiles")).toBe("http://x:1/tiles/status_index.bin");
  });

  it("decodes once per URL and digest, and answers through @app/core", async () => {
    const s = serve(tiny());
    const ix = await loadStatusIndex("u1", "147b20dce7d8576c", s.fetcher);
    await loadStatusIndex("u1", "147b20dce7d8576c", s.fetcher);
    expect(s.calls()).toBe(1);
    expect(ix?.codeOn(5, new Date(2026, 2, 31))).toBe("closed");
    expect(ix?.codeOn(5, new Date(2026, 3, 1))).toBe("own");
    expect(ix?.waterCodeOn("gnis:1", new Date(2026, 0, 1))).toBe("closed");
  });

  it("is null — not asked — for another atlas, a 404, or a bundle with no digest", async () => {
    expect(await loadStatusIndex("u2", "0000000000000000", serve(tiny()).fetcher)).toBeNull();
    expect(await loadStatusIndex("u3", "147b20dce7d8576c", serve(null).fetcher)).toBeNull();
    const s = serve(tiny());
    expect(await loadStatusIndex("u4", null, s.fetcher)).toBeNull();
    expect(s.calls()).toBe(0);
  });
});

describe("the map's status values", () => {
  it("tells EVERY visible section its status — base included, so yesterday's red is cleared",
    async () => {
      const ix = await loadStatusIndex("u5", "147b20dce7d8576c", serve(tiny()).fetcher);
      const d = statusData(ix, [5, 7], new Date(2026, 0, 10));
      expect(d.stream).toEqual({ 5: { status: "closed" }, 7: { status: "base" } });
      expect(d.lake).toEqual(d.stream);
      // no index: every value is null (the mode's `missing`), never a guessed base
      expect(statusData(null, [5], new Date()).stream).toEqual({ 5: { status: null } });
    });
});
