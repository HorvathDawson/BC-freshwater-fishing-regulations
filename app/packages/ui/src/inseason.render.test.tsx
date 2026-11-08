/**
 * (T230) The in-season notice opens by itself once per row per device, re-opens from the bell, and drops a row once it
 * is matched or out of effect (a new unmatched list). Mounted (jsdom) because it is a hook.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { INSEASON_FAILURES, setFailureSink, type InSeasonChange } from "@app/core";
import { InSeasonContext, useInSeasonCovers, useInSeasonNotice, type InSeasonNoticeState, type InSeasonOnPage } from "./inseason";

afterEach(cleanup);
const ch = (id: string): InSeasonChange =>
  ({ id, region: "4", water: "Kaslo River", text: "No fishing", from: null, to: null, source: "s", waters: null, onRelease: true });
const mem = () => { let t: string | null = null; return { read: () => t, write: (x: string) => { t = x; } }; };

function Probe({ rows, store, out }: { rows: InSeasonChange[]; store: ReturnType<typeof mem>; out: { s?: InSeasonNoticeState } }) {
  out.s = useInSeasonNotice(rows, store);
  return null;
}

describe("useInSeasonNotice", () => {
  it("opens for an unseen row, once per device; the bell re-opens it; a matched row leaves it", () => {
    const store = mem(), out: { s?: InSeasonNoticeState } = {};
    const a = [ch("is-a"), ch("is-b")];
    const { rerender, unmount } = render(<Probe rows={a} store={store} out={out} />);
    expect([out.s!.open, out.s!.count, out.s!.label]).toEqual([true, 2, "2 in-season changes not on the map yet"]);
    act(() => out.s!.close());
    expect([out.s!.open, store.read()]).toEqual([false, JSON.stringify(["is-a", "is-b"])]);
    unmount();
    // the next launch: both seen — closed, the bell still counts them
    const again = render(<Probe rows={a} store={store} out={out} />);
    expect([out.s!.open, out.s!.count]).toEqual([false, 2]);
    act(() => out.s!.show());
    expect(out.s!.open).toBe(true);
    act(() => out.s!.close());
    // is-a matched: it leaves; a new row opens the notice again
    again.rerender(<Probe rows={[ch("is-b"), ch("is-c")]} store={store} out={out} />);
    expect([out.s!.open, out.s!.rows.map((r) => r.id)]).toEqual([true, ["is-b", "is-c"]]);
    void rerender;
  });
  it("nothing unmatched: no badge, never open", () => {
    const out: { s?: InSeasonNoticeState } = {};
    render(<Probe rows={[]} store={mem()} out={out} />);
    expect([out.s!.open, out.s!.count]).toEqual([false, 0]);
  });
});

describe("useInSeasonCovers (T520): one load path for the banner and the water map", () => {
  // a water (ord 5) of parts 0 [sids 1, 2] and 1 [sids 3, 4] on stream 7; a change covering sids 2-3
  const nav = { sections: [{ sid: 1, partIx: 0 }, { sid: 2, partIx: 0 }, { sid: 3, partIx: 1 }, { sid: 4, partIx: 1 }] };
  const store = { navRows: async () => nav } as never;
  const lines = { hasLines: async () => true, sectionLine: async (h: number) => ({ blk: 7, from: (h - 1) * 100, to: h * 100 }) };
  const feed = { format: "inseason-feed/1" as const, fetchedAt: "x", source: "s", removed: [],
                 rows: [{ id: "is-a", region: "2", water: "W", effective: "", from: null, to: null, change: "No fishing" }] };
  const links = (release: string) => ({ format: "inseason-links/2" as const, release,
    notices: { "is-a": { waters: { "gnis:1": { runs: [[7, 100, 300]] as const, ends: "From A to B" } } } } });
  function Covers({ partIx, out }: { partIx: number | null; out: { v?: readonly InSeasonOnPage[] } }) {
    out.v = useInSeasonCovers(store, lines, { item: "gnis:1", ord: 5, partIx }, null);
    return null;
  }
  const at = (release: string, deviceRelease: string, partIx: number | null) => {
    const out: { v?: readonly InSeasonOnPage[] } = {};
    render(<InSeasonContext.Provider value={{ matched: [], unmatched: [], release: deviceRelease,
                                              data: { feed, links: links(release) } }}>
      <Covers partIx={partIx} out={out} /></InSeasonContext.Provider>);
    return out;
  };
  it("a part page: its own covered sections and the ends; the water's page: every covered section", async () => {
    const p0 = at("r-1", "r-1", 0);
    await waitFor(() => expect(p0.v).toHaveLength(1));
    expect(p0.v![0]!.cover).toEqual({ sections: [2], ends: "From A to B", failure: null });
    cleanup();
    const all = at("r-1", "r-1", null);
    await waitFor(() => expect(all.v).toHaveLength(1));
    expect(all.v![0]!.cover.sections).toEqual([2, 3]);
  });
  it("off-release: the whole water with its ends, and reported", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const off = at("r-1", "r-2", 0);
      await waitFor(() => expect(off.v).toHaveLength(1));
      expect(off.v![0]!.cover).toEqual({ sections: null, ends: "From A to B", failure: INSEASON_FAILURES.offRelease });
      await waitFor(() => expect(String(seen[0])).toContain("another data release"));
    } finally { undo(); }
  });
});
