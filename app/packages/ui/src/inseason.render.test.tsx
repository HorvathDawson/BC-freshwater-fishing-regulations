import type React from "react";
/**
 * (T230) The in-season notice opens by itself once per row per device, re-opens from the bell, and drops a row once it
 * is matched or out of effect (a new unmatched list). Mounted (jsdom) because it is a hook.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { INSEASON_FAILURES, INSEASON_LINKS_FORMAT, setFailureSink, type InSeasonChange } from "@app/core";
import { InSeasonContext, useInSeason, useInSeasonCovers, useInSeasonNotice, type InSeasonNoticeState, type InSeasonNow, type InSeasonOnPage } from "./inseason";

afterEach(cleanup);
const ch = (id: string): InSeasonChange =>
  ({ id, region: "4", water: "Kaslo River", text: "No fishing", from: null, to: null, source: "s", waters: null });
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
  // (T517) the release's `display.ends`: what "Where" says
  const statics = async () => ({ ends: { a: ["A"], b: ["B"] } });
  const store = { navRows: async () => nav, statics } as never;
  const lines = { hasLines: async () => true, sectionLine: async (h: number) => ({ blk: 7, from: (h - 1) * 100, to: h * 100 }) };
  const feed = { format: "inseason-feed/1" as const, fetchedAt: "x", source: "s", removed: [],
                 rows: [{ id: "is-a", region: "2", water: "W", effective: "", from: null, to: null, change: "No fishing" }] };
  // (T673) one file for every release: the device's release is not compared with anything
  const links = (_release: string) => ({ format: INSEASON_LINKS_FORMAT,
    notices: { "is-a": { waters: { "gnis:1": { runs: [[7, 100, 300]] as const, ends: [["a", "b"]] as const } } } } });
  function Covers({ pieces, out, st = store, ln = links }: { pieces: readonly number[] | null; out: { v?: readonly InSeasonOnPage[] };
                                                             st?: never; ln?: typeof links }) {
    out.v = useInSeasonCovers(st, lines, { item: "gnis:1", ord: 5, pieces }, null);
    void ln;
    return null;
  }
  const at = (release: string, deviceRelease: string, partIx: number | null | readonly number[], o: { st?: never; ends?: [string, string] } = {}) => {
    const out: { v?: readonly InSeasonOnPage[] } = {};
    const l = links(release);
    const data = o.ends ? { feed, links: { ...l, notices: { "is-a": { waters: { "gnis:1": { runs: [[7, 100, 300]] as const, ends: [o.ends] } } } } } }
      : { feed, links: l };
    render(<InSeasonContext.Provider value={{ matched: [], unmatched: [], release: deviceRelease, data }}>
      <Covers pieces={partIx === null ? null : typeof partIx === "number" ? [partIx] : partIx} out={out} st={o.st} /></InSeasonContext.Provider>);
    return out;
  };
  it("a part page: its own covered sections and the ends; the water's page: every covered section", async () => {
    const p0 = at("r-1", "r-1", 0);
    await waitFor(() => expect(p0.v).toHaveLength(1));
    expect(p0.v![0]!.cover).toEqual({ sections: [2], where: "From A to B", failure: null, whereFailure: null });
    cleanup();
    const all = at("r-1", "r-1", null);
    await waitFor(() => expect(all.v).toHaveLength(1));
    expect(all.v![0]!.cover.sections).toEqual([2, 3]);
  });
  it("(T517) a page whose part the live salmon JOINS with the next covers the whole entry (every piece)", async () => {
    const joined = at("r-1", "r-1", [0, 1]);
    await waitFor(() => expect(joined.v).toHaveLength(1));
    expect(joined.v![0]!.cover).toEqual({ sections: [2, 3], where: "From A to B", failure: null, whereFailure: null });
  });
  it("(T517) an end display.ends lacks drops only \"Where\": the sections stay, and it is reported", async () => {
    const seen: unknown[] = [];
    const off = setFailureSink((e) => { seen.push(e); });
    const p0 = at("r-1", "r-1", 0, { ends: ["a", "gone"] });
    await waitFor(() => expect(p0.v).toHaveLength(1));
    expect(p0.v![0]!.cover).toEqual({ sections: [2], where: null, failure: null,
      whereFailure: INSEASON_FAILURES.noWords("is-a", "ends: token gone is not in this release's display.ends") });
    await waitFor(() => expect(seen.map((e) => (e as Error).message)).toContain(
      INSEASON_FAILURES.noWords("is-a", "ends: token gone is not in this release's display.ends")));
    off();
  });
  it("(T517) display.ends that could not be read never hides a notice: placed as ever, no \"Where\", reported", async () => {
    const seen: unknown[] = [];
    const off = setFailureSink((e) => { seen.push(e); });
    const broken = { navRows: async () => nav, statics: async () => { throw new Error("statics unreadable"); } } as never;
    const p0 = at("r-1", "r-1", 0, { st: broken });
    await waitFor(() => expect(p0.v).toHaveLength(1));
    expect(p0.v![0]!.cover).toEqual({ sections: [2], where: null, failure: null, whereFailure: INSEASON_FAILURES.noTable });
    await waitFor(() => expect(seen.map((e) => (e as Error).message)).toContain(INSEASON_FAILURES.noTable));
    off();
  });
  it("moving to another water never places its changes on the last water's lines (useAsync keeps the old answer)", async () => {
    // water 5 (Fraser-like, ord 5) and water 6 (Pitt-like, ord 6, sids 11-12): 6's lines are slow to come
    let release6: () => void = () => {};
    const slow = new Promise<void>((r) => { release6 = r; });
    const store2 = { navRows: async (ord: number) => (ord === 5 ? nav
      : (await slow, { sections: [{ sid: 11, partIx: 0 }, { sid: 12, partIx: 0 }] })), statics } as never;
    const both = { format: INSEASON_LINKS_FORMAT, notices: {
      "is-a": { waters: { "gnis:1": { runs: [[7, 100, 300]] as const, ends: [["a", "b"]] as const },
                          "gnis:2": { runs: [[7, 1000, 1100]] as const, ends: [["a", "b"]] as const } } } } };
    const lines2 = { hasLines: async () => true,
                     sectionLine: async (h: number) => ({ blk: 7, from: h < 10 ? (h - 1) * 100 : 1000 + (h - 11) * 100, to: 0 }) };
    const out: { v?: readonly InSeasonOnPage[] } = {};
    function P({ item, ord }: { item: string; ord: number }) {
      out.v = useInSeasonCovers(store2, lines2, { item, ord, pieces: null }, null);
      return null;
    }
    const wrap = (n: React.ReactNode) => <InSeasonContext.Provider value={{ matched: [], unmatched: [], release: "r-1",
                                                                            data: { feed, links: both } }}>{n}</InSeasonContext.Provider>;
    const r = render(wrap(<P item="gnis:1" ord={5} />));
    await waitFor(() => expect(out.v![0]!.cover.sections).toEqual([2, 3]));
    r.rerender(wrap(<P item="gnis:2" ord={6} />));
    // while 6's lines load, nothing is placed — never water 5's sections read as water 6's
    expect(out.v).toEqual([]);
    release6();
    await waitFor(() => expect(out.v![0]!.cover.sections).toEqual([11]));
  });
  it("(T673) a device on another release places the same file by its own lines, nothing reported", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const other = at("r-1", "r-2", 0);
      await waitFor(() => expect(other.v).toHaveLength(1));
      expect(other.v![0]!.cover).toEqual({ sections: [2], where: "From A to B", failure: null, whereFailure: null });
      expect(seen).toEqual([]);
    } finally { undo(); }
  });
});

describe("useInSeason (T693): a notice matched to a water this release does not hold is in the bell, not nowhere", () => {
  const feed = { format: "inseason-feed/1" as const, fetchedAt: "x", source: "s", removed: [],
                 rows: [{ id: "is-a", region: "2", water: "Gone Creek", effective: "", from: null, to: null, change: "No fishing" },
                        { id: "is-b", region: "2", water: "Here Creek", effective: "", from: null, to: null, change: "No bait" },
                        { id: "is-c", region: "2", water: "Both", effective: "", from: null, to: null, change: "Closed" }] };
  const links = { format: INSEASON_LINKS_FORMAT, notices: {
    "is-a": { waters: { "gnis:gone": { all: true as const } } },
    "is-b": { waters: { "gnis:here": { all: true as const } } },
    "is-c": { waters: { "gnis:here": { all: true as const }, "gnis:gone": { all: true as const } } } } };
  const source = { latest: async () => ({ feed, links }) } as never;
  const holds = async (id: string) => id !== "gnis:gone";
  function Now({ out, h }: { out: { v?: InSeasonNow }; h: ((id: string) => Promise<boolean>) | null }) {
    out.v = useInSeason(source, 0, "r-1", h);
    return null;
  }

  it("every water absent: the bell takes it (not matched); one still held stays matched; the absent ones are reported once", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const out: { v?: InSeasonNow } = {};
      const r = render(<Now out={out} h={holds} />);
      await waitFor(() => expect(out.v!.unmatched.map((c) => c.id)).toEqual(["is-a"]));
      expect(out.v!.matched.map((c) => c.id)).toEqual(["is-b", "is-c"]);
      await waitFor(() => expect(seen.map((e) => (e as Error).message)).toEqual([
        INSEASON_FAILURES.absentWaters("is-a", ["gnis:gone"]), INSEASON_FAILURES.absentWaters("is-c", ["gnis:gone"])]));
      r.rerender(<Now out={out} h={holds} />);        // a re-render and a re-ask say nothing twice
      await new Promise((res) => setTimeout(res, 20));
      expect(seen).toHaveLength(2);
    } finally { undo(); }
  });
  it("no way to ask (no release open): matched as ever, nothing reported", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const out: { v?: InSeasonNow } = {};
      render(<Now out={out} h={null} />);
      await waitFor(() => expect(out.v!.matched).toHaveLength(3));
      expect([out.v!.unmatched, seen]).toEqual([[], []]);
    } finally { undo(); }
  });
  it("a failed ask is reported, and the notices stay matched (never dropped)", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const out: { v?: InSeasonNow } = {};
      render(<Now out={out} h={async () => { throw new Error("store: damaged"); }} />);
      await waitFor(() => expect(seen.map((e) => (e as Error).message)).toEqual([INSEASON_FAILURES.noHolds("store: damaged")]));
      expect([out.v!.matched.length, out.v!.unmatched.length]).toEqual([3, 0]);
    } finally { undo(); }
  });
});
