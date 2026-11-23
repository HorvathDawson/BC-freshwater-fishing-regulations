import type React from "react";
/**
 * (T230, T700) The in-season hooks, mounted (jsdom): the inbox (the device's history, the bell's count, opening by itself
 * once for a notice not on the map yet), where a change lands on a page, and the waters this release does not hold.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { INBOX_WORDS, INSEASON_FAILURES, INSEASON_LINKS_FORMAT, bcNow, isoDayOf, joinInSeason, setFailureSink } from "@app/core";
import { httpInSeason } from "@app/data";
import { InSeasonContext, useInSeason, useInSeasonCovers, useInSeasonInbox, type InboxTrack, type InSeasonInboxState, type InSeasonNow, type InSeasonOnPage } from "./inseason";

afterEach(cleanup);
const mem = (t: string | null = null) => ({ read: () => t, write: (x: string) => { t = x; } });

/**
 * (T700, T694) THE INBOX HOOK: one feed read folded into the device's history, the count, opening by itself once, a
 * row's tap, opening the sheet (read on open), the update rows, and a restart (the store read back).
 */
describe("useInSeasonInbox (T700)", () => {
  const SRC = "https://www2.gov.bc.ca/in-season";
  const row = (id: string, water: string, o: Record<string, unknown> = {}) =>
    ({ id, region: "4", water, effective: "Nov 1, 2026", from: null, to: null, change: `No fishing on ${water}`,
       status: "unchanged", firstSeen: "2026-11-01T00:00:00Z", changedAt: "2026-11-01T00:00:00Z", ...o });
  const nowOf = (rows: ReturnType<typeof row>[], waters: Record<string, string[]>): InSeasonNow => {
    const feed = { format: "inseason-feed/1" as const, fetchedAt: "x", source: SRC, removed: [], rows };
    const links = { format: INSEASON_LINKS_FORMAT, notices: Object.fromEntries(Object.entries(waters).map(([id, ws]) =>
      [id, { waters: Object.fromEntries(ws.map((w) => [w, { all: true as const }])) }])) };
    return { ...joinInSeason(feed, links, isoDayOf(bcNow())), data: { feed, links }, release: "r-1" };
  };
  function Inbox({ now, store, seen, out, opened, track, updates }: { now: InSeasonNow; store: ReturnType<typeof mem>; seen?: ReturnType<typeof mem>;
                                                    out: { s?: InSeasonInboxState }; opened: string[]; track?: InboxTrack;
                                                    updates?: string[] }) {
    out.s = useInSeasonInbox(now, store, seen ?? null, (item, id) => opened.push(`${item}|${id}`), track ?? null,
                             (kind) => updates?.push(kind));
    return null;
  }
  const A = row("is-a", "Kaslo River"), B = row("is-b", "Okanagan River", { region: "8" });

  it("(T867) a first read: the matched stay out of the history, opens once for one not on the map; survives a restart (the store read back)", async () => {
    const store = mem(), out: { s?: InSeasonInboxState } = {}, opened: string[] = [];
    const first = render(<Inbox now={nowOf([A, B], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} />);
    await waitFor(() => expect(out.s!.open).toBe(true));
    expect([out.s!.count, out.s!.label]).toEqual([1, "1 unread in-season notice"]);
    act(() => out.s!.close());
    expect(out.s!.open).toBe(false);
    first.unmount();
    // the next launch: the same history, read back; it does not open again; the count holds
    const saved = store.read();
    render(<Inbox now={nowOf([A, B], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} />);
    expect([out.s!.open, out.s!.count]).toEqual([false, 1]);
    expect(store.read()).toBe(saved);
    expect(out.s!.vm.groups.map((g) => [g.title, g.rows.map((r) => r.id)])).toEqual([["New", ["is-b"]]]);
  });

  it("a row with one water opens its page (at the notice); one with none shows its text; opening the sheet reads what it shows", async () => {
    const store = mem(), out: { s?: InSeasonInboxState } = {}, opened: string[] = [];
    const r = render(<Inbox now={nowOf([], {})} store={store} out={out} opened={opened} />);
    r.rerender(<Inbox now={nowOf([A, B], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} />);
    await waitFor(() => expect(out.s!.count).toBe(2));
    act(() => out.s!.pick("is-a"));
    expect([opened, out.s!.count, out.s!.detail]).toEqual([["gnis:1|is-a"], 1, null]);
    // the page it opened shows the mark it had (read now), and takes the focus once (its notices open)
    expect(out.s!.reads.markOf("is-a")).toBe("new");
    expect([out.s!.reads.takeFocus(["is-a"]), out.s!.reads.takeFocus(["is-a"])]).toEqual([true, false]);
    expect(out.s!.reads.markOf("is-a")).toBeNull();
    act(() => out.s!.pick("is-b"));
    expect([opened.length, out.s!.count, out.s!.detail?.head]).toEqual([1, 0, "Okanagan River · Region 8"]);
    act(() => out.s!.back());
    expect(out.s!.detail).toBeNull();
    const C = row("is-c", "Duncan River");
    r.rerender(<Inbox now={nowOf([A, B, C], { "is-a": ["gnis:1"], "is-c": ["gnis:3"] })} store={store} out={out} opened={opened} />);
    await waitFor(() => expect(out.s!.count).toBe(1));
    expect(out.s!.reads.markOf("is-c")).toBe("new");
    // (T867) the sheet opens: it lists the unread as New, the badge clears, and the history reads
    act(() => out.s!.shown(true));
    expect([out.s!.count, out.s!.label, out.s!.reads.markOf("is-c")]).toEqual([0, null, null]);
    expect(out.s!.vm.groups.map((g) => [g.title, g.rows.map((r) => r.id)])).toEqual([["New", ["is-c"]], ["Earlier", ["is-a", "is-b"]]]);
    expect(JSON.parse(store.read()!).items.every((i: { read: boolean }) => i.read)).toBe(true);
    // closed, and opened again: they are Earlier now
    act(() => out.s!.shown(false));
    act(() => out.s!.shown(true));
    expect(out.s!.vm.groups.map((g) => [g.title, g.rows.map((r) => r.id)])).toEqual([["Earlier", ["is-a", "is-b", "is-c"]]]);
  });

  it("(T867) data and app updates are rows: recorded at first sight, a row when the release or version moves; a tap opens it", async () => {
    const store = mem(), out: { s?: InSeasonInboxState } = {}, opened: string[] = [], updates: string[] = [];
    const t1: InboxTrack = { release: { id: "r-1", day: "2026-11-10" }, app: "0.8.5" };
    // the first read: the matched notice stays out of the history, and the release and version are recorded, no row
    const r = render(<Inbox now={nowOf([A], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} track={t1} updates={updates} />);
    await waitFor(() => expect(JSON.parse(store.read()!).base).toEqual({ release: "r-1", app: "0.8.5" }));
    expect([out.s!.count, out.s!.vm.empty, JSON.parse(store.read()!).items]).toEqual([0, true, []]);
    r.rerender(<Inbox now={nowOf([A], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} updates={updates}
                      track={{ release: { id: "r-2", day: "2026-11-20" }, app: "0.9.0" }} />);
    await waitFor(() => expect(out.s!.count).toBe(2));
    expect(out.s!.vm.groups[0]!.rows.map((x) => x.label).sort()).toEqual([
      "Unread. App update: Updated to 0.9.0", "Unread. Data update: Regulations data updated Nov 20"]);
    act(() => out.s!.pick("app:0.9.0"));
    act(() => out.s!.pick("data:r-2"));
    expect([updates, opened, out.s!.count, out.s!.detail]).toEqual([["app", "data"], [], 0, null]);
  });

  it("(T694) the feed's link makes it Updated; a new id it does not link is New", async () => {
    const store = mem(), out: { s?: InSeasonInboxState } = {}, opened: string[] = [];
    const r = render(<Inbox now={nowOf([A], { "is-a": ["gnis:1"] })} store={store} out={out} opened={opened} />);
    await waitFor(() => expect(store.read()).not.toBeNull());
    const A2 = row("is-a2", "Kaslo River", { change: "No fishing until Dec 31", status: "changed",
                                             previous: { change: A.change, effective: A.effective } });
    const D = row("is-d", "Kaslo River", { change: "Bait ban", status: "new" });
    r.rerender(<Inbox now={nowOf([A2, D], { "is-a2": ["gnis:1"], "is-d": ["gnis:1"] })} store={store} out={out} opened={opened} />);
    await waitFor(() => expect(out.s!.count).toBe(2));
    expect([out.s!.reads.markOf("is-a2"), out.s!.reads.markOf("is-d")]).toEqual(["updated", "new"]);
    expect(out.s!.vm.groups.flatMap((g) => g.rows.map((x) => `${x.id} ${x.tag}`))).toEqual(["is-a2 Updated", "is-d New"]);
  });

  it("a saved history it cannot read is reported and starts again — never a quiet empty inbox", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const out: { s?: InSeasonInboxState } = {};
      render(<Inbox now={nowOf([B], {})} store={mem("{not json")} out={out} opened={[]} />);
      await waitFor(() => expect(out.s!.count).toBe(1));
      expect((seen[0] as Error).message).toMatch(/^in-season inbox: the saved history could not be read \(.+\) — started again$/);
    } finally { undo(); }
  });

  it("an index in a format this app does not read FAILS LOUDLY: reported, and the inbox says so while the capture answers", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const feed = { format: "inseason-feed/2", fetchedAt: "2026-11-13T00:00:00Z", source: SRC, removed: [], rows: [A] };
      const serve = (async (u: string) => (String(u).endsWith("index.json") ? new Response(JSON.stringify(feed))
        : new Response(JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} })))) as unknown as typeof fetch;
      const source = httpInSeason("http://x/feeds/inseason", mem(), serve);
      const out: { s?: InSeasonInboxState; now?: InSeasonNow } = {};
      function Both() {
        const now = useInSeason(source, 0, "r-1", null);
        out.now = now;
        out.s = useInSeasonInbox(now, mem(), null, () => {});
        return null;
      }
      render(<Both />);
      await waitFor(() => expect(out.s!.failure).toBe(INBOX_WORDS.failure));
      expect(out.now!.failure).toBe("in-season index.json: not a readable inseason-feed/1 feed (format inseason-feed/2) — refused");
      expect(seen.map(String)).toEqual(["Error: in-season index.json: not a readable inseason-feed/1 feed (format inseason-feed/2) — refused"]);
    } finally { undo(); }
  });

  it("(T548 item 4) a row it cannot read: left out and SAID on the inbox line (and reported); over half: the index is refused whole", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    const C = row("is-c", "Duncan River");
    const bad = { id: "is-bad", region: "4", water: "W", change: 7 };
    const mount = (rows: unknown[], store = mem()) => {
      const feed = { format: "inseason-feed/1", fetchedAt: "2026-11-20T00:00:00Z", source: SRC, removed: [], rows };
      const serve = (async (u: string) => (String(u).endsWith("index.json") ? new Response(JSON.stringify(feed))
        : new Response(JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} })))) as unknown as typeof fetch;
      const source = httpInSeason("http://x/feeds/inseason", store, serve);
      const out: { s?: InSeasonInboxState; now?: InSeasonNow } = {};
      function Both() {
        const now = useInSeason(source, 0, "r-1", null);
        out.now = now;
        out.s = useInSeasonInbox(now, mem(), null, () => {});
        return null;
      }
      render(<Both />);
      return out;
    };
    try {
      const some = mount([A, bad, C]);
      await waitFor(() => expect(some.s!.failure).toBe(INBOX_WORDS.unreadRows(1)));
      expect(some.now!.data!.feed.rows.map((r) => r.id)).toEqual(["is-a", "is-c"]);
      expect(seen.map(String)).toEqual(["Error: in-season index.json: 1 row this app could not read (is-bad) — dropped, the other changes shown"]);
      cleanup();
      seen.length = 0;
      // most rows unreadable: refused whole, the feed's own failure line, the capture (none here) answers
      const most = mount([A, bad, { ...bad, id: "is-bad2" }]);
      // (T548 L5) and both say it was the rows: the inbox line how many of how many, the report which
      await waitFor(() => expect(most.s!.failure).toBe(INBOX_WORDS.failureRows(2, 3)));
      expect(most.s!.failure).toBe("The latest in-season notices could not be read by this version of the app (2 of 3 could not be read). Shown: the last ones it read.");
      expect(seen.map(String)).toEqual(["Error: in-season index.json: not a readable inseason-feed/1 feed: 2 of 3 rows could not be read (is-bad, is-bad2), more than half — refused"]);
    } finally { undo(); }
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
    render(<InSeasonContext.Provider value={{ matched: [], unmatched: [], ended: [], release: deviceRelease, data }}>
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
    const wrap = (n: React.ReactNode) => <InSeasonContext.Provider value={{ matched: [], unmatched: [], ended: [], release: "r-1",
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
