import { describe, expect, it } from "vitest";
import { INSEASON_LINKS_FORMAT, setFailureSink } from "@app/core";
import { httpInSeason } from "./inseason";

const FEED = { format: "inseason-feed/1", fetchedAt: "2026-10-28T12:00:00Z", source: "s", removed: [],
               rows: [{ id: "is-a", region: "4", water: "W", effective: "", from: null, to: null, change: "No fishing" }] };
const LINKS = { format: INSEASON_LINKS_FORMAT, notices: { "is-a": { waters: { "gnis:1": { all: true } } } } };
const mem = () => { let t: string | null = null; return { read: () => t, write: (x: string) => { t = x; } }; };
const serve = (files: Record<string, unknown>): typeof fetch => (async (u: string) => {
  const name = String(u).split("/").pop()!;
  return name in files ? new Response(JSON.stringify(files[name])) : new Response("", { status: 404 });
}) as unknown as typeof fetch;

describe("httpInSeason", () => {
  it("reads the pair, keeps the capture, and answers offline from it", async () => {
    const store = mem();
    const got = await httpInSeason("http://x/feeds/inseason/", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    expect([got?.feed.rows.length, Object.keys(got?.links?.notices ?? {})]).toEqual([1, ["is-a"]]);
    const off = await httpInSeason("http://x", store, (async () => { throw new Error("offline"); }) as unknown as typeof fetch).latest();
    expect(off?.links?.notices["is-a"]).toBeTruthy();
  });
  it("a feed fetched without its links keeps the captured links, never none", async () => {
    const store = mem();
    await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    const got = await httpInSeason("http://x", store, serve({ "index.json": FEED })).latest();
    expect(got?.links?.notices["is-a"]).toBeTruthy();
  });
  it("(T520) links of another format: reported, and the captured links answer meanwhile", async () => {
    const store = mem();
    await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const old = { format: "inseason-links/1", matches: {} };
      const got = await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": old })).latest();
      expect(got?.links?.notices["is-a"]).toBeTruthy();
      expect(String(seen[0])).toBe(`Error: in-season matches.json: inseason-links/1, but this app reads ${INSEASON_LINKS_FORMAT} — refused (matched notices fall back to the last links read, else to the list of changes not on the map)`);
    } finally { undo(); }
  });
  it("(T673) a FRESH device meeting a links format it does not read: says so, and holds no links (every change goes to the bell, none is dropped)", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const old = { format: "inseason-links/3", release: "r-1", notices: { "is-a": { waters: { "gnis:1": { all: true } } } } };
      const got = await httpInSeason("http://x", mem(), serve({ "index.json": FEED, "matches.json": old })).latest();
      expect([got?.feed.rows.length, got?.links]).toEqual([1, null]);
      expect(seen.map(String)).toEqual([`Error: in-season matches.json: inseason-links/3, but this app reads ${INSEASON_LINKS_FORMAT} — refused (matched notices fall back to the last links read, else to the list of changes not on the map)`]);
    } finally { undo(); }
  });
  it("(T548) an index that came back unreadable (a format bump): reported, the capture answers", async () => {
    const store = mem();
    await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const got = await httpInSeason("http://x", store, serve({ "index.json": { ...FEED, format: "inseason-feed/2" },
                                                                "matches.json": LINKS })).latest();
      expect(got?.feed.fetchedAt).toBe(FEED.fetchedAt);
      expect(seen.map(String)).toEqual(["Error: in-season index.json: not a readable inseason-feed/1 feed (format inseason-feed/2) — refused"]);
    } finally { undo(); }
  });
  it("(T548) the update service's offer: a feed it cannot read throws (never marked seen); a readable one re-fetches", async () => {
    let asked = 0;
    const f = (async (u: string) => { asked++; return serve({ "index.json": FEED, "matches.json": LINKS })(u); }) as unknown as typeof fetch;
    const src = httpInSeason("http://x", mem(), f);
    await src.latest();
    expect(() => src.offer!(JSON.stringify({ ...FEED, format: "inseason-feed/2" }))).toThrow("not a readable inseason-feed/1 feed");
    await src.latest();
    expect(asked).toBe(2);                              // refused: what was held still answers
    src.offer!(JSON.stringify(FEED));
    await src.latest();
    expect(asked).toBe(4);                              // taken: the pair is fetched again
  });
  it("(T1167) a notice whose range cannot be dated: kept (undated) and reported once per set; a curated date silences it", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const fraser = { id: "is-8cd37f6e5bf9", region: "2", water: "Fraser River", effective: "September 16, 2025", from: "2025-09-16", to: null,
                       change: "No fishing for steelhead downstream of Spuzzum Creek confluence with the Fraser River (mainstem) August 15 to December 31.",
                       parsed: { kinds: ["closure"], window: { from_month: 8, from_day: 15, to_month: 12, to_day: 31 } } };
      const index = { ...FEED, rows: [...FEED.rows, fraser] };
      const src = httpInSeason("http://x", mem(), serve({ "index.json": index, "matches.json": LINKS }),
                              { ttlMs: 1, now: (() => { let t = 0; return () => (t += 10); })() });   // every ask fetches again
      const got = await src.latest();
      expect(got?.feed.rows.map((r) => r.id)).toEqual(["is-a", "is-8cd37f6e5bf9"]);
      await src.latest();
      expect(seen.map(String)).toEqual(["Error: in-season index.json: 1 notice prints a range this app could not date (is-8cd37f6e5bf9: its range does not open on its effective day, so its year cannot be read) — shown until the page drops it, never ended by a guess"]);
      seen.length = 0;
      const dated = { ...LINKS, notices: { ...LINKS.notices, "is-8cd37f6e5bf9": { to: "2025-12-31", waters: { "gnis:2": { all: true } } } } };
      await httpInSeason("http://x", mem(), serve({ "index.json": index, "matches.json": dated })).latest();
      expect(seen).toEqual([]);
    } finally { undo(); }
  });
  it("(T548 item 4) one row it cannot read: dropped and reported once per fresh read, every other change shown and kept", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      const store = mem();
      const index = { ...FEED, rows: [...FEED.rows, { id: "is-bad", region: "4", water: "W", change: 7 }] };
      const got = await httpInSeason("http://x", store, serve({ "index.json": index, "matches.json": LINKS })).latest();
      expect(got?.feed.rows.map((r) => r.id)).toEqual(["is-a"]);
      expect(seen.map(String)).toEqual(["Error: in-season index.json: 1 row this app could not read (is-bad) — dropped, the other changes shown"]);
      // the capture answers offline with the same rows, and reports nothing again
      seen.length = 0;
      const off = await httpInSeason("http://x", store, (async () => { throw new TypeError("Network request failed"); }) as unknown as typeof fetch).latest();
      expect([off?.feed.rows.map((r) => r.id), seen]).toEqual([["is-a"], []]);
      // the update service's offer takes it (a readable feed), never refuses the whole index for one row
      expect(() => httpInSeason("http://x", mem(), serve({})).offer!(JSON.stringify(index))).not.toThrow();
      // most rows unreadable: refused whole — the offer throws, a read keeps the capture
      const most = { ...FEED, fetchedAt: "2026-12-01T00:00:00Z", rows: [...index.rows, { ...index.rows[1]!, id: "is-bad2" }] };
      // (T548 L5) and the words say it was the rows: how many of how many, and which
      const ROWS = "not a readable inseason-feed/1 feed: 2 of 3 rows could not be read (is-bad, is-bad2), more than half";
      expect(() => httpInSeason("http://x", mem(), serve({})).offer!(JSON.stringify(most))).toThrow(ROWS);
      seen.length = 0;
      const src = httpInSeason("http://x", store, serve({ "index.json": most, "matches.json": LINKS }));
      const kept = await src.latest();
      expect([kept?.feed.fetchedAt, seen.map(String)]).toEqual([FEED.fetchedAt, [`Error: in-season index.json: ${ROWS} — refused`]]);
      expect([src.failure!(), src.failureRows!()]).toEqual([`in-season index.json: ${ROWS} — refused`, { unread: 2, of: 3 }]);
      // over five unreadable: the first five named, the rest counted
      const many = { ...FEED, rows: [FEED.rows[0], ...[1, 2, 3, 4, 5, 6, 7].map((n) => ({ id: `is-x${n}`, change: 7 }))] };
      expect(() => httpInSeason("http://x", mem(), serve({})).offer!(JSON.stringify(many))).toThrow(
        "not a readable inseason-feed/1 feed: 7 of 8 rows could not be read (is-x1, is-x2, is-x3, is-x4, is-x5 and 2 more), more than half");
    } finally { undo(); }
  });
  it("nothing ever captured and nothing reached: null, never a throw", async () => {
    expect(await httpInSeason("http://x", mem(), serve({})).latest()).toBeNull();
  });
});
