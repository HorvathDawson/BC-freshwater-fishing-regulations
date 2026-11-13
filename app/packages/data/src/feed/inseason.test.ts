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
      expect(seen.map(String)).toEqual(["Error: in-season index.json: not a readable inseason-feed/1 feed — refused"]);
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
  it("nothing ever captured and nothing reached: null, never a throw", async () => {
    expect(await httpInSeason("http://x", mem(), serve({})).latest()).toBeNull();
  });
});
