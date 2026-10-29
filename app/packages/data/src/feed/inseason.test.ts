import { describe, expect, it } from "vitest";
import { httpInSeason } from "./inseason";

const FEED = { format: "inseason-feed/1", fetchedAt: "2026-10-28T12:00:00Z", source: "s", removed: [],
               rows: [{ id: "is-a", region: "4", water: "W", effective: "", from: null, to: null, change: "No fishing" }] };
const LINKS = { format: "inseason-links/1", matches: { "is-a": { extents: [{ item: "gnis:1", op: "whole" }] } } };
const mem = () => { let t: string | null = null; return { read: () => t, write: (x: string) => { t = x; } }; };
const serve = (files: Record<string, unknown>): typeof fetch => (async (u: string) => {
  const name = String(u).split("/").pop()!;
  return name in files ? new Response(JSON.stringify(files[name])) : new Response("", { status: 404 });
}) as unknown as typeof fetch;

describe("httpInSeason", () => {
  it("reads the pair, keeps the capture, and answers offline from it", async () => {
    const store = mem();
    const got = await httpInSeason("http://x/feeds/inseason/", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    expect([got?.feed.rows.length, Object.keys(got?.links?.matches ?? {})]).toEqual([1, ["is-a"]]);
    const off = await httpInSeason("http://x", store, (async () => { throw new Error("offline"); }) as unknown as typeof fetch).latest();
    expect(off?.links?.matches["is-a"]).toBeTruthy();
  });
  it("a feed fetched without its links keeps the captured links, never none", async () => {
    const store = mem();
    await httpInSeason("http://x", store, serve({ "index.json": FEED, "matches.json": LINKS })).latest();
    const got = await httpInSeason("http://x", store, serve({ "index.json": FEED })).latest();
    expect(got?.links?.matches["is-a"]).toBeTruthy();
  });
  it("nothing ever captured and nothing reached: null, never a throw", async () => {
    expect(await httpInSeason("http://x", mem(), serve({})).latest()).toBeNull();
  });
});
