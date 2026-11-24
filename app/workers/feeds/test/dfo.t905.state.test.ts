// T905 (owner 2026-11-24, "Move worker state out"): each region's own bookkeeping (STATE_KEYS: the page hash and when it
// last moved, the fetches that failed in a row, the header digest, the header names no water binds, the page's general
// text) lives in priv state.json, never in the published index. THE CUTOVER: the first run after the deploy reads a last
// index that a worker before T905 wrote (the keys on each region) and no state.json — it seeds the state from that
// index once, so no page counts as changed, no page text is re-told, the failures count on, no phone is woken, and the
// published index loses only those keys: every place's year and rows, every word an app reads, as before.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runDfo } from "../src/dfo";
import { pageTexts, STATE_KEY, STATE_KEYS } from "../src/dfo/live";
import { dfoAnswersChanged } from "../src/push";
import { MemStore, privStore } from "./dfo.scenario";

const FIX = join(__dirname, "fixtures/dfo");
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8").replace(/\r\n?/g, "\n");
const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
const V = JSON.parse(readFileSync(join(FIX, "python-vectors.generated.json"), "utf-8")) as { pages: Record<string, unknown> };
const SLUGS = Object.keys(V.pages);
const T0 = "2026-10-28T12:00:00Z", T1 = "2026-10-28T18:00:00Z", T2 = "2026-10-29T00:00:00Z";
type Json = any; // eslint-disable-line @typescript-eslint/no-explicit-any

const run = (feed: MemStore, priv: MemStore, now: string, o: { pageOf?: (s: string) => string; changeAware?: boolean; down?: string } = {}) =>
  runDfo({ fetch: async () => new Response(""), feed, priv, now, regions: SLUGS, entries, changeAware: o.changeAware,
           fetcher: async (s) => { if (s === o.down) throw new Error("HTTP 503"); return [(o.pageOf ?? page)(s), null]; } });

/** every place as an app reads it, in words (each run numbers its tables its own way) */
const words = (ix: Json) => Object.fromEntries(Object.entries<Json>(ix.locs).map(([lid, c]) => {
  const w = (runs: number[], items: unknown[]) => runs.map((x, k) => (k % 2 === 0 || x < 0 ? x : items[x]));
  return [lid, { ...c, sp: c.sp.map((r: number[]) => w(r, ix.verdicts)), g: w(c.g, ix.gears), ...(c.o ? { o: w(c.o, ix.gears) } : {}) }];
}));
const without = (r: Json, drop: readonly string[]) => Object.fromEntries(Object.entries(r).filter(([k]) => !drop.includes(k)));

/** the feed + private store as a worker before T905 left them: the bookkeeping on each published region, no state.json */
async function beforeT905(pageOf = page) {
  const feed = new MemStore(), priv = privStore();
  const first = await run(feed, priv, T0, { pageOf });
  const old = JSON.parse(feed.m.get("index.json")!);
  for (const [slug, r] of Object.entries<Json>(old.regions)) Object.assign(r, first.state.regions[slug]);
  feed.m.set("index.json", JSON.stringify(old));
  priv.m.delete(STATE_KEY);
  return { feed, priv, old };
}

describe("T905: the region bookkeeping is private", () => {
  it("the published index carries none of it; priv state.json carries all of it, for every region", async () => {
    const feed = new MemStore(), priv = privStore();
    const got = await run(feed, priv, T0);
    const ix = JSON.parse(feed.m.get("index.json")!);
    for (const r of Object.values<Json>(ix.regions)) expect(Object.keys(r).filter((k) => (STATE_KEYS as readonly string[]).includes(k))).toEqual([]);
    const st = JSON.parse(priv.m.get(STATE_KEY)!);
    expect(st).toEqual(JSON.parse(JSON.stringify(got.state)));
    expect(st.format).toBe("dfo-state/1");
    expect(Object.keys(st.regions).sort()).toEqual(Object.keys(ix.regions).sort());
    for (const r of Object.values<Json>(st.regions)) expect(Object.keys(r).sort()).toEqual([...STATE_KEYS].sort());
    expect(st.regions["4"].sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(st.regions["4"].texts.length).toBeGreaterThan(0);
  });

  it.each([true, false])("the cutover (change-aware %s): seeded once from the last index — nothing changed, nothing told, no wake, no row lost", async (aware) => {
    const { feed, priv, old } = await beforeT905();
    const logs: string[] = [];
    const got = await runDfo({ fetch: async () => new Response(""), feed, priv, now: T1, regions: SLUGS, entries, changeAware: aware,
                               log: (s) => logs.push(s), fetcher: async (s) => [page(s), null] });
    expect(logs.filter((l) => l.includes("seeded once from the last index"))).toHaveLength(1);
    const ix = JSON.parse(feed.m.get("index.json")!);
    // the bookkeeping carried as the last index held it: no page "changed" (still T0), the same hash, header, texts
    for (const [slug, r] of Object.entries<Json>(old.regions))
      expect(got.state.regions[slug]).toEqual(Object.fromEntries(STATE_KEYS.map((k) => [k, r[k]])));
    expect(Object.values<Json>(got.state.regions).every((r) => r.changed === T0)).toBe(true);
    expect(got.notices.filter((n) => n.key.startsWith("dfo-text:") || n.key.startsWith("dfo-header"))).toEqual([]);
    // the phones: not woken by the keys leaving the index (push.ts REGION_BOOKKEEPING)
    expect(dfoAnswersChanged(old, got.index)).toBe(false);
    // nothing an app reads is lost: every place in words (its year, gear lines, rows as printed `pr` and in DFO's
    // words `x`), every region's record but its bookkeeping and its `checked` (a later read), the issues
    expect(words(ix)).toEqual(words(old));
    expect(Object.values<Json>(ix.locs).reduce((n, c) => n + (c.pr?.length ?? 0), 0)).toBeGreaterThan(400);
    for (const [slug, r] of Object.entries<Json>(old.regions))
      expect(without(ix.regions[slug], ["checked"])).toEqual(without(r, [...STATE_KEYS, "checked"]));
    expect(ix.issues.waters).toEqual(old.issues.waters);
    // the next run reads state.json (the index has no keys left): still nothing changed, nothing seeded again
    logs.length = 0;
    const again = await runDfo({ fetch: async () => new Response(""), feed, priv, now: T2, regions: SLUGS, entries, changeAware: aware,
                                 log: (s) => logs.push(s), fetcher: async (s) => [page(s), null] });
    expect(logs.filter((l) => l.includes("seeded"))).toEqual([]);
    expect(again.state).toEqual(got.state);
    expect(dfoAnswersChanged(got.index, again.index)).toBe(false);
  });

  it("the cutover counts a failing region's fetches on from the last index's", async () => {
    const { feed, priv, old } = await beforeT905();
    old.regions["4"].failures = 2;
    feed.m.set("index.json", JSON.stringify(old));
    const got = await run(feed, priv, T1, { down: "4" });
    expect(got.state.regions["4"]!.failures).toBe(3);
    expect(got.state.regions["4"]!.sha256).toBe(old.regions["4"].sha256);
  });

  it("a page's general text that changed after the cutover is told once, from the seeded text", async () => {
    const { feed, priv } = await beforeT905();
    const was = "Learn more about fishing for salmon in B.C.";
    expect(pageTexts(page("1"), "1")).toContain(was);
    const moved = (s: string) => (s === "1" ? page("1").replace(was, "Learn more about salmon in B.C.") : page(s));
    const got = await run(feed, priv, T1, { pageOf: moved });
    const told = got.notices.filter((n) => n.key.startsWith("dfo-text:"));
    expect(told.map((n) => n.text.split("\n"))).toEqual([[
      "DFO Region 1: the page's general text changed — review the header; the feed reads no rule from it (1 gone, 1 new)",
      `- was: ${was}`, "+ now: Learn more about salmon in B.C."]]);
    expect(got.state.regions["1"]!.changed).toBe(T1);           // its bytes moved: changed, in the private state only
  });

  it("a state.json that will not read, or is not a dfo-state/1, fails the run; the last index stands", async () => {
    for (const bad of ['{"format": "dfo-state/1", "regi', '{"format": "dfo-state/2", "regions": {}}', "[]", '{"format": "dfo-state/1", "regions": []}']) {
      const feed = new MemStore(), priv = privStore();
      await run(feed, priv, T0);
      const last = feed.m.get("index.json");
      priv.m.set(STATE_KEY, bad);
      await expect(run(feed, priv, T1)).rejects.toThrow(/state\.json (is stored but does not parse|is not a dfo-state\/1)/);
      expect(feed.m.get("index.json")).toBe(last);
      expect(priv.m.get(STATE_KEY)).toBe(bad);
    }
  });

  it("the order: the cutover puts the state first; steady state the index first, and a failure between them tells the change once", async () => {
    const order: string[] = [];
    const watch = (s: MemStore, tag: string, fail?: () => string | null) => {
      const put = s.put.bind(s);
      s.put = async (k: string, b: string) => {
        order.push(`${tag}:${k}`);
        if (fail?.() === k) throw new Error("R2 PUT 500");
        await put(k, b);
      };
      return s;
    };
    let failKey: string | null = null;
    const feed = watch(new MemStore(), "feed"), priv = watch(privStore(), "priv", () => failKey);
    const posted: string[] = [];
    const go = (now: string, pageOf = page) => runDfo({ feed, priv, now, regions: ["1"], entries, notifyUrl: "https://ntfy.example/dfo",
      fetch: async (_u: unknown, init?: RequestInit) => { posted.push(String(init?.body ?? "")); return new Response("ok"); },
      fetcher: async (s) => [pageOf(s), null] });
    const firstTwo = () => order.filter((k) => k === `priv:${STATE_KEY}` || k === "feed:index.json");
    await go(T0);
    expect(firstTwo()).toEqual([`priv:${STATE_KEY}`, "feed:index.json"]);            // the cutover: state first
    const was = "Learn more about fishing for salmon in B.C.";
    const moved = (s: string) => (s === "1" ? page("1").replace(was, "Learn more about salmon in B.C.") : page(s));
    const last = priv.m.get(STATE_KEY);
    order.length = 0;
    failKey = STATE_KEY;
    await expect(go(T1, moved)).rejects.toThrow(/R2 PUT 500/);
    expect(firstTwo()).toEqual(["feed:index.json", `priv:${STATE_KEY}`]);            // steady: the index first
    expect(JSON.parse(feed.m.get("index.json")!).at).toBe(T1);
    expect([priv.m.get(STATE_KEY) === last, posted.some((b) => b.includes("general text changed"))]).toEqual([true, false]);
    failKey = null;
    const again = await go(T2, moved);
    await go("2026-10-29T06:00:00Z", moved);
    expect(posted.filter((b) => b.includes("general text changed"))).toHaveLength(1);  // told once, by the next run
    expect(again.state.regions["1"]!.changed).toBe(T2);                              // what repeats: `changed` moves on
  });

  it("after a rollback the state wins over keys an old worker left on the index", async () => {
    const feed = new MemStore(), priv = privStore();
    const first = await run(feed, priv, T0);
    const ix = JSON.parse(feed.m.get("index.json")!);
    Object.assign(ix.regions["1"], { sha256: "0".repeat(64), changed: "2020-01-01T00:00:00Z", failures: 7, header: "x", unresolved: [],
                                     texts: ["a text the rolled-back worker read"] });
    feed.m.set("index.json", JSON.stringify(ix));
    const got = await run(feed, priv, T1, { changeAware: false });
    expect(got.state.regions["1"]).toEqual(first.state.regions["1"]);              // the same page: nothing moved
    expect(got.notices.filter((n) => n.key.startsWith("dfo-text:"))).toEqual([]);
    expect(Object.keys(JSON.parse(feed.m.get("index.json")!).regions["1"]).filter((k) => (STATE_KEYS as readonly string[]).includes(k))).toEqual([]);
  });

  it("an unchanged state is not rewritten (one R2 put saved a run)", async () => {
    const feed = new MemStore(), priv = privStore();
    await run(feed, priv, T0);
    const puts: string[] = [];
    const put = priv.put.bind(priv);
    priv.put = async (k: string, b: string) => { puts.push(k); await put(k, b); };
    await run(feed, priv, T1);
    expect(puts).not.toContain(STATE_KEY);
  });
});
