// (review 2026-11-20, M1) A DFO run wakes the iPhones only when the answers the phones read changed. Before, any run with
// a notice woke them: an unchanged region's notices are re-offered every run (dfo/live.ts, the outbox tells a key once),
// so every 6-hourly run pushed with nothing new; and a T851 one-region retry woke too (`regions` counted the whole index,
// `unchangedRegions` only that run's region). Real runs over the saved pages, then the reason the run would give.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runDfo } from "../src/dfo";
import { dfoAnswersChanged, wakeReason } from "../src/push";
import { MemStore, privStore } from "./dfo.scenario";

const FIX = join(__dirname, "fixtures/dfo");
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8").replace(/\r\n?/g, "\n");
const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
const V = JSON.parse(readFileSync(join(FIX, "python-vectors.generated.json"), "utf-8")) as {
  pages: Record<string, unknown>; live: { cases: Record<string, { slug: string; edits: [string, string][] }> };
};
const SLUGS = Object.keys(V.pages);

/** one DFO run as the worker counts it for the wake (index.ts): the index before, the run, the reason */
async function dfoRun(feed: MemStore, priv: MemStore, now: string, regions: string[], pageOf = page) {
  const before = feed.m.has("index.json") ? JSON.parse(feed.m.get("index.json")!) : null;
  const got = await runDfo({ fetch: async () => new Response(""), feed, priv, now, regions, entries, fetcher: async (s) => [pageOf(s), null] });
  const counts = { regions: Object.keys(got.index.regions as object).length, notices: got.notices.length,
                   unchangedRegions: (got.skipped ?? []).length, answersChanged: dfoAnswersChanged(before, got.index) ? 1 : 0 };
  return { got, counts, reason: wakeReason("dfo", true, counts) };
}

describe("a DFO run wakes only when the answers changed (M1)", () => {
  it("the first run (no feed before) wakes; the same pages again do not, though their notices are re-offered", async () => {
    // region 6 with a closed area no place binds (a vectors case): a notice every run while the page stays so
    const c = V.live.cases.closed_area_unbound!;
    let html = page(c.slug);
    for (const [a, b] of c.edits) html = html.replace(a, b);
    const pageOf = (s: string) => (s === c.slug ? html : page(s));
    const feed = new MemStore(), priv = privStore();
    const first = await dfoRun(feed, priv, "2026-10-28T12:00:00Z", SLUGS, pageOf);
    expect(first.reason).toBe("dfo");
    expect(first.counts.notices).toBeGreaterThan(0);
    const again = await dfoRun(feed, priv, "2026-10-28T18:00:00Z", SLUGS, pageOf);
    expect(again.got.skipped?.length).toBe(SLUGS.length);       // every region carried (page + entries unchanged)
    expect(again.counts.notices).toBeGreaterThan(0);            // the carried notices, re-offered
    expect(again.counts.answersChanged).toBe(0);
    expect(again.reason).toBe(null);
  });

  it("a one-region retry (T851) of an unchanged region does not wake", async () => {
    const feed = new MemStore(), priv = privStore();
    await dfoRun(feed, priv, "2026-10-28T12:00:00Z", SLUGS);
    const retry = await dfoRun(feed, priv, "2026-10-28T13:50:00Z", ["4"]);
    expect(retry.counts.regions - retry.counts.unchangedRegions).toBeGreaterThan(0);   // what the old rule woke on
    expect(retry.reason).toBe(null);
  });

  it("a region whose answers changed wakes (a place DFO dropped)", async () => {
    const c = V.live.cases.gone!;
    let html = page(c.slug);
    for (const [a, b] of c.edits) html = html.replace(a, b);
    const feed = new MemStore(), priv = privStore();
    await dfoRun(feed, priv, "2026-10-28T12:00:00Z", SLUGS);
    const changed = await dfoRun(feed, priv, "2026-10-28T18:00:00Z", [c.slug], (s) => (s === c.slug ? html : page(s)));
    expect(changed.counts.answersChanged).toBe(1);
    expect(changed.reason).toBe("dfo");
  });

  it("(re-review) a re-typed header note under the same asof wakes; the next run of that page does not", async () => {
    const NOTE = "There is no fishing for salmon in Region 4 - Kootenays until further notice.";
    const html = page("4").replace(NOTE, "There is no fishing for salmon in Region 4 - Kootenays until 2027.");
    expect(html).not.toBe(page("4"));
    const pageOf = (s: string) => (s === "4" ? html : page(s));
    const feed = new MemStore(), priv = privStore();
    await dfoRun(feed, priv, "2026-10-28T12:00:00Z", SLUGS);
    const prior = JSON.parse(feed.m.get("index.json")!);
    const before = prior.regions["4"];
    const moved = await dfoRun(feed, priv, "2026-10-28T18:00:00Z", ["4"], pageOf);
    // (on these pages the closure note also reaches the places' answers; the header alone is pinned below, by record)
    const after = (moved.got.index.regions as Record<string, { asof: string; notes: { t: string }[] }>)["4"]!;
    expect(after.asof).toBe(before.asof);                                    // the same date on the page
    expect(after.notes.map((n) => n.t)).toContain("There is no fishing for salmon in Region 4 - Kootenays until 2027.");
    expect(moved.reason).toBe("dfo");
    expect((await dfoRun(feed, priv, "2026-10-29T00:00:00Z", ["4"], pageOf)).reason).toBe(null);
  });

  it("the run's own bookkeeping is not a change; a region's date, stale mark or hold is", () => {
    const ix = { at: "a", locs: { L1: { r: "4", sp: [[0, 1]] } }, verdicts: ["x", "y"], gears: [], issues: { waters: {} },
                 regions: { "4": { asof: "2025-04-01", stale: false, held: null, checked: "t1", failures: 0, sha256: "s" } } };
    const run2 = { ...ix, at: "b", regions: { "4": { ...ix.regions["4"], checked: "t2", failures: 1, sha256: "s2", changed: "b" } } };
    expect(dfoAnswersChanged(ix, run2)).toBe(false);
    // (re-review) the region's whole record counts: a header note, a limit; and the top-level pairs
    expect(dfoAnswersChanged(ix, { ...ix, regions: { "4": { ...ix.regions["4"], notes: [{ k: "closed", t: "until 2027" }] } } })).toBe(true);
    // (T905) the keys that moved to the private state are bookkeeping too: the first run after that deploy reads a last
    // index carrying them (its header digest, unresolved names, texts…) and publishes one without — no wake
    const before = { ...ix, regions: { "4": { ...ix.regions["4"], changed: "a", header: "d1", unresolved: [{ name: "X" }], texts: ["t"] } } };
    expect(dfoAnswersChanged(before, ix)).toBe(false);
    expect(dfoAnswersChanged(ix, { ...ix, pairs: { a: "b" } })).toBe(true);
    expect(dfoAnswersChanged(ix, { ...ix, regions: { "4": { ...ix.regions["4"], stale: true } } })).toBe(true);
    expect(dfoAnswersChanged(ix, { ...ix, regions: { "4": { ...ix.regions["4"], asof: "2025-05-01" } } })).toBe(true);
    expect(dfoAnswersChanged(ix, { ...ix, verdicts: ["x", "z"] })).toBe(true);
    expect(dfoAnswersChanged(null, ix)).toBe(true);
    // notices never wake on their own
    expect(wakeReason("dfo", true, { regions: 9, unchangedRegions: 0, notices: 12, answersChanged: 0 })).toBe(null);
  });
});
