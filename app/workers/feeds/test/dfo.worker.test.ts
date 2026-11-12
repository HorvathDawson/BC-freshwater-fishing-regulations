// T225: the Worker-only parts of the DFO port the Python parity cannot see — the live fetch (validation, retries,
// Retry-After), what lands in the PRIVATE store (raw page, manifest, history, notified state) vs the published one,
// and the default entry source (priv `entries/region-<slug>.json`).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runDfo } from "../src/dfo";
import { fetchRegion } from "../src/dfo/fetch";
import { MemStore, privStore, REPO } from "./dfo.scenario";

const FIX = join(REPO, "app/workers/feeds/test/fixtures/dfo");
const page = (slug: string) => readFileSync(join(FIX, `pages/region${slug}-eng.html`), "utf-8");
const noSleep = async () => {};

describe("fetchRegion", () => {
  it("retries a 503 (honouring Retry-After) and an invalid body, then takes the page", async () => {
    const waits: number[] = [];
    const bodies = [new Response("busy", { status: 503, headers: { "Retry-After": "7" } }), new Response("<html>maintenance</html>"), new Response(page("4"))];
    const got = await fetchRegion(async () => bodies.shift()!, "4", { sleep: async (ms) => void waits.push(ms), random: () => 0 });
    expect(got.snap.attempts).toBe(3);
    expect(got.snap.warnings).toEqual(["attempt 1: HTTP 503", "attempt 2: body too small (24 B < 8000 B)"]);
    expect(waits).toEqual([7000, 4000]);
    expect(got.snap.date_modified).toBe("2025-04-01");
  });
  it("gives up after four attempts with FetchError", async () => {
    await expect(fetchRegion(async () => new Response("x", { status: 500 }), "6", { sleep: noSleep })).rejects.toThrow(
      "region 6: HTTP 500 after 4 attempts",
    );
  });
  it("a 404 is a transport failure and is retried too (raise_for_status)", async () => {
    let n = 0;
    await expect(
      fetchRegion(async () => (n++, new Response("gone", { status: 404 })), "6", { sleep: noSleep }),
    ).rejects.toThrow(/HTTPError: HTTP Error 404/);
    expect(n).toBe(4);
  });
});

describe("runDfo in the Worker shape", () => {
  it("live fetch: publishes only index/unplaced; raw, manifest, history, entries and notified stay private", async () => {
    const feed = new MemStore();
    const priv = privStore();
    for (const s of ["4", "7"]) priv.m.set(`entries/region-${s}.json`, readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8"));
    const posted: string[] = [];
    const fetch = async (url: string, init?: RequestInit) => {
      if (url.startsWith("https://ntfy.example/")) {
        posted.push(String(init?.body));
        return new Response("ok");
      }
      const slug = /region(\w+)-eng\.html$/.exec(url)![1]!;
      return new Response(slug === "7" ? "<html>down</html>" : page(slug));
    };
    // the previous feed says region 7 already failed once: this failure is the second in a row → notify
    feed.m.set("index.json", JSON.stringify({ regions: { "7": { failures: 1, checked: "2026-10-27T00:00:00+00:00", asof: "2026-04-01" } } }));
    {
      const got = await runDfo({ fetch, feed, priv, now: "2026-10-28T12:00:00Z", regions: ["4", "7"], notifyUrl: "https://ntfy.example/dfo", fetchOptions: { sleep: noSleep } });
      expect([...feed.m.keys()].sort()).toEqual(["index.json", "unplaced.json"]);
      expect([...priv.m.keys()].sort()).toEqual([
        "entries/region-4.json",
        "entries/region-7.json",
        "history/region4_20261028120000.html",
        "inputs.json",
        "manifest.json",
        "notified.json",
        "raw/region4-eng.html",
        "rows/region-4.json",               // (T366) the row digest's baseline: this page's rows
        "wording.json",                     // (T324) the synced registry, read only
        "writes.json",
      ]);
      const r7 = (got.index.regions as Record<string, { stale: boolean; failures: number }>)["7"]!;
      expect(r7).toMatchObject({ stale: true, failures: 2 });
      expect(got.notices.map((n) => n.key)).toEqual(["dfo-fetch:7:2026-10-27T00:00:00+00:00"]);
      expect(posted).toHaveLength(1);
      expect(JSON.parse(priv.m.get("notified.json")!).sent).toEqual(["dfo-fetch:7:2026-10-27T00:00:00+00:00"]);
      const manifest = JSON.parse(priv.m.get("manifest.json")!);
      expect(manifest["4"].changed).toBe(true);
      // a second run on the same page: no history write, no repeat notice (dedupe by the private notified state)
      priv.m.delete("history/region4_20261028120000.html");
      await runDfo({ fetch, feed, priv, now: "2026-10-28T18:00:00Z", regions: ["4", "7"], notifyUrl: "https://ntfy.example/dfo", fetchOptions: { sleep: noSleep } });
      expect([...priv.m.keys()].filter((k) => k.startsWith("history/"))).toEqual([]);
      expect(posted).toHaveLength(1); // the third failure has the same key (`checked` does not move while down): told once
    }
  });
});

// T225 follow-up (b): Python held the WHOLE region when one row's rules failed validation. Robust mode holds only the
// location that row binds to (its last answers carried, flagged in `holds`), and publishes the rest.
describe("one bad row holds only its location", () => {
  const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
  const clean = page("8");
  // a hook gap of 5 mm fails the catalogue (smaller than any gap the book prints) → ValidationError on that row
  const bad = clean.replace("<td>4 per day, only 2 over 50&nbsp;cm.</td>", "<td>4 per day, single barbless hook less than 5mm from point to shank</td>"); // (T321) spelled as DFO does: no new-wording notice
  const run = (feed: MemStore, html: string, extra: Partial<Parameters<typeof runDfo>[0]> = {}) =>
    runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), entries, now: "2026-10-28T12:00:00Z", regions: ["8"], fetcher: async () => [html, null], ...extra });

  it("robust: the region publishes, the bad row's location keeps its last answers and is listed", async () => {
    expect(bad).not.toBe(clean);
    const feed = new MemStore();
    const first = await run(feed, clean);
    const before = first.index as any;
    const got = await run(feed, bad);
    const idx = got.index as any;
    expect(idx.regions["8"].held).toBeNull();
    const holds = idx.regions["8"].holds;
    expect(holds).toHaveLength(1);
    expect(holds[0]).toMatchObject({ kept: "carried", why: expect.stringMatching(/^ValidationError: 1 validation error for CatalogueRule/) });
    const lid = holds[0].location_id;
    const verdictsOf = (ix: any, l: string) => ix.locs[l].sp.map((runs: number[]) => runs.map((x, k) => (k % 2 ? ix.verdicts[x] : x)));
    expect(verdictsOf(idx, lid)).toEqual(verdictsOf(before, lid)); // carried forward
    for (const l of Object.keys(before.locs)) if (l !== lid) expect(verdictsOf(idx, l)).toEqual(verdictsOf(before, l)); // the rest published
    expect(got.notices.map((n) => n.key)).toEqual([expect.stringMatching(/^dfo-row-held:8:/)]);
  });

  it("robust: with no last answer the held location reads `check`, not a gap", async () => {
    const got = await run(new MemStore(), bad);
    const idx = got.index as any;
    const h = idx.regions["8"].holds[0];
    expect(h.kept).toBe("check");
    const v = idx.locs[h.location_id].sp.map((runs: number[]) => idx.verdicts[runs[1]!]);
    expect(v.every((x: any) => x.s === "check")).toBe(true);
  });

  it("pythonParity: the same page holds the whole region (the Python's behaviour, kept for the parity tests)", async () => {
    const got = await run(new MemStore(), bad, { pythonParity: true });
    expect((got.index as any).regions["8"].held).toMatch(/^the page did not parse \(ValidationError/);
  });
});

// Owner 2026-10-28: be smart about change — an unchanged page + entry file is not re-typed (its answers carried), and
// unplaced.json is not rewritten while its content is the same. index.json is written by every run (T549 items 7, 10:
// its `at` is the check time the app's "Checked with DFO" and stale line read).
describe("change-aware DFO runs", () => {
  const slugs = ["4", "6", "8"];
  const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
  const run = (feed: MemStore, priv: MemStore, now: string, pages: Record<string, string>, extra: Partial<Parameters<typeof runDfo>[0]> = {}) =>
    runDfo({ fetch: async () => new Response(""), feed, priv, entries, now, regions: slugs, fetcher: async (s) => [pages[s]!, null], ...extra });
  const pages = Object.fromEntries(slugs.map((s) => [s, page(s)]));

  it("unchanged pages: typing skipped, answers identical to a full run; index.json still written (its `at` is this check)", async () => {
    const { newMeter } = await import("../src/io");
    const feed = new MemStore();
    const priv = privStore();
    await run(feed, priv, "2026-10-28T12:00:00Z", pages);
    const first = feed.m.get("index.json")!;
    const m = newMeter();
    const puts: string[] = [];
    const watched = Object.assign(Object.create(feed), { put: async (k: string, b: string) => (puts.push(k), feed.put(k, b)) });
    const second = await run(watched, priv, "2026-10-28T18:00:00Z", pages, { meter: m });
    expect(second.skipped).toEqual(slugs);
    expect(m.sourcesUnchanged).toBe(3);
    expect(puts).toEqual(["index.json"]); // unplaced.json unchanged: not rewritten
    expect(m.writesSkipped).toBe(1);
    expect(JSON.parse(feed.m.get("index.json")!).at).toBe("2026-10-28T18:00:00Z");
    // the carried answers ARE the full run's answers
    const full = await run(new MemStore(), privStore(), "2026-10-28T18:00:00Z", pages, { changeAware: false });
    const { feedText } = await import("../src/dfo/pyjson");
    // what a run stamps (`at`, each region's `checked` / `changed`) aside
    const strip = (x: unknown) => {
      const j = JSON.parse(feedText(x));
      delete j.at;
      for (const r of Object.values<any>(j.regions ?? {})) { delete r.checked; delete r.changed; }
      return JSON.stringify(j);
    };
    expect(strip(second.index)).toBe(strip(full.index));
    expect(JSON.parse(first).locs).toEqual((second.index as any).locs);
  });

  // T549 items 7, 10: with the 20 h heartbeat the in-season runs at +6, +12 and +18 h skipped the write, `at` moved once
  // a day and sat exactly on the app's stale line
  it("N unchanged runs at each cron cadence never leave `at` older than the app's stale line", async () => {
    const { STALE_AFTER } = await import("../../../packages/core/src/staleness");
    for (const cadenceH of [6, 24]) {
      const feed = new MemStore();
      const priv = privStore();
      const t0 = Date.parse("2026-10-28T00:15:00Z");
      for (let n = 0; n < 6; n++) {
        const now = t0 + n * cadenceH * 3600_000;
        if (n) {
          // just before this run publishes, the feed a phone holds is the last run's: one cadence old (+ a slow run)
          const age = now + 10 * 60_000 - Date.parse(JSON.parse(feed.m.get("index.json")!).at);
          expect(age).toBeLessThan(STALE_AFTER.dfo.ms);
        }
        const got = await run(feed, priv, new Date(now).toISOString().replace(".000Z", "Z"), pages);
        if (n) expect(got.skipped).toEqual(slugs);
        expect(Date.parse(JSON.parse(feed.m.get("index.json")!).at)).toBe(now);
      }
    }
  });

  it("a changed page re-types only its region", async () => {
    const feed = new MemStore();
    const priv = privStore();
    await run(feed, priv, "2026-10-28T12:00:00Z", pages);
    const changed = { ...pages, "8": pages["8"]!.replace("4 per day, only 2 over 50&nbsp;cm.", "3 per day, only 2 over 50&nbsp;cm.") };
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", changed);
    expect(got.skipped).toEqual(["4", "6"]);
  });

  // T366 (owner 2026-11-03): a rule-row change on a known place is ONE digest notice per page (`urgent: false`), with
  // old → new verbatim and how each new row was read; places stay urgent (their own notices)
  it("a changed row is one digest notice (urgent: false): was / now verbatim and how it was read", async () => {
    const feed = new MemStore();
    const priv = privStore();
    const first = await run(feed, priv, "2026-10-28T12:00:00Z", pages);
    expect(first.notices.filter((n) => n.key.startsWith("dfo-rows"))).toEqual([]); // no baseline yet: this page is it
    expect(JSON.parse(priv.m.get("rows/region-8.json")!).sha256).toMatch(/^[0-9a-f]{64}$/);
    const changed = { ...pages, "8": pages["8"]!.replace("4 per day, only 2 over 50&nbsp;cm.", "3 per day, only 2 over 50&nbsp;cm.") };
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", changed);
    const rows = got.notices.filter((n) => n.key.startsWith("dfo-rows"));
    expect(rows).toHaveLength(1);
    const n = rows[0]!;
    expect(n.key).toMatch(/^dfo-rows:8:[0-9a-f]{12}$/);
    expect(n.urgent).toBe(false);
    expect(got.notices.filter((x) => x !== n).every((x) => x.urgent !== false)).toBe(true);
    expect(n.text).toMatch(/^DFO Region 8: 1 place\(s\) with changed rows on the page of /);
    expect(n.text).toMatch(/\n {4}- was: .*4 per day, only 2 over 50/);
    expect(n.text).toMatch(/\n {4}\+ now: .*3 per day, only 2 over 50/);
    expect(n.text).toMatch(/\n {8}read: .*take 3/);
    expect(n.data).toMatchObject({ region: "8", kind: "rows", rows: [expect.objectContaining({ old: [expect.stringContaining("4 per day")], new: [expect.stringContaining("3 per day")] })] });
    // the same page again: no second notice (the baseline moved to it)
    const again = await run(feed, priv, "2026-10-29T00:00:00Z", changed, { changeAware: false });
    expect(again.notices.filter((x) => x.key.startsWith("dfo-rows"))).toEqual([]);
  });

  it("an unreadable row baseline is told (urgent), never read as 'no change', and is rewritten", async () => {
    const feed = new MemStore();
    const priv = privStore();
    await run(feed, priv, "2026-10-28T12:00:00Z", pages);
    priv.m.set("rows/region-8.json", "{not json");
    const changed = { ...pages, "8": pages["8"]!.replace("4 per day, only 2 over 50&nbsp;cm.", "3 per day, only 2 over 50&nbsp;cm.") };
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", changed);
    const n = got.notices.filter((x) => x.key.startsWith("dfo-rows"));
    expect(n.map((x) => x.key)).toEqual([expect.stringMatching(/^dfo-rows-baseline:8:/)]);
    expect(n[0]!.urgent).not.toBe(false);
    expect(n[0]!.text).toContain("were NOT compared");
    expect(() => JSON.parse(priv.m.get("rows/region-8.json")!)).not.toThrow();
  });

  it("a new entry file (curation) re-types its region even on the same page", async () => {
    const feed = new MemStore();
    const priv = privStore();
    await run(feed, priv, "2026-10-28T12:00:00Z", pages);
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", pages, {
      entries: async (s) => (await entries(s)) + (s === "6" ? "\n" : ""),
    });
    expect(got.skipped).toEqual(["4", "8"]);
  });
});

// (T521) a DFO place no curated location knows lands in unplaced.json with the app items of the water it names, so the
// app shows it on that water's page ("DFO rule, location not yet on the map"); the Python's `_items_of` does the same.
describe("an unplaced row names its water's app items (T521)", () => {
  const raw = readFileSync(join(FIX, "entries/region-8.json"), "utf-8");
  const d = JSON.parse(raw);
  const gone = d.locations.find((l: any) => l.water === "Mabel Lake");
  const forgot = JSON.stringify({ ...d, locations: d.locations.filter((l: any) => l !== gone) });
  const run = (extra: Partial<Parameters<typeof runDfo>[0]> = {}) =>
    runDfo({ fetch: async () => new Response(""), feed: new MemStore(), priv: privStore(), entries: async () => forgot,
             now: "2026-10-28T12:00:00Z", regions: ["8"], fetcher: async () => [page("8"), null], ...extra });

  for (const pythonParity of [false, true])
    it(`${pythonParity ? "pythonParity" : "robust"}: the row carries \`items\` (the binding's item ids)`, async () => {
      const got = await run({ pythonParity });
      const u = got.unplaced.find((x: any) => x.water === "Mabel Lake") as any;
      expect(u).toMatchObject({ region: "8", kind: "new", items: ["wbk:329663955"] });
      expect(u.rows.length).toBeGreaterThan(0);
    });
});

// T549 item 4: a curated place no row of the page matches any more left `locs` in silence, so the WIDER rule of its chain
// answered on the map and nobody was told. Reworded (a drift row names it as its likely place): it reads `check`.
// Gone (no row explains it): listed in the region's `gone`, the owner told once per page it left on.
describe("a curated place that leaves DFO's page (T549 item 4)", () => {
  const MABEL = "8:mabel-lake:northerly-of-a-line-drawn-from-a-w";
  // the entry file without its recorded page structure: Region 8's table is 3 waters, so one dropped row is a third of
  // it and trips the STRUCTURAL hold (the whole region held, its own notice) — a bigger region loses a row without it
  const entries = async () => JSON.stringify({ ...JSON.parse(readFileSync(join(FIX, "entries/region-8.json"), "utf-8")), structure: {} });
  const run = (feed: MemStore, priv: MemStore, now: string, html: string) =>
    runDfo({ fetch: async () => new Response(""), feed, priv, entries, now, regions: ["8"], fetcher: async () => [html, null] });
  const reworded = page("8").replace("approximately 1&nbsp;km from Wap Creek", "approximately 2&nbsp;km from Wap Creek");
  const dropped = page("8").replace(/<tr>\s*<td>Mabel Lake<\/td>[\s\S]*?<\/tr>/, "");
  const verdictsOf = (ix: any, lid: string) => [...new Set((ix.locs[lid].sp as number[][]).flatMap((runs) => runs.filter((x, k) => k % 2 === 1 && x >= 0)))].map((v) => ix.verdicts[v]);

  it("the fixtures do what the test says: one page rewords the Mabel Lake place, the other drops its row", () => {
    expect(reworded).not.toBe(page("8"));
    expect(dropped).not.toContain("<td>Mabel Lake</td>");
    expect(dropped.length).toBeLessThan(page("8").length - 200);
  });

  it("reworded: the place stays in the feed as `check` (never the wider rule), is in `holds`, and its row is held unplaced", async () => {
    const feed = new MemStore();
    const priv = privStore();
    const first = await run(feed, priv, "2026-10-28T12:00:00Z", page("8"));
    expect(verdictsOf(first.index, MABEL).some((v: any) => v.s === "check")).toBe(false);
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", reworded);
    const ix = got.index as any;
    expect(ix.regions["8"].held).toBeNull();
    expect(verdictsOf(ix, MABEL)).toEqual([{ s: "check", t: "DFO reworded this place and the new wording is not confirmed yet — check DFO" }]);
    expect(ix.regions["8"].holds).toEqual([expect.objectContaining({ location_id: MABEL, kept: "check", why: "DFO reworded this place; the new wording is held unplaced until it is confirmed" })]);
    expect(ix.regions["8"].gone).toEqual([]);
    expect(got.unplaced).toEqual([expect.objectContaining({ kind: "drift", water: "Mabel Lake", candidates: [expect.objectContaining({ location_id: MABEL })] })]);
    expect(got.notices.filter((n) => n.key.startsWith("dfo-drift:"))).toHaveLength(1);
    expect(got.notices.filter((n) => n.key.startsWith("dfo-gone:"))).toEqual([]);
    // the same page next run (typing skipped): the place still reads check and is still listed
    const again = await run(feed, priv, "2026-10-29T00:00:00Z", reworded);
    expect(again.skipped).toEqual(["8"]);
    expect(verdictsOf(again.index, MABEL).every((v: any) => v.s === "check")).toBe(true);
    expect((again.index as any).regions["8"].holds).toHaveLength(1);
    // the published feed is what the app's parser accepts as holds (location_id + kept)
    expect(JSON.parse(feed.m.get("index.json")!).regions["8"].holds[0]).toMatchObject({ location_id: MABEL, kept: "check" });
  });

  it("gone: the place leaves `locs`, is listed in the region's `gone`, and the owner is told once (re-offered while it lasts)", async () => {
    const feed = new MemStore();
    const priv = privStore();
    await run(feed, priv, "2026-10-28T12:00:00Z", page("8"));
    const got = await run(feed, priv, "2026-10-28T18:00:00Z", dropped);
    const ix = got.index as any;
    expect(ix.regions["8"].held).toBeNull();
    expect(MABEL in ix.locs).toBe(false);
    expect(ix.regions["8"].gone).toEqual([{ location_id: MABEL, water: "Mabel Lake", place: expect.stringMatching(/^Northerly of a line/), page: expect.stringMatching(/^[0-9a-f]{12}$/) }]);
    const told = got.notices.filter((n) => n.key.startsWith("dfo-gone:"));
    expect(told).toHaveLength(1);
    expect(told[0]!.key).toBe(`dfo-gone:8:${MABEL}:${ix.regions["8"].gone[0].page}`);
    expect(told[0]!.urgent).not.toBe(false);
    expect(told[0]!.text).toMatch(new RegExp(`^DFO Region 8: ${MABEL} \\(Mabel Lake: '.*'\\) is no longer on DFO's page: its rule left the feed and the wider rule of its water answers\\. Read DFO's page: if the row is still printed, the page was misread$`));
    expect(told[0]!.data).toMatchObject({ region: "8", kind: "gone", water: "Mabel Lake" });
    // the same page next run (typing skipped): the same key is offered again, so a send that failed is retried
    const again = await run(feed, priv, "2026-10-29T00:00:00Z", dropped);
    expect(again.skipped).toEqual(["8"]);
    expect(again.notices.filter((n) => n.key.startsWith("dfo-gone:")).map((n) => n.key)).toEqual([told[0]!.key]);
    // the row is back: the place is published again and no longer listed
    const back = await run(feed, priv, "2026-10-30T00:00:00Z", page("8"));
    expect(MABEL in (back.index as any).locs).toBe(true);
    expect((back.index as any).regions["8"].gone).toEqual([]);
    expect(back.notices.filter((n) => n.key.startsWith("dfo-gone:"))).toEqual([]);
  });

  it("a place the last feed never published says nothing (a first run, an entry curated before its row)", async () => {
    const got = await run(new MemStore(), privStore(), "2026-10-28T12:00:00Z", dropped);
    expect((got.index as any).regions["8"].gone).toEqual([]);
    expect(got.notices.filter((n) => n.key.startsWith("dfo-gone:"))).toEqual([]);
  });
});

// T668 follow-up, THE KNOWN DIFFERENCE (pinned, not fixed): on a section in two DFO regions the RELEASE keeps the stricter
// of two readable openings by the freshwater Z4 / Z4a ladder (pipeline `deliver.salmon.stricter_opening`: `ladder.stricter`
// + `rules.statement`), while under a LIVE feed the app shows both regions' words with no number (never looser; core
// salmonFeed.ts, salmonStraddle.test.ts "under the feed"). Closing it means the feed carrying the composed straddler
// answer. The worker cannot make it without a TypeScript copy of that ladder (owner 2026-11-02: the cascade is resolved
// in Python; the worker and the app only read results), and it does not know the release's chains (which places share a
// section is the release's `chains[].s`). So the feed stays what this test says: one answer per DFO PLACE, nothing
// composed across places. When Python composes the straddlers for the feed, this test changes with it.
describe("(T668) the feed answers per place; it composes nothing across places or regions", () => {
  it("index.json is places and their own answers: no chains, no region groups", async () => {
    const feed = new MemStore();
    const slugs = ["6", "7"];                                              // the straddler pair of the release (Skeena / Fraser side)
    await runDfo({ fetch: async () => new Response(""), feed, priv: privStore(), now: "2026-10-28T12:00:00Z", regions: slugs,
                   entries: async (s) => readFileSync(join(FIX, `entries/region-${s}.json`), "utf-8"), fetcher: async (s) => [page(s), null] });
    const ix = JSON.parse(feed.m.get("index.json")!);
    expect(Object.keys(ix).sort()).toEqual(["at", "format", "gears", "locs", "regions", "unplaced", "verdicts"]);
    const locKeys = new Set(Object.values<Record<string, unknown>>(ix.locs).flatMap((l) => Object.keys(l)));
    expect([...locKeys].sort().filter((k) => !["g", "r", "sp", "x"].includes(k))).toEqual([]);
    // every place is ONE region's
    expect(new Set(Object.values<{ r: string }>(ix.locs).map((l) => l.r))).toEqual(new Set(slugs));
  });
});
