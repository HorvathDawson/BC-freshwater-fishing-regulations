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
// a feed object whose content did not change is not rewritten (index.json: a 24 h heartbeat keeps "checked" fresh).
describe("change-aware DFO runs", () => {
  const slugs = ["4", "6", "8"];
  const entries = async (slug: string) => readFileSync(join(FIX, `entries/region-${slug}.json`), "utf-8");
  const run = (feed: MemStore, priv: MemStore, now: string, pages: Record<string, string>, extra: Partial<Parameters<typeof runDfo>[0]> = {}) =>
    runDfo({ fetch: async () => new Response(""), feed, priv, entries, now, regions: slugs, fetcher: async (s) => [pages[s]!, null], ...extra });
  const pages = Object.fromEntries(slugs.map((s) => [s, page(s)]));

  it("unchanged pages: typing skipped, answers identical to a full run, feed writes skipped until the heartbeat", async () => {
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
    expect(puts).toEqual([]); // neither index.json nor unplaced.json rewritten
    expect(m.writesSkipped).toBe(2);
    // the carried answers ARE the full run's answers
    const full = await run(new MemStore(), privStore(), "2026-10-28T18:00:00Z", pages, { changeAware: false });
    const { feedText } = await import("../src/dfo/pyjson");
    const { stableIndex } = await import("../src/dfo/live");
    const strip = (x: unknown) => stableIndex(feedText(x)).replace(/"changed":"[^"]*"/g, "");
    expect(strip(second.index)).toBe(strip(full.index));
    expect(JSON.parse(first).locs).toEqual((second.index as any).locs);
    // a day later the heartbeat rewrites index.json (unplaced.json still unchanged)
    puts.length = 0;
    await run(watched, priv, "2026-10-29T19:00:00Z", pages);
    expect(puts).toEqual(["index.json"]);
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
