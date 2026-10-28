// T225: the Worker-only parts of the DFO port the Python parity cannot see — the live fetch (validation, retries,
// Retry-After), what lands in the PRIVATE store (raw page, manifest, history, notified state) vs the published one,
// and the default entry source (priv `entries/region-<slug>.json`).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { runDfo } from "../src/dfo";
import { fetchRegion } from "../src/dfo/fetch";
import { MemStore, REPO } from "./dfo.scenario";

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
    const priv = new MemStore();
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
        "manifest.json",
        "notified.json",
        "raw/region4-eng.html",
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
