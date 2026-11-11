/**
 * The line between the bundle and a feed, enforced.
 *
 * ONE RULE: a feed carries what CHANGES; the bundle carries what a thing IS. Nothing
 * appears in both. This is not tidiness — two copies of a name is two names that can
 * disagree, and the artifacts are refreshed on different clocks, so the disagreement is
 * guaranteed rather than possible.
 *
 * The specific failure this guards is worse than duplication. v1's gauge index shipped
 * `section: "356363467:4738"`. Section ids survive a rebuild only 94% of the time
 * (AGENTS rule 5), so a feed keyed on one is silently wrong for 6% of reaches the moment
 * the tiles are rebuilt — and nothing anywhere would report it.
 *
 * (T557 item 3 / T633 item 3) The checks themselves — format, fetchedAt, no section id, nothing the bundle owns, only
 * keys the app reads, percentiles where a colour is picked — are ONE function, `gaugeFeedContract`
 * (./gauge-feed-contract.ts), run here on the fixture and in workers/feeds/test/gauges.contract.test.ts on the worker's
 * own output. The fixture once kept an index `forecast` array the publisher never wrote, and nothing said so.
 */
import { describe, expect, it } from "vitest";
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { HORIZONS } from "@app/core";
import { gaugeFeedContract } from "./gauge-feed-contract";
import { FIXTURE_RELEASE_ROOT, FIXTURE_RELEASES } from "../conformance/src/fixtureRelease";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const FEEDS = here("../packages/data/dev/feeds/gauge");
const read = (f: string) => JSON.parse(readFileSync(`${FEEDS}/${f}`, "utf8"));
const files = readdirSync(FEEDS).filter((f) => f.endsWith(".json"));

describe("the feed / bundle contract", () => {
  it("has feeds to check at all", () => {
    expect(files.length).toBeGreaterThan(1);
  });

  it("the fixture feed keeps the gauge feed contract (the one the worker's output is held to)", () => {
    expect(gaugeFeedContract(Object.fromEntries(files.map((f) => [f, read(f)])))).toEqual([]);
  });

  it("carries every forecast horizon we draw (a real feed leaves out a day no run reaches)", () => {
    const idx = read("index.json");
    for (const s of Object.values(idx.stations as Record<string, { ahead?: Record<string, unknown> }>))
      if (s.ahead !== undefined)
        expect(Object.keys(s.ahead).sort((a, b) => Number(a) - Number(b))).toEqual(HORIZONS.filter((d) => d > 0).map(String));
  });

  it("is fetchable by station id and nothing else", () => {
    // The name of the file IS the whole request. A cron that needed anything more would
    // need the bundle, and then it would need matching, and then it is not a feed.
    // the stations of the release the fixture feed was cut with: its conditions file's `gauge` table
    const dir = join(FIXTURE_RELEASE_ROOT, FIXTURE_RELEASES.plain);
    const conditions = readdirSync(dir).find((f) => f.startsWith("conditions-"))!;
    const path = join(mkdtempSync(join(tmpdir(), "feed-contract-")), "conditions.sqlite");
    writeFileSync(path, gunzipSync(readFileSync(join(dir, conditions))));
    const db = new DatabaseSync(path, { readOnly: true });
    const known = new Set((db.prepare("SELECT station FROM gauge").all() as
      { station: string }[]).map((r) => r.station));
    db.close();
    for (const f of files.filter((x) => x !== "index.json"))
      expect(known, `${f} is not a station in the fixture release`).toContain(f.replace(".json", ""));
  });
});
