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
 */
import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const FEEDS = here("../packages/data/dev/feeds/gauge");
const read = (f: string) => JSON.parse(readFileSync(`${FEEDS}/${f}`, "utf8"));
const files = readdirSync(FEEDS).filter((f) => f.endsWith(".json"));

/** Every string anywhere in a JSON tree, keys included. */
function strings(v: unknown, out: string[] = []): string[] {
  if (typeof v === "string") out.push(v);
  else if (Array.isArray(v)) for (const x of v) strings(x, out);
  else if (v && typeof v === "object")
    for (const [k, x] of Object.entries(v)) { out.push(k); strings(x, out); }
  return out;
}

describe("the feed / bundle contract", () => {
  it("has feeds to check at all", () => {
    expect(files.length).toBeGreaterThan(1);
  });

  it("never puts a section id in a feed", () => {
    // "{blk}:{measure}" — the shape section ids take. They must not leave the bundle.
    const looksLikeSection = /^\d{6,}:\d+$/;
    for (const f of files)
      for (const s of strings(read(f)))
        expect(looksLikeSection.test(s), `${f} carries a section id: ${s}`).toBe(false);
  });

  it("never re-ships station identity the bundle already holds", () => {
    // Each of these was in v1's index and is now a column of `gauge`.
    const owned = ["name", "at", "areaKm2", "area_km2", "river", "section", "lon", "lat",
                   "mag", "active"];
    const idx = read("index.json");
    for (const station of Object.values(idx.stations as Record<string, object>))
      for (const k of Object.keys(station))
        expect(owned, `the index re-ships '${k}', which the bundle owns`).not.toContain(k);
  });

  it("keeps the forecast to percentiles, which is what colours a dot", () => {
    // Discharges would force every client to hold the envelope to colour anything, and
    // then two clients could disagree about the same river. The publisher decides once.
    const idx = read("index.json");
    for (const s of Object.values(idx.stations as Record<string, { forecast: unknown }>)) {
      if (s.forecast === null) continue;
      expect(Array.isArray(s.forecast)).toBe(true);
      for (const v of s.forecast as unknown[])
        expect(v === null || (typeof v === "number" && v >= 0 && v <= 1)).toBe(true);
    }
  });

  it("keeps the index to what actually colours a dot", () => {
    // Its whole job is 450 stations without 450 fetches. Anything past the value that
    // picks the colour and the time that says whether to trust it belongs in the file a
    // tap fetches.
    const idx = read("index.json");
    for (const station of Object.values(idx.stations as Record<string, object>))
      // `forecast` earns its place: it is three more numbers that colour the same dots on
      // a 1/2/3-day view, which is the index's whole job. Anything past that is sheet data.
      expect(Object.keys(station).sort())
        .toEqual(["forecast", "observedAt", "percentile"]);
  });

  it("is fetchable by station id and nothing else", () => {
    // The name of the file IS the whole request. A cron that needed anything more would
    // need the bundle, and then it would need matching, and then it is not a feed.
    const db = new DatabaseSync(here("../packages/data/dev/bundle.sqlite"));
    const known = new Set((db.prepare("SELECT station FROM gauge").all() as
      { station: string }[]).map((r) => r.station));
    db.close();
    for (const f of files.filter((x) => x !== "index.json"))
      expect(known, `${f} is not a station in the bundle`).toContain(f.replace(".json", ""));
  });

  it("stamps every feed with when it was fetched, because staleness is the risk", () => {
    for (const f of files) expect(read(f).fetchedAt, `${f} has no fetchedAt`).toBeTruthy();
  });
});
