/**
 * (T620) THE SAME ANSWER ON EVERY SURFACE: one water on one day says one thing wherever the app says it. The surfaces
 * that state a water's regulation status or an instant are asked here for the pipeline's own fixture waters on a
 * closed day and the day after, and must agree: the search row (`rowStatus`), the status the map is handed
 * (`statusFeed`, then colours by `waterCodeOn` at ITS day), the core answer both read (`waterStatusOn`); a saved spot's
 * observed time and the Conditions card's time (`localStamp`). The day is always passed in: the Shell hands the search
 * sheet the map's day, and the source checks below fail if that wire is cut.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { decodeStatusIndex, localStamp, statusOfCode, waterStatusOn } from "@app/core";
import { statusFeed, rowStatus } from "@app/ui";
import { observedLine } from "@app/data/spots";
import { FIXTURE } from "../packages/core/src/statusIndex.fixture";

const INDEX = decodeStatusIndex(Uint8Array.from(atob(FIXTURE.bytes), (c) => c.charCodeAt(0)), FIXTURE.handles, FIXTURE.reach_digest);
const day = (iso: string) => { const [y, m, d] = iso.split("-").map(Number); return new Date(y!, m! - 1, d!); };
const ITEMS = [...new Set(FIXTURE.waters.map((w) => w[0] as string))];
const DAYS = [...new Set(FIXTURE.waters.map((w) => w[1] as string))];

describe("one water, one day, one status on every surface", () => {
  it("search row = the status the map is fed = core's answer, for every fixture water on every fixture day", () => {
    const disagree: unknown[] = [];
    for (const item of ITEMS) for (const iso of DAYS) {
      const d = day(iso);
      const feed = statusFeed({ statusOk: true, index: INDEX, day: d })!;
      const row = rowStatus(item as never, INDEX, d);
      const map = statusOfCode(feed.index.waterCodeOn(item, feed.day));
      const core = waterStatusOn(INDEX, item, d);
      if (row !== core || map !== core) disagree.push([item, iso, row, map, core]);
    }
    expect(disagree).toEqual([]);
    expect(ITEMS.length).toBeGreaterThan(1);
    // the fixture really has a water whose answer moves with the day (else the test could not see a wrong day)
    expect(new Set(DAYS.map((iso) => waterStatusOn(INDEX, "gnis:1", day(iso)))).size).toBeGreaterThan(1);
  });

  it("a saved spot's observed time is the Conditions card's time (one B.C. clock, named)", () => {
    for (const iso of ["2026-08-30T16:00:00Z", "2026-11-09T02:15:00Z"])
      expect(observedLine(iso)).toBe(`observed ${localStamp(Date.parse(iso))}`);
  });

  it("the wires: the Shell hands the search sheet the map's day, and a row's status has no day of its own", () => {
    const shell = readFileSync(new URL("../packages/ui-native/src/Shell.tsx", import.meta.url), "utf8");
    expect(/<SearchSheet[^>]*day=\{mapDay\}/s.test(shell)).toBe(true);
    const row = readFileSync(new URL("../packages/ui-native/src/search/SheetRow.tsx", import.meta.url), "utf8");
    expect(row).not.toMatch(/bcNow\(|new Date\(/);
    const search = readFileSync(new URL("../packages/ui/src/search.ts", import.meta.url), "utf8");
    expect(search).toMatch(/export function rowStatus\(item: ItemId, index: StatusIndex \| null,\s*on: Date\)/);
  });
});
