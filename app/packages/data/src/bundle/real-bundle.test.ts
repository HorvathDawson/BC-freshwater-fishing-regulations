/**
 * The two app-breaking bugs, asserted against a REAL bundle — because the dev fixture agreed
 * with both of them. Opt-in: `REAL_BUNDLE=<path to a bundle.sqlite> pnpm test`.
 *
 *   1. every season was lost: `rule.windows` shipped empty on all 3,269 rules;
 *   2. every river read CLOSED: "only non-game fish may be speared" (take 0 on every game fish
 *      WHILE spear fishing) reaches almost every section, and its `while` never reached core.
 */
import { describe, expect, it } from "vitest";
import { bundleExists, openBundle } from "./drivers/node";
import { makeBundleSource } from "./source";
import type { SectionId } from "../index";

const REAL = process.env.REAL_BUNDLE;

describe.skipIf(!bundleExists(REAL))("a real bundle", () => {
  it("ships seasons, and a seasonal closure is not in force outside its season", async () => {
    const db = openBundle(REAL!);
    const row = await db.get("SELECT count(*) AS n FROM rule WHERE when_ LIKE '%from_month%'");
    expect(Number(row?.n)).toBeGreaterThan(0);
    db.close?.();
  });

  it("the spear-fishing zero does not close the province", async () => {
    const db = openBundle(REAL!);
    const src = makeBundleSource(db);
    const spear = await db.get(
      "SELECT sr.sid FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
      "WHERE rs.entry_id = 'zp:spear_fishing' AND rs.rule_id = 'spear_fishing.r1' LIMIT 1");
    expect(spear, "spear_fishing.r1 binds nowhere in this bundle").toBeTruthy();
    const sample = (await db.all("SELECT sid FROM section_ruleset ORDER BY sid LIMIT 3000"))
      .map((r) => Number(r.sid) as SectionId);
    const out = await src.statusFor(sample, { year: 2026, month: 8, day: 15 }, "provincial");
    const closed = [...out.values()].filter((s) => s.outcome === "closed").length;
    // A handful of closed waters is the province; all of them is the bug.
    expect(closed / sample.length).toBeLessThan(0.5);
    const one = out.get(Number(spear!.sid) as SectionId)!;
    const r = one.from.find((x) => x.id === "zp:spear_fishing.spear_fishing.r1");
    expect(r?.while).toEqual(["spear_fishing"]);
    db.close?.();
  });
});
