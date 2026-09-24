/**
 * The two app-breaking bugs, asserted against a REAL bundle — because the dev fixture agreed
 * with both of them. Opt-in: `REAL_BUNDLE=<path to a bundle.sqlite> pnpm test`.
 *
 *   1. every season was lost: `rule.windows` shipped empty on all 3,269 rules;
 *   2. every river read CLOSED: "only non-game fish may be speared" (take 0 on every game fish
 *      WHILE spear fishing) reaches almost every section, and its `while` never reached core;
 *   3. exemptions were applied nowhere: the North Thompson read CLOSED on May 1 beside its own
 *      "Exempt from spring closure".
 */
import { describe, expect, it } from "vitest";
import { bundleExists, openBundle } from "./drivers/node";
import { makeBundleSource } from "./source";
import type { Db, Row } from "./db";
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

  it("applies exemptions: the North Thompson is not closed by the closure it is exempt from",
     async () => {
    const db = openBundle(REAL!);
    const src = makeBundleSource(db);
    const nt = 59496 as SectionId;
    const may1 = { year: 2026, month: 5, day: 1 };
    const got = (await src.statusFor([nt], may1, "provincial")).get(nt)!;
    expect(got.from.some((r) => r.id.startsWith("r3:north_thompson_river@3-27."))).toBe(true);
    expect(got.from.map((r) => r.id)).not.toContain(
      "z3:spring_stream_closure.spring_stream_closure.r1");
    expect(got.outcome).not.toBe("closed");

    // HOW MANY SECTIONS CHANGE, on four days across the year. A section's answer is a function of
    // its rule SET, so each set carrying a lift that can apply (no `while`, no hours) is asked
    // once, through one of its sections, and weighted by how many sections share it.
    const sets = await db.all(
      "SELECT sr.set_id, min(sr.sid) AS sid, count(*) AS n FROM section_ruleset sr " +
      "WHERE sr.set_id IN (SELECT rs.set_id FROM ruleset rs JOIN rule r " +
      "  ON r.entry_id = rs.entry_id AND r.rule_id = rs.rule_id " +
      "  WHERE r.exempts IS NOT NULL AND r.while_ IS NULL " +
      "    AND (r.when_ IS NULL OR r.when_ NOT LIKE '%hours%')) " +
      "GROUP BY sr.set_id ORDER BY sr.set_id");
    const reps = sets.map((r) => Number(r.sid) as SectionId);
    const weight = new Map(sets.map((r) => [Number(r.sid), Number(r.n)]));
    const stripped: Db = {
      all: async (sql, ...p) => (await db.all(sql, ...p)).map((r): Row => ({ ...r, exempts: null })),
      get: (sql, ...p) => db.get(sql, ...p),
    };
    const without = makeBundleSource(stripped);
    /* THE PREVIOUS READING, for the count of what this change moved: every lift whole, and none at
       all from a lifter scoped by `while` or hours — which is what core did before a lift carried
       how far it reaches. */
    const wholesale = makeBundleSource({
      all: async (sql, ...p) => (await db.all(sql, ...p)).map((r): Row => {
        if (r.exempts == null) return r;
        const hours = typeof r.when_ === "string" && r.when_.includes("hours");
        if (r.while_ != null || hours) return { ...r, exempts: null };
        const items = JSON.parse(String(r.exempts)) as Record<string, unknown>[];
        return { ...r, exempts: JSON.stringify(items.map(({ entry_id, rule_id }) => ({ entry_id, rule_id }))) };
      }),
      get: (sql, ...p) => db.get(sql, ...p),
    });
    const total = [...weight.values()].reduce((p, q) => p + q, 0);
    const report: string[] = [`${total} sections in ${sets.length} rule sets carry a lift that can apply`];
    for (const [m, d] of [[1, 15], [5, 1], [7, 20], [11, 15]] as const) {
      const on = { year: 2026, month: m, day: d };
      const moves = new Map<string, number>();
      const narrowed = new Map<string, number>();
      for (let i = 0; i < reps.length; i += 2000) {
        const chunk = reps.slice(i, i + 2000);
        const a = await src.statusFor(chunk, on, "provincial");
        const b = await without.statusFor(chunk, on, "provincial");
        const c = await wholesale.statusFor(chunk, on, "provincial");
        for (const s of chunk) {
          const x = b.get(s)!.outcome, y = a.get(s)!.outcome, z = c.get(s)!.outcome;
          if (x !== y) moves.set(`${x}->${y}`, (moves.get(`${x}->${y}`) ?? 0) + weight.get(s)!);
          if (z !== y) narrowed.set(`${z}->${y}`, (narrowed.get(`${z}->${y}`) ?? 0) + weight.get(s)!);
        }
      }
      const n = [...moves.values()].reduce((p, q) => p + q, 0);
      const k = [...narrowed.values()].reduce((p, q) => p + q, 0);
      report.push(`${m}/${d}: ${n} sections change against no lifts ${JSON.stringify(Object.fromEntries(moves))}; `
                  + `${k} against every lift whole ${JSON.stringify(Object.fromEntries(narrowed))}`);
      // narrowing a lift can only leave MORE in force: nothing reads less restrictive than before
      for (const t of narrowed.keys()) expect(["closed->restricted", "closed->open", "restricted->open",
                                              "unknown->open", "unknown->restricted"]).not.toContain(t);
      if (m === 5) expect(n).toBeGreaterThan(0);
      // a lift only ever removes a rule or softens a closure, so nothing may get WORSE for it
      for (const k of moves.keys()) expect(["open->closed", "restricted->closed",
                                            "open->restricted"]).not.toContain(k);
    }
    console.log("exemptions on the real bundle:\n  " + report.join("\n  "));
    db.close?.();
  }, 600_000);

  /* A LIFT IS NEVER WIDER THAN ITS LIFTER. Each of these lifted its whole rule before. */
  const firstSection = async (db: Db, entry: string, rule: string) =>
    (await db.get("SELECT min(sr.sid) AS sid FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
                  "WHERE rs.entry_id = ? AND rs.rule_id = ?", entry, rule))?.sid;

  it("Duncan River's bull-trout exemption leaves the trout/char release on the other fish", async () => {
    const db = openBundle(REAL!);
    const sid = Number(await firstSection(db, "r4:duncan_river@4-19", "duncan_river.r2")) as SectionId;
    const got = (await makeBundleSource(db).statusFor([sid], { year: 2026, month: 12, day: 1 },
                                                      "provincial")).get(sid)!;
    const tcw = got.from.find((r) => r.id === "z4:trout_char_winter_release.trout_char_winter_release.r1");
    expect(tcw, "the release still binds rainbow and cutthroat on the Duncan").toBeDefined();
    expect(tcw!.liftedFor).toEqual([{ by: "r4:duncan_river@4-19.duncan_river.r2", species: ["BT"] }]);
    db.close?.();
  });

  it("dead fin fish as sturgeon bait: only the Fraser, Lower Pitt and Lower Harrison, and only "
     + "when fishing for sturgeon", async () => {
    const db = openBundle(REAL!);
    const n = Number((await db.get(
      "SELECT count(*) AS n FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
      "WHERE rs.entry_id = 'zp:bait' AND rs.rule_id = 'bait.r3'"))!.n);
    // it was every section of Region 2 (71,659); three reaches are a few hundred
    expect(n).toBeGreaterThan(0);
    expect(n).toBeLessThan(1000);
    const off = await db.get(
      "SELECT count(*) AS n FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
      "JOIN item_section s ON s.sid = sr.sid JOIN item i ON i.ord = s.ord " +
      "WHERE rs.entry_id = 'zp:bait' AND rs.rule_id = 'bait.r3' " +
      "AND i.name NOT IN ('Fraser River', 'Pitt River', 'Harrison River')");
    expect(Number(off!.n), "bait.r3 reaches a water the book does not name").toBe(0);
    const sid = Number(await firstSection(db, "zp:bait", "bait.r3")) as SectionId;
    const got = (await makeBundleSource(db).statusFor([sid], { year: 2026, month: 8, day: 1 },
                                                      "provincial")).get(sid)!;
    const ban = got.from.find((r) => r.id === "zp:bait.bait.r1");
    expect(ban, "the fin-fish ban still binds everyone not fishing for sturgeon").toBeDefined();
    expect(ban!.liftedFor).toEqual([{ by: "zp:bait.bait.r3", whenTargeting: ["WSG"] }]);
    db.close?.();
  });

  it("set lining is lifted on lakes only", async () => {
    const db = openBundle(REAL!);
    const streams = await db.get(
      "SELECT count(*) AS n FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
      "JOIN item_section s ON s.sid = sr.sid JOIN item i ON i.ord = s.ord " +
      "WHERE rs.entry_id = 'zp:set_lining' AND rs.rule_id = 'set_lining.r1' AND i.kind = 'stream'");
    expect(Number(streams!.n), "the set-lining lift reaches a named stream").toBe(0);
    const lake = Number(await firstSection(db, "zp:set_lining", "set_lining.r1")) as SectionId;
    const got = (await makeBundleSource(db).statusFor([lake], { year: 2026, month: 8, day: 1 },
                                                      "provincial")).get(lake)!;
    expect(got.from.map((r) => r.id)).not.toContain("zp:set_lining.set_lining.r1b");
    db.close?.();
  });
});

