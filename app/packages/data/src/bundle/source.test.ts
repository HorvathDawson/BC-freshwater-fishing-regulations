/**
 * The bundle source, against the real development bundle.
 *
 * These are not unit tests of SQL. They assert the ANSWERS, because the whole reason the
 * bundle is SQLite is that three different drivers must produce the same ones — and the
 * only way that holds is if "the same" is written down somewhere executable.
 */
import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { DEV_BUNDLE, hasDevBundle, openBundle, schemaBundle } from "./drivers/node";
import { str } from "./db";
import { makeBundleSource } from "./source";
import type { Db } from "./db";
import type { ItemId, RegsSource, SectionId } from "../index";

const ON = { year: 2026, month: 8, day: 30 } as const;

let db: Db;
let src: RegsSource;
beforeAll(() => {
  expect(hasDevBundle(), `run \`pnpm fixture\` first (${DEV_BUNDLE})`).toBe(true);
  db = openBundle();
  src = makeBundleSource(db);
});
afterAll(() => db.close?.());

describe("the bundle source", () => {
  it("finds a water by name", async () => {
    // Taken from the bundle rather than typed in: the fixture is a real slice, and the
    // waters in it are whatever the build produced. An asserted literal here was wrong —
    // this valley's sections are named Vedder, not Chilliwack River.
    const name = str((await db.get("SELECT name FROM item WHERE length(name) > 8 LIMIT 1"))!.name);
    const hits = await src.searchNames(name.slice(0, 6), 10);
    expect(hits.length).toBeGreaterThan(0);
    expect(hits.map((h) => h.name)).toContain(name);
    // every hit knows how much water it is
    for (const h of hits) expect(h.pieces).toBeGreaterThan(0);
  });

  it("finds a water by an ALIAS, and says which one matched", async () => {
    // The failure this prevents: someone types an old name, gets a row headed with a
    // different one, and cannot tell whether it is the right river.
    //
    // The alias has to be one that does NOT also appear in its item's own name, or the
    // name match wins and nothing is proved. That is a property of the data, so it is
    // queried for rather than assumed.
    const row = await db.get(
      "SELECT a.alias, i.name FROM alias a JOIN item i USING(item_id) " +
      "WHERE lower(i.name) NOT LIKE '%' || lower(substr(a.alias, 1, 5)) || '%' LIMIT 1");
    expect(row, "the fixture has no alias distinct from its item's name").toBeTruthy();
    const alias = str(row!.alias);

    const hits = await src.searchNames(alias.slice(0, 6), 20);
    const viaAlias = hits.find((h) => h.matchedAs !== null);
    expect(viaAlias, `no alias match for '${alias}'`).toBeTruthy();
    expect(viaAlias!.name).not.toBe(viaAlias!.matchedAs);
  });

  it("prefers a prefix match over a match in the middle", async () => {
    const name = str((await db.get(
      "SELECT name FROM item WHERE name LIKE 'S%' AND length(name) > 8 LIMIT 1"))!.name);
    const q = name.slice(0, 5);
    const hits = await src.searchNames(q, 8);
    expect(hits[0]!.name.toLowerCase().startsWith(q.toLowerCase())).toBe(true);
  });

  it("resolves a tapped section to the water it belongs to", async () => {
    const row = (await db.all("SELECT sid FROM item_section LIMIT 1"))[0]!;
    const section = Number(row.sid) as SectionId;
    const item = await src.itemForSection(section);
    expect(item).toBeTruthy();
    const sheet = await src.regsForItem(item!, ON, "provincial");
    expect(sheet!.reaches.map((r) => r.section)).toContain(section);
  });

  it("answers for EVERY section asked about, including ones with no rule", async () => {
    // "Open under the general rules" is an answer. A missing map entry would make the
    // caller fall back to a default, and the map would be coloured by an assumption.
    const ids = (await db.all("SELECT sid FROM item_section LIMIT 25"))
      .map((r) => Number(r.sid) as SectionId);
    const out = await src.statusFor(ids, ON, "provincial");
    expect(out.size).toBe(ids.length);
    for (const id of ids) expect(out.get(id)).toBeTruthy();
  });

  it("never lets an unplaceable rule decide an outcome", async () => {
    const row = await db.get(
      "SELECT entry_id, item_id FROM entry WHERE entry_id IN " +
      "(SELECT entry_id FROM rule WHERE uncertain = 1) LIMIT 1");
    if (!row?.item_id) return;                    // no such entry in this slice
    const sheet = await src.regsForItem(row.item_id as ItemId, ON, "provincial");
    expect(sheet).toBeTruthy();
    // it is SHOWN...
    expect(sheet!.unplaceable.length).toBeGreaterThan(0);
    // ...and it never appears among the rules that produced a reach's answer
    for (const r of sheet!.reaches)
      for (const rule of r.status.from) expect(rule.uncertain).toBeFalsy();
  });

  it("returns what is near a town, nearest first", async () => {
    // The place id is an integer now, so ask the source rather than typing a name — which
    // is also the path a caller actually takes: searchPlaces, then watersNear.
    const [place] = await src.searchPlaces("Chilliwack", 1);
    expect(place, "no place named Chilliwack in the fixture").toBeTruthy();
    const near = await src.watersNear(place!.place);
    expect(near.length).toBeGreaterThan(3);
    for (let i = 1; i < near.length; i++)
      expect(near[i]!.km).toBeGreaterThanOrEqual(near[i - 1]!.km);
    // every hit is a real item, not an id minted from a display name
    for (const h of near) expect(await src.itemExists(h.item), h.name).toBe(true);
  });

  it("stores only bands the pipeline can produce — a refusal is an absent row", async () => {
    // There is no `none` band. `pipeline/gauges/consume/shed.py` writes NO ROW for a reach the gauge
    // drains far too much to describe, so the refusal reaches the client as a null link.
    // A stored `none` would mean the opposite: a row asserting a station and then
    // retracting it, which is what the app used to test for and the bundler never wrote.
    const bands = await db.all("SELECT DISTINCT trust FROM section_gauge");
    expect(bands.map((r) => r.trust).sort()).toEqual(["fair", "good", "weak"]);

    const ungauged = await db.get(
      "SELECT s.sid FROM item_section s " +
      "LEFT JOIN section_gauge g USING(sid) WHERE g.sid IS NULL LIMIT 1");
    if (!ungauged) return;
    expect(await src.gaugeForSection(Number(ungauged.sid) as SectionId)).toBeNull();
  });

  it("answers whether a WATER has a gauge, not just the reach you tapped", async () => {
    // A river's reaches disagree — most of a well-gauged river has no station on the exact
    // stretch in view. Asking the water is the question a person means.
    const link = await src.gaugeForItem("gnis:8634" as ItemId);
    expect(link, "the fixture's Chilliwack has four stations on it").not.toBeNull();
    expect(link!.trust).toBe("good");
    expect(link!.section, "the answer says WHICH reach it is for").not.toBeNull();
  });

  it("picks the gauge on the water's biggest reach, not the first good one", async () => {
    // The defect this replaced: ordering on the trust band alone. `good` runs from a tenth
    // of a watershed to all of it, so a station clipping one small section outranked the
    // river's own gauge — on the real bundle a 13 km2 creek station won the Cowichan.
    const link = await src.gaugeForItem("gnis:8634" as ItemId);
    const biggest = await db.get(
      "SELECT sg.station FROM item_section it " +
      "JOIN item i ON i.ord = it.ord " +
      "JOIN section_gauge sg ON sg.sid = it.sid " +
      "LEFT JOIN gauge g USING(station) " +
      "WHERE i.item_id = 'gnis:8634' AND sg.trust = 'good' " +
      "ORDER BY sg.mag DESC, sg.station LIMIT 1");
    expect(link!.station).toBe(biggest!.station);
  });

  it("reports liveness as unknown when no feed is wired, never as stopped", async () => {
    // `src` here has no feed. A false would tell an offline reader the gauge shut down.
    const link = await src.gaugeForItem("gnis:8634" as ItemId);
    expect(link!.live).toBeNull();
  });

  it("carries both magnitudes so a sheet can show its working", async () => {
    const row = await db.get(
      "SELECT sid FROM section_gauge WHERE trust = 'good' AND mag IS NOT NULL LIMIT 1");
    const link = await src.gaugeForSection(Number(row!.sid) as SectionId);
    expect(link!.reachMagnitude).toBeGreaterThan(0);
    expect(link!.gaugeMagnitude).toBeGreaterThan(link!.reachMagnitude);
  });

  it("distinguishes not-transmitting from not-checked", async () => {
    // Three states, and the third is the one that matters: with no feed wired the honest
    // answer is null. A false here tells an offline reader the gauge shut down.
    const link = await src.gaugeForItem("gnis:8634" as ItemId);
    expect(link!.live).toBeNull();

    const withFeed = makeBundleSource(db, {
      feed: { now: async () => null, observations: async () => null,
              live: async () => new Set([link!.station]) },
    });
    expect((await withFeed.gaugeForItem("gnis:8634" as ItemId))!.live).toBe(true);

    const quiet = makeBundleSource(db, {
      feed: { now: async () => null, observations: async () => null,
              live: async () => new Set<string>() },
    });
    expect((await quiet.gaugeForItem("gnis:8634" as ItemId))!.live).toBe(false);
  });

  it("traces downstream without hanging on a braid", async () => {
    const downRow = (await db.all("SELECT sid FROM section_down LIMIT 1"))[0]!;
    const from = Number(downRow.sid) as SectionId;
    const path = await src.traceToGauge(from);
    expect(path[0]).toBe(from);
    expect(new Set(path).size).toBe(path.length);    // no repeats
  });

  it("reports no reading at all when there is no feed", async () => {
    // Absent is not zero. A source with no feed must say nothing rather than imply a
    // river is at 0 m³/s.
    expect(await src.gaugeNow("08MH001" as never)).toBeNull();
  });
});

describe("the gauge model", () => {
  it("never breaks a tie on the station id, which is alphabetical and meaningless", async () => {
    // The regression this catches, on the real province bundle: with several stations per
    // reach, the item-level pick fell through to `ORDER BY station` and the Fraser
    // answered "Nechako River at Vanderhoof". `seq` is the build's own ranking and must
    // come first.
    const sql = (await import("./queries")).GAUGE_FOR_ITEM;
    const order = sql.slice(sql.indexOf("ORDER BY"));
    // Representativeness decides, and the station id is only ever the last resort that
    // keeps a rebuild byte-identical. If it ever leads, the Fraser answers "Nechako River
    // at Vanderhoof" — which is exactly what happened.
    expect(order.indexOf("sg.mag"), "magnitude must rank before the station id")
      .toBeLessThan(order.indexOf("sg.station"));
  });

  it("holds exactly one station per reach — the one that most nearly is it", async () => {
    // A second gauge on the same reach drains more country than the first, or less, so it
    // is a worse answer to the same question rather than a second opinion. The variety
    // that matters lives at river level, where reaches have different bests.
    const dupe = await db.get(
      "SELECT sid, COUNT(*) n FROM section_gauge GROUP BY sid " +
      "HAVING n > 1 LIMIT 1");
    expect(dupe, "a reach with two gauges").toBeUndefined();
  });

  it("records how each station found its node, so a fragile link is visible", async () => {
    // v1's last four points of match rate came from hand-written aliases. Without a record
    // of which rows lean on that, a link that is holding looks like one that is not.
    const cols = (await db.all("SELECT name FROM pragma_table_info('gauge')"))
      .map((c) => String(c.name));
    expect(cols).toContain("matched_by");
    expect(cols).toContain("match_m");
  });

  it("keeps lake stations out of section_gauge entirely", async () => {
    // A lake station reports a level in metres; a stream station a discharge in m3/s.
    // Mixing them had 11,049 stream sections being told a reservoir's level.
    const cols = await db.all("SELECT name FROM pragma_table_info('lake_gauge')");
    expect(cols.map((c) => String(c.name))).toEqual(["item_id", "station"]);
  });

  it("holds no liveness column of any kind", async () => {
    // `realtime` was obviously an hour-to-hour fact. `active` looked safe and was not: a
    // station is discontinued between builds, and a bundle cut in March would still be
    // asserting it in November. Any boolean here goes stale slowly enough to go unnoticed.
    const cols = (await db.all("SELECT name FROM pragma_table_info('gauge')"))
      .map((c) => String(c.name));
    for (const c of ["realtime", "active", "live", "transmitting"])
      expect(cols, `'${c}' is a live fact and must come from the feed`).not.toContain(c);
  });

  it("colours a viewport without binding more parameters than SQLite allows", async () => {
    /*
     * SQLite's parameter ceiling is 999 in the classic build and 32,766 in newer ones, and
     * we do not get to choose which one a phone's wasm or native driver was compiled with.
     * So the assertion is on OUR side of the line — no single statement binds more than the
     * lowest limit — rather than on whether this machine happens to survive a big query.
     * The first version of this test passed at 5,000 parameters and proved nothing.
     *
     * It matters now because it did not before: `rule_section` was declared and never
     * written, so every lookup returned nothing and the query was never asked a big
     * question. Real rules turn that into a crash on a zoomed-in map.
     */
    const CEILING = 999;
    let worst = 0;
    const spy: Db = {
      ...db,
      all: (sql: string, ...args: Parameters<Db["all"]>[1][]) => {
        worst = Math.max(worst, args.length);
        return db.all(sql, ...args);
      },
    };
    // Handles, not names — and starting at 1, because 0 is reserved for "no section".
    const many = Array.from({ length: 2500 }, (_, i) => (i + 1) as SectionId);
    const out = await makeBundleSource(spy).statusFor(many, ON, "provincial");
    // Every id gets an answer — "open under the general rules" is an answer, not an absence.
    expect(out.size).toBe(many.length);
    expect(worst).toBeLessThanOrEqual(CEILING);
  });

  it("evaluates a rule that has a season on it", async () => {
    /*
     * THE ASSERTION THAT WAS MISSING — twice. First `rule.windows` held the curated strings
     * verbatim and every regulation screen threw on `.month`. Then the catalogue moved seasons
     * into `when`, the bundler went on reading the retired field, and every rule shipped an
     * EMPTY window list — which is "all year", so every seasonal closure in the province was in
     * force every day, and nothing threw at all.
     *
     * Both survived a full suite because the FIXTURE agreed with the bug. This asks a seasonal
     * rule, out of the bundle, whether it is in force on both sides of its window.
     */
    const seasonal = (await db.all(
      "SELECT entry_id, rule_id, when_ FROM rule WHERE when_ LIKE '%from_month%' LIMIT 1"))[0];
    expect(seasonal, "the fixture must carry at least one seasonal rule").toBeTruthy();
    const parsed = JSON.parse(str(seasonal!.when_)) as {
      dates: { from_month: number; from_day: number; to_month: number; to_day: number }[] };
    // The SHAPE, named explicitly: a bare string, or an empty list, here is the bug.
    expect(parsed.dates[0]!.from_month).toBeTypeOf("number");
    expect(parsed.dates[0]!.to_day).toBeTypeOf("number");

    // And it has to survive the thing that actually reads it. A section under this rule,
    // asked on a day inside its window and a day outside, must not throw either time.
    const covered = (await db.all(
      "SELECT sr.sid FROM section_ruleset sr JOIN ruleset rs USING(set_id) " +
      "WHERE rs.entry_id = ? AND rs.rule_id = ? LIMIT 1",
      str(seasonal!.entry_id), str(seasonal!.rule_id)))[0];
    if (!covered) return;                    // this rule binds nowhere in the slice
    const id = Number(covered.sid) as SectionId;
    const w = parsed.dates[0]!;
    for (const day of [{ year: 2026, month: w.from_month, day: w.from_day },
                       { year: 2026, month: w.to_month, day: w.to_day },
                       { year: 2026, month: 1, day: 1 }]) {
      const out = await src.statusFor([id], day, "provincial");
      expect(out.get(id), `no answer for ${day.month}/${day.day}`).toBeTruthy();
    }
  });
});

describe("rules out of the bundle's own schema", () => {
  /* The dev fixture predates `exempts` and every type is one core knows, so these build a bundle
     from `pipeline/deliver/bundle/schema.sql` — the one definition of the format — with rows
     shaped as `pipeline/deliver/bundle/rules.py` writes them. */
  const SPRING = ["z3:spring_stream_closure", "spring_stream_closure.r1"] as const;
  const EXEMPT = ["r3:north_thompson_river@3-27", "north_thompson_river.r1"] as const;
  const MAY_1 = { year: 2026, month: 5, day: 1 } as const;

  function rules(extra: (db: Parameters<Parameters<typeof schemaBundle>[0]>[0]) => void) {
    return schemaBundle((db) => {
      const r = db.prepare(
        "INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, scope, when_, " +
        "exempts, take, may_target, species, uncertain) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)");
      r.run(...SPRING, "retention_limit", "retention", "daily", "No fishing", "area",
            JSON.stringify({ dates: [{ from_month: 1, from_day: 1, to_month: 6, to_day: 30 }] }),
            null, 0, 0, '["ALL_GAME_FISH"]');
      r.run(...EXEMPT, "retention_limit", "retention", "daily", "Exempt from spring closure",
            "section", null,
            JSON.stringify([{ entry_id: SPRING[0], rule_id: SPRING[1] }]),
            null, null, '["ALL_GAME_FISH"]');
      const set = db.prepare("INSERT INTO ruleset VALUES (?,?,?,?)");
      set.run(0, ...SPRING, "reach");                       // a stream with only the closure
      set.run(1, ...SPRING, "reach");                       // the North Thompson: both
      set.run(1, ...EXEMPT, "reach");
      db.prepare("INSERT INTO section_ruleset (sid, set_id) VALUES (?,?)").run(10, 0);
      db.prepare("INSERT INTO section_ruleset (sid, set_id) VALUES (?,?)").run(11, 1);
      extra(db);
    });
  }

  it("applies an exemption: the lifted zone default does not count where the lift is", async () => {
    const s = makeBundleSource(rules(() => {}));
    const out = await s.statusFor([10 as SectionId, 11 as SectionId], MAY_1, "provincial");
    expect(out.get(10 as SectionId)!.outcome).toBe("closed");
    const nt = out.get(11 as SectionId)!;
    expect(nt.outcome).toBe("open");
    expect(nt.from.map((r) => r.id)).toEqual([EXEMPT.join(".")]);
  });

  it("refuses a rule type this client does not know — never reads it as advisory", async () => {
    const s = makeBundleSource(rules((db) => {
      db.prepare("UPDATE rule SET type = 'teleport_ban' WHERE rule_id = ?").run(SPRING[1]);
    }));
    await expect(s.statusFor([10 as SectionId], MAY_1, "provincial"))
      .rejects.toThrow(/unknown rule type/);
  });

  it("reads a partial lift: the rule stays, marked, and no longer closes the water", async () => {
    const s = makeBundleSource(rules((db) => {
      db.prepare("UPDATE rule SET exempts = ? WHERE rule_id = ?").run(JSON.stringify(
        [{ entry_id: SPRING[0], rule_id: SPRING[1], when_targeting: ["WSG"] }]), EXEMPT[1]);
    }));
    const nt = (await s.statusFor([11 as SectionId], MAY_1, "provincial")).get(11 as SectionId)!;
    expect(nt.outcome).toBe("restricted");
    const spring = nt.from.find((r) => r.id === SPRING.join("."))!;
    expect(spring.liftedFor).toEqual([{ by: EXEMPT.join("."), whenTargeting: ["WSG"] }]);
  });

  it("refuses an exemption it cannot read — including the retired whole-entry shape", async () => {
    for (const bad of [[{ default_id: "spring_stream_closure", entry_id: SPRING[0] }],
                       [{ entry_id: SPRING[0], target: SPRING[1] }],
                       [{ entry_id: SPRING[0], rule_id: SPRING[1], water: "lake" }],
                       [{ entry_id: SPRING[0], rule_id: SPRING[1], species: [] }]]) {
      const s = makeBundleSource(rules((db) => {
        db.prepare("UPDATE rule SET exempts = ? WHERE rule_id = ?")
          .run(JSON.stringify(bad), EXEMPT[1]);
      }));
      await expect(s.statusFor([11 as SectionId], MAY_1, "provincial"), JSON.stringify(bad))
        .rejects.toThrow(/exemption/);
    }
  });
});
