/**
 * The licensing reader, against the bundle's OWN schema.
 *
 * The dev fixture is cut from `design/riffle.html`, which predates licensing, so it holds no
 * licensing rows. These tests build a bundle from `pipeline/deliver/bundle/schema.sql` — the one
 * definition of the format — and write rows shaped exactly as `pipeline/deliver/bundle/licensing.py`
 * writes them, so a column renamed on either side fails here.
 *
 * `REAL_BUNDLE=<path>` additionally runs the reader over a real bundle (a side build is fine).
 */
import { describe, expect, it } from "vitest";
import { makeLicensingReader } from "./licensing";
import { bundleExists, openBundle, schemaBundle as bundle } from "./drivers/node";
import type { SectionId } from "../index";

type Sqlite = Parameters<Parameters<typeof bundle>[0]>[0];

const SKEENA = "r6:skeena_river_mainstem_only@6-10";
const ELK = "r4:elk_river_s_tributaries_see_exceptions@4-2+4-23";
const COAL = "r4:coal_creek_downstream_of_old_mf_m_railway_bridge_7_km_upstre@4-23";
const j = (x: unknown) => JSON.stringify(x);

function sample(db: Sqlite) {
  const des = db.prepare(
    "INSERT INTO designation (entry_id, designation_id, classified, unit, unit_name, placement, " +
    "uncertain, unresolved, label, verbatim, review_reason, record) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
  des.run(SKEENA, "skeena_river_2", "II", "skeena_river_2", "Skeena River 2", "sections", 0, null,
          "Class II Classified Water, Jul 1-Sep 30 (licence unit: Skeena River 2). Steelhead " +
          "Stamp not required here unless you fish for steelhead.", "(a) …", null,
          j({ kind: "designation", id: "skeena_river_2", classified: "II",
              unit: "skeena_river_2", unit_name: "Skeena River 2",
              when: { dates: [{ from_month: 7, from_day: 1, to_month: 9, to_day: 30 }] },
              steelhead_stamp_waived: { verbatim: "Steelhead Stamp not mandatory …" },
              verbatim: "(a) …" }));
  des.run(ELK, "elk_river", "II", "elk_river", "Elk River", "sections", 0, null,
          "Class II Classified Water (licence unit: Elk River).", "ALL tributaries …",
          "the carve-out is not drawn", j({ kind: "designation", id: "elk_river" }));
  des.run("r9:nowhere@9-1", "nowhere", "I", "nowhere", "Nowhere", "unresolved", 1,
          "no_registry: entry has no matched registry item", "Class I Classified Water.",
          "Class I water", null, j({ kind: "designation", id: "nowhere" }));
  db.prepare("INSERT INTO not_classified (entry_id, not_classified_id, placement, uncertain, " +
             "unresolved, label, verbatim, review_reason, record) VALUES (?,?,?,?,?,?,?,?,?)")
    .run(COAL, "not_classified", "sections", 0, null, "Not a Classified Water.",
         "Part described is NOT a Classified Water", null, j({ kind: "not_classified" }));
  const req = db.prepare(
    "INSERT INTO requirement (entry_id, req_id, on_designation, authority, water, placement, " +
    "uncertain, unresolved, label, verbatim, review_reason, record) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
  req.run("zp:basic_licence", "basic_licence", null, null, null, "province", 0, null,
          "Anglers 16 and over need a basic angling licence to fish.", "If you are 16 …", null,
          j({ kind: "requirement", id: "basic_licence", who: { age: ["16_plus"] },
              doing: { act: "fishing" }, satisfied_by: [{ hold: ["basic_licence"] }] }));
  req.run("zp:classified_waters_licence", "classified_waters_licence", "classified_period",
          null, "stream", "on_designation", 0, null, "…", "Anglers are required …", null,
          j({ kind: "requirement", id: "classified_waters_licence", who: { age: ["16_plus"] },
              doing: { act: "fishing" }, water: "stream", on: "classified_period",
              satisfied_by: [{ hold: ["basic_licence", "classified_waters_licence"] }] }));
  req.run("z4:creston_valley_permit", "creston_valley_wma_permit", null, null, null, "sections",
          0, null, "You need a Creston Valley Wildlife Management Area permit to fish.", "A permit …",
          null, j({ kind: "requirement", id: "creston_valley_wma_permit",
                    doing: { act: "fishing" },
                    satisfied_by: [{ hold: ["creston_valley_wma_permit"] }] }));
  db.prepare("INSERT INTO licence_terms (entry_id, terms_id, document, classified, units, label, " +
             "verbatim, review_reason, record) VALUES (?,?,?,?,?,?,?,?,?)")
    .run("zp:classified_waters_licence", "cwl_non_resident", "classified_waters_licence", null,
         "[]", "…", "If you are a NON-GUIDED …", null,
         j({ kind: "licence_terms", id: "cwl_non_resident", document: "classified_waters_licence",
             who: { residency: ["non_resident", "non_resident_alien"] }, sold: "per_day",
             covers: "one_unit", max_consecutive_days: 8, verbatim: "If you are a NON-GUIDED …" }));
  db.prepare("INSERT INTO exemption (entry_id, exemption_id, documents, label, verbatim, " +
             "review_reason, record) VALUES (?,?,?,?,?,?,?)")
    .run("zp:basic_licence", "indian_bc_resident", j(["basic_licence", "salmon_stamp"]), "…",
         "If you are an Indian …", null,
         j({ kind: "exemption", who: { status: ["indian_bc_resident"] } }));
  db.prepare("INSERT INTO licence (doc_id, name, provincial) VALUES (?,?,?)")
    .run("basic_licence", "basic angling licence", 1);
  // Sets: section 10 is Skeena 2 (reach); 20 is lower Coal Creek, reached by the Elk's walk
  // AND asserted not classified — acknowledged, so the designation there is `contested`; 30 is
  // a pending walk; 40 is inside the Creston WMA.
  const set = db.prepare("INSERT INTO licensing_set (set_id, kind, entry_id, record_id, via) " +
                         "VALUES (?,?,?,?,?)");
  set.run(0, "designation", SKEENA, "skeena_river_2", "reach");
  set.run(1, "designation", ELK, "elk_river", "contested");
  set.run(1, "not_classified", COAL, "not_classified", "reach");
  set.run(2, "designation", ELK, "elk_river", "trib_pending");
  set.run(3, "requirement", "z4:creston_valley_permit", "creston_valley_wma_permit", "reach");
  const sec = db.prepare("INSERT INTO section_licensing (sid, set_id) VALUES (?,?)");
  for (const [s, k] of [[10, 0], [20, 1], [30, 2], [40, 3]]) sec.run(s!, k!);
}

describe("the licensing reader", () => {
  const src = makeLicensingReader(bundle(sample));
  const S = (n: number) => n as SectionId;

  it("says which designation binds a section, and how it got there", async () => {
    const got = await src.forSections([S(10), S(20), S(30), S(40), S(99)]);
    const skeena = got.get(S(10))!.designations[0]!;
    expect(skeena.via).toBe("reach");
    expect(skeena.record.unitName).toBe("Skeena River 2");
    expect(skeena.record.classified).toBe("II");
    expect(skeena.record.when?.dates?.[0]?.from_month).toBe(7);
    expect(skeena.record.stampWaived).not.toBeNull();
    expect(skeena.record.stampDuring).toBeNull();
    // A section with nothing is ABSENT — "we asked and there are none" is not an empty list.
    expect(got.has(S(99))).toBe(false);
  });

  it("keeps the doubtful bindings, flagged, rather than dropping them", async () => {
    const got = await src.forSections([S(20), S(30)]);
    const coal = got.get(S(20))!;
    expect(coal.designations.map((d) => d.via)).toEqual(["contested"]);
    expect(coal.notClassified.map((n) => n.record.entryId)).toEqual([COAL]);
    expect(got.get(S(30))!.designations[0]!.via).toBe("trib_pending");
    expect(coal.designations[0]!.record.reviewReason).toBe("the carve-out is not drawn");
  });

  it("returns place-bound requirements per section and the rest once", async () => {
    const got = await src.forSections([S(40)]);
    expect(got.get(S(40))!.requirements[0]!.record.satisfiedBy[0]!.hold)
      .toEqual(["creston_valley_wma_permit"]);
    const u = await src.unplaced();
    expect(u.province.map((r) => r.id)).toEqual(["basic_licence"]);
    expect(u.province[0]!.who).toEqual({ age: ["16_plus"] });
    expect(u.onDesignation.map((r) => [r.id, r.onDesignation, r.water]))
      .toEqual([["classified_waters_licence", "classified_period", "stream"]]);
    // An unplaced designation is returned, uncertain, with its reason.
    expect(u.unresolved.map((r) => [r.id, r.uncertain])).toEqual([["nowhere", true]]);
    expect(u.unresolved[0]!.unresolved).toMatch(/^no_registry/);
    expect(u.terms[0]!.terms).toEqual({ sold: "per_day", covers: "one_unit",
                                         max_consecutive_days: 8 });
    expect(u.exemptions[0]!.documents).toEqual(["basic_licence", "salmon_stamp"]);
    expect((await src.licences())[0]).toEqual(
      { id: "basic_licence", name: "basic angling licence", provincial: true });
  });

  it("refuses a set row naming a record the bundle does not hold", async () => {
    const broken = makeLicensingReader(bundle((db) => {
      sample(db);
      db.prepare("INSERT INTO licensing_set VALUES (9, 'designation', 'r0:gone', 'gone', 'reach')")
        .run();
      db.prepare("INSERT INTO section_licensing VALUES (90, 9)").run();
    }));
    await expect(broken.forSections([S(90)])).rejects.toThrow(/does not hold/);
  });

  it("reads an unknown via as contested, never as a clean binding", async () => {
    const odd = makeLicensingReader(bundle((db) => {
      sample(db);
      db.prepare("INSERT INTO licensing_set VALUES (9, 'designation', ?, 'skeena_river_2', " +
                 "'teleported')").run(SKEENA);
      db.prepare("INSERT INTO section_licensing VALUES (90, 9)").run();
    }));
    expect((await odd.forSections([S(90)])).get(S(90))!.designations[0]!.via)
      .toBe("contested");
  });
});

const REAL = process.env.REAL_BUNDLE;
describe.skipIf(!bundleExists(REAL))("the licensing reader on a real bundle", () => {
  it("reads every record and every binding the build wrote", async () => {
    const db = openBundle(REAL!);
    const src = makeLicensingReader(db);
    const u = await src.unplaced();
    expect(u.province.length).toBeGreaterThan(0);
    expect(u.onDesignation.length).toBeGreaterThan(0);
    const sids = (await db.all("SELECT sid FROM section_licensing")).map((r) => Number(r.sid));
    const got = await src.forSections(sids as SectionId[]);
    expect(got.size).toBe(sids.length);
    const n = [...got.values()].reduce((a, s) => a + s.designations.length, 0);
    expect(n).toBeGreaterThan(0);
    db.close?.();
  });
});
