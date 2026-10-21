/**
 * T04, store-free (always run): the reads over an in-memory release seeded here — the codecs undo each family's
 * interning as encode.py's decode_frame does, every read goes to the file that holds its table (a frame to the region
 * file of the KEY it was reached from), concurrent asks share one read, and a damaged release says so. The live release
 * (the ticket's Vedder values, the 134 golden taps, the Python decoder's frames) is conformance/src/regs/reads.release.
 */
import { deflateSync } from "fflate";
import { describe, expect, it } from "vitest";
import { openInMemory } from "../bundle/drivers/node";
import type { Db } from "../bundle/db";
import type { Manifest } from "../manifest";
import { RegsBlobError } from "./blob";
import { openRegsStore, RegsStoreError, type RegsLoader } from "./store";

const X = (v: unknown) => `X'${Buffer.from(typeof v === "string" ? v : JSON.stringify(v)).toString("hex")}'`;
const H = "aaaaaaaaaaaaaaaa", R = "bbbbbbbbbbbbbbbb", I = "cccccccccccccccc";
const META = (rows: Record<string, string>) => "CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);" +
  Object.entries(rows).map(([k, v]) => `INSERT INTO meta VALUES ('${k}', '${v}');`).join("");
const VERSIONS = { ladder: 1, rows: 29, gear: 12, licence: 6, display: 17 };
const FISH = ["BB", "CT", "RB"];
const MOMENTS = [{ weekdays: ["Monday"], hours: null }, { weekdays: ["Sunday"], hours: null }];
const STATICS: Record<string, unknown> = {
  "answers.glossary": { version: 5, terms: [] }, "answers.fish": FISH, "answers.moments": MOMENTS,
  "display.strip": [{ state: "open", bit: 0, label: "open", says: "Open." }], "display.reasons": {},
  "gear.count_nouns": {}, "gear.moments": [], "gear.conduct_means": {}, "licence.profiles": ["p0"],
  "licence.profile_dims": [], "licence.profile_labels": [], "licence.residency_labels": {},
  "export.species": { fish: {}, groups: {} }, "export.licences": {}, "export.conduct": {},
  "ladder.reasons": [], "ladder.reason_state": [],
};
const RULE = { entry_id: "r2:x@1", rule_id: "x.r1", type: "t", family: "f", label: "L", verbatim: "V", fields: {}, prov: { rank: 0 } };
const FACTS = { kind: "gate", closure: true, book: [[22, 24]] };
const DICT = Buffer.from(JSON.stringify(RULE));

/** Two regions; akey 1 → region 1, akey 2 → region 2. Frame id 5 exists in BOTH region files with DIFFERENT content,
 *  so a read routed to the wrong file is seen. regs-sid: handle 4097 (block 1, slot 1) → akey 2, place 0. */
function seed(o: { badCells?: boolean; ruleGap?: boolean; noDict?: boolean } = {}) {
  const asked: string[] = [];
  const spy = (name: string, d: Db): Db => ({
    all: (q, ...p) => { asked.push(`${name}: ${q.split(" FROM ")[1]?.split(" ")[0]}`); return d.all(q, ...p); },
    get: (q, ...p) => { asked.push(`${name}: ${q.split(" FROM ")[1]?.split(" ")[0]}`); return d.get(q, ...p); },
  });
  const water = openInMemory("", META({ format: "water/1", blob_codec: "zdict", section_handles: H, reach_digest: R,
    rule_ids_sha256: I, answers_sha256: "f", layout: JSON.stringify({ top: [], sections: {}, versions: VERSIONS }) }) + `
    CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB NOT NULL);
    ${o.noDict ? "" : `INSERT INTO zdict VALUES ('rule', ${X(RULE)});`}
    CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB NOT NULL);
    ${Object.entries(STATICS).map(([k, v]) => `INSERT INTO static VALUES ('${k}', ${X(v)});`).join("")}
    CREATE TABLE str (id INTEGER PRIMARY KEY, text TEXT NOT NULL);
    INSERT INTO str VALUES (0, 'gnis:1'), (1, 'stream'), (2, 'Mouth to the bridge'), (3, 'Mouth → Bridge'), (4, 'runs'),
      (5, 'hint'), (6, 'Region 2'), (7, 'lake');
    CREATE TABLE item (item_ord INTEGER PRIMARY KEY, item_id INTEGER, shown INTEGER, picker INTEGER, unresolved TEXT, name TEXT,
      kind INTEGER, sections INTEGER, outside_bc INTEGER, entries TEXT, steelhead INTEGER, x BLOB);
    INSERT INTO item VALUES (3, 0, 1, 0, '[]', 'Test River', 1, 6, 1, '[4,9]', 2, ${X({ steelhead_source: ["curated list"] })});
    CREATE TABLE part (item_ord INTEGER, part_ix INTEGER, akey INTEGER, ord INTEGER, label_s INTEGER, short_s INTEGER,
      runs_s INTEGER, place_s INTEGER, hint_s INTEGER, km REAL, closed_all_year INTEGER, paper_licence TEXT, xp INTEGER,
      sections INTEGER, xruns INTEGER, PRIMARY KEY (item_ord, part_ix));
    INSERT INTO part VALUES (3, 0, 2, 0, 2, 3, 4, NULL, 5, 12.5, 0, '[7]', 0, 5, 0), (3, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
      NULL, NULL, NULL, 0, 1, 0);
    CREATE TABLE picker (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
    INSERT INTO picker VALUES (0, ${X({ choices: [], headed: false })});
    CREATE TABLE part_section (ord INTEGER NOT NULL, sid INTEGER NOT NULL, part_ix INTEGER NOT NULL, PRIMARY KEY (ord, sid));
    INSERT INTO part_section VALUES (3, 4097, 1), (3, 4098, 0);
    CREATE TABLE akey (akey INTEGER PRIMARY KEY, ruleset INTEGER, licensing_set INTEGER, steelhead_water INTEGER, steelhead INTEGER,
      steelhead_rules INTEGER, province_except INTEGER, home_region INTEGER, kind INTEGER, tidal INTEGER, seg INTEGER, mom INTEGER,
      nseg INTEGER);
    INSERT INTO akey VALUES (1, 0, NULL, 0, NULL, 0, 0, 0, 7, 0, 0, 0, 2), (2, 0, 4, 1, 1, 1, 0, 0, 1, 1, 1, NULL, 2);
    CREATE TABLE seglist (seg INTEGER PRIMARY KEY, starts TEXT NOT NULL);
    INSERT INTO seglist VALUES (0, '[1,1]'), (1, '[1,100]');
    CREATE TABLE segmom (mom INTEGER PRIMARY KEY, ix TEXT NOT NULL);
    INSERT INTO segmom VALUES (0, '[1,0]');
    CREATE TABLE ruleset (set_id INTEGER PRIMARY KEY, j BLOB NOT NULL);
    INSERT INTO ruleset VALUES (0, ${X({ sections: 5, reach: [0, 1] })}), (1, ${X({ sections: 1, reach: [1], trib: [0] })});
    CREATE TABLE rule (rix INTEGER PRIMARY KEY, j BLOB NOT NULL);
    INSERT INTO rule VALUES (0, X'${Buffer.concat([Buffer.from([0x02]), deflateSync(DICT, { dictionary: DICT })]).toString("hex")}'),
      (1, ${X({ ...RULE, rule_id: "x.r2" })});
    CREATE TABLE display_rule (id INTEGER PRIMARY KEY, j BLOB NOT NULL);
    INSERT INTO display_rule VALUES (0, ${X(FACTS)})${o.ruleGap ? "" : `, (1, ${X({ kind: "sizecap" })})`};
    CREATE TABLE lic (lix INTEGER PRIMARY KEY, j BLOB NOT NULL);
    INSERT INTO lic VALUES (0, ${X({ entry_id: "e", record_id: "r" })});
    CREATE TABLE place (place INTEGER PRIMARY KEY, regions TEXT, label INTEGER, names TEXT);
    INSERT INTO place VALUES (0, '["2"]', 6, '{"stream":"Unnamed stream · Region 2","lake":"Unnamed lake · Region 2"}');
  `);
  const FRAMES = `
    CREATE TABLE cell (akey INTEGER, s INTEGER, ladder INTEGER, rows INTEGER, gear INTEGER, licence INTEGER, display INTEGER);
    CREATE TABLE ladder_frame (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE ladder_verdict (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE rows_frame (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE rows_decided (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE rows_row (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE gear_frame (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE licence_frame (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE licence_hold (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE licence_doc (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE licence_answer (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE licence_u16 (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE display_frame (id INTEGER PRIMARY KEY, j BLOB);`;
  const region = (r: string, extra: string) => openInMemory("", META({ format: "regs-region/1", region: r, section_handles: H,
    reach_digest: R, rule_ids_sha256: I }) + FRAMES + extra);
  const regs2 = region("2", `
    INSERT INTO cell VALUES ${o.badCells ? "(2, 1, 5, 5, 5, 5, 5)" : "(2, 0, 5, 5, 5, 5, 5), (2, 1, 5, 6, 5, 6, 6)"};
    INSERT INTO ladder_frame VALUES (5, ${X([10, [[2, 11], [1, 12, 13, 14]]])});
    INSERT INTO ladder_verdict VALUES (10, ${X([[0], [], [], [], [], []])}), (11, ${X([[1], [], [], [], [[1, [0]]], []])}),
      (12, ${X([[], [1], [], [], [], []])}), (13, ${X([[], [], [1], [], [], []])}), (14, ${X([[], [], [], [], [], [[1, 0, 0]]])});
    INSERT INTO rows_frame VALUES (5, ${X([["RB", "CT"], { RB: [7, null], CT: [null, 8] }, [3, 4, 3], "known_with_rules"])}),
      (6, ${X([[], {}, [], null])});
    INSERT INTO rows_decided VALUES (7, ${X({ status: "keep", win: 0 })}), (8, ${X({ status: "release", win: 1 })});
    INSERT INTO rows_row VALUES (3, ${X({ kind: "keep", pool: 0 })}), (4, ${X({ kind: "closed", pool: null })});
    INSERT INTO gear_frame VALUES (5, ${X({ summary: "g", counts: {} })});
    INSERT INTO licence_frame VALUES (5, ${X([0, 1, 2])}), (6, ${X([0, 1])});
    INSERT INTO licence_hold VALUES (0, ${X({ holds: ["basic"] })});
    INSERT INTO licence_doc VALUES (1, ${X([3, 4, 3])});
    INSERT INTO licence_answer VALUES (3, ${X({ need: "a" })}), (4, ${X({ need: "b" })});
    INSERT INTO licence_u16 VALUES (2, ${X([{ residency: "resident" }])});
    INSERT INTO display_frame VALUES (5, ${X({ status: "own", closing: [], strip: 0 })}),
      (6, ${X({ status: "tidal", tidal: true, note: "n", see: "s", licence: null })});`);
  const regs1 = region("1", `
    INSERT INTO cell VALUES (1, 0, 5, 5, 5, 5, 5), (1, 1, 5, 5, 5, 5, 5);
    INSERT INTO display_frame VALUES (5, ${X({ status: "closed", closing: [[0, ["RB"]]], strip: 1 })});`);
  // handle 4097 = block 1, slot 1: akey bytes [2, 0] at offset 2 (little-endian 2), place byte 1 at offset 1 (place 0)
  const akeyBlock = Buffer.alloc(8192); akeyBlock[2] = 2; akeyBlock[3] = 0; akeyBlock[8191] = 1;   // slot 4095 → akey 256
  const placeBlock = Buffer.alloc(4096); placeBlock[1] = 1;
  const sid = openInMemory("", META({ format: "regs-sid/1", section_handles: H, reach_digest: R, rule_ids_sha256: I }) + `
    CREATE TABLE section_akey (block INTEGER PRIMARY KEY, b BLOB); CREATE TABLE section_place (block INTEGER PRIMARY KEY, b BLOB);
    INSERT INTO section_akey VALUES (1, X'${akeyBlock.toString("hex")}');
    INSERT INTO section_place VALUES (1, X'${placeBlock.toString("hex")}');`);
  const core = openInMemory("", "CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT UNIQUE); INSERT INTO item VALUES (3, 'gnis:1');");
  const dbs: Record<string, Db> = { water: spy("water", water), "regs-1": spy("regs-1", regs1), "regs-2": spy("regs-2", regs2),
                                    "regs-sid": spy("regs-sid", sid), core: spy("core", core) };
  const regionOf: Record<number, string> = { 1: "1", 2: "2" };
  const manifest = { data_release: "r-t", files: { water: {} }, tiles: { current: null },
                     regulations: { valid_until: null, versions: VERSIONS } } as unknown as Manifest;
  const loader: RegsLoader = {
    release: "r-t", manifest, db: (async (n: string) => dbs[n]!) as RegsLoader["db"],
    regionsOf: async (a) => (regionOf[a] ? [regionOf[a]] : []) as never,
    regionDb: async (a) => dbs[`regs-${regionOf[a]}`]!,
  };
  return { loader, asked };
}

describe("reads over water", () => {
  it("item: str refs resolved, x through its table's codec, entries parsed", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.item(3)).toEqual({ ord: 3, id: "gnis:1", name: "Test River", kind: "stream", shown: true, picker: 0,
      sections: 6, outsideBc: 1, steelhead: 2, entries: [4, 9], x: { steelhead_source: ["curated list"] } });
  });

  it("parts: facts as decode.py shapes them, null where the part has no order (outside B.C.)", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.parts(3)).toEqual([
      { itemOrd: 3, partIx: 0, akey: 2, sections: 5, facts: { order: 0, label: "Mouth to the bridge", short: "Mouth → Bridge",
        runs: "runs", place: null, hint: "hint", km: 12.5, closed_all_year: false, paper_licence: [7] } },
      { itemOrd: 3, partIx: 1, akey: null, sections: 1, facts: null },
    ]);
  });

  it("akey, segments, moments (segmom.ix → statics.moments), picker, ruleset (trib defaults to [])", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.akey(1)).toEqual({ akey: 1, ruleset: 0, licensingSet: null, steelheadWater: false, steelhead: null,
      steelheadRules: false, kind: "lake", tidal: false, seg: 0, mom: 0, nseg: 2 });
    expect(await st.akey(2)).toEqual({ akey: 2, ruleset: 0, licensingSet: 4, steelheadWater: true, steelhead: 1,
      steelheadRules: true, kind: "stream", tidal: true, seg: 1, mom: null, nseg: 2 });
    expect(await st.segStarts(1)).toEqual([1, 100]);
    expect(await st.segMoments(0)).toEqual([MOMENTS[1], MOMENTS[0]]);
    expect(await st.picker(0)).toEqual({ choices: [], headed: false });
    expect(await st.ruleset(0)).toEqual({ sections: 5, reach: [0, 1], trib: [] });
    expect(await st.ruleset(1)).toEqual({ sections: 1, reach: [1], trib: [0] });
  });

  it("rules: rule + display_rule by rix, a 0x02 blob inflated with the rule table's dictionary; lics; statics", async () => {
    const st = await openRegsStore(seed().loader);
    const rules = await st.rules();
    expect([...rules.keys()]).toEqual([0, 1]);
    expect(rules.get(0)).toEqual({ rix: 0, export: RULE, facts: FACTS });
    expect(rules.get(1)!.export.rule_id).toBe("x.r2");
    expect(await st.lics()).toEqual(new Map([[0, { entry_id: "e", record_id: "r" }]]));
    const s = await st.statics();
    expect(Object.keys(s).sort()).toEqual(["conductMeans", "countNouns", "exportConduct", "exportLicences", "exportSpecies",
      "fish", "gearMoments", "glossary", "ladderReasonState", "ladderReasons", "moments", "profileDims", "profileLabels",
      "profiles", "reasons", "residencyLabels", "strip"]);
    expect(s.fish).toEqual(FISH);
    expect(s.glossary).toEqual({ version: 5, terms: [] });
  });

  it("a 0x02 blob in a table with no dictionary is refused by the codec, not decoded to something", async () => {
    const st = await openRegsStore(seed({ noDict: true }).loader);
    await expect(st.rules()).rejects.toThrow(new RegsBlobError("regs store: a dictionary-deflated blob in a table with no dictionary"));
  });

  it("a rule with no display_rule is a damaged release", async () => {
    const st = await openRegsStore(seed({ ruleGap: true }).loader);
    await expect(st.rules()).rejects.toThrow(new RegsStoreError("damaged", "regs store: rule 1 has no display_rule"));
  });

  it("itemOrd from core's indexed item_id (null when absent), nothing from water", async () => {
    const { loader, asked } = seed();
    const st = await openRegsStore(loader);
    asked.length = 0;
    expect(await st.itemOrd("gnis:1")).toBe(3);
    expect(await st.itemOrd("nope")).toBeNull();
    expect(asked).toEqual(["core: item", "core: item"]);
  });

  it("sectionPart: water part_section (ord = the item_ord, sid) → part_ix; null for a section not the item's", async () => {
    const { loader, asked } = seed();
    const st = await openRegsStore(loader);
    asked.length = 0;
    expect([await st.sectionPart(3, 4097), await st.sectionPart(3, 4098), await st.sectionPart(3, 1), await st.sectionPart(4, 4097)])
      .toEqual([1, 0, null, null]);
    expect(asked).toEqual(["water: part_section", "water: part_section", "water: part_section", "water: part_section"]);
  });
});

describe("cells and frames: routed to the key's region file", () => {
  it("cell / cells from the key's region; the same frame id reads the KEY's file", async () => {
    const { loader, asked } = seed();
    const st = await openRegsStore(loader);
    asked.length = 0;
    expect(await st.cell(2, 1)).toEqual({ ladder: 5, rows: 6, gear: 5, licence: 6, display: 6 });
    expect(await st.cells(2)).toEqual([{ ladder: 5, rows: 5, gear: 5, licence: 5, display: 5 },
                                       { ladder: 5, rows: 6, gear: 5, licence: 6, display: 6 }]);
    expect(await st.display(1, 5)).toEqual({ status: "closed", closing: [[0, ["RB"]]], strip: 1 });
    expect(asked.filter((a) => a.startsWith("regs-")))
      .toEqual(["regs-2: meta", "regs-2: cell", "regs-2: cell", "regs-1: meta", "regs-1: display_frame"]);
  });

  it("ladder: wire verdicts; a 2-element row is one verdict for all three origins; fish index → code", async () => {
    const st = await openRegsStore(seed().loader);
    const v = (speaks: number[], beside: number[], shown: number[], partly: unknown[], lost: unknown[]) =>
      [speaks, beside, shown, [], partly, lost];
    expect(await st.ladder(2, 5)).toEqual({
      common: v([0], [], [], [], []),
      rows: [["RB", v([1], [], [], [[1, [0]]], []), v([1], [], [], [[1, [0]]], []), v([1], [], [], [[1, [0]]], [])],
             ["CT", v([], [1], [], [], []), v([], [], [1], [], []), v([], [], [], [], [[1, 0, 0]])]],
    });
  });

  it("rows: decided and rows joined back in (RowsCodec.decode_frame)", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.rows(2, 5)).toEqual({ spp: ["RB", "CT"],
      fish: { RB: { hatchery: { status: "keep", win: 0 }, wild: null }, CT: { hatchery: null, wild: { status: "release", win: 1 } } },
      rows: [{ kind: "keep", pool: 0 }, { kind: "closed", pool: null }, { kind: "keep", pool: 0 }],
      steelhead_line: "known_with_rules" });
  });

  it("licence: holds, the document's answers in order, under_16 only when the frame has it", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.licence(2, 5)).toEqual({ holds: { holds: ["basic"] }, profiles: [{ need: "a" }, { need: "b" }, { need: "a" }],
      under_16: [{ residency: "resident" }] });
    expect(await st.licence(2, 6)).toEqual({ holds: { holds: ["basic"] }, profiles: [{ need: "a" }, { need: "b" }, { need: "a" }] });
  });

  it("gear and display ARE the frame (FrameCodec); frames() reads one cell's five", async () => {
    const st = await openRegsStore(seed().loader);
    const f = await st.frames(2, await st.cell(2, 1));
    expect(f.gear).toEqual({ summary: "g", counts: {} });
    expect(f.display).toEqual({ status: "tidal", tidal: true, note: "n", see: "s", licence: null });
    expect(f.rows.spp).toEqual([]);
    expect(f.licence).toEqual({ holds: { holds: ["basic"] }, profiles: [{ need: "a" }, { need: "b" }, { need: "a" }] });
  });

  it("concurrent asks share one read, and a key's cells out of sequence are damaged", async () => {
    const { loader, asked } = seed();
    const st = await openRegsStore(loader);
    await st.region(2);
    asked.length = 0;
    await Promise.all([st.rows(2, 5), st.rows(2, 5), st.rows(2, 5)]);
    expect(asked.filter((a) => a === "regs-2: rows_frame")).toEqual(["regs-2: rows_frame"]);
    expect(asked.filter((a) => a === "regs-2: rows_row")).toEqual(["regs-2: rows_row", "regs-2: rows_row"]);   // ids 3, 4
    const bad = await openRegsStore(seed({ badCells: true }).loader);
    await expect(bad.cells(2)).rejects.toThrow(new RegsStoreError("damaged", "regs store: akey 2's cells are out of sequence"));
  });
});

describe("regs-sid: section handles", () => {
  it("akey is little-endian u16 at 2·slot of block h >> 12; place byte − 1; absent block → 0 / null", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.sectionAkey(4097)).toBe(2);
    expect(await st.sectionAkey(4096 + 4095)).toBe(256);          // the high byte: bytes [.., 1] at offset 8190
    expect(await st.sectionPlace(4097)).toBe(0);
    expect(await st.sectionAkey(4096)).toBe(0);                   // slot 0 holds no key
    expect(await st.sectionPlace(4096)).toBeNull();
    expect(await st.sectionAkey(2_000_000_000)).toBe(0);
    expect(await st.sectionPlace(2_000_000_000)).toBeNull();
  });

  it("unnamedName: the place's name for the key's kind; null outside B.C.", async () => {
    const st = await openRegsStore(seed().loader);
    expect(await st.unnamedName(4097)).toBe("Unnamed stream · Region 2");
    expect(await st.place(0)).toEqual({ place: 0, regions: ["2"], label: "Region 2",
      names: { stream: "Unnamed stream · Region 2", lake: "Unnamed lake · Region 2" } });
    expect(await st.unnamedName(4096)).toBeNull();
  });

  it("close() drops every cache: the next read asks again", async () => {
    const { loader, asked } = seed();
    const st = await openRegsStore(loader);
    await st.item(3);
    st.close();
    asked.length = 0;
    await st.item(3);
    expect(asked).toEqual(["water: item", "water: str", "water: str"]);
  });
});
