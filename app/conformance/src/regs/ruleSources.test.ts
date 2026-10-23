/**
 * storeSource() reads a release through `@app/data/regs` `openRegsStore(loader)` — the app's own path, so it is as strict
 * as the app: a 0x02 (dictionary-deflated) blob in a table with no `zdict` row and an empty blob throw RegsBlobError
 * instead of decoding to something, and a release the app refuses is refused here too. Store-free: each case seeds a
 * small in-memory water file (the tables storeSource reads) behind a loader.
 */
import { DatabaseSync } from "node:sqlite";
import { deflateRawSync } from "node:zlib";
import { describe, expect, it } from "vitest";
import type { Db, Manifest, Row } from "@app/data";
import { RegsBlobError, RegsStoreError, type RegsLoader } from "@app/data/regs";
import { storeSource } from "./ruleSources";

const RULE = { entry_id: "r1:x", rule_id: "x.r1", type: "retention_limit", family: "retention", label: "L", verbatim: "V",
  fields: { species: ["RB"], take: 2, period: "daily" }, prov: { rank: 0 } };
const FACTS = { kind: "limit" };
const BOOK = { fish: { RB: { name: "Rainbow trout" } }, groups: { ALL_GAME_FISH: { name: "All game fish", members: ["RB"] } } };
const VERSIONS = { ladder: 1, rows: 33, gear: 12, licence: 7, display: 18 };
const DICT = Buffer.from(JSON.stringify(RULE));
const bytes = (o: unknown) => Buffer.from(JSON.stringify(o));
const zdict = (o: unknown) => Buffer.concat([Buffer.from([0x02]), deflateRawSync(bytes(o), { dictionary: DICT })]);

function release(ruleBlob: Uint8Array, dicts: Record<string, Uint8Array>, format = "water/1"): RegsLoader {
  const db = new DatabaseSync(":memory:");
  db.exec(`CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT); CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB);
    CREATE TABLE display_rule (id INTEGER PRIMARY KEY, j BLOB); CREATE TABLE rule (rix INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB); CREATE TABLE akey (akey INTEGER PRIMARY KEY, ruleset INTEGER);
    CREATE TABLE ruleset (set_id INTEGER PRIMARY KEY, j BLOB);`);
  const meta = { format, blob_codec: "zdict", section_handles: "h", reach_digest: "r", rule_ids_sha256: "i", answers_sha256: "a",
                 layout: JSON.stringify({ top: [], sections: {}, versions: VERSIONS }) };
  for (const [k, v] of Object.entries(meta)) db.prepare("INSERT INTO meta VALUES (?, ?)").run(k, v);
  for (const [t, d] of Object.entries(dicts)) db.prepare("INSERT INTO zdict VALUES (?, ?)").run(t, d);
  db.prepare("INSERT INTO rule VALUES (0, ?)").run(ruleBlob);
  db.prepare("INSERT INTO display_rule VALUES (0, ?)").run(bytes(FACTS));
  const statics: Record<string, unknown> = { "answers.glossary": { version: 5, terms: [] }, "export.species": BOOK };
  for (const k of ["answers.fish", "answers.moments", "display.strip", "display.reasons", "gear.count_nouns", "gear.moments",
                   "gear.conduct_means", "licence.profiles", "licence.profile_dims", "licence.profile_labels",
                   "licence.residency_labels", "export.licences", "export.conduct", "export.pdf_pages", "ladder.reasons", "ladder.reason_state"])
    statics[k] = [];
  for (const [k, v] of Object.entries(statics)) db.prepare("INSERT INTO static VALUES (?, ?)").run(k, bytes(v));
  db.prepare("INSERT INTO akey VALUES (7, 3)").run();
  db.prepare("INSERT INTO ruleset VALUES (3, ?)").run(bytes({ sections: 1, reach: [0] }));
  const water: Db = {
    all: async (q, ...p) => db.prepare(q).all(...p) as unknown as Row[],
    get: async (q, ...p) => db.prepare(q).get(...p) as unknown as Row | undefined,
  };
  return {
    release: "r-test", db: (async () => water) as RegsLoader["db"], regionsOf: async () => [], regionDb: async () => water,
    manifest: { data_release: "r-test", files: { water: {} }, tiles: { current: null },
                regulations: { valid_until: null, versions: VERSIONS } } as unknown as Manifest,
  };
}

describe("storeSource reads a release the app's way (openRegsStore → rules, statics, rulesets)", () => {
  it("inflates a 0x02 rule blob with the rule table's dictionary; the book and a key's ruleset come with it", async () => {
    const s = (await storeSource(release(zdict(RULE), { rule: DICT })))!;
    expect(s.name).toBe("release r-test");
    expect(s.stored.get(0)).toEqual({ export: RULE, facts: FACTS });
    expect(s.book).toEqual(BOOK);
    expect(s.ruleset({ item: "x", partIx: 0, akey: 7 })).toEqual({ reach: [0], trib: [] });
    expect(() => s.ruleset({ item: "x", partIx: 0, akey: 8 })).toThrow("release r-test: no ruleset for akey 8");
  });

  it("refuses a 0x02 blob in a table with no zdict row (another table's dictionary does not count)", async () => {
    const p = storeSource(release(zdict(RULE), { display_rule: DICT }));
    await expect(p).rejects.toThrow(new RegsBlobError("regs store: a dictionary-deflated blob in a table with no dictionary"));
  });

  it("refuses an empty blob", async () => {
    await expect(storeSource(release(new Uint8Array(0), {}))).rejects.toThrow(new RegsBlobError("regs store: an empty blob"));
  });

  it("refuses a release the app refuses", async () => {
    await expect(storeSource(release(zdict(RULE), { rule: DICT }, "water/2")))
      .rejects.toThrow(new RegsStoreError("format", "regs store: format water/2, this app reads water/1 — refused"));
  });

  it("is null with no release", async () => {
    expect(await storeSource(null)).toBeNull();
  });
});
