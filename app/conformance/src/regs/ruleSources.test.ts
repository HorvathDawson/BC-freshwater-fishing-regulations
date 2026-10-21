/**
 * storeSource() decodes the store's blobs through @app/data/regs `unpackBlob`, so it is as strict as the app's codec:
 * a 0x02 (dictionary-deflated) blob in a table with no `zdict` row and an empty blob throw RegsBlobError instead of
 * decoding to something. Store-free: each case writes a small store with the tables storeSource reads into a temp dir.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { deflateRawSync } from "node:zlib";
import { afterAll, describe, expect, it } from "vitest";
import { RegsBlobError } from "@app/data/regs";
import { storeSource } from "./ruleSources";

const DIR = mkdtempSync(join(tmpdir(), "rulesources-"));
afterAll(() => rmSync(DIR, { recursive: true, force: true }));

const RULE = { entry_id: "r1:x", rule_id: "x.r1", type: "retention_limit", family: "retention", label: "L", verbatim: "V",
  fields: { species: ["RB"], take: 2, period: "daily" }, prov: { rank: 0 } };
const BOOK = { fish: { RB: { name: "Rainbow trout" } }, groups: { ALL_GAME_FISH: { name: "All game fish", members: ["RB"] } } };
const DICT = Buffer.from(JSON.stringify(RULE));
const bytes = (o: unknown) => Buffer.from(JSON.stringify(o));
const zdict = (o: unknown) => Buffer.concat([Buffer.from([0x02]), deflateRawSync(bytes(o), { dictionary: DICT })]);

let n = 0;
function store(ruleBlob: Uint8Array, dicts: Record<string, Uint8Array>): string {
  const path = join(DIR, `s${n++}.sqlite`);
  const db = new DatabaseSync(path);
  db.exec(`CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB); CREATE TABLE display_rule (id INTEGER PRIMARY KEY, j BLOB);
    CREATE TABLE rule (rix INTEGER PRIMARY KEY, j BLOB); CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB);
    CREATE TABLE akey (akey INTEGER PRIMARY KEY, ruleset INTEGER); CREATE TABLE ruleset (set_id INTEGER PRIMARY KEY, j BLOB);`);
  for (const [t, d] of Object.entries(dicts)) db.prepare("INSERT INTO zdict VALUES (?, ?)").run(t, d);
  db.prepare("INSERT INTO rule VALUES (7, ?)").run(ruleBlob);
  db.prepare("INSERT INTO static VALUES ('export.species', ?)").run(bytes(BOOK));
  db.close();
  return path;
}

describe("storeSource decodes with @app/data's unpackBlob", () => {
  it("inflates a 0x02 rule blob with the rule table's dictionary", () => {
    const s = storeSource(store(zdict(RULE), { rule: DICT }))!;
    expect(s.stored.get(7)).toEqual({ export: RULE, facts: null });
    expect(s.book).toEqual(BOOK);
  });

  it("refuses a 0x02 blob in a table with no zdict row (another table's dictionary does not count)", () => {
    const path = store(zdict(RULE), { display_rule: DICT });
    expect(() => storeSource(path)).toThrow(RegsBlobError);
    expect(() => storeSource(path)).toThrow("regs store: a dictionary-deflated blob in a table with no dictionary");
  });

  it("refuses an empty blob", () => {
    const path = store(new Uint8Array(0), {});
    expect(() => storeSource(path)).toThrow(RegsBlobError);
    expect(() => storeSource(path)).toThrow("regs store: an empty blob");
  });

  it("is null with no store", () => {
    expect(storeSource(null)).toBeNull();
  });
});
