/**
 * T03, store-free (always run): `openRegsStore(loader)` over an in-memory release — a water file, two region files and
 * regs-sid, each a node:sqlite database seeded here — refuses everything that is not the release this app reads, with
 * the reason and the whole sentence. The live release (the Vedder, every digest) is `conformance/src/regs/
 * store.release.test.ts`.
 */
import { describe, expect, it } from "vitest";
import { KNOWN_VERSIONS, OPTIONAL_VERSIONS, REGS_FORMATS, SECTION_NAMES } from "@app/core";
import { PACK_OPTIONAL_SECTION_TABLES } from "../pack.generated";
import { openInMemory } from "../sql/drivers/node";
import type { Db } from "../sql/db";
import type { Manifest } from "../manifest";
import { releaseLoader, ReleaseError } from "../release/loader";
import { asBytes, openRegsStore, RegsStoreError, type RegsLoader } from "./store";

const hex = (s: string) => `X'${Buffer.from(s).toString("hex")}'`;
const META = "CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);";
/** The section versions this app reads (T810 M0: from `KNOWN_VERSIONS`, never retyped; the writer's side is held to the
 *  same line by pipeline/tests/test_answers_versions.py, the golden's store by conformance store.release.test.ts). */
const VERSIONS: Record<string, number> = Object.fromEntries(SECTION_NAMES.map((n) => [n, KNOWN_VERSIONS[n]]));
const K = KNOWN_VERSIONS, TROUT = KNOWN_VERSIONS.trout;
/** (T810 S) the region files' format: this app's, and the one before the trout cutover (a held set's) */
const REGION = REGS_FORMATS.region, OLD_REGION = "regs-region/1";
const H = "aaaaaaaaaaaaaaaa", R = "bbbbbbbbbbbbbbbb", I = "cccccccccccccccc";

interface Opts {
  water?: Record<string, string | null>;
  glossary?: string;
  versions?: Record<string, number>;
  manifestVersions?: Record<string, number> | null;
  tiles?: string | null;
  region2?: Record<string, string>;
  sid?: Record<string, string>;
  noWater?: boolean;
}

const metaSql = (rows: Record<string, string | null>) =>
  META + Object.entries(rows).filter(([, v]) => v !== null)
    .map(([k, v]) => `INSERT INTO meta VALUES ('${k}', '${String(v).replace(/'/g, "''")}');`).join("");

function release(o: Opts = {}): { loader: RegsLoader; opened: string[] } {
  const water = openInMemory("", metaSql({
    format: "water/3", blob_codec: "json", section_handles: H, reach_digest: R, rule_ids_sha256: I, answers_sha256: "f",
    layout: JSON.stringify({ top: ["about"], sections: { rows: ["version"] }, versions: o.versions ?? VERSIONS }),
    ...o.water,
  }) + "CREATE TABLE zdict (tbl TEXT PRIMARY KEY, d BLOB NOT NULL);" +
    `INSERT INTO zdict VALUES ('rule', ${hex("dictionary")});` +
    "CREATE TABLE static (k TEXT PRIMARY KEY, j BLOB NOT NULL);" +
    `INSERT INTO static VALUES ('answers.glossary', ${hex(o.glossary ?? '{"version":5,"terms":[]}')});` +
    "CREATE TABLE akey_region (akey INTEGER, region TEXT);" +
    "INSERT INTO akey_region VALUES (7, '1'), (9, '2'), (11, '2'), (11, '1');");
  const region = (r: string, extra: Record<string, string> = {}) => openInMemory("", metaSql({
    format: REGION, region: r, section_handles: H, reach_digest: R, rule_ids_sha256: I, ...extra }));
  const dbs: Record<string, Db> = {
    water, "regs-1": region("1"), "regs-2": region("2", o.region2),
    "regs-sid": openInMemory("", metaSql({ format: "regs-sid/1", section_handles: H, reach_digest: R, rule_ids_sha256: I, ...o.sid })),
  };
  const regionOf: Record<number, string[]> = { 7: ["1"], 9: ["2"], 11: ["2", "1"] };
  const opened: string[] = [];
  const files = Object.fromEntries(Object.keys(dbs).filter((k) => !(o.noWater && k === "water")).map((k) => [k, {}]));
  const manifest = {
    data_release: "r-test", files,
    tiles: { current: o.tiles === null ? null : { id: o.tiles ?? `${H}.123456789abc`, url: "" }, compatible: [], assets: null, mask: null },
    regulations: { valid_until: null, versions: o.manifestVersions === undefined ? VERSIONS : o.manifestVersions },
  } as unknown as Manifest;
  const db = async (n: string) => { opened.push(n); return dbs[n]!; };
  return {
    opened,
    loader: {
      release: "r-test", manifest, db: db as RegsLoader["db"],
      regionsOf: async (a) => (regionOf[a] ?? []) as never,
      regionDb: async (a) => db(`regs-${regionOf[a]![0]}`),
    },
  };
}

const refusal = async (p: Promise<unknown>): Promise<{ why: string; message: string }> => {
  const e = await p.then(() => null, (x: unknown) => x);
  expect(e).toBeInstanceOf(RegsStoreError);
  return { why: (e as RegsStoreError).why, message: (e as RegsStoreError).message };
};

describe("openRegsStore: what it accepts", () => {
  it("opens water only, and reads its meta, versions and dictionaries", async () => {
    const { loader, opened } = release();
    const st = await openRegsStore(loader);
    expect(opened).toEqual(["water"]);
    expect(st.meta).toEqual({
      release: "r-test", format: "water/3", blobCodec: "json", sectionHandles: H, reachDigest: R, ruleIdsSha256: I,
      answersSha256: "f", versions: VERSIONS, optional: {}, glossaryVersion: 5, layout: { top: ["about"], sections: { rows: ["version"] } },
    });
    expect(new TextDecoder().decode(st.dict("rule")!)).toBe("dictionary");
    expect(st.dict("ladder_frame")).toBeNull();
    expect(st.refused()).toBeNull();
  });

  it("opens a region file on first need, checked against water, and regs-sid the same way", async () => {
    const { loader, opened } = release();
    const st = await openRegsStore(loader);
    await st.region(9);
    await st.sid();
    expect(opened).toEqual(["water", "regs-2", "regs-sid"]);
  });

  it("is not held to tiles when the manifest names none, or when told so", async () => {
    await expect(openRegsStore(release({ tiles: null }).loader)).resolves.toBeDefined();
    await expect(openRegsStore(release({ tiles: "ffffffffffffffff.x" }).loader, { tiles: null })).resolves.toBeDefined();
  });

  it("holds to the tiles DRAWN when given them, over the manifest's", async () => {
    expect(await refusal(openRegsStore(release().loader, { tiles: "dddddddddddddddd.000000000000" }))).toEqual({
      why: "digest", message: `regs store: built for atlas ${H}, the map is dddddddddddddddd.000000000000 — refused` });
  });
});

describe("openRegsStore: what it refuses at open (each a RegsStoreError naming why)", () => {
  it("a release with no water file", async () => {
    expect(await refusal(openRegsStore(release({ noWater: true }).loader)))
      .toEqual({ why: "missing", message: "regs store: release r-test has no water file — refused" });
  });
  it("no meta.format", async () => {
    expect(await refusal(openRegsStore(release({ water: { format: null } }).loader)))
      .toEqual({ why: "missing", message: "regs store: water has no meta.format — refused" });
  });
  it("another format — water/2, the schema before the marks' end tokens (`part_mark.label`, `part.short_s`) — is refused", async () => {
    expect(await refusal(openRegsStore(release({ water: { format: "water/2" } }).loader)))
      .toEqual({ why: "format", message: "regs store: format water/2, this app reads water/3 — refused" });
  });
  it("display 19 — a store whose parts carry composed names (`run_name`, words in `lo` / `hi`), before the end tokens — is refused", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, display: 19 }, manifestVersions: { ...VERSIONS, display: 19 } })
      .loader))).toEqual({ why: "version", message: `regs store: display section version 19, this app reads ${K.display} — refused` });
  });
  it("a blob codec this layer cannot decode", async () => {
    expect(await refusal(openRegsStore(release({ water: { blob_codec: "brotli" } }).loader)))
      .toEqual({ why: "codec", message: "regs store: blob codec brotli, this app reads json / deflate / zdict — refused" });
  });
  it("a section version the presenters were not ported against (every one named)", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, rows: K.rows + 1, display: 16 } }).loader))).toEqual({
      why: "version",
      message: `regs store: rows section version ${K.rows + 1}, this app reads ${K.rows}, display section version 16, this app reads ${K.display} — refused` });
  });
  it("rows 29 — a store from before the year strip (rows[].year) — is refused: this app draws no strip it would derive", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, rows: 29 }, manifestVersions: { ...VERSIONS, rows: 29 } })
      .loader))).toEqual({ why: "version", message: `regs store: rows section version 29, this app reads ${K.rows} — refused` });
  });
  it("rows 30 / display 17 — a store from before the Keep tab's last choices moved to the pipeline (stamps, parent, narrow_all, " +
     "the record duty; closers) — is refused: this app re-derives none of them", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, rows: 30, display: 17 }, manifestVersions: { ...VERSIONS, rows: 30, display: 17 } })
      .loader))).toEqual({ why: "version",
      message: `regs store: rows section version 30, this app reads ${K.rows}, display section version 17, this app reads ${K.display} — refused` });
  });
  it("rows 31 / licence 6 — a store from before the four pipeline answers (Other fish rows' stamps; the steelhead stamp left " +
     "out where steelhead are closed; an under-16 document's alternatives) — is refused: this app derives none of them", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, rows: 31, licence: 6 }, manifestVersions: { ...VERSIONS, rows: 31, licence: 6 } })
      .loader))).toEqual({ why: "version",
      message: `regs store: rows section version 31, this app reads ${K.rows}, licence section version 6, this app reads ${K.licence} — refused` });
  });
  it("rows 32 — a store from before the leading origin was shipped (T161: `fish[S].lead`) — is refused: this app picks no " +
     "leading origin of its own", async () => {
    expect(await refusal(openRegsStore(release({ versions: { ...VERSIONS, rows: 32 }, manifestVersions: { ...VERSIONS, rows: 32 } })
      .loader))).toEqual({ why: "version", message: `regs store: rows section version 32, this app reads ${K.rows} — refused` });
  });
  it("a manifest that says other versions than its water file", async () => {
    expect(await refusal(openRegsStore(release({ manifestVersions: { ...VERSIONS, gear: K.gear + 1 } }).loader))).toEqual({
      why: "version", message: `regs store: the manifest says gear ${K.gear + 1}, the water file ${K.gear} — refused` });
  });
  it("another glossary version", async () => {
    expect(await refusal(openRegsStore(release({ glossary: '{"version":4,"terms":[]}' }).loader)))
      .toEqual({ why: "version", message: "regs store: glossary version 4, this app reads 5 — refused" });
  });
  it("water cut for another atlas than the manifest's tiles", async () => {
    expect(await refusal(openRegsStore(release({ tiles: "eeeeeeeeeeeeeeee.123456789abc" }).loader))).toEqual({
      why: "digest", message: `regs store: built for atlas ${H}, the map is eeeeeeeeeeeeeeee.123456789abc — refused` });
  });
});

/**
 * (T810 S) THE TROUT SECTION IS REQUIRED from the cutover (`SECTION_NAMES`, `KNOWN_VERSIONS.trout`); no section is
 * optional today (`OPTIONAL_VERSIONS` empty, the generated contract's `PACK_OPTIONAL_SECTION_TABLES` the same). A water
 * file without it, or at another version, is another reader's release.
 */
describe("openRegsStore: the trout section (T810 S: required)", () => {
  const without = (v: Record<string, number>) => Object.fromEntries(Object.entries(v).filter(([n]) => n !== "trout"));

  it("no optional section is left, on either side of the contract", () => {
    expect([Object.keys(OPTIONAL_VERSIONS), Object.keys(PACK_OPTIONAL_SECTION_TABLES)]).toEqual([[], []]);
    expect(SECTION_NAMES).toContain("trout");
  });

  it("held at the version this app reads: opens, and meta.versions names it with the others", async () => {
    const st = await openRegsStore(release().loader);
    expect(st.meta.versions).toEqual(VERSIONS);
    expect(st.meta.versions.trout).toBe(TROUT);
    expect(st.meta.optional).toEqual({});
  });

  it("absent (a release from before the cutover): refused", async () => {
    expect(await refusal(openRegsStore(release({ versions: without(VERSIONS), manifestVersions: without(VERSIONS) }).loader))).toEqual({
      why: "version", message: `regs store: trout section version absent, this app reads ${TROUT} — refused` });
  });

  it("at another version: refused", async () => {
    const v = { ...VERSIONS, trout: TROUT + 1 };
    expect(await refusal(openRegsStore(release({ versions: v, manifestVersions: v }).loader))).toEqual({
      why: "version", message: `regs store: trout section version ${TROUT + 1}, this app reads ${TROUT} — refused` });
  });
});

describe("openRegsStore: a region or sid file from another build refuses the WHOLE store", () => {
  it.each([
    ["section_handles", "atlas", "0000000000000000"],
    ["reach_digest", "rule bindings", "1111111111111111"],
    ["rule_ids_sha256", "export rules", "ffffffffffffffff"],
  ])("region %s", async (k, what, v) => {
    const st = await openRegsStore(release({ region2: { [k]: v } }).loader);
    await expect(st.region(7)).resolves.toBeDefined();           // region 1 is the same build
    const want = { why: "digest", message: `regs store: a region file is cut for ${what} ${v}, water's are ${
      { section_handles: H, reach_digest: R, rule_ids_sha256: I }[k]} — refused` };
    expect(await refusal(st.region(9))).toEqual(want);
    // poisoned: every read after it says the same, even of files that were fine
    expect(await refusal(st.region(7))).toEqual(want);
    expect(await refusal(st.water())).toEqual(want);
    expect(st.refused()?.message).toBe(want.message);
  });

  it("a region file of another format (T810 S: a held set from before the trout cutover)", async () => {
    const st = await openRegsStore(release({ region2: { format: OLD_REGION } }).loader);
    expect(await refusal(st.region(9))).toEqual({
      why: "format", message: `regs store: a region file is ${OLD_REGION}, this app reads ${REGION} — refused` });
  });

  it("a region file that is not the key's region", async () => {
    const st = await openRegsStore(release({ region2: { region: "5" } }).loader);
    expect(await refusal(st.region(9))).toEqual({ why: "damaged",
      message: "regs store: the region file opened for akey 9 is region 5, not 2 — refused" });
  });

  it("regs-sid from another export", async () => {
    const st = await openRegsStore(release({ sid: { rule_ids_sha256: "9999999999999999" } }).loader);
    expect(await refusal(st.sid())).toEqual({ why: "digest",
      message: `regs store: regs-sid is cut for export rules 9999999999999999, water's are ${I} — refused` });
  });

  it("an akey in no region is damaged, and does not poison the store", async () => {
    const st = await openRegsStore(release().loader);
    expect(await refusal(st.region(404))).toEqual({ why: "damaged", message: "regs store: akey 404 is in no region file" });
    expect(st.refused()).toBeNull();
  });
});

/**
 * The REAL release loader (P11) over the same in-memory files, its manifest naming each file's format and handles: a
 * file the release lacks, or one the loader itself refuses (format / handles not the manifest's), is the store's
 * refusal too — a RegsStoreError that poisons it — while a fetch that merely failed is not, and is tried again.
 */
function realLoader(o: { drop?: string; manifestFormat?: Record<string, string>; fail?: Set<string> } = {}) {
  const { loader: fake } = release();
  const FMT: Record<string, string> = { water: "water/3", "regs-1": REGION, "regs-2": REGION, "regs-sid": "regs-sid/1" };
  const files = Object.fromEntries(Object.keys(FMT).filter((n) => n !== o.drop)
    .map((n) => [n, { format: o.manifestFormat?.[n] ?? FMT[n], section_handles: H, url: n, sha256: "", bytes: 0 }]));
  const manifest = { ...fake.manifest, files } as unknown as Manifest;
  const opened: string[] = [];
  const L = releaseLoader(manifest, { bytes: async () => new Uint8Array() }, async (n) => {
    if (o.fail?.has(n)) { o.fail.delete(n); throw new TypeError("Failed to fetch"); }
    opened.push(n);
    return fake.db(n as never);
  });
  return { L, opened };
}

describe("openRegsStore over the real loader: a file the loader refuses refuses the store", () => {
  it("a release with no file for the key's region: missing, and the store is poisoned", async () => {
    const { L } = realLoader({ drop: "regs-2" });
    const st = await openRegsStore(L);
    const want = { why: "missing", message: "regs store: release r-test has no file regs-2 — refused" };
    expect(await refusal(st.region(9))).toEqual(want);
    expect(await refusal(st.region(7))).toEqual(want);           // region 1 is in the release, but the store is refused
    expect(await refusal(st.water())).toEqual(want);
    expect(st.refused()?.message).toBe(want.message);
  });

  it("a release with no regs-sid: missing, and poisoned", async () => {
    const { L } = realLoader({ drop: "regs-sid" });
    const st = await openRegsStore(L);
    expect(await refusal(st.sid())).toEqual({ why: "missing", message: "regs store: release r-test has no file regs-sid — refused" });
    expect(st.refused()?.why).toBe("missing");
  });

  it("a region file the loader refuses (not the manifest's format): format, and poisoned", async () => {
    const { L } = realLoader({ manifestFormat: { "regs-2": "regs-region/9" } });
    const st = await openRegsStore(L);
    const want = { why: "format", message: `regs store: regs-2: format ${REGION} / handles ` + H +
      " are not the manifest's regs-region/9 / " + H + " — refused" };
    expect(await refusal(st.region(9))).toEqual(want);
    expect(await refusal(st.sid())).toEqual(want);
  });

  it("water the loader refuses at open is a RegsStoreError, not the loader's ReleaseError", async () => {
    const { L } = realLoader({ manifestFormat: { water: "water/9" } });
    const e = await openRegsStore(L).then(() => null, (x: unknown) => x);
    expect(e).toBeInstanceOf(RegsStoreError);
    expect(e).not.toBeInstanceOf(ReleaseError);
    expect((e as RegsStoreError).why).toBe("format");
  });

  it("a fetch that failed is not a refusal: it is thrown as is, the store is not poisoned, and the next ask opens it", async () => {
    const { L, opened } = realLoader({ fail: new Set(["regs-2"]) });
    const st = await openRegsStore(L);
    await expect(st.region(9)).rejects.toThrow(new TypeError("Failed to fetch"));
    expect(st.refused()).toBeNull();
    await expect(st.region(9)).resolves.toBeDefined();
    expect(opened).toContain("regs-2");
  });
});

describe("asBytes", () => {
  it("passes bytes, wraps an ArrayBuffer, refuses anything else as damaged", () => {
    const b = new Uint8Array([1, 2]);
    expect(asBytes(b)).toBe(b);
    expect([...asBytes(new Uint8Array([3, 4]).buffer as unknown as Uint8Array)]).toEqual([3, 4]);
    expect(() => asBytes("x")).toThrow(new RegsStoreError("damaged", "regs store: a blob column holds string — refused"));
    expect(() => asBytes(null)).toThrow("regs store: a blob column holds null — refused");
  });
});
