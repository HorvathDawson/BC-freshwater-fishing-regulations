/**
 * THE REGS STORE (T03): one view over the regulations half of a release — the `water` file, the `regs-<region>` file
 * holding a key's frames, and `regs-sid` — opened through the `ReleaseLoader` (P11), never a file of its own. It is
 * REFUSED, as a whole, unless the release is the one this app reads:
 *
 *   water      meta.format `water/1`, a blob codec this layer decodes, `layout.versions` = core `KNOWN_VERSIONS` (and =
 *              the manifest's `regulations.versions`), `static answers.glossary.version` = 5, and its `section_handles`
 *              the sid space of the tiles drawn (the P10 pair: tile version `<handles16>.<build>`).
 *   region,    checked on their first open (web fetches them lazily): their format, and `section_handles`,
 *   sid        `reach_digest` and `rule_ids_sha256` equal to water's — a region file from another build numbers rules
 *              differently, and every rix it names would be another rule.
 *
 * A refusal is a `RegsStoreError` (the UI says "Regulations unavailable", never a partly-read page). The loader's own
 * refusal of a file (a `ReleaseError`: the release has no such file, or the file is not the manifest's format/handles)
 * is the store's too: `missing` / `format`, the loader's sentence. A fetch that merely failed is not a refusal; it is
 * thrown as is and the next ask tries again. A refusal found after open — a region file from another build, or one the
 * release lacks — POISONS the store: every later read throws the same error, so a page
 * is never drawn half from one build and half from another. Keeping the last good pair is the boot's (P10 `pair`).
 *
 * The loader already holds every file to the manifest (its format and `section_handles`); this holds the manifest and
 * the files to the APP (what it was written against) and to each other.
 *
 * RECONCILED WITH THE TICKET (written for a single `regs.sqlite`, before P08/P11): `openRegsStore(db, expect)` became
 * `openRegsStore(loader, expect)`; the `regs-store/4` format check became a format per file (`REGS_FORMATS`); the three
 * digests are no longer held to the province bundle's `BundleInfo` (there is no bundle on the release path) but to the
 * tiles drawn and to each other; `meta.store_digest` / `meta.counts` are not in the release files and are not read.
 */
import { BLOB_CODECS, KNOWN_VERSIONS, REGS_FORMATS, SECTION_NAMES, type SectionName } from "@app/core";
import type { Cell, Db } from "../sql/db";
import { ReleaseError, type ReleaseLoader } from "../release/loader";
import { unpackBlob } from "./blob";
import { asBytes, RegsStoreError } from "./errors";
import { makeReads, type RegsReads } from "./reads";

export { asBytes, RegsStoreError } from "./errors";

/** What the store is opened through: the release loader (P11), or any object with its shape (tests). */
export type RegsLoader = Pick<ReleaseLoader, "release" | "manifest" | "db" | "regionsOf" | "regionDb">;

/**
 * What the store is held to beyond the release itself. `tiles`: the tile version the map DRAWS (`<handles16>.<build>`);
 * absent → the manifest's `tiles.current.id`; null (or a manifest naming no tiles, the fixture release) → not held.
 */
export interface RegsExpect { readonly tiles?: string | null }

export interface RegsMeta {
  readonly release: string;
  readonly format: typeof REGS_FORMATS.water;
  readonly blobCodec: "json" | "deflate" | "zdict";
  readonly sectionHandles: string;
  readonly reachDigest: string;
  readonly ruleIdsSha256: string;
  readonly answersSha256: string;
  readonly versions: { readonly [K in SectionName]: number };
  readonly glossaryVersion: number;
  readonly layout: { readonly top: readonly string[]; readonly sections: Readonly<Record<string, readonly string[]>> };
}

/** The store: T03's open surface and T04's reads (reads.ts), routed by table to the file that holds it. */
export interface RegsStore extends RegsReads {
  readonly meta: RegsMeta;
  /** The water file (keys, segments, items, parts, rules, statics, dictionaries). */
  water(): Promise<Db>;
  /** The region file holding `akey`'s cells and frames, checked against water on its first open. */
  region(akey: number): Promise<Db>;
  /** regs-sid (section → key / place), checked against water on its first open. */
  sid(): Promise<Db>;
  /** core (the release's names file): only `itemOrd` reads it. */
  core(): Promise<Db>;
  /** The table's preset dictionary (water `zdict`, loaded once at open; region tables' are there too). */
  dict(table: string): Uint8Array | null;
  /** `unpackBlob` with the table's dictionary. */
  val<T = unknown>(table: string, blob: Cell): T;
  /** The refusal found after open (a region or sid file from another build), else null. */
  refused(): RegsStoreError | null;
  close(): void;
}

const metaOf = async (d: Db): Promise<Map<string, string>> =>
  new Map((await d.all("SELECT k, v FROM meta")).map((r) => [String(r.k), String(r.v)]));

/** A `ReleaseError` (the loader refusing a file) as the store's refusal; anything else (a failed fetch) unchanged. */
const asRefusal = (e: unknown, missing: boolean): unknown =>
  e instanceof ReleaseError ? new RegsStoreError(missing ? "missing" : "format", `regs store: ${e.message} — refused`) : e;

const DIGESTS = [["section_handles", "atlas"], ["reach_digest", "rule bindings"], ["rule_ids_sha256", "export rules"]] as const;

export async function openRegsStore(loader: RegsLoader, expect: RegsExpect = {}): Promise<RegsStore> {
  if (!loader.manifest.files.water)
    throw new RegsStoreError("missing", `regs store: release ${loader.release} has no water file — refused`);
  const water = await loader.db("water").catch((e) => { throw asRefusal(e, false); });
  const m = await metaOf(water);
  const need = (k: string): string => {
    const v = m.get(k);
    if (v === undefined || v === "") throw new RegsStoreError("missing", `regs store: water has no meta.${k} — refused`);
    return v;
  };

  // 1–3: the format and codec this app reads
  const format = need("format");
  if (format !== REGS_FORMATS.water)
    throw new RegsStoreError("format", `regs store: format ${format}, this app reads ${REGS_FORMATS.water} — refused`);
  const codec = need("blob_codec");
  if (!BLOB_CODECS.includes(codec))
    throw new RegsStoreError("codec", `regs store: blob codec ${codec}, this app reads ${BLOB_CODECS.join(" / ")} — refused`);

  // 4: the section versions the presenters were ported against, and the manifest saying the same
  let layout: { top?: string[]; sections?: Record<string, string[]>; versions?: Record<string, number> };
  try { layout = JSON.parse(need("layout")); }
  catch (e) { if (e instanceof RegsStoreError) throw e; throw new RegsStoreError("damaged", "regs store: meta.layout is not JSON — refused"); }
  const versions = layout.versions ?? {};
  const off = SECTION_NAMES.filter((n) => versions[n] !== KNOWN_VERSIONS[n])
    .map((n) => `${n} section version ${versions[n] ?? "absent"}, this app reads ${KNOWN_VERSIONS[n]}`);
  if (off.length) throw new RegsStoreError("version", `regs store: ${off.join(", ")} — refused`);
  const said = loader.manifest.regulations?.versions as Readonly<Record<string, number>> | undefined;
  const unsaid = SECTION_NAMES.filter((n) => said && said[n] !== versions[n])
    .map((n) => `the manifest says ${n} ${said![n] ?? "absent"}, the water file ${versions[n]}`);
  if (unsaid.length) throw new RegsStoreError("version", `regs store: ${unsaid.join(", ")} — refused`);

  // 5: every table's dictionary (water holds the region tables' too)
  const dicts = new Map<string, Uint8Array>();
  for (const r of await water.all("SELECT tbl, d FROM zdict")) dicts.set(String(r.tbl), asBytes(r.d));
  const dict = (t: string) => dicts.get(t) ?? null;
  const val = <T>(t: string, b: Cell): T => unpackBlob(asBytes(b), dict(t)) as T;

  // 6: the glossary version
  const g = await water.get("SELECT j FROM static WHERE k = 'answers.glossary'");
  if (!g) throw new RegsStoreError("missing", "regs store: no static answers.glossary — refused");
  const gv = val<{ version?: number }>("static", g.j).version;
  if (gv !== KNOWN_VERSIONS.glossary)
    throw new RegsStoreError("version", `regs store: glossary version ${gv ?? "absent"}, this app reads ${KNOWN_VERSIONS.glossary} — refused`);

  // 7: paired with the tiles drawn (P10): the tile version's sid space is the water's
  const handles = need("section_handles"), reach = need("reach_digest"), ruleIds = need("rule_ids_sha256");
  const tiles = expect.tiles === undefined ? loader.manifest.tiles?.current?.id ?? null : expect.tiles;
  if (tiles !== null && tiles.split(".")[0] !== handles)
    throw new RegsStoreError("digest", `regs store: built for atlas ${handles}, the map is ${tiles} — refused`);

  // the others, on first open: same build as water, or the whole store is refused
  let refusal: RegsStoreError | null = null;
  const checked = new WeakMap<Db, Promise<Db>>();
  const hold = (name: string, d: Db, want: string, extra?: (meta: Map<string, string>) => string | null): Promise<Db> => {
    let p = checked.get(d);
    if (!p) {
      p = metaOf(d).then((fm) => {
        const f = fm.get("format");
        if (f !== want) throw new RegsStoreError("format", `regs store: ${name} is ${f ?? "unformatted"}, this app reads ${want} — refused`);
        for (const [k, what] of DIGESTS) {
          const mine = fm.get(k) ?? "absent", theirs = m.get(k)!;
          if (mine !== theirs)
            throw new RegsStoreError("digest", `regs store: ${name} is cut for ${what} ${mine}, water's are ${theirs} — refused`);
        }
        const bad = extra?.(fm);
        if (bad) throw new RegsStoreError("damaged", `regs store: ${bad} — refused`);
        return d;
      }).catch((e) => {
        if (e instanceof RegsStoreError) refusal ??= e;
        checked.delete(d);                     // a read failure (not a refusal) is tried again
        throw e;
      });
      checked.set(d, p);
    }
    return p;
  };
  const live = () => { if (refusal) throw refusal; };
  /** The loader's refusal of a file, as the store's (and it poisons the store); any other failure passes through. */
  const opened = (p: Promise<Db>, missing: boolean): Promise<Db> => p.catch((e) => {
    const r = asRefusal(e, missing);
    if (r instanceof RegsStoreError) refusal ??= r;
    throw r;
  });

  const meta: RegsMeta = {
    release: loader.release, format: REGS_FORMATS.water, blobCodec: codec as RegsMeta["blobCodec"],
    sectionHandles: handles, reachDigest: reach, ruleIdsSha256: ruleIds, answersSha256: need("answers_sha256"),
    versions: Object.fromEntries(SECTION_NAMES.map((n) => [n, versions[n]!])) as RegsMeta["versions"],
    glossaryVersion: gv,
    layout: { top: layout.top ?? [], sections: layout.sections ?? {} },
  };

  const files = {
    async water(): Promise<Db> { live(); return water; },
    async region(akey: number): Promise<Db> {
      live();
      const regions = await loader.regionsOf(akey);
      if (!regions.length) throw new RegsStoreError("damaged", `regs store: akey ${akey} is in no region file`);
      const d = await opened(loader.regionDb(akey), regions.some((r) => !loader.manifest.files[`regs-${r}`]));
      return hold("a region file", d, REGS_FORMATS.region, (fm) =>
        (regions as readonly string[]).includes(fm.get("region") ?? "")
          ? null : `the region file opened for akey ${akey} is region ${fm.get("region")}, not ${regions.join(" / ")}`);
    },
    async sid(): Promise<Db> {
      live();
      return hold("regs-sid", await opened(loader.db("regs-sid"), !loader.manifest.files["regs-sid"]), REGS_FORMATS.sid);
    },
    /** core: only `itemOrd` (item_id -> ord, its UNIQUE index); the loader holds it to the manifest. */
    async core(): Promise<Db> { live(); return opened(loader.db("core"), !loader.manifest.files.core); },
    /** (T182) the water map's boxes (`part_geo`): the geo file, or water in a release cut before the move. */
    async geo(): Promise<Db> { live(); return loader.manifest.files.geo ? opened(loader.db("geo"), false) : water; },
  };
  const { clear, ...reads } = makeReads({ ...files, val });
  return {
    meta, ...files, ...reads, dict, val,
    refused: () => refusal,
    close() { clear(); dicts.clear(); },
  };
}
