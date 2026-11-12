/**
 * THE STORE READS (T04): every question the Regulations page asks of the release, each one or two keyed SQL reads on
 * the file that holds its table, returning the answers-file values exactly as `pipeline/deliver/answers/encode.py`'s
 * codecs decode them — so the TS side sees the same `RowsValue` / `LicenceValue` / `StatusFrame` the Python reference
 * and page v36 see. The per-family interning is undone here, never in core (rows 30: a row's year strip, held once
 * per distinct strip in the static `rows.strips` and named by the rows frame's fifth slot, goes back on its row here).
 *
 *   core       itemOrd (core `item.item_id` is UNIQUE: one indexed read — core's ord IS water's item_ord, P08)
 *   water      item, parts, picker, akey, segStarts, segMoments, ruleset, rules, lics, statics, str, place, sectionPart,
 *              partLines (T388)
 *   regs-<r>   cell, cells and the five frame families, in the file water's `akey_region` names for the KEY — a frame
 *              id is the store's, the same in every region file that holds it, but only those files hold it; so every
 *              frame read is asked with the key it was reached from (`ladder(akey, id)`, not `ladder(id)`)
 *   regs-sid   sectionAkey, sectionPlace (one 8 KB / 4 KB block per lookup, cached), and so unnamedName
 *
 * CACHING: the promise, not the value (two concurrent taps share one read). Statics, rules, lics and str once per store;
 * every id-keyed family unbounded per store (ids are immutable within a release, a session touches a few hundred).
 * `close()` drops them all.
 *
 * Ports: `pipeline/deliver/store/decode.py` `Store.answers` / `export_subset` / `_cell` / `place_of` / `unnamed_name` /
 * `akey_of`; `pipeline/deliver/answers/encode.py` `LadderCodec.decode_frame` (to WIRE verdicts: core T07 reads them as
 * page v36 `verdictList` does), `RowsCodec.decode_frame`, `LicenceCodec.decode_frame`, `FrameCodec.decode_frame`;
 * `store/common.py` `BLOCK_BITS = 12`.
 */
import type {
  AkeyRow, AnswersPlace, CellIds, Confidence, ContinueRow, DisplayFrame, ExportRule, GearValue, LadderFrameWire, LicenceValue,
  Moment, PartFacts, Picker, RowsValue, RuleFacts, Statics, VerdictWire,
} from "@app/core";
import { isConfidence } from "@app/core";
import type { Cell, Db, Row } from "../sql/db";
import { CROSS } from "../release/queries";
import { RegsStoreError } from "./errors";

export interface ItemRow {
  readonly ord: number; readonly id: string; readonly name: string; readonly kind: string;
  readonly shown: boolean; readonly picker: number | null; readonly sections: number; readonly outsideBc: number;
  readonly steelhead: 1 | 2 | null; readonly entries: readonly number[]; readonly x: Record<string, unknown> | null;
}
export interface PartRow {
  readonly itemOrd: number; readonly partIx: number; readonly akey: number | null;
  /** null when part.ord IS NULL (outside B.C.) — decode.py `Store.answers` shape. */
  readonly facts: PartFacts | null;
  readonly sections: number;
  /** (T517) the part's rule-set group (the picker's one choice) and its connected run in it: parts sharing both are one
   *  run the DFO salmon chain cuts (the regular map joins them) */
  readonly grp: number; readonly run: number;
  /** (T278; owner T166 2026-10-28: no parts on tidal water) a tidal cut's piece: no key, every section tidal (its
   *  `xpart.province_except` holds "tidal"). The store keeps it at its part index; nothing lists it as a part. */
  readonly tidal: boolean;
}
export interface PlaceRow extends AnswersPlace { readonly place: number }
/**
 * (T388) ONE RUN OF A PART'S OWN STREAM (water `part_line`; pipeline `release/part_line.py`): the part's stream sections
 * that lie end to end on one blue line, as the span [m0, m1) metres along stream key `blk` — the units of `lines` /
 * `StreamPlace`. Only a water whose links carry a place has rows (two or more parts); a part with no stream section (a
 * lake) has none.
 */
export interface PartLine {
  readonly partIx: number; readonly blk: number; readonly m0: number; readonly m1: number;
}
export interface RulesetRow { readonly sections: number; readonly reach: readonly number[]; readonly trib: readonly number[] }
export interface StoredRule { readonly rix: number; readonly export: ExportRule; readonly facts: RuleFacts }
/** The store's statics (`static` rows) — `pdfPages` too: the release ships the synopsis's printed → PDF page map
 *  (`export.pdf_pages`, the export guide's `pdf_pages`, pipeline.common.synopsis_pages); the app never computes one. */
export type StoreStatics = Statics;
export interface Frames {
  readonly ladder: LadderFrameWire; readonly rows: RowsValue; readonly gear: GearValue;
  readonly licence: LicenceValue; readonly display: DisplayFrame;
}

export interface RegsReads {
  itemOrd(itemId: string): Promise<number | null>;
  item(itemOrd: number): Promise<ItemRow>;
  parts(itemOrd: number): Promise<readonly PartRow[]>;
  picker(id: number): Promise<Picker>;
  akey(akey: number): Promise<AkeyRow>;
  segStarts(seg: number): Promise<readonly number[]>;
  /** segmom.ix → statics.moments[i] */
  segMoments(mom: number): Promise<readonly Moment[]>;
  cell(akey: number, s: number): Promise<CellIds>;
  /** Every segment's cell of the key, index = s (= 0..nseg-1, else damaged). */
  cells(akey: number): Promise<readonly CellIds[]>;
  ladder(akey: number, id: number): Promise<LadderFrameWire>;
  rows(akey: number, id: number): Promise<RowsValue>;
  gear(akey: number, id: number): Promise<GearValue>;
  licence(akey: number, id: number): Promise<LicenceValue>;
  /** StatusFrame | TidalStatus (a tidal key's display frame has status "tidal"). */
  display(akey: number, id: number): Promise<DisplayFrame>;
  /** The five frames of one cell. */
  frames(akey: number, cell: CellIds): Promise<Frames>;
  ruleset(setId: number): Promise<RulesetRow>;
  /** Every rule (rule + display_rule), by rix, once. */
  rules(): Promise<ReadonlyMap<number, StoredRule>>;
  /** Every licensing record, by lix, once. */
  lics(): Promise<ReadonlyMap<number, unknown>>;
  statics(): Promise<StoreStatics>;
  str(id: number): Promise<string>;
  /** A section handle's key; 0 = outside B.C. */
  sectionAkey(handle: number): Promise<number>;
  /** A section handle's place index; null = outside B.C. */
  sectionPlace(handle: number): Promise<number | null>;
  place(ix: number): Promise<PlaceRow>;
  /** An unnamed section's whole name ("Unnamed stream · Region 5"); null outside B.C. */
  unnamedName(handle: number): Promise<string | null>;
  /** The part of item `itemOrd` section `sid` belongs to (water `part_section`, P09; its `ord` is the item_ord);
   *  null: the section is not one of the item's. */
  sectionPart(itemOrd: number, sid: number): Promise<number | null>;
  /**
   * (T387) Each section of part `partIx` of item `itemOrd`, with its length along the water in metres (water
   * `part_section.m`: a line's length, a polygon's half perimeter — pipeline `bundle/access.py`); a section the build did
   * not measure has null.
   */
  partLengths(itemOrd: number, partIx: number): Promise<ReadonlyMap<number, number | null>>;
  /**
   * (T388) Item `itemOrd`'s part runs (water `part_line`, by part, stream key, start): what a named water's link place
   * is made from and resolved by, with no `lines` file. Empty: the water has none (one part, or no stream section).
   */
  partLines(itemOrd: number): Promise<readonly PartLine[]>;
  /**
   * THE WATER MAP'S ROWS (P15) for item `itemOrd`, each one keyed read on its table's primary key: `part_geo` (box,
   * midpoint — microdegrees as degrees; its `part_ix` is river order, mouth first), `part_mark` (boundaries: down part,
   * up parts), `part_section` (the sids of each part) and `water_exit` (the receiving item, named from core; one core
   * does not hold: damaged), and `water_continue` both ways (the stream this one goes on as, and the one it goes on
   * from — the same watercourse under another name; pipeline `continuation.py`).
   */
  navRows(itemOrd: number): Promise<NavRows>;
  /**
   * The ords of the items `part_of` the same water as `itemOrd` (a split lake's closure areas), itself included; for
   * the split lake itself (it owns no part), itself and its closure areas.
   */
  siblingOrds(itemOrd: number): Promise<readonly number[]>;
}

/** `navRows`: what the water map reads of one item beyond its `item` / `part` rows. */
export interface NavRows {
  readonly geo: readonly { readonly partIx: number; readonly midSid: number;
                           readonly mid: readonly [number, number];
                           readonly box: readonly [number, number, number, number] | null }[];
  readonly marks: readonly { readonly lon: number; readonly lat: number; readonly token: string | null;
                             readonly down: number; readonly up: readonly number[];
                             /** T207: the named cut's confidence where it is not high (`part_mark.confidence`) */
                             readonly confidence?: Confidence }[];
  readonly sections: readonly { readonly sid: number; readonly partIx: number }[];
  readonly exit: { readonly ord: number; readonly part: number | null; readonly item: string;
                   readonly name: string } | null;
  /** `water_continue`: the other water (named from core; one core does not hold: damaged), this water's part at the
   *  junction (`part`) and the other's (`otherPart`). */
  readonly continuesAs: ContinueRow | null;
  readonly continuesFrom: ContinueRow | null;
}

/** What the reads stand on (store.ts hands them its own). */
export interface ReadBase {
  water(): Promise<Db>;
  region(akey: number): Promise<Db>;
  sid(): Promise<Db>;
  core(): Promise<Db>;
  /** (T182) the file holding `part_geo`: geo. */
  geo(): Promise<Db>;
  val<T = unknown>(table: string, blob: Cell): T;
}

/** The family tables a frame read may name — a fixed list, never caller input. */
const FAMILY = {
  ladderFrame: "ladder_frame", ladderVerdict: "ladder_verdict", rowsFrame: "rows_frame", rowsDecided: "rows_decided",
  rowsRow: "rows_row", gearFrame: "gear_frame", licenceFrame: "licence_frame", licenceHold: "licence_hold",
  licenceDoc: "licence_doc", licenceAnswer: "licence_answer", licenceU16: "licence_u16", displayFrame: "display_frame",
} as const;
type Family = (typeof FAMILY)[keyof typeof FAMILY];

/** `store/common.py` BLOCK_BITS: a section handle's block is `h >> 12`, its slot `h & 4095`. */
export const BLOCK_BITS = 12;
const SLOT = (1 << BLOCK_BITS) - 1;

const STATICS: Readonly<Record<keyof StoreStatics, string>> = {
  glossary: "answers.glossary", fish: "answers.fish", moments: "answers.moments",
  strip: "display.strip", reasons: "display.reasons", ends: "display.ends",
  countNouns: "gear.count_nouns", gearMoments: "gear.moments", conductMeans: "gear.conduct_means",
  profiles: "licence.profiles", profileDims: "licence.profile_dims", profileLabels: "licence.profile_labels",
  residencyLabels: "licence.residency_labels",
  exportSpecies: "export.species", exportLicences: "export.licences", exportConduct: "export.conduct",
  ladderReasons: "ladder.reasons", ladderReasonState: "ladder.reason_state",
  pdfPages: "export.pdf_pages",
};

type YearStrip = RowsValue["rows"][number]["year"];
/** (T286) The test bed's "before" build: batching off (apps/mobile index.js, `EXPO_PUBLIC_REGS_ONE_BY_ONE=1`). */
const ONE_BY_ONE = (): boolean => (globalThis as { __regsOneByOne?: boolean }).__regsOneByOne === true;
/** (T287) Rules inflated per turn of the JS thread, and the turn (a macrotask: the screen draws between). */
const RULES_SLICE = 250;
const turnOf = () => new Promise<void>((r) => setTimeout(r, 0));
/** (T286) The most keys one batched read names (SQLite's bound-parameter limit is far above it). */
const BATCH = 500;
/**
 * (T286) Run `f` once the asks made with this one have been made: a few promise hops later, so asks one or two `await`s
 * behind it (a `Promise.all` of reads that each await a cached one) join the same batch. Microtasks only — no timer,
 * so a test's fake clock never holds a read back.
 */
const afterHops = (f: () => void, hops = 8): void => {
  let p = Promise.resolve();
  for (let i = 0; i < hops; i++) p = p.then();
  void p.then(f);
};
const damaged = (what: string) => new RegsStoreError("damaged", `regs store: ${what}`);
const n = (c: Cell): number => Number(c);
const nn = (c: Cell): number | null => (c === null || c === undefined ? null : Number(c));
const json = <T>(c: Cell): T => JSON.parse(String(c)) as T;
/** A microdegree column (`PACK_UNITS` geo.part_geo / water.part_mark) in degrees. */
const micro = (c: Cell): number => Number(c) / 1e6;

export function makeReads(base: ReadBase): RegsReads & { clear(): void } {
  const caches = new Map<string, Map<string | number, Promise<unknown>>>();
  /** The promise for `key` in cache `name`, made once; a failure is not cached (the next ask tries again). */
  const once = <T>(name: string, key: string | number, make: () => Promise<T>): Promise<T> => {
    let c = caches.get(name);
    if (!c) caches.set(name, c = new Map());
    let p = c.get(key) as Promise<T> | undefined;
    if (!p) {
      p = make();
      c.set(key, p);
      p.catch(() => c!.delete(key));
    }
    return p;
  };
  const row = async (d: Promise<Db>, sql: string, key: number | string, what: string): Promise<Row> => {
    const r = await (await d).get(sql, key);
    if (!r) throw damaged(`no ${what} ${key}`);
    return r;
  };
  /**
   * (T286) ONE CALL FOR MANY KEYS. A tap reads a few hundred rows, most of them one keyed row at a time (a part's
   * strings, a ladder's verdicts, a rows frame's rows); on a phone each read is a native round trip through T264's
   * one-at-a-time queue, so the count WAS the time. Asks made together — in the same turn of the JS thread, give or
   * take a few promise hops — are read in one `WHERE <key> IN (…)` per file. The cache is still per key (`once`), the
   * values exactly what one-row reads gave; a key the file does not hold rejects its own asker (damaged), and a failed
   * read rejects every asker of that batch (and, uncached, is asked again next time).
   */
  type Waiter<T> = { resolve: (v: T) => void; reject: (e: unknown) => void };
  const batched = <K extends number | string, T>(name: string, what: string,
                                                  readMany: (keys: K[]) => Promise<ReadonlyMap<K, T>>) => {
    let pending: Map<K, Waiter<T>> | null = null;
    const flush = async (b: Map<K, Waiter<T>>) => {
      try {
        const keys = [...b.keys()];
        const got = new Map<K, T>();
        for (let i = 0; i < keys.length; i += BATCH) for (const [k, v] of await readMany(keys.slice(i, i + BATCH))) got.set(k, v);
        for (const [k, w] of b) if (got.has(k)) w.resolve(got.get(k)!); else w.reject(damaged(`no ${what} ${k}`));
      } catch (e) { for (const w of b.values()) w.reject(e); }
    };
    return (key: K): Promise<T> => once(name, key, () => new Promise<T>((resolve, reject) => {
      // the test bed's "before" build (`EXPO_PUBLIC_REGS_ONE_BY_ONE=1`): every key its own read, as before T286
      if (ONE_BY_ONE()) { void flush(new Map([[key, { resolve, reject }]])); return; }
      if (!pending) {
        const b = pending = new Map();
        afterHops(() => { pending = null; void flush(b); });
      }
      pending.set(key, { resolve, reject });
    }));
  };
  /** The rows of `sql` (its `?` the key list) on one file, by their `id` column. */
  const byKey = async <K extends number | string>(d: Db, sql: (marks: string) => string, keys: K[], col = "id") =>
    new Map((await d.all(sql(keys.map(() => "?").join(", ")), ...keys)).map((r) => [r[col] as K, r] as const));

  /** A frame family's blobs by id, batched per region file (an id is the same frame in every file that holds it). */
  const families = new Map<Family, (akey: number, id: number) => Promise<unknown>>();
  const familyOf = (t: Family) => {
    let f = families.get(t);
    if (!f) {
      const akeyOf = new Map<number, number>();         // id → the key it was reached from (its region file)
      const load = batched<number, unknown>(t, t, async (ids) => {
        const byDb = new Map<Db, number[]>();
        for (const id of ids) {
          const d = await base.region(akeyOf.get(id)!);
          byDb.set(d, [...(byDb.get(d) ?? []), id]);
        }
        const out = new Map<number, unknown>();
        for (const [d, some] of byDb) {
          for (const [id, r] of await byKey(d, (m) => `SELECT id, j FROM ${t} WHERE id IN (${m})`, some)) out.set(n(id), base.val(t, r.j));
        }
        return out;
      });
      families.set(t, f = (akey, id) => { if (!akeyOf.has(id)) akeyOf.set(id, akey); return load(id); });
    }
    return f;
  };
  /** One blob of a family, by id, from the region file holding the key's frames. */
  const blob = <T>(t: Family, akey: number, id: number): Promise<T> => familyOf(t)(akey, id) as Promise<T>;
  /** Every cell of a key (`s` order), batched per region file. */
  const keyCells = batched<number, Row[]>("keyCells", "cells of akey", async (keys) => {
    const byDb = new Map<Db, number[]>();
    for (const k of keys) { const d = await base.region(k); byDb.set(d, [...(byDb.get(d) ?? []), k]); }
    const out = new Map<number, Row[]>(keys.map((k) => [k, []]));
    for (const [d, some] of byDb) {
      const rs = await d.all(`SELECT akey, s, ladder, rows, gear, licence, display FROM cell WHERE akey IN (${some.map(() => "?").join(", ")}) `
        + "ORDER BY akey, s", ...some);
      for (const r of rs) out.get(n(r.akey))?.push(r);
    }
    return out;
  });
  /** water `str`, batched. */
  const strs = batched<number, string>("str", "str", async (ids) => {
    const rs = await byKey(await base.water(), (m) => `SELECT id, text FROM str WHERE id IN (${m})`, ids);
    return new Map([...rs].map(([id, r]) => [n(id), String(r.text)]));
  });
  /** water `akey` rows, batched (decoded by `reads.akey`). */
  const akeyRows = batched<number, Row>("akeyRow", "akey", async (keys) => {
    const rs = await byKey(await base.water(), (m) =>
      "SELECT akey, ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, kind, tidal, seg, mom, nseg " +
      `FROM akey WHERE akey IN (${m})`, keys, "akey");
    return new Map([...rs].map(([k, r]) => [n(k), r]));
  });
  /** water `segmom` and `ruleset` rows, batched. */
  const segMoms = batched<number, Row>("segmomRow", "segmom", async (keys) => {
    const rs = await byKey(await base.water(), (m) => `SELECT mom, ix FROM segmom WHERE mom IN (${m})`, keys, "mom");
    return new Map([...rs].map(([k, r]) => [n(k), r]));
  });
  const rulesets = batched<number, Row>("rulesetRow", "ruleset", async (keys) => {
    const rs = await byKey(await base.water(), (m) => `SELECT set_id, j FROM ruleset WHERE set_id IN (${m})`, keys, "set_id");
    return new Map([...rs].map(([k, r]) => [n(k), r]));
  });
  /** water `seglist` starts, batched. */
  const segLists = batched<number, Row>("seglistRow", "seglist", async (keys) => {
    const rs = await byKey(await base.water(), (m) => `SELECT seg, starts FROM seglist WHERE seg IN (${m})`, keys, "seg");
    return new Map([...rs].map(([k, r]) => [n(k), r]));
  });
  /** rows 30: every distinct year strip, once (water static `rows.strips`; a rows frame names its rows' strips). */
  const yearStrips = (): Promise<readonly YearStrip[]> => once("strips", 0, async () =>
    base.val<YearStrip[]>("static", (await row(base.water(), "SELECT j FROM static WHERE k = ?", "rows.strips", "static")).j));

  const reads: RegsReads = {
    itemOrd: (itemId) => once("itemOrd", itemId, async () => {
      const core = await base.core();
      const r = await core.get("SELECT ord FROM item WHERE item_id = ?", itemId);
      if (r) return n(r.ord);
      // (T555) a saved id a later build absorbed opens the water it became (`core.item_alias`, the release source's
      // `currentItem` rule); a core cut before the table has no aliases
      if (!(await core.get(CROSS.HAS_ITEM_ALIAS.sql))) return null;
      const now = await core.get(CROSS.ITEM_CURRENT.sql, itemId);
      const a = now ? await core.get("SELECT ord FROM item WHERE item_id = ?", String(now.item_id)) : null;
      return a ? n(a.ord) : null;
    }),

    item: (ord) => once("item", ord, async () => {
      const r = await row(base.water(),
        "SELECT item_ord, item_id, shown, picker, name, kind, sections, outside_bc, entries, steelhead, x " +
        "FROM item WHERE item_ord = ?", ord, "item");
      const [id, kind] = await Promise.all([reads.str(n(r.item_id)), reads.str(n(r.kind))]);
      return {
        ord: n(r.item_ord), id, name: String(r.name), kind, shown: n(r.shown) === 1, picker: nn(r.picker),
        sections: n(r.sections), outsideBc: n(r.outside_bc), steelhead: nn(r.steelhead) as 1 | 2 | null,
        entries: json<number[]>(r.entries), x: r.x === null || r.x === undefined ? null : base.val<Record<string, unknown>>("item", r.x),
      };
    }),

    parts: (ord) => once("parts", ord, async () => {
      const rows = await (await base.water()).all(
        "SELECT part_ix, akey, ord, label_s, lo_s, hi_s, runs_s, place_s, km, closed_all_year, " +
        "paper_licence, sections, grp, run, " +
        "(SELECT j FROM xpart WHERE id = part.xp) AS xj " +
        "FROM part WHERE item_ord = ? ORDER BY part_ix", ord);
      return Promise.all(rows.map(async (p, i): Promise<PartRow> => {
        if (n(p.part_ix) !== i) throw damaged(`item ${ord}'s parts are out of sequence`);
        const s = (c: Cell) => reads.str(n(c));
        // (T286) every part's strings asked at once: one read for the whole water
        const opt = (c: Cell) => (c === null || c === undefined ? null : s(c));
        // (T517, owner 2026-11-09) `lo_s` / `hi_s` are the part's END TOKENS (keys of `display.ends`), never words: its name
        // is composed by `@app/core` `spanName` (no ends: the water's name — no part ships a stored name)
        const [label, runs, place, lo, hi] = p.ord === null || p.ord === undefined ? [] : await Promise.all([
          s(p.label_s), s(p.runs_s), opt(p.place_s), opt(p.lo_s), opt(p.hi_s)]);
        const facts: PartFacts | null = p.ord === null || p.ord === undefined ? null : {
          order: n(p.ord), label: label!, lo: lo ?? null, hi: hi ?? null,
          runs: runs!, place: place ?? null, km: nn(p.km),
          closed_all_year: n(p.closed_all_year) === 1, paper_licence: json<number[]>(p.paper_licence),
        };
        const xp = p.akey === null && p.xj !== null && p.xj !== undefined
          ? base.val<{ province_except?: readonly string[] }>("xpart", p.xj) : null;
        return { itemOrd: ord, partIx: i, akey: nn(p.akey), facts, sections: n(p.sections), grp: n(p.grp), run: n(p.run),
                 tidal: (xp?.province_except ?? []).includes("tidal") };
      }));
    }),

    picker: (id) => once("picker", id, async () =>
      base.val<Picker>("picker", (await row(base.water(), "SELECT j FROM picker WHERE id = ?", id, "picker")).j)),

    akey: (akey) => once("akey", akey, async () => {
      const r = await akeyRows(akey);
      return {
        akey: n(r.akey), ruleset: n(r.ruleset), licensingSet: nn(r.licensing_set), steelheadWater: n(r.steelhead_water) === 1,
        steelhead: nn(r.steelhead) as 1 | 2 | null, steelheadRules: n(r.steelhead_rules) === 1, kind: await reads.str(n(r.kind)),
        tidal: n(r.tidal) === 1, seg: n(r.seg), mom: nn(r.mom), nseg: n(r.nseg),
      };
    }),

    segStarts: (seg) => once("seglist", seg, async () => json<number[]>((await segLists(seg)).starts)),

    segMoments: (mom) => once("segmom", mom, async () => {
      const [r, st] = await Promise.all([segMoms(mom), reads.statics()]);
      return json<number[]>(r.ix).map((i) => {
        const m = st.moments[i];
        if (!m) throw damaged(`segmom ${mom} names moment ${i}, which is not in answers.moments`);
        return m;
      });
    }),

    // (T286) a key's cells are read whole (a key has a handful), batched across keys: one read per region file
    cell: (akey, s) => once("cell", `${akey}:${s}`, async () => {
      const r = (await keyCells(akey)).find((c) => n(c.s) === s);
      if (!r) throw damaged(`no cell (${akey}, ${s})`);
      return cellOf(r);
    }),

    cells: (akey) => once("cells", akey, async () => {
      const rs = await keyCells(akey);
      rs.forEach((r, i) => { if (n(r.s) !== i) throw damaged(`akey ${akey}'s cells are out of sequence`); });
      if (!rs.length) throw damaged(`akey ${akey} has no cells`);
      return rs.map(cellOf);
    }),

    ladder: (akey, id) => once("ladder", id, async () => {
      const [[common, rows], st] = await Promise.all([
        blob<[number, number[][]]>(FAMILY.ladderFrame, akey, id), reads.statics()]);
      const fish = st.fish as readonly string[];
      const verdict = (v: number) => blob<VerdictWire>(FAMILY.ladderVerdict, akey, v);
      return {
        common: await verdict(common),
        rows: await Promise.all(rows.map(async (r) => {
          const code = fish[r[0]!];
          if (code === undefined) throw damaged(`ladder frame ${id} names fish ${r[0]}, which is not in answers.fish`);
          const own = r.length === 2 ? [r[1]!, r[1]!, r[1]!] : r.slice(1);
          const [none, hatchery, wild] = await Promise.all(own.map(verdict));
          return [code, none!, hatchery!, wild!] as const;
        })),
      };
    }),

    rows: (akey, id) => once("rows", id, async () => {
      // rows 30: the frame's fifth slot names each row's year strip in the static `rows.strips` (each distinct strip
      // held once); the stored row has no `year` — it goes back on as the row's last field, as RowsCodec.decode_frame
      const [[spp, fs, rs, line, ys], strips] = await Promise.all([
        blob<[string[], Record<string, [number | null, number | null, "hatchery" | "wild" | null]>, number[], RowsValue["steelhead_line"], number[]]>(
          FAMILY.rowsFrame, akey, id),
        yearStrips()]);
      if (!Array.isArray(ys) || ys.length !== rs.length) throw damaged(`rows frame ${id} has ${rs.length} rows and ${ys?.length ?? 0} strips`);
      for (const y of ys) {
        if (!(y >= 0 && y < strips.length)) throw damaged(`rows frame ${id} names strip ${y}, which is not in rows.strips (${strips.length})`);
      }
      const decided = (d: number | null) => (d === null ? Promise.resolve(null) : blob<NonNullable<RowsValue["fish"][string]["hatchery"]>>(FAMILY.rowsDecided, akey, d));
      const fish: Record<string, RowsValue["fish"][string]> = {};
      // rows 33 (T161): [decided h, decided w, lead] — the lead is the pipeline's leading origin, carried as stored
      for (const [S, x] of Object.entries(fs)) {
        if (!Array.isArray(x) || x.length !== 3) throw damaged(`rows frame ${id} holds ${S} as ${JSON.stringify(x)}, not [h, w, lead]`);
      }
      // (T286) every decided row and every row asked at once (one read each family), then put back in order
      const [decidedOf, rows] = await Promise.all([
        Promise.all(Object.entries(fs).map(async ([S, [h, w, lead]]) => [S, { hatchery: await decided(h), wild: await decided(w), lead }] as const)),
        Promise.all(rs.map((r) => blob<Omit<RowsValue["rows"][number], "year">>(FAMILY.rowsRow, akey, r)))]);
      for (const [S, f] of decidedOf) fish[S] = f;
      return { spp, fish, rows: rows.map((r, i) => ({ ...r, year: strips[ys[i]!]! })), steelhead_line: line };
    }),

    gear: (akey, id) => blob<GearValue>(FAMILY.gearFrame, akey, id),

    licence: (akey, id) => once("licence", id, async () => {
      const [h, d, ...u] = await blob<number[]>(FAMILY.licenceFrame, akey, id);
      const [holds, doc] = await Promise.all([
        blob<LicenceValue["holds"]>(FAMILY.licenceHold, akey, h!), blob<number[]>(FAMILY.licenceDoc, akey, d!)]);
      const profiles = await Promise.all(doc.map((a) => blob<LicenceValue["profiles"][number]>(FAMILY.licenceAnswer, akey, a)));
      return u.length
        ? { holds, profiles, under_16: await blob<NonNullable<LicenceValue["under_16"]>>(FAMILY.licenceU16, akey, u[0]!) }
        : { holds, profiles };
    }),

    display: (akey, id) => blob<DisplayFrame>(FAMILY.displayFrame, akey, id),

    async frames(akey, c) {
      const [ladder, rows, gear, licence, display] = await Promise.all([
        reads.ladder(akey, c.ladder), reads.rows(akey, c.rows), reads.gear(akey, c.gear), reads.licence(akey, c.licence),
        reads.display(akey, c.display)]);
      return { ladder, rows, gear, licence, display };
    },

    ruleset: (setId) => once("ruleset", setId, async () => {
      const j = base.val<{ sections: number; reach: number[]; trib?: number[] }>("ruleset", (await rulesets(setId)).j);
      return { sections: j.sections, reach: j.reach, trib: j.trib ?? [] };     // the store leaves out an empty trib list
    }),

    rules: () => once("rules", 0, async () => {
      const rs = await (await base.water()).all(
        "SELECT r.rix AS rix, r.j AS e, d.j AS f FROM rule r LEFT JOIN display_rule d ON d.id = r.rix ORDER BY r.rix");
      const out = new Map<number, StoredRule>();
      // (T286/T287) every rule is inflated and parsed here, once: a few hundred at a time with the screen's turn between,
      // never one long hold of the JS thread (all at once it was a ~0.3 s freeze on the emulator)
      for (let i = 0; i < rs.length; i++) {
        if (i && i % RULES_SLICE === 0) await turnOf();
        const r = rs[i]!;
        if (n(r.rix) !== i) throw damaged(`the rules are out of sequence at ${i}`);
        if (r.f === null || r.f === undefined) throw damaged(`rule ${i} has no display_rule`);
        out.set(i, { rix: i, export: base.val<ExportRule>("rule", r.e), facts: base.val<RuleFacts>("display_rule", r.f) });
      }
      return out as ReadonlyMap<number, StoredRule>;
    }),

    lics: () => once("lics", 0, async () => {
      const out = new Map<number, unknown>();
      for (const r of await (await base.water()).all("SELECT lix, j FROM lic ORDER BY lix")) out.set(n(r.lix), base.val("lic", r.j));
      return out as ReadonlyMap<number, unknown>;
    }),

    statics: () => once("statics", 0, async () => {
      const by = new Map<string, Cell>();
      for (const r of await (await base.water()).all("SELECT k, j FROM static")) by.set(String(r.k), r.j);
      const out: Record<string, unknown> = {};
      for (const [field, k] of Object.entries(STATICS)) {
        if (!by.has(k)) throw damaged(`no static ${k}`);
        out[field] = base.val("static", by.get(k)!);
      }
      return out as unknown as StoreStatics;
    }),

    str: (id) => strs(id),

    sectionAkey: async (h) => {
      const b = await block("section_akey", h);
      if (!b) return 0;
      const o = 2 * (h & SLOT);
      return b[o]! | (b[o + 1]! << 8);
    },

    sectionPlace: async (h) => {
      const b = await block("section_place", h);
      const v = b ? b[h & SLOT]! : 0;
      return v ? v - 1 : null;
    },

    place: (ix) => once("place", ix, async () => {
      const r = await row(base.water(), "SELECT place, regions, label, names FROM place WHERE place = ?", ix, "place");
      return { place: n(r.place), regions: json<PlaceRow["regions"]>(r.regions), label: await reads.str(n(r.label)),
               names: json<PlaceRow["names"]>(r.names) };
    }),

    sectionPart: (ord, sid) => once("sectionPart", `${ord}:${sid}`, async () => {
      const r = await (await base.water()).get("SELECT part_ix FROM part_section WHERE ord = ? AND sid = ?", ord, sid);
      return r ? n(r.part_ix) : null;
    }),

    partLengths: (ord, partIx) => once("partLengths", `${ord}:${partIx}`, async () => {
      const w = await base.water();
      const rows = await w.all("SELECT sid, m FROM part_section WHERE ord = ? AND part_ix = ? ORDER BY sid", ord, partIx);
      return new Map(rows.map((r) => [n(r.sid), r.m === null || r.m === undefined ? null : n(r.m)]));
    }),

    partLines: (ord) => once("partLines", ord, async () => {
      const rows = await (await base.water()).all("SELECT part_ix, blk, m0, m1 FROM part_line WHERE ord = ? "
                                                  + "ORDER BY part_ix, blk, m0", ord);
      return rows.map((r): PartLine => ({ partIx: n(r.part_ix), blk: n(r.blk), m0: n(r.m0), m1: n(r.m1) }));
    }),

    navRows: (ord) => once("navRows", ord, async () => {
      const w = await base.water(), g = await base.geo();
      const [geo, marks, sections, exit] = await Promise.all([
        g.all("SELECT part_ix, mid_sid, mid_lon, mid_lat, w, s, e, n FROM part_geo WHERE ord = ? ORDER BY part_ix", ord),
        w.all("SELECT lon, lat, token, down_part, up_parts, confidence FROM part_mark WHERE ord = ? ORDER BY mark_ix", ord),
        w.all("SELECT sid, part_ix FROM part_section WHERE ord = ? ORDER BY sid", ord),
        w.get("SELECT down_ord, down_part FROM water_exit WHERE ord = ?", ord)]);
      const core = await base.core();
      // the other water of an exit or a continuation is core's; one core does not hold is a damaged release (said)
      const named = async (o: number, what: string) => {
        const it = await core.get("SELECT item_id, name FROM item WHERE ord = ?", o);
        if (!it) throw damaged(`water ${ord}'s ${what} names item ${o}, which core does not hold`);
        return { item: String(it.item_id), name: String(it.name) };
      };
      const down = exit ? await named(n(exit.down_ord), "water_exit") : null;
      // `water_continue`, both ways, each on its key (ord, the primary key; next_ord, UNIQUE)
      const [as, from] = await Promise.all([
        w.get("SELECT part, next_ord AS other, next_part AS other_part FROM water_continue WHERE ord = ?", ord),
        w.get("SELECT next_part AS part, ord AS other, part AS other_part FROM water_continue WHERE next_ord = ?", ord),
      ]);
      const cont = async (r: Row | undefined): Promise<ContinueRow | null> => {
        if (!r) return null;
        return { ord: n(r.other), part: n(r.part), otherPart: n(r.other_part), ...(await named(n(r.other), "water_continue")) };
      };
      const none = (c: Cell) => c === null || c === undefined;
      return {
        geo: geo.map((g) => ({
          partIx: n(g.part_ix), midSid: n(g.mid_sid), mid: [micro(g.mid_lon), micro(g.mid_lat)] as const,
          box: none(g.w) || none(g.s) || none(g.e) || none(g.n) ? null
            : [micro(g.w), micro(g.s), micro(g.e), micro(g.n)] as const })),
        marks: marks.map((m) => {
          const c = isConfidence(m.confidence) && m.confidence !== "high" ? m.confidence : null;
          return { lon: micro(m.lon), lat: micro(m.lat), token: none(m.token) ? null : String(m.token),
                   down: n(m.down_part), up: json<number[]>(m.up_parts), ...(c ? { confidence: c } : {}) };
        }),
        sections: sections.map((r) => ({ sid: n(r.sid), partIx: n(r.part_ix) })),
        exit: exit ? { ord: n(exit.down_ord), part: nn(exit.down_part), ...down! } : null,
        continuesAs: await cont(as), continuesFrom: await cont(from),
      };
    }),

    siblingOrds: (ord) => once("siblingOrds", ord, async () => {
      // the water's own parent (a closure area of a split lake) else the water itself (the lake, opened whole): its
      // children by the `item_by_parent` index; the item itself is always one of the answer
      const rs = await (await base.core()).all(
        "SELECT ord FROM item WHERE part_of = (SELECT COALESCE(part_of, item_id) FROM item WHERE ord = ?) ORDER BY ord",
        ord);
      const out = rs.map((r) => n(r.ord));
      return out.includes(ord) ? out : [ord, ...out];
    }),

    async unnamedName(h) {
      const [a, pl] = await Promise.all([reads.sectionAkey(h), reads.sectionPlace(h)]);
      if (!a || pl === null) return null;
      const [k, p] = await Promise.all([reads.akey(a), reads.place(pl)]);
      const name = (p.names as Readonly<Record<string, string>>)[k.kind];
      if (name === undefined) throw damaged(`place ${pl} has no name for a ${k.kind}`);
      return name;
    },
  };


  /** One block of a regs-sid table (the table name is one of two literals). */
  const block = (t: "section_akey" | "section_place", h: number): Promise<Uint8Array | null> =>
    once(t, h >> BLOCK_BITS, async () => {
      const r = await (await base.sid()).get(`SELECT b FROM ${t} WHERE block = ?`, h >> BLOCK_BITS);
      if (!r) return null;
      const b = r.b;
      if (!(b instanceof Uint8Array)) throw damaged(`${t} block ${h >> BLOCK_BITS} is not bytes`);
      return b;
    });

  return { ...reads, clear: () => caches.clear() };
}

const cellOf = (r: Row): CellIds =>
  ({ ladder: n(r.ladder), rows: n(r.rows), gear: n(r.gear), licence: n(r.licence), display: n(r.display) });
