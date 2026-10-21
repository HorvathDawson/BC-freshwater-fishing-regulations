/**
 * THE STORE READS (T04): every question the Regulations page asks of the release, each one or two keyed SQL reads on
 * the file that holds its table, returning the answers-file values exactly as `pipeline/deliver/answers/encode.py`'s
 * codecs decode them — so the TS side sees the same `RowsValue` / `LicenceValue` / `StatusFrame` the Python reference
 * and page v36 see. The per-family interning is undone here, never in core (rows 30: a row's year strip, held once
 * per distinct strip in the static `rows.strips` and named by the rows frame's fifth slot, goes back on its row here).
 *
 *   core       itemOrd (core `item.item_id` is UNIQUE: one indexed read — core's ord IS water's item_ord, P08)
 *   water      item, parts, picker, akey, segStarts, segMoments, ruleset, rules, lics, statics, str, place, sectionPart
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
  AkeyRow, AnswersPlace, CellIds, DisplayFrame, ExportRule, GearValue, LadderFrameWire, LicenceValue, Moment, PartFacts,
  Picker, RowsValue, RuleFacts, Statics, VerdictWire,
} from "@app/core";
import type { Cell, Db, Row } from "../bundle/db";
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
}
export interface PlaceRow extends AnswersPlace { readonly place: number }
export interface RulesetRow { readonly sections: number; readonly reach: readonly number[]; readonly trib: readonly number[] }
export interface StoredRule { readonly rix: number; readonly export: ExportRule; readonly facts: RuleFacts }
/** The store's statics (`static` rows). `pdfPages` is not in the store: T36 supplies it (contracts.md §3). */
export type StoreStatics = Omit<Statics, "pdfPages">;
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
}

/** What the reads stand on (store.ts hands them its own). */
export interface ReadBase {
  water(): Promise<Db>;
  region(akey: number): Promise<Db>;
  sid(): Promise<Db>;
  core(): Promise<Db>;
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
  strip: "display.strip", reasons: "display.reasons",
  countNouns: "gear.count_nouns", gearMoments: "gear.moments", conductMeans: "gear.conduct_means",
  profiles: "licence.profiles", profileDims: "licence.profile_dims", profileLabels: "licence.profile_labels",
  residencyLabels: "licence.residency_labels",
  exportSpecies: "export.species", exportLicences: "export.licences", exportConduct: "export.conduct",
  ladderReasons: "ladder.reasons", ladderReasonState: "ladder.reason_state",
};

type YearStrip = RowsValue["rows"][number]["year"];
const damaged = (what: string) => new RegsStoreError("damaged", `regs store: ${what}`);
const n = (c: Cell): number => Number(c);
const nn = (c: Cell): number | null => (c === null || c === undefined ? null : Number(c));
const json = <T>(c: Cell): T => JSON.parse(String(c)) as T;

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
  /** One blob of a family, by id, from the region file holding the key's frames. */
  const blob = <T>(t: Family, akey: number, id: number): Promise<T> =>
    once(t, id, async () => base.val<T>(t, (await row(base.region(akey), `SELECT j FROM ${t} WHERE id = ?`, id, t)).j));
  /** rows 30: every distinct year strip, once (water static `rows.strips`; a rows frame names its rows' strips). */
  const yearStrips = (): Promise<readonly YearStrip[]> => once("strips", 0, async () =>
    base.val<YearStrip[]>("static", (await row(base.water(), "SELECT j FROM static WHERE k = ?", "rows.strips", "static")).j));

  const reads: RegsReads = {
    itemOrd: (itemId) => once("itemOrd", itemId, async () => {
      const r = await (await base.core()).get("SELECT ord FROM item WHERE item_id = ?", itemId);
      return r ? n(r.ord) : null;
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
        "SELECT part_ix, akey, ord, label_s, short_s, runs_s, place_s, hint_s, km, closed_all_year, paper_licence, sections " +
        "FROM part WHERE item_ord = ? ORDER BY part_ix", ord);
      return Promise.all(rows.map(async (p, i): Promise<PartRow> => {
        if (n(p.part_ix) !== i) throw damaged(`item ${ord}'s parts are out of sequence`);
        const s = (c: Cell) => reads.str(n(c));
        const facts: PartFacts | null = p.ord === null || p.ord === undefined ? null : {
          order: n(p.ord), label: await s(p.label_s), short: await s(p.short_s), runs: await s(p.runs_s),
          place: p.place_s === null ? null : await s(p.place_s), hint: await s(p.hint_s), km: nn(p.km),
          closed_all_year: n(p.closed_all_year) === 1, paper_licence: json<number[]>(p.paper_licence),
        };
        return { itemOrd: ord, partIx: i, akey: nn(p.akey), facts, sections: n(p.sections) };
      }));
    }),

    picker: (id) => once("picker", id, async () =>
      base.val<Picker>("picker", (await row(base.water(), "SELECT j FROM picker WHERE id = ?", id, "picker")).j)),

    akey: (akey) => once("akey", akey, async () => {
      const r = await row(base.water(),
        "SELECT akey, ruleset, licensing_set, steelhead_water, steelhead, steelhead_rules, kind, tidal, seg, mom, nseg " +
        "FROM akey WHERE akey = ?", akey, "akey");
      return {
        akey: n(r.akey), ruleset: n(r.ruleset), licensingSet: nn(r.licensing_set), steelheadWater: n(r.steelhead_water) === 1,
        steelhead: nn(r.steelhead) as 1 | 2 | null, steelheadRules: n(r.steelhead_rules) === 1, kind: await reads.str(n(r.kind)),
        tidal: n(r.tidal) === 1, seg: n(r.seg), mom: nn(r.mom), nseg: n(r.nseg),
      };
    }),

    segStarts: (seg) => once("seglist", seg, async () =>
      json<number[]>((await row(base.water(), "SELECT starts FROM seglist WHERE seg = ?", seg, "seglist")).starts)),

    segMoments: (mom) => once("segmom", mom, async () => {
      const [r, st] = await Promise.all([row(base.water(), "SELECT ix FROM segmom WHERE mom = ?", mom, "segmom"), reads.statics()]);
      return json<number[]>(r.ix).map((i) => {
        const m = st.moments[i];
        if (!m) throw damaged(`segmom ${mom} names moment ${i}, which is not in answers.moments`);
        return m;
      });
    }),

    cell: (akey, s) => once("cell", `${akey}:${s}`, async () => {
      const r = await (await base.region(akey)).get(
        "SELECT ladder, rows, gear, licence, display FROM cell WHERE akey = ? AND s = ?", akey, s);
      if (!r) throw damaged(`no cell (${akey}, ${s})`);
      return cellOf(r);
    }),

    cells: (akey) => once("cells", akey, async () => {
      const rs = await (await base.region(akey)).all(
        "SELECT s, ladder, rows, gear, licence, display FROM cell WHERE akey = ? ORDER BY s", akey);
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
        blob<[string[], Record<string, [number | null, number | null]>, number[], RowsValue["steelhead_line"], number[]]>(
          FAMILY.rowsFrame, akey, id),
        yearStrips()]);
      if (!Array.isArray(ys) || ys.length !== rs.length) throw damaged(`rows frame ${id} has ${rs.length} rows and ${ys?.length ?? 0} strips`);
      for (const y of ys) {
        if (!(y >= 0 && y < strips.length)) throw damaged(`rows frame ${id} names strip ${y}, which is not in rows.strips (${strips.length})`);
      }
      const decided = (d: number | null) => (d === null ? Promise.resolve(null) : blob<NonNullable<RowsValue["fish"][string]["hatchery"]>>(FAMILY.rowsDecided, akey, d));
      const fish: Record<string, RowsValue["fish"][string]> = {};
      for (const [S, [h, w]] of Object.entries(fs)) fish[S] = { hatchery: await decided(h), wild: await decided(w) };
      const rows = await Promise.all(rs.map((r) => blob<Omit<RowsValue["rows"][number], "year">>(FAMILY.rowsRow, akey, r)));
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
      const j = base.val<{ sections: number; reach: number[]; trib?: number[] }>("ruleset",
        (await row(base.water(), "SELECT j FROM ruleset WHERE set_id = ?", setId, "ruleset")).j);
      return { sections: j.sections, reach: j.reach, trib: j.trib ?? [] };     // the store leaves out an empty trib list
    }),

    rules: () => once("rules", 0, async () => {
      const rs = await (await base.water()).all(
        "SELECT r.rix AS rix, r.j AS e, d.j AS f FROM rule r LEFT JOIN display_rule d ON d.id = r.rix ORDER BY r.rix");
      const out = new Map<number, StoredRule>();
      rs.forEach((r, i) => {
        if (n(r.rix) !== i) throw damaged(`the rules are out of sequence at ${i}`);
        if (r.f === null || r.f === undefined) throw damaged(`rule ${i} has no display_rule`);
        out.set(i, { rix: i, export: base.val<ExportRule>("rule", r.e), facts: base.val<RuleFacts>("display_rule", r.f) });
      });
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

    str: (id) => once("str", id, async () =>
      String((await row(base.water(), "SELECT text FROM str WHERE id = ?", id, "str")).text)),

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
