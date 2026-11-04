/**
 * (T256 item 1) THE ANSWER SLICES, READ BY WHAT THEY COVER. A release cut with slices (pipeline
 * `deliver/release/slices.py`) carries, per region, `core-<r>` and `water-<r>`: the whole file's rows for every water
 * of that region (and the waters those name), complete, ~1/4 of the bytes. On a poor line a named water's page read core
 * + water + regs-<r> ≈ 3.9 MB; from its region's slices it is ≈ 1.2 MB.
 *
 * A slice is only ever read for a key it COVERS — present in it is complete, absent means "ask the whole file" — so a
 * key outside it costs bytes, never a wrong answer:
 *
 *   ords      core `item.ord` / water `item.item_ord` present: every row of that water, in every sliced table
 *   items     core `item.item_id` present: the same water
 *   sids      core `cover_sid` bit set: every water holding that section (or none: an unnamed stream) is held; or the
            section is in a held water's `item_section` (a section is held by ONE water: `slices.covered_ords` refuses
            a release where it is not)
 *   places    core `place.place_id` present (the towns the held `item_near` rows name)
 *   strs      water `str.id` present; pickers: water `picker.id` present
 *   (none)    a read of tables every region reads WHOLE (rules, statics, akeys, …): `water-shared` (T370: one web
 *             file, cut beside the slices; they were byte-identical in all nine water slices). `meta` and `sqlite_master`
 *             are in every one. A release cut before it holds them in each slice, and any slice answers.
 *
 * Which keys a read names is read off its SQL (`sliceKeys`): the tables it names must all be the slice's, and its first
 * WHERE predicate must be a covered key column (`= ?` or `IN (?, …)`) — anything else (search, counts, a scan, a
 * column no slice is keyed by) is the whole file's. This keeps the readers' SQL as it is: the store and the release
 * source read through `ReleaseLoader.routed`, which routes each read.
 */
import type { Db, Row } from "../sql/db";

export interface SliceKeys {
  /** (T370) the read names only region-wide water tables: `water-shared` answers it */
  shared?: true;
  ords?: readonly number[];
  items?: readonly string[];
  sids?: readonly number[];
  places?: readonly number[];
  strs?: readonly number[];
  pickers?: readonly number[];
}
export type SliceKind = keyof SliceKeys;
export type Sliced = "core" | "water";

/** Per sliced file, per table it holds: the columns it is COMPLETE by (`whole`: every row, in every slice; `shared`:
 *  every row, in `water-shared` (T370); `follows`: complete for the rows that reach it — read only beside a keyed
 *  table). A table not listed is not in the slice. */
const TABLES: Readonly<Record<Sliced, Readonly<Record<string, "whole" | "shared" | "follows" | Readonly<Record<string, SliceKind>>>>>> = {
  core: {
    item: { ord: "ords", item_id: "items" }, item_section: { sid: "sids", ord: "ords" }, item_near: { ord: "ords" },
    item_area: { ord: "ords" }, name_source: { item_id: "items" }, place: { place_id: "places" },
    meta: "whole", sqlite_master: "whole",
  },
  water: {
    item: { item_ord: "ords" }, part: { item_ord: "ords" }, part_section: { ord: "ords" }, part_mark: { ord: "ords" },
    // (T388) each part's own stretch of stream (a named link's place): the water's rows, by ord, as part_section
    part_line: { ord: "ords" },
    water_exit: { ord: "ords" }, water_continue: { ord: "ords", next_ord: "ords" }, str: { id: "strs" },
    picker: { id: "pickers" }, xpart: "follows",
    akey: "shared", akey_region: "shared", ruleset: "shared", seglist: "shared", segmom: "shared", rule: "shared",
    display_rule: "shared", lic: "shared", static: "shared", place: "shared", zdict: "shared", meta: "whole",
    sqlite_master: "whole",
  },
};

const TABLE_RE = /\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)/gi;
/** The first WHERE predicate: `[alias.]column = ?[n]` or `[alias.]column IN (?…)`. */
const PRED_RE = /\bWHERE\s+(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z_][A-Za-z0-9_]*)\s*(=\s*\?(\d*)|IN\s*\(([^)]*)\))/i;

/**
 * The keys a read names, for the slice of `file` — or null: this read is not one a slice may answer (it reads a table
 * the slice lacks, scans, or filters on a column no slice is complete by). `{}`: it reads only tables a slice holds whole.
 */
export function sliceKeys(file: Sliced, sql: string, params: readonly (string | number | null)[]): SliceKeys | null {
  const spec = TABLES[file];
  const tables = [...sql.matchAll(TABLE_RE)].map((m) => m[1]!.toLowerCase());
  if (!tables.length || tables.some((t) => !(t in spec))) return null;
  const keyed = tables.filter((t) => typeof spec[t] === "object");
  const shared = tables.some((t) => spec[t] === "shared");
  // (T370) a read beside a region-wide table and a keyed one: no one web file holds both — the whole file's
  if (shared && (keyed.length || tables.some((t) => spec[t] === "follows"))) return null;
  if (!keyed.length) return tables.some((t) => spec[t] === "follows") ? null : shared ? { shared: true } : {};
  const m = PRED_RE.exec(sql);
  if (!m) return null;                                             // a keyed table read with no key: a scan
  const col = m[1]!.toLowerCase();
  const kinds = new Set(keyed.map((t) => (spec[t] as Readonly<Record<string, SliceKind>>)[col]).filter(Boolean));
  if (kinds.size !== 1) return null;
  const kind = [...kinds][0]!;
  let vals: (string | number | null)[];
  if (m[2]!.startsWith("=")) {
    const at = m[3] ? Number(m[3]) - 1 : 0;                        // `?1` names the first parameter
    vals = at < params.length ? [params[at]!] : [];
  } else {
    const n = (m[4]!.match(/\?/g) ?? []).length;
    if (n === 0 || /\?\d/.test(m[4]!)) return null;
    vals = params.slice(0, n);
  }
  if (!vals.length || vals.some((v) => v === null)) return null;
  return kind === "items" ? { items: vals.map(String) } : { [kind]: vals.map(Number) };
}

const CHUNK = 500;
const marks = (n: number) => Array.from({ length: n }, () => "?").join(", ");

/** Whether an open slice covers every key named (module docstring). */
export async function covers(file: Sliced, d: Db, keys: SliceKeys): Promise<boolean> {
  const present = async (table: string, col: string, vals: readonly (string | number)[]): Promise<boolean> => {
    const want = [...new Set(vals)];
    for (let i = 0; i < want.length; i += CHUNK) {
      const part = want.slice(i, i + CHUNK);
      const r = await d.get(`SELECT count(*) AS n FROM ${table} WHERE ${col} IN (${marks(part.length)})`, ...part);
      if (Number(r?.n ?? 0) !== part.length) return false;
    }
    return true;
  };
  if (keys.ords?.length && !(await present("item", file === "core" ? "ord" : "item_ord", keys.ords))) return false;
  if (keys.items?.length && !(file === "core" && await present("item", "item_id", keys.items))) return false;
  if (keys.places?.length && !(file === "core" && await present("place", "place_id", keys.places))) return false;
  if (keys.strs?.length && !(file === "water" && await present("str", "id", keys.strs))) return false;
  if (keys.pickers?.length && !(file === "water" && await present("picker", "id", keys.pickers))) return false;
  if (keys.sids?.length) {
    if (file !== "core") return false;
    const blocks = [...new Set(keys.sids.map((s) => s >> 12))];
    const got = new Map<number, Uint8Array>();
    for (let i = 0; i < blocks.length; i += CHUNK) {
      const part = blocks.slice(i, i + CHUNK);
      for (const r of await d.all(`SELECT block, b FROM cover_sid WHERE block IN (${marks(part.length)})`, ...part))
        got.set(Number(r.block), r.b instanceof Uint8Array ? r.b : new Uint8Array(r.b as unknown as ArrayBuffer));
    }
    const outside = keys.sids.filter((s) => {
      const b = got.get(s >> 12), i = s & 4095;
      return !b || !((b[i >> 3]! >> (i & 7)) & 1);
    });
    // a section outside the region but of a held water (a water straddling two regions): a section is held by ONE water
    // (the pipeline refuses a release where it is not), so that water's row is the whole answer
    if (outside.length && !(await present("item_section", "sid", outside))) return false;
  }
  return true;
}

/** A Db whose every read goes to the file `pick` chooses for it (`ReleaseLoader.routed`). */
export function routedDb(pick: (sql: string, params: readonly (string | number | null)[]) => Promise<Db>): Db {
  return {
    all: async (sql, ...p): Promise<Row[]> => (await pick(sql, p)).all(sql, ...p),
    get: async (sql, ...p): Promise<Row | undefined> => (await pick(sql, p)).get(sql, ...p),
  };
}
