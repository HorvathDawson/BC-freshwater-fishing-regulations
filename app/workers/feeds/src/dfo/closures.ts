// T757 port of what pipeline/regs/dfo_salmon/closures.py `live_piece_rules` needs: CLOSED AREAS INSIDE A DFO PLACE (T573
// entry B) as live locations of their own, `<water_id>#closed:<key>`. Read it beside the Python: the functions keep its
// names (camelCase) and its order, line for line where the languages allow.
//
// A DFO row whose own words say no fishing / closed over a list of named areas ("Deadman - from … to …", the Babine
// mouths "within a 400 m radius of …", an inline "except closed from A to B") becomes ENTRY B: one rule (all salmon,
// take 0) per piece, each piece keyed to DFO's own words for it (`pieceKey`). The live feed emits one location per item
// TODAY's row names (`livePieceRules`); an item DFO dropped emits nothing, so its place answers there. Confirmation
// (bound or carried) is the release's matter, never the feed's.
//
// Only the read side the feed needs is here: no geometry (`PieceGeometry`, `attach`, `build_pieces` stay Python).
// Anything outside the id grammar THROWS (`pieceId`), as the Python raises: the run fails, the last good feed stands.
import { byFingerprint, type EntryFile } from "./entries";
import { sha1 } from "./hash";
import type { Location } from "./locations";
import { cmpStr, finditer, formatG, match, pyRe, PyError, pyRound, repr, search, split, strip, sub } from "./py";

type Json = any; // eslint-disable-line @typescript-eslint/no-explicit-any

/** (T757) the feed shape this port emits, served on /status.json as `dfo.pieces` (pipeline/deliver/release/pieces_gate.py
 *  `PIECES_SHAPE`): 1 = one `locs["<water_id>#closed:<key>"]` per closed area today's row names */
export const PIECES_SHAPE = 1;

/** the words that make an exclusion list a CLOSURE */
const CLOSED_RE = pyRe("\\bno\\s+fishing\\b|\\bclosed\\b|\\bclosure\\b", "i");
/** "… year-round" in the place's words: the closure has no season */
const YEAR_ROUND_RE = pyRe("year[\\s-]*round", "i");
/** "Deadman - from the fishing boundary signs …": the area's name, then DFO's words for it */
const AREA_RE = pyRe("^\\s*(?P<name>[^-–—:]{2,60}?)\\s*[-–—:]\\s+(?P<words>.+?)\\s*$", "s");
const SUFFIX = "#closed";
/** "within a 400 m radius of the following tributary streams" — the radius of every circle the list names */
const RADIUS_RE = pyRe("within\\s+a?\\s*(?P<m>\\d+(?:\\.\\d+)?)\\s*(?P<unit>km|m)\\b[^.:]*?\\bradius\\b", "i");
/** "(Also) closed east of a line from Gullwing Creek to south shore of Babine Lake" */
const LINE_RE = pyRe("(?:also\\s+)?closed\\s+(?P<words>(?:east|west|north|south)\\w*\\s+of\\s+a\\s+line[^.]*)", "i");
/** AN EXCEPTED STRETCH INSIDE THE PLACE'S OWN WORDS (T743, the middle Shuswap): only "except closed / except no fishing
 *  from <A> to <B>" (the Python's comment says which other "except" wordings are not one) */
const INLINE_RE = pyRe("\\bexcept\\s+(?P<verb>closed|no\\s+fishing)\\s+(?P<words>from\\s+.+?)\\s*(?:\\bnote\\s*:.*)?$", "is");
/** a printed coordinate, degrees + decimal minutes (seconds optional): 50°19.105' N, 118°48.857' W */
const COORD_RE = pyRe(
  "(?P<deg>\\d{1,3})\\s*°\\s*(?P<min>\\d{1,2}(?:\\.\\d+)?)\\s*['’′]\\s*" +
    "(?:(?P<sec>\\d{1,2}(?:\\.\\d+)?)\\s*[\"”″]\\s*)?(?P<hem>[NSEW])\\b",
);

/** int() / float() of a `\d` match: the Python reads any Unicode digit; this port reads ASCII ones and THROWS on any
 *  other (never a silent NaN) */
function num(text: string): number {
  if (!/^[0-9]+(?:\.[0-9]+)?$/.test(text)) throw new PyError("ValueError", `closures: a number the port cannot read: ${repr(text)}`);
  return Number(text);
}
/** s[:n] over code points (Python slices characters, not UTF-16 units) */
const head = (s: string, n: number): string => [...s].slice(0, n).join("");

export interface Coord { printed: string; lat: number; lon: number | null }

/** every printed coordinate pair in `words`, AS PRINTED and converted exactly: {printed, lat, lon} (decimal degrees, W
 *  and S negative). A latitude with no longitude after it is kept with `lon` null (never guessed). */
export function coordsIn(words: string): Coord[] {
  const out: Coord[] = [];
  let lat: [string, number] | null = null;
  for (const m of finditer(COORD_RE, words)) {
    const g = m.groups!;
    let v = num(g.deg!) + num(g.min!) / 60.0 + num(g.sec ?? "0") / 3600.0;
    const hem = g.hem!.toUpperCase();
    v = pyRound("SW".includes(hem) ? -v : v, 7);
    if ("NS".includes(hem)) {
      if (lat !== null) out.push({ printed: lat[0], lat: lat[1], lon: null });
      lat = [strip(m[0]), v];
    } else if (lat !== null) {
      out.push({ printed: `${lat[0]} ${strip(m[0])}`, lat: lat[1], lon: v });
      lat = null;
    }
  }
  if (lat !== null) out.push({ printed: lat[0], lat: lat[1], lon: null });
  return out;
}

const FROM_RE = pyRe("^\\s*from\\s+", "i");
const TO_AT = pyRe("\\s(?:down(?:stream)?\\s+|up(?:stream)?\\s+)?to\\s", "i");
const TO_HEAD = pyRe("^\\s(?:down(?:stream)?\\s+|up(?:stream)?\\s+)?to\\s+", "i");

/** "from <A> to <B>": the first " to " outside parentheses after "from" (a printed "(located at …)" holds none) */
function splitFromTo(words: string): [string, string] | null {
  const w = sub(FROM_RE, "", words);
  let depth = 0;
  for (let i = 0; i < w.length; i++) {
    const ch = w[i]!;
    depth += ch === "(" ? 1 : 0;
    depth -= ch === ")" && depth > 0 ? 1 : 0;
    if (depth === 0 && match(TO_AT, w, i)) {
      const a = w.slice(0, i);
      const b = sub(TO_HEAD, "", w.slice(i));
      return [strip(a, " ,;"), strip(b, " ,;.")];
    }
  }
  return null;
}

const HEAD_SPLIT = pyRe("\\s*\\(|,|\\s+(?:approximately|approx\\.?|located)\\b", "i");
/** an end's landmark, before its "(located …)", ", " or "approximately …": "the water survey station" */
const endHead = (end: string): string => head(strip(split(HEAD_SPLIT, end, 1)[0]!), 60);

const PAREN_SP = pyRe("\\(\\s+");
const NUM_UNIT = pyRe("(\\d)\\s*(k?m)\\b");
/** THE KEY OF AN INLINE ITEM: `pieceKey` of its words with DFO's typing slips aside ("( located" / "(located", "200m" /
 *  "200 m": the two stored Shuswap wordings), so one closed stretch is one piece across them */
export function inlineKey(item: string): string {
  return pieceKey(sub(NUM_UNIT, "\\1 \\2", sub(PAREN_SP, "(", item)));
}

export interface InlineItem { item: string; verb: string; ends: { words: string; coords: Coord[] }[] | null }

const WS_RUN = pyRe("\\s+");
/** THE EXCEPTED STRETCHES WRITTEN INSIDE THE PLACE'S WORDS (`INLINE_RE`), only when the scrape stored no bulleted list
 *  (a bulleted list is the row's list; never both): [{item, verb, ends: [{words, coords}, …] | null}] */
export function inlineItems(loc: Json): InlineItem[] {
  if ((loc.source_text || {}).excludes?.length) return [];
  const m = search(INLINE_RE, text(loc));
  if (m === null) return [];
  const g = m.groups!;
  const item = strip(rstrip_(strip(sub(WS_RUN, " ", `${g.verb} ${g.words}`)), "."));
  const ft = splitFromTo(g.words!);
  const ends = ft === null ? null : ft.map((e) => ({ words: e, coords: coordsIn(e) }));
  return [{ item, verb: g.verb!.toLowerCase(), ends }];
}
// str.rstrip(chars), spelled here so the line above reads as the Python's `.strip().rstrip(".").strip()`
const rstrip_ = (s: string, chars: string): string => {
  let j = s.length;
  while (j > 0 && chars.includes(s[j - 1]!)) j--;
  return s.slice(0, j);
};

/** THE OWNER'S READING of words DFO leaves open, per water (never guessed in code): the Python's table, as it is */
export const OWNER_READINGS: Readonly<Record<string, { verbatim: string; of: string; reading: string }>> = {
  "6:babine-lake": {
    verbatim: "No fishing for salmon",
    of: "Closed",
    reading: "the owner's reading, 2026-11-10, of DFO's word “closed”: “babine is closed in the circles (no fishing salmon)”",
  },
};

/** THE KEY OF ONE EXCLUDED ITEM: its words as DFO wrote them, case and spacing aside — sha1, the first 12 hex digits */
export function pieceKey(t: string): string {
  return sha1(sub(WS_RUN, " ", strip(t).toLowerCase())).slice(0, 12);
}

/** the radius the place's words give its list of mouths, in metres */
function radiusM(loc: Json): number | null {
  const m = search(RADIUS_RE, text(loc));
  return m === null ? null : num(m.groups!.m!) * (m.groups!.unit!.toLowerCase() === "km" ? 1000.0 : 1.0);
}

const text = (loc: Json): string => String((loc.source_text || {}).specific_area || "");

const excludes = (loc: Json): string[] =>
  ((loc.source_text || {}).excludes || []).filter((e: unknown) => typeof e === "string" && strip(e)).map((e: string) => strip(e));

/** the place's own words (or an exclusion's) say no fishing / closed, AND the list is one of the two shapes: named
 *  stretches ("Deadman - from … to …": at least one) or mouths with a radius ("within a 400 m radius of …") */
export function saysClosed(loc: Json): boolean {
  if (inlineItems(loc).length) return true; // "except closed from … to …": the clause itself says closed
  const ex = excludes(loc);
  return ex.length > 0 && (ex.some((e) => match(AREA_RE, e) !== null) || radiusM(loc) !== null) &&
    search(CLOSED_RE, text(loc) + " " + ex.join(" ")) !== null;
}

export interface Area {
  key: string;
  item: string;
  kind: "stretch" | "circle" | "line" | "unknown";
  name: string;
  words: string;
  radius_m?: number;
  inline?: true;
  ends?: { words: string; coords: Coord[] }[];
}

/** EACH EXCLUDED ITEM AS A PIECE {key, item, kind, name, words, radius_m?}: "Deadman - from … to …" is a stretch named
 *  Deadman; under "within a 400 m radius of …" a bare name is a circle of that radius at that stream's mouth; "closed
 *  east of a line …" in the place's words is a line. One of no known shape keeps its whole text (never dropped) as kind
 *  unknown. */
export function areas(loc: Json): Area[] {
  const out: Area[] = [];
  const r = radiusM(loc);
  excludes(loc).forEach((e, i0) => {
    const i = i0 + 1;
    const m = match(AREA_RE, e);
    if (m) out.push({ key: pieceKey(e), item: e, kind: "stretch", name: strip(m.groups!.name!), words: strip(m.groups!.words!) });
    else if (r !== null)
      out.push({ key: pieceKey(`${formatG(r)} m radius: ${e}`), item: e, kind: "circle", name: e, radius_m: r,
                 words: `within a ${formatG(r)} m radius of the mouth of ${e}` });
    else out.push({ key: pieceKey(e), item: e, kind: "unknown", name: `area ${i}`, words: e });
  });
  for (const m of finditer(LINE_RE, text(loc))) {
    const w = strip(m.groups!.words!);
    out.push({ key: pieceKey(w), item: w, kind: "line", name: head(w, 60), words: "closed " + w });
  }
  for (const it of inlineItems(loc)) { // an inline "except closed from A to B": one stretch, each end with its printed points
    const ends = it.ends;
    const name = ends ? `${endHead(ends[0]!.words)} to ${endHead(ends[1]!.words)}` : head(it.item, 60);
    out.push({ key: inlineKey(it.item), item: it.item, kind: ends ? "stretch" : "unknown", name, words: it.item, inline: true,
               ...(ends ? { ends } : {}) });
  }
  return out;
}

/** ENTRY B of one location: the closure derived from its exclusion list. `because`: the wording of the same water whose
 *  words say it, when this one only says "except:". */
export function closureEntry(loc: Json, because: string | null = null): Json {
  const lid = String(loc.location_id);
  const yearRound = search(YEAR_ROUND_RE, text(loc)) !== null || because !== null;
  // THE RULE IS WHAT DFO'S WORDS SAY for the areas: "no fishing … year-round" (the Thompson), or only that the waters are
  // excluded and "closed" (Babine Lake: no season is named, so none is claimed)
  const saysNoFishing = search(pyRe("\\bno\\s+fishing\\b", "i"), text(loc)) !== null || because !== null;
  let words = saysNoFishing ? (yearRound ? "No fishing, year-round" : "No fishing") : "Closed";
  let read = OWNER_READINGS[String(loc.water_id ?? "None")] ?? null;
  read = read !== null && read.of === words ? read : null; // only of the words it was a reading of
  const dfoWords = words;
  words = read ? read.verbatim : words;
  const n = areas(loc).length;
  return {
    id: lid + SUFFIX, derived_from: lid, kind: "closure", water: loc.water ?? null,
    water_id: loc.water_id ?? null, region: loc.region ?? null, status: loc.status || "active",
    title: strip(`${loc.water || ""}: ${words.toLowerCase()} in ${n} named area${n === 1 ? "" : "s"}`, ": "),
    // the rule in the catalogue's own shape (what the Region 6 section-F closure carries): all salmon, take 0
    rule: {
      type: "retention_limit", species: ["SA"], take: 0, may_target: false, verbatim: words,
      ...(yearRound ? { when: { dates: [{ from_month: 1, from_day: 1, to_month: 12, to_day: 31 }] } } : {}),
      ...(read ? { dfo_words: dfoWords, reading: read.reading } : {}),
    },
    areas: areas(loc), source_words: text(loc),
    ...(because ? { said_by: because } : {}),
  };
}

/** every closure entry the stored rows give: location id + "#closed" -> entry B. Pure. */
export function derive(locations: Iterable<Json>): Map<string, Json> {
  const locs = [...locations].filter((l) => excludes(l).length || inlineItems(l).length);
  const said = new Map<string, string>();
  const k = (l: Json) => JSON.stringify([l.water_id ?? null, excludes(l)]);
  for (const l of locs) if (saysClosed(l)) said.set(k(l), String(l.location_id));
  const out = new Map<string, Json>();
  for (const l of locs) {
    let e: Json;
    if (saysClosed(l)) e = closureEntry(l);
    else if (said.has(k(l))) e = closureEntry(l, said.get(k(l))!); // "…, except:" — the list is another wording's closure list
    else continue;
    out.set(e.id, e);
  }
  return out;
}

const PIECE_SEP = SUFFIX + ":";

/** THE GRAMMAR OF A PIECE ID — `<water_id>#closed:<key>`: the water id is `<region>:<slug>` (a region key of digits /
 *  lowercase letters, a slug of `[a-z0-9-]`), the key 12 lowercase hex digits (`pieceKey`, sha1). Nothing in it needs
 *  escaping. ASCII only: no `u`/`i` flag, so `[a-z]` is exactly the 26 letters. */
export const PIECE_ID_RE = /^(?<water>[0-9a-z]+:[a-z0-9-]+)#closed:(?<key>[0-9a-f]{12})$/;

/** THE LOCATION ID OF ONE CLOSED AREA: `<water_id>#closed:<piece key>` (`PIECE_ID_RE`; anything else THROWS) */
export function pieceId(waterId: string, key: string): string {
  const lid = `${waterId}${PIECE_SEP}${key}`;
  // the Python's `re.match(… $)` also takes a final "\n"; JS `$` does not — a lid ending in "\n" is refused here, and
  // the Python's piece id can never end in one (the water id is `[a-z0-9-]` and the key hex)
  if (!PIECE_ID_RE.test(lid)) throw new PyError("ValueError", `closures: ${repr(lid)} is not a piece id (\`<region>:<slug>#closed:<12 hex>\`)`);
  return lid;
}

/** THE RULE OF ONE PIECE: entry B's rule, its words followed by DFO's own words for the area (a stretch: the item as
 *  printed; a circle: its radius and the creek it names) */
export function pieceRule(entry: Json, area: Area, lid: string): Json {
  const rule = { ...entry.rule };
  const words = area.kind === "circle" ? area.words : area.item;
  return { ...rule, rule_id: `${lid}#0`, verbatim: `${rule.verbatim} — ${words}` };
}

/** entry B of every ACTIVE row (a dormant wording builds nothing of its own), in id order — the order that decides which
 *  row a piece of two live wordings takes its place's tier from */
function activeEntries(locations: Iterable<Json>): Json[] {
  const got = derive(locations);
  return [...got.keys()].sort(cmpStr).map((k) => got.get(k)!).filter((e) => (e.status || "active") === "active");
}

/** THE LIVE FEED'S PIECES (`live.ts` after the join, as `live.run`): piece id -> [its rule], for every item TODAY'S page
 *  names — read from the live rows (`found`, the page's scraped locations, joined to their curated place by fingerprint),
 *  so a piece DFO dropped leaves the feed on the run it goes. The feed carries every live one, confirmed or not. */
export function livePieceRules(ef: EntryFile, found: Iterable<Pick<Location, "fingerprint" | "specific_area" | "excludes">>): Map<string, Json[]> {
  const index = byFingerprint(ef);
  const rows: Json[] = [];
  for (const f of found) {
    const hit = index.get(f.fingerprint);
    if (hit === undefined || !hit.water_id) continue;
    rows.push({ location_id: hit.location_id, water_id: hit.water_id, water: hit.water, region: hit.region,
                status: "active", source_text: { specific_area: f.specific_area, excludes: [...(f.excludes || [])] } });
  }
  const out = new Map<string, Json[]>();
  for (const e of activeEntries(rows))
    for (const a of e.areas as Area[]) {
      const lid = pieceId(e.water_id, a.key);
      if (!out.has(lid)) out.set(lid, [pieceRule(e, a, lid)]);
    }
  return out;
}
