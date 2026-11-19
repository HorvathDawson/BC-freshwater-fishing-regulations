/**
 * (T810 N) THE TROUT AND CHAR TABLE'S VIEW MODEL: `troutOf(inputs, day, today)` turns today's decoded trout cell
 * (`RegsInputs.trout`, the payload of `trout-table-contract.md` 2.3) into what the table draws (`TroutVM`, model.ts).
 *
 * Owner rule 2026-11-16 16:50 (contract guarantees 1 to 5): Python decides every word, number and date, and which rows
 * exist; the app draws only. So this file states NO rule. It composes exactly two things (contract 2.12):
 * (1) the top line's one `{day}` slot, filled by `dayPhrase` ("today" / "on Jul 15", the strip's helper), and
 * (2) a row's screen-reader label: the row's payload texts in payload order (name, origin, label, tag, sub, `lines`,
 *     `quiet`, the band text that covers it, then the nested band's (trout 4: nested after outer), then its date),
 *     joined by ". ", nothing else (contract 2.8).
 * Everything else is the payload, drawn unchanged; a date sits at the one level the frame names.
 *
 * Before drawing, the frame is held to its shape: the same refusals as the pydantic model (`model/trout.py`), pinned by
 * the shared hand cases (`app/fixtures/trout/trout-hand.json`), plus the two the shape cannot know alone: a rule index
 * the store lacks and a `covers` index past the rows frame. A refused frame is REPORTED (`reportFailure`) and no table is
 * drawn: never a fallback to the old card (contract 2.12).
 *
 * Imports: no rows, group or card type and no word but the navigation words and `dayPhrase` (trout.test.ts holds the
 * import list). The one literal below, `REOPENS`, is the payload's own marker for a reopening date, read to refuse a
 * misplaced one, never drawn.
 */
import type { Band, Box, DateSet, TroutValue } from "../answers.generated";
import { reportFailure } from "../failure";
import type { RegsInputs } from "./inputs";
import type { Day, TroutBandVM, TroutBoxVM, TroutRefusedVM, TroutRowVM, TroutVM } from "./model";
import { dayPhrase } from "./strip";
import { TROUT_NAV } from "./words";

/** The top line's one day slot (contract Q-D2). */
export const DAY_SLOT = "{day}";
/** The payload's reopening form (`model/trout.py` REOPENS): only where something is closed today. */
const REOPENS = "reopens ";
/** A row key: a fish of `common.trout_char()`, or one origin of a twin pair (`model/trout.py` RowKey). */
const ROW_KEY = /^(CT|DV|EB|GB|LT|RB|ST)(\/(wild|hatchery))?$/;
/** (T892) `TRow.down`'s two forms (`model/trout.py` DOWN_FORM): a shape refusal, as the model's, never drawn. */
const DOWN_FORM = /^\S.*’s? rule for its tributaries$|^\S.*’s? and \S.*’s? rules for their tributaries$/;
/** (T907) `TRow.tag`'s form (`model/trout.py` TAG_FORM): "Trout Lake’s tributaries", "A’s and B’s tributaries". */
const TAG_FORM = /^\S.*’s? tributaries$/;

/** A trout frame the app will not draw: reported once per read, with the contract's reason. */
export class TroutRefused extends Error {
  constructor(readonly why: string) { super(`trout table refused: ${why}`); this.name = "TroutRefused"; }
}

/** What the shape check needs beyond the frame: whether a rule index exists, and how many rows today's rows frame has. */
export interface TroutBounds { readonly hasRule: (ix: number) => boolean; readonly rows: number }

const bad = (why: string): never => { throw new TroutRefused(why); };

/** A whole phrase (`Txt`): not empty, no edge space, no brace, no day slot. */
function txt(v: string, where: string): void {
  if (!v) bad(`${where}: an empty text`);
  if (v !== v.trim()) bad(`${where}: a leading or trailing space`);
  if (v.includes(DAY_SLOT)) bad(`${where}: ${DAY_SLOT} is the top line's slot only`);
  if (v.includes("{") || v.includes("}")) bad(`${where}: a brace`);
}
const opt = (v: string | null, where: string) => { if (v !== null) txt(v, where); };

function lead(v: string): void {
  if (!v) bad("top.lead: an empty text");
  if (v !== v.trim()) bad("top.lead: a leading or trailing space");
  if (v.split(DAY_SLOT).length - 1 > 1) bad(`the top line holds ${DAY_SLOT} more than once`);
  const rest = v.replace(DAY_SLOT, "");
  if (rest.includes("{") || rest.includes("}")) bad("the top line holds a brace outside its slot");
}

function band(b: Band, where: string): void {
  txt(b.text, where);
  if (b.to < b.from) bad(`${where}: a band runs backwards`);
  const spans = b.kind === "shared" || b.kind === "size";
  if (!spans && b.to !== b.from) bad(`${where}: a ${b.kind} band is one row's`);
  if (spans && b.to === b.from) bad(`${where}: a ${b.kind} band spans two rows or more`);
}

function box(x: Box, bi: number): void {
  const at = `box ${bi}`;
  txt(x.head, `${at} head`);
  opt(x.label, `${at} label`); opt(x.strip, `${at} strip`); opt(x.possession, `${at} possession`); opt(x.tag, `${at} tag`);
  if (!x.rows.length) bad(`${at}: a box holds a row`);
  x.rows.forEach((r, ri) => {
    const rw = `row ${bi}:${ri}`;
    if (!ROW_KEY.test(r.key)) bad(`${rw}: the key ${r.key} is no trout or char row`);
    const [S, o] = r.key.split("/");
    if (S !== r.fish) bad(`${rw}: its fish is ${r.fish}`);
    if ((o ?? null) !== r.origin) bad(`${rw}: origin ${String(r.origin)} <=> the key's origin`);
    if (r.origin !== null && r.label !== null) bad(`${rw}: a twin row carries its origin, never a label`);
    if (r.split && (r.origin !== null || r.label !== null)) bad(`${rw}: a merged hatchery and wild row has no origin and no label`);
    txt(r.name, `${rw} name`); opt(r.label, `${rw} label`); opt(r.sub, `${rw} sub`);
    // (T892) the tributary line, optional (absent: none); its two forms as the model holds them (`model/trout.py` DOWN_FORM)
    if (r.down != null) { txt(r.down, `${rw} down`); if (!DOWN_FORM.test(r.down)) bad(`${rw}: the tributary line ${r.down} is not one of its forms`); }
    // (T907) the tributary tag, optional; never without the line (`model/trout.py` `_down`)
    if (r.tag != null) {
      txt(r.tag, `${rw} tag`);
      if (!TAG_FORM.test(r.tag)) bad(`${rw}: the tributary tag ${r.tag} is not "<water>’s tributaries"`);
      if (r.down == null) bad(`${rw}: a tributary tag without its line`);
    }
    r.lines.forEach((l) => txt(l, `${rw} line`)); r.quiet.forEach((l) => txt(l, `${rw} quiet`));
    txt(r.sheet.head, `${rw} sheet head`); r.sheet.here.forEach((l) => txt(l.t, `${rw} sheet line`));
    if (r.sheet.src.length > 4) bad(`${rw}: a sheet cites at most 4 rules`);
  });
  x.bands.forEach((b, k) => band(b, `band ${bi}:${k}`));
  x.inner.forEach((b, k) => band(b, `nested band ${bi}:${k}`));
  if (x.kind !== "keep") {
    if (x.bands.length || x.label || x.strip || x.possession || x.tag || x.inner.length)
      bad(`a ${x.kind} box has no bands, label, strip, possession or tag`);
    return;
  }
  // (trout 4) a nested band: shared or size, inside ONE band and never that band whole (its 2+ rows: `band`), none overlapping
  const taken = new Set<number>();
  for (const y of x.inner) {
    if (y.kind !== "shared" && y.kind !== "size") bad(`a nested band is shared or size, not ${y.kind}`);
    const out = x.bands.find((b) => b.from <= y.from && y.to <= b.to);
    if (!out || (out.from === y.from && out.to === y.to)) bad(`a nested band ${y.from}..${y.to} lies inside no band, or is one whole`);
    for (let r = y.from; r <= y.to; r++) { if (taken.has(r)) bad(`nested bands overlap at row ${r}`); taken.add(r); }
  }
  let next = 0;
  x.bands.forEach((b) => {
    if (b.from !== next) bad(`the bands leave a gap or overlap at row ${next} (a band from ${b.from})`);
    next = b.to + 1;
  });
  if (next !== x.rows.length) bad(`the bands cover rows 0..${next - 1} of ${x.rows.length}`);
  if (x.label !== null && x.rows.some((r) => r.label !== null)) bad("a label on a box and on its rows");
}

function dates(d: DateSet, t: TroutValue["table"]): void {
  const boxes = t.boxes;
  for (const v of [d.top, d.card, d.fold]) opt(v, "a date");
  for (const m of [d.box, d.band, d.row]) for (const v of Object.values(m)) txt(v, "a date");
  for (const v of Object.values(d.sheet)) v.forEach((l) => txt(l, "a sheet date"));
  if (!(d.top || d.card || d.fold || Object.keys(d.box).length || Object.keys(d.band).length || Object.keys(d.row).length
        || Object.keys(d.sheet).length)) bad("an empty date set is null");
  if ((d.fold !== null) !== (d.top !== null) || (d.fold !== null && d.fold !== d.top))
    bad("the folded entry's date is the top line's, set <=> it is");
  const boxOf = (k: string): number => {
    const b = /^\d+$/.test(k) ? Number(k) : -1;
    if (!(b >= 0 && b < boxes.length)) bad(`a date names box ${k}, which does not exist`);
    return b;
  };
  // (trout 3, "said once") a date holds every row under its level without a date of its own: the most specific level wins
  // (row, band, box, then the card line or top). A level never repeats the date of the level above it, compared as the
  // pydantic model does (the whole-table date with its first letter lowered)
  const whole = d.card ?? d.top;
  const levels = new Map<string, string[]>();
  boxes.forEach((x, b) => x.rows.forEach((_, r) => levels.set(`${b}:${r}`, whole === null ? [] : [whole.slice(0, 1).toLowerCase() + whole.slice(1)])));
  const mark = (b: number, r: number, t: string) => levels.get(`${b}:${r}`)!.push(t);
  for (const [k, t] of Object.entries(d.box)) { const b = boxOf(k); boxes[b]!.rows.forEach((_, r) => mark(b, r, t)); }
  for (const [k, t] of Object.entries(d.band)) {
    const [b, i = ""] = k.split(":"), bi = boxOf(b!);
    const x = /^\d+$/.test(i) ? boxes[bi]!.bands[Number(i)] : undefined;
    if (!x) bad(`a date names band ${k}, which does not exist`);
    for (let r = x!.from; r <= x!.to; r++) mark(bi, r, t);
  }
  for (const [k, t] of Object.entries(d.row)) {
    const [b, i = ""] = k.split(":"), bi = boxOf(b!);
    if (!/^\d+$/.test(i) || Number(i) >= boxes[bi]!.rows.length) bad(`a date names row ${k}, which does not exist`);
    mark(bi, Number(i), t);
  }
  for (const [at, got] of levels) if (got.some((x, i) => i > 0 && x === got[i - 1])) bad(`row ${at} carries one date at two levels`);
  const keys = new Set(boxes.flatMap((x) => x.rows.map((r) => r.key)));
  for (const k of Object.keys(d.sheet)) if (!keys.has(k)) bad(`a sheet date names row ${k}, which the table does not hold`);
  const kinds = new Set(boxes.map((x) => x.kind));
  const form = kinds.size === 1 && kinds.has("closed") ? "closed" : kinds.size === 1 && kinds.has("release") ? "release" : null;
  if (d.top !== null && form === null) bad("a top-line date only on a closed or release-every-one table");
  if (d.card !== null && d.top !== null) bad("a card line and a top-line date at once");
  if (d.card !== null && d.card.toLowerCase().includes(REOPENS)) bad("a reopening date on the card line");
  if (d.top !== null && d.top.startsWith(REOPENS) && form !== "closed") bad("a reopening date under the top line of a table that is not closed today");
  const placed = [...Object.entries(d.box), ...Object.entries(d.band), ...Object.entries(d.row)]
    .map(([k, v]) => [Number(k.split(":")[0]), v] as const);
  for (const [b, v] of placed) if (v.startsWith(REOPENS) && boxes[b]!.kind !== "closed") bad(`a reopening date in a ${boxes[b]!.kind} box`);
}

/**
 * THE SHAPE CHECK: the first reason the frame breaks the contract, thrown as `TroutRefused`; nothing when it holds. The
 * same refusals as the pydantic model (`model/trout.py` validators, in its order), so both sides refuse every case of
 * `trout-hand.json`; then a rule index past the store's rules and a `covers` index past the rows frame (the reader's).
 */
export function checkTrout(f: TroutValue, at: TroutBounds): void {
  const t = f.table;
  lead(t.top.lead); opt(t.top.sub, "top.sub"); opt(t.top.second, "top.second");
  txt(t.fold.value, "fold.value");
  if (!t.boxes.length) bad("a table holds a box");
  t.boxes.forEach(box);
  const order = { keep: 0, release: 1, closed: 2 } as const, ks = t.boxes.map((x) => order[x.kind]);
  if (ks.some((k, i) => i > 0 && k < ks[i - 1]!) || ks.filter((k) => k === 1).length > 1 || ks.filter((k) => k === 2).length > 1)
    bad(`boxes: keep boxes first, then at most one release and one closed (${ks.join(",")})`);
  const keys = t.boxes.flatMap((x) => x.rows.map((r) => r.key));
  if (new Set(keys).size !== keys.length) bad("a row key twice");
  const fishBox = new Map<string, number>();
  t.boxes.forEach((x, bi) => x.rows.forEach((r) => { if ((fishBox.get(r.fish) ?? bi) !== bi) bad(`${r.fish} sits in two boxes`); fishBox.set(r.fish, bi); }));
  const marked = t.boxes.some((x) => x.label !== null || x.rows.some((r) => r.label !== null || r.origin !== null || r.split));
  if (t.hatch_link !== marked) bad("hatch_link <=> some row or box carries a label, an origin or a hatchery/wild split");
  // T810 G2: a limit across two boxes is said under the top line AND under the table (Lois); the lakes' own count
  // (Echoes) only under the top line, `second` without `together`
  if (t.together !== null && t.together.text !== t.top.second) bad("together is set => top.second is, in the same words");
  if (t.together) txt(t.together.text, "together");
  if (t.year !== null) {
    txt(t.year.title, "year.title");
    if (t.boxes.some((x) => x.kind === "keep") || t.boxes.length !== 1)
      bad("the year bar only on a table whose fish all share one state (all release or all closed)");
  }
  t.elsewhere.forEach((l) => txt(l, "elsewhere"));
  const cited = new Set(t.src);
  for (const x of t.boxes) for (const r of x.rows)
    if (![...r.src, ...r.sheet.src].every((i) => cited.has(i))) bad(`row ${r.key} cites a rule the table's src leaves out`);
  if (t.together && !t.together.src.every((i) => cited.has(i))) bad("together cites a rule the table's src leaves out");
  if (cited.size !== t.src.length) bad("the table's src names a rule twice");
  if (!f.covers.length) bad("covers: the table replaces a row");
  if (f.covers.some((c, i) => !Number.isInteger(c) || c < 0 || (i > 0 && c <= f.covers[i - 1]!)))
    bad("covers: rows-frame row indexes, ascending, each once");
  if (f.dates) dates(f.dates, t);
  // the reader's two: the store's rule table, today's rows frame
  const dangling = t.src.find((i) => !at.hasRule(i));
  if (dangling !== undefined) bad(`a dangling rule index ${dangling}`);
  const past = f.covers.find((c) => c >= at.rows);
  if (past !== undefined) bad(`covers names row ${past} of a rows frame of ${at.rows}`);
}

/** A text that already ends a sentence: the join adds no second stop after it (contract 2.8, amended 2026-11-17). */
const ENDS = /[.!?]$/;
/** A row's screen-reader label (contract 2.8): its payload texts in payload order, joined by ". ", nothing else; after a
 *  text that already ends in ".", "!" or "?" the join is a plain space, so the steelhead quiet line never reads "..". */
export function a11yOf(texts: readonly (string | null | undefined)[]): string {
  return texts.filter((x): x is string => !!x).reduce((out, t) => (out ? out + (ENDS.test(out) ? " " : ". ") : "") + t, "");
}

/**
 * Today's trout table, ready to draw: null when the release holds no trout section (`trout` absent) or the cell has none
 * (null: the banner or the tidal page speaks, or no trout or char fish here, contract Q-D3). A frame that breaks the
 * contract is reported and comes back as `refused` (no table; the reader is told).
 */
export function troutOf(inp: RegsInputs, day: Day, today: Day): TroutVM | TroutRefusedVM | null {
  const f = inp.trout;
  if (!f) return null;
  try {
    checkTrout(f, { hasRule: (ix) => inp.rules.has(ix), rows: inp.frames?.rows.rows.length ?? 0 });
  } catch (e) {
    if (!(e instanceof TroutRefused)) throw e;
    reportFailure(e);
    return { kind: "refused", why: e.why, covers: f.covers };
  }
  const t = f.table, d = f.dates;
  const boxes = t.boxes.map((x, b): TroutBoxVM => {
    const bands = x.bands.map((y, k): TroutBandVM => ({ from: y.from, to: y.to, text: y.text, kind: y.kind, date: d?.band[`${b}:${k}`] ?? null }));
    const inner = x.inner.map((y): TroutBandVM => ({ from: y.from, to: y.to, text: y.text, kind: y.kind, date: null }));
    const rows = x.rows.map((r, i): TroutRowVM => {
      const at = (y: TroutBandVM) => y.from <= i && i <= y.to;
      const by = bands.find(at) ?? null, nest = inner.find(at) ?? null, date = d?.row[`${b}:${i}`] ?? null;
      return {
        key: r.key, fish: r.fish, name: r.name, origin: r.origin, label: r.label, split: r.split, tag: r.tag ?? null, sub: r.sub,
        lines: r.lines, quiet: r.quiet, src: r.src, sheet: r.sheet, coming: d?.sheet[r.key] ?? [], date,
        // (T907) the tributary tag where it is drawn: beside the name, after its badges, before the sub
        a11y: a11yOf([r.name, r.origin, r.label, r.tag, r.sub, ...r.lines, ...r.quiet, by?.text, nest?.text, date ?? by?.date]),
      };
    });
    return { kind: x.kind, head: x.head, label: x.label, strip: x.strip, possession: x.possession, tag: x.tag, date: d?.box[String(b)] ?? null, rows, bands, inner };
  });
  return {
    kind: "table",
    top: { lead: t.top.lead.replace(DAY_SLOT, dayPhrase(day, today)), sub: t.top.sub, second: t.top.second, date: d?.top ?? null },
    card: d?.card ?? null,
    fold: { name: TROUT_NAV.ENTRY, value: t.fold.value, tone: t.fold.tone, date: d?.fold ?? null },
    hatchLink: t.hatch_link ? TROUT_NAV.HATCH : null,
    boxes, together: t.together, year: t.year,
    elsewhere: t.elsewhere, elsewhereLink: t.elsewhere.length ? TROUT_NAV.ELSEWHERE : null,
    sourcesLink: TROUT_NAV.SOURCES, src: t.src, covers: f.covers,
  };
}
