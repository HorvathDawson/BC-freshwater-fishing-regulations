/**
 * T192 — THE PAGE MODEL of one water, built ONLY from the release bundle (the regs store over the release's own files:
 * `water`, `regs-<region>`, `regs-sid`, `core`, `geo`) and the app's own presenter (`buildRegsPage` over `loadRegsInputs`).
 * Nothing here decides a regulation, an open/closed answer or a sentence: every word is a string the presenter produced
 * (decisions.md "water pages": the page and the app can never disagree). This file only SELECTS which presenter strings
 * a crawler-readable page carries and puts them in a stable order. `verify.ts` proves it.
 *
 * The angler is the adult resident, as the app defaults (the licence never changes open/closed).
 */
import { buildRegsPage, MONTHS as CORE_MONTHS, WEEKDAYS, type BannerVM, type Day, type Line, type RegsPage, type TreeNodeVM, type Weekday } from "@app/core";
import { loadRegsInputs, type RegsStore } from "@app/data/regs";

export interface BookRef { readonly printed: number; readonly pdf: number | null }
/** One sentence of the page: the presenter's words (no markup) and the synopsis pages it cites. */
export interface Item {
  readonly text: string; readonly book: readonly BookRef[]; readonly head?: true;
  /** names the build day ("Today, Oct 27"): shown, but not part of the rules' content hash */
  readonly dated?: true;
}
export interface Section { readonly title: string; readonly items: readonly Item[] }
export type Status = RegsPage["status"];
/** a day within the next two months on which the water opens or closes (read from the strip's year runs) */
export interface Turn { readonly inDays: number; readonly on: string; readonly to: "open" | "closed" }
export interface PartModel {
  readonly turns: readonly Turn[];
  readonly partIx: number; readonly label: string;
  readonly status: Status;
  /** the presenter's strip words ("Open today", "Closed", ...) or its banner's headline */
  readonly today: string;
  readonly sections: readonly Section[];
}
export type WaterStatus = Status | "mixed";
export interface Neighbour { readonly id: string; readonly name: string; readonly how: "flows-into" | "continues-as" | "continues-from" }
export interface WaterModel {
  readonly ord: number; readonly id: string; readonly name: string; readonly kind: string;
  readonly regions: readonly number[]; readonly region: number;
  readonly status: WaterStatus; readonly answer: string;
  readonly parts: readonly PartModel[];
  readonly neighbours: readonly Neighbour[];
  readonly mid: readonly [lon: number, lat: number] | null;
  readonly book: readonly BookRef[];
}

/** The day a page is built for: its Day, the weekday word and the calendar year the presenter wants. */
export interface BuildDay { readonly day: Day; readonly weekday: Weekday; readonly year: number; readonly iso: string }
export function buildDayOf(d: Date): BuildDay {
  const pad = (n: number) => String(n).padStart(2, "0");
  return { day: { month: d.getMonth() + 1, day: d.getDate() }, weekday: WEEKDAYS[(d.getDay() + 6) % 7]!, year: d.getFullYear(),
           iso: `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` };
}

const ADULT_RESIDENT = { age: "adult", residency: "resident" } as const;

// ── the presenter's words, flattened ─────────────────────────────────────────────────────────────────────────────────
const squash = (s: string) => s.replace(/\s+/g, " ").trim();

/** A Line as words: text and numbers as they read; the ⓘ, the in-app links and the book runs are not words (books are cited). */
function wordsOf(l: Line | null | undefined): string {
  return l ? squash(l.map((r) => (r.t === "text" || r.t === "num" ? r.s : "")).join("")) : "";
}
function booksOf(l: Line | null | undefined): BookRef[] {
  return (l ?? []).flatMap((r) => (r.t === "book" ? [{ printed: r.printed, pdf: r.pdf }] : []));
}
const item = (l: Line | string | null | undefined, extra: Line | null = null, head = false, dated = false): Item | null => {
  const text = typeof l === "string" ? squash(l) : wordsOf(l);
  if (!text) return null;
  return { text, book: [...booksOf(typeof l === "string" ? null : l), ...booksOf(extra)], ...(head ? { head: true as const } : {}),
           ...(dated ? { dated: true as const } : {}) };
};
const items = (xs: readonly (Item | null)[]): Item[] => xs.filter((x): x is Item => x !== null);

/** the tree's sentences, in draw order */
function treeItems(nodes: readonly TreeNodeVM[] | null): Item[] {
  const out: Item[] = [];
  for (const n of nodes ?? []) {
    if (n.t === "tier") {
      out.push(...items([item(n.line), item(n.relation), item(n.sub), n.origin ? item(n.origin.says, n.origin.book) : null]));
      out.push(...treeItems(n.children));
    } else if (n.t === "kind") {
      out.push(...items([item(n.says), item(n.size?.text), item(n.qualifiers), ...n.also.map((a) => item(a)), item(n.duty),
                         ...n.stamps.map((s) => item(s)), item(n.presence)]));
    } else out.push(...items([item(n.names)]));
  }
  return out;
}

function bannerItems(b: BannerVM | null): Item[] {
  if (!b) return [];
  return items([...b.big.map((t) => item(t)), ...b.paras.map((t) => item(t)),
                ...b.chain.map((c) => item(c.text, c.book)),
                b.reopen ? item(b.reopen.text) : null,
                ...(b.merged ? [item(b.merged.label), ...b.merged.items.map((i) => item(i.text, i.book))] : [])]);
}

/** The sections of one part's page: what the Regulations page says on its Keep, Gear, Methods and Licence tabs. */
function sectionsOf(page: Omit<RegsPage, "sheet">): Section[] {
  const k = page.keep, out: Section[] = [];
  const put = (title: string, xs: readonly (Item | null)[]) => { const it = items(xs); if (it.length) out.push({ title, items: it }); };
  put(page.status === "closed" || page.status === "tidal" ? "Why it is closed" : "Closures", bannerItems(k.banner));
  if (k.inherited) put(k.inherited.label, k.inherited.lines.map((l) => item(l.says)));
  for (const c of k.caveats) {
    // (T208) a rule held on one area of the water reads "Outlet area only (see map 1): …", then the map's own line ("See map A ·") with its book page
    put(c.title, [item(c.calm), ...c.items.flatMap((i) => [item(i.area ? `${i.area.tag}:` : null), item(i.what, i.book), item(i.where), item(i.fish), item(i.area?.map)]),
                  ...(c.later ?? []).flatMap((i) => [item(i.area ? `${i.area.tag}:` : null), item(i.what, i.book), item(i.where), item(i.area?.map)])]);
  }
  const keep: (Item | null)[] = [];
  for (const g of k.groups) {
    keep.push(item(g.title, null, true), item(g.scope), item(g.head.lead), item(g.head.words), item(g.head.whyHere),
              item(g.origin?.says ?? null, g.origin?.book ?? null), item(g.year?.says), ...treeItems(g.tree),
              ...g.notKept.flatMap((n) => [item(n.says), item(n.whose), item(n.presence), ...n.stamps.map((s) => item(s))]), item(g.quiet));
  }
  for (const p of k.plainRows) {
    keep.push(item(p.title, null, true), item(p.pill?.text), item(p.scope), item(p.sum), item(p.week?.says),
              ...(p.week?.parts ?? []).map((w) => item(`${w.days}: ${w.says}`, w.book)), ...(p.week?.notes ?? []).map((n) => item(n)),
              item(p.presence), item(p.names), item(p.release?.names));
  }
  put("What you can keep", keep);
  if (k.otherFish?.length) {
    put("Other fish", k.otherFish.flatMap((o) => [item([o.name, o.value, o.week, o.sizeHint].filter(Boolean).join(" · ")), ...o.stamps.map((s) => item(s))]));
  }
  put("Possession", [k.possession ? item(`${k.possession.label}: ${k.possession.value}`) : null, item(k.possession?.says)]);
  if (k.whereNot) put(k.whereNot.heading, [item(k.whereNot.scope), ...k.whereNot.items.map((w) => item(w.text))]);
  const g = page.gear;
  if (g) {
    put("Gear", [item(g.closed), ...g.closedMore.map((t) => item(t)), item(g.day, null, false, true), item(g.mode?.value),
                 ...(g.mode?.changes ?? []).map((c) => item(`${c.chip}: ${c.when} ${c.says}`)),
                 ...g.lines.flatMap((l) => [item(`${l.label}: ${l.values.join(", ")}`), ...l.changes.map((c) => item(`${c.chip}: ${c.when} ${c.says}`))]),
                 ...(g.bait ? [item(g.bait.label), ...g.bait.notes.map((n) => item(n)),
                               ...g.bait.list.flatMap((c) => [item(c.head), ...c.items.map((i) => item(i.note ? `${i.name} (${i.note})` : i.name))])] : [])]);
  }
  const m = page.methods;
  if (m) {
    put("Methods", [item(m.closed), ...(m.ways ? [item(m.ways.label), ...m.ways.list.flatMap((c) => [item(c.head), ...c.items.map((i) => item(i.name))])] : []),
                    ...(m.boats ? [item(m.boats.label), ...m.boats.items.map((b) => item(b))] : [])]);
  }
  const lic = page.licence;
  if (lic) {
    put("Licence", lic.blocks.flatMap((b): (Item | null)[] => {
      switch (b.cls) {
        case "classbox": case "classbox youth": return [item(b.head), item(b.body)];
        case "flag": case "muted": return [item(b.text)];
        case "scopenote": return [item(b.text)];
        case "paper": return [item(`${b.lead} ${b.strong} ${b.tail}`)];
        default: return [];
      }
    }));
  }
  return out;
}

const TODAY_WORDS: Record<Status, string> = {
  open: "Open to fishing today, with limits.",
  closed: "Closed to fishing today.",
  tidal: "Tidal water: the tidal rules apply.",
  outside: "Outside British Columbia.",
  unknown: "We could not tell whether this water is open today. Check the synopsis.",
};

/** the calendar spec's month names, short (AGENTS dataflow gate 4: a calendar table is spelled once) */
const MONTHS = CORE_MONTHS.map((m) => m.slice(0, 3));
/** the book calendar's 365 days (a non-leap year), day index -> "Nov 1" */
const bookDay = (i: number): string => { const d = new Date(Date.UTC(2001, 0, 1 + ((i % 365) + 365) % 365)); return `${MONTHS[d.getUTCMonth()]} ${d.getUTCDate()}`; };

/** Where the water turns open <-> closed in the next `within` days, read from the strip's year runs (bit 1 = closed). */
function turnsOf(page: RegsPage, within: number): Turn[] {
  const s = page.keep.strip;
  if (!s) return [];
  const closed: boolean[] = [];
  for (const r of s.runs) for (let i = 0; i < r.days; i++) closed.push(((r.bits ?? 0) & 1) === 1);
  if (closed.length < 365) return [];
  const at = Math.round(s.todayAt * 365), out: Turn[] = [];
  for (let n = 1; n <= within; n++) {
    const was = closed[(at + n - 1) % closed.length], now = closed[(at + n) % closed.length];
    if (was !== now) out.push({ inDays: n, on: bookDay(at + n), to: now ? "closed" : "open" });
  }
  return out;
}

/** One part's model on `day`. */
export function partModel(page: RegsPage): PartModel {
  const { sheet: _sheet, ...bare } = page;
  const strip = page.keep.strip?.line.map((l) => l.text).join(" ") ?? "";
  const banner = page.keep.banner;
  return {
    partIx: page.target.kind === "part" ? page.target.partIx : 0, label: page.header.place?.full ?? page.header.name,
    status: page.status, today: banner?.big[0] ?? banner?.paras[0] ?? (strip || TODAY_WORDS[page.status]),
    sections: sectionsOf(bare), turns: turnsOf(page, 60),
  };
}

/** The water's verdict: the parts' agreement, else "mixed". */
export function rollup(parts: readonly PartModel[]): { status: WaterStatus; answer: string } {
  const set = new Set(parts.map((p) => p.status));
  if (set.size === 1) {
    const s = parts[0]!.status;
    const closedWhy = s === "closed" ? parts[0]!.sections.find((x) => x.title === "Why it is closed")?.items[0]?.text : undefined;
    return { status: s, answer: closedWhy ? `${TODAY_WORDS[s]} ${closedWhy}` : TODAY_WORDS[s] };
  }
  return { status: "mixed", answer: "Open in some parts and closed or different in others today: see each part below." };
}

/** Every water with a page: shown, in B.C., at least one part with a rule set (a key). Ordered by item ord. */
export async function listWaterOrds(store: RegsStore): Promise<number[]> {
  const w = await store.water();
  const rows = await w.all("SELECT i.item_ord AS ord FROM item i WHERE i.shown = 1 AND i.outside_bc = 0 " +
    "AND EXISTS (SELECT 1 FROM part p WHERE p.item_ord = i.item_ord AND p.akey IS NOT NULL AND p.ord IS NOT NULL) ORDER BY i.item_ord");
  return rows.map((r) => Number(r.ord));
}

async function regionsOf(store: RegsStore, akeys: readonly number[]): Promise<number[]> {
  const w = await store.water();
  const out = new Set<number>();
  for (const a of akeys) for (const r of await w.all("SELECT region FROM akey_region WHERE akey = ?", a)) out.add(parseInt(String(r.region), 10));
  return [...out].filter((n) => n >= 1 && n <= 8).sort((a, b) => a - b);
}

/** The app's page for one part on `on` (the angler defaults as the app's do). */
export async function appPage(store: RegsStore, itemId: string, partIx: number, on: BuildDay, today: Day = on.day): Promise<RegsPage> {
  const inp = await loadRegsInputs(store, { kind: "part", item: itemId, partIx }, on.day, on.weekday, on.year);
  return buildRegsPage(inp, { today, who: ADULT_RESIDENT });
}

/** The water's status on `on` alone (the weekend page asks it for the other days); null: no page. */
export async function statusOn(store: RegsStore, ord: number, on: BuildDay, today: Day): Promise<WaterStatus | null> {
  const [it, parts] = await Promise.all([store.item(ord), store.parts(ord)]);
  const ss: Status[] = [];
  for (const p of parts) {
    if (p.akey === null || p.facts === null) continue;
    const s = (await appPage(store, it.id, p.partIx, on, today)).status;
    if (s !== "outside") ss.push(s);
  }
  return ss.length ? (new Set(ss).size === 1 ? ss[0]! : "mixed") : null;
}

/** The water at `ord`, as of `on`; null when it has no page (every part outside B.C. or without rules). */
export async function waterModel(store: RegsStore, ord: number, on: BuildDay): Promise<WaterModel | null> {
  const [it, parts, nav] = await Promise.all([store.item(ord), store.parts(ord), store.navRows(ord)]);
  const live = parts.filter((p) => p.akey !== null && p.facts !== null);
  if (!live.length) return null;
  const regions = await regionsOf(store, live.map((p) => p.akey!));
  const models: PartModel[] = [];
  for (const p of live) {
    const page = await appPage(store, it.id, p.partIx, on);
    if (page.status === "outside") continue;
    models.push(partModel(page));
  }
  if (!models.length) return null;
  const r = rollup(models);
  const neighbours: Neighbour[] = [];
  const add = (x: { item: string | null; name: string | null } | null, how: Neighbour["how"]) => { if (x?.item && x.name) neighbours.push({ id: x.item, name: x.name, how }); };
  add(nav.exit, "flows-into"); add(nav.continuesAs, "continues-as"); add(nav.continuesFrom, "continues-from");
  const book = new Map<string, BookRef>();
  for (const m of models) for (const s of m.sections) for (const i of s.items) for (const b of i.book) book.set(`${b.printed}`, b);
  const first = nav.geo[0]?.mid ?? null;
  return {
    ord, id: it.id, name: it.name, kind: it.kind, regions, region: regions[0] ?? 0, status: r.status, answer: r.answer, parts: models,
    neighbours, mid: first ? [first[0], first[1]] : null, book: [...book.values()].sort((a, b) => a.printed - b.printed),
  };
}

/** The nearest settlement (city, town, village, hamlet, locality) to each water, by the bundle's own `item_near`; empty where the release has none. */
export async function nearestTowns(store: RegsStore): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  try {
    const core = await store.core();
    const rows = await core.all("SELECT c.item_id AS id, p.name AS town FROM item_near n JOIN item c ON c.ord = n.ord JOIN place p ON p.place_id = n.place_id " +
      "WHERE p.name IS NOT NULL AND p.kind IN ('city','town','village','hamlet','locality') ORDER BY c.item_id, n.ckm, p.place_id");
    for (const r of rows) if (!out.has(String(r.id))) out.set(String(r.id), String(r.town));
  } catch { /* a release without item_near has no towns */ }
  return out;
}

/** Each water's management unit(s) ("3-12", "7a-4"): the `@<unit>` of the book entries it carries (water.item.entries -> water.entry.id). */
export async function managementUnits(store: RegsStore): Promise<Map<string, string[]>> {
  const w = await store.water();
  const unit = new Map<number, string>();
  for (const r of await w.all("SELECT e, id FROM entry")) { const u = /@([0-9]+[A-Za-z]?-[0-9]+)$/.exec(String(r.id))?.[1]; if (u) unit.set(Number(r.e), u); }
  const out = new Map<string, string[]>();
  for (const r of await w.all("SELECT s.text AS id, i.entries AS entries FROM item i JOIN str s ON s.id = i.item_id")) {
    let es: unknown = [];
    try { es = JSON.parse(String(r.entries)); } catch { /* none */ }
    const us = [...new Set((Array.isArray(es) ? es : []).map((e) => unit.get(Number(e))).filter((u): u is string => !!u))];
    if (us.length) out.set(String(r.id), us);
  }
  return out;
}
