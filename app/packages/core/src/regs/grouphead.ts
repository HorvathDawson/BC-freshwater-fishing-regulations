/**
 * T17 — the group head: everything above a group's tree. The title ("Trout and char"), the head kicker ("This water",
 * "Region 6-wide", "Separate from your 4 trout and char", or null when it moved onto the share), the number line in form
 * a (the share leads: "2 a day per angler from streams", then "of the 4 a day per angler across Region 2, lakes
 * included") or form b, or the plain root line, and the origin block under it.
 *
 * Owner rule 2026-10-21: every number, sentence and rule here is the store's (`rows_row.group` — headline, header, tree,
 * origin — the row's pool / narrow / scope badge, and the stored role `lifted` on a fish's answer). This file SELECTS
 * (which tier is the head, which share leads, where the scope tag is drawn) and WORDS them; it decides no regulation.
 *
 * Ports (inventory names @ 7fb6809f): page_v36 rowTitle, scopeOf, scopeBadge, rowHtml (the scope part and its
 * "· streams" rewrite, A0 "Separate from", parentRow), narrowAll, nar0; prototype buildKeep (moveTag/tagShare, hdrRoot,
 * relTier, bigWords, rootLine, rootBook, rootPages, tierOrg, orig, orig2) and shortOf (T28 re-uses it).
 */
import { ruleBookLine } from "./book";
import type { GroupTotal, TreeKind, TreeTier } from "../answers.generated";
/** The store's group shapes the Keep presenters read (`rows_row.group`), for their callers and tests. */
export type { GroupTotal, Presence, TreeKind, TreeTier } from "../answers.generated";
import type { CardModel, HydRow } from "./card";
import { mainRes } from "./card";
import type { RegsInputs, RuleView } from "./inputs";
import { nbsp, text, type Line, type Run } from "./line";
import type { GroupHeadVM, OriginVM } from "./model";
import { cap, lc, Names, spName } from "./names";
import { WORDS } from "./words";

const TROUT_CHAR = "TROUT_CHAR";
const CHAR = "CHAR";

/** page v36 rowTitle: a pool written as one book group is that group's name ("Trout" for trout-and-char except char). */
export function rowTitle(row: HydRow, inp: RegsInputs): string {
  const sp = inp.statics.exportSpecies;
  if (row.pool) {
    const w = (row.pool.f.species as readonly string[] | undefined) ?? [];
    if (w.length === 1 && sp.groups[w[0]!]) {
      return w[0] === TROUT_CHAR && ((row.pool.f.species_except as readonly string[] | undefined) ?? []).includes(CHAR)
        ? WORDS.TROUT_TITLE : sp.groups[w[0]!]!.name;
    }
    return Names(sp, row.members, WORDS.AND);
  }
  if (row.prot) return WORDS.PROTECTED;
  if (!row.members.length && row.win) return cap(spName(sp, ((row.win.f.species as readonly string[] | undefined) ?? [])[0]!).toLowerCase());
  return Names(sp, row.members, WORDS.AND);
}

/** page v36 parentRow: a pooled row another pooled row cross-references (its `xref` fact) and holds; page v36 draws no
 *  article for it (rowHtml returns '').
 *
 *  PIPELINE NEED (T17/T18 review 2026-10-21, owner rule): which pool holds a pooled row's fish is structural regulation
 *  information, and this infers it from `xref` facts. It decides which rows are drawn at all (`drawnRows`), suppresses
 *  "Separate from your N …" (`separateFrom`) and feeds tree.ts presOf's undrawn-row presence. The store must answer it:
 *  `rows_row.parent: int | null` — the index of the pooled row whose pool counts this row's fish (rows.py adds the `xref`
 *  line at that point, `keep_rows[k]["xref"]`). Until it ships this stays a faithful page v36 port, held by the parity;
 *  when it ships, read `row.raw.parent` and delete this. */
export function parentRow(card: CardModel, row: HydRow): HydRow | null {
  if (!row.pool) return null;
  return card.rows.find((r) => r !== row && !!r.pool
    && row.members.every((S) => r.allMembers.includes(S) || r.groups.some((g) => g.members.includes(S)))
    && r.groups.some((g) => g.facts.some((l) => l.t === "xref" && row.members.some((S) => g.members.includes(S))))) ?? null;
}

/** The row indices page v36 draws an article for (every row but a `parentRow`'s). */
export function drawnRows(card: CardModel): ReadonlySet<number> {
  return new Set(card.rows.map((r, i) => [r, i] as const).filter(([r]) => !parentRow(card, r)).map(([, i]) => i));
}

/** page v36 rowHtml A0: the pooled row whose pool every fish of this row's stored main answer names as `lifted`. */
export function separateFrom(card: CardModel, row: HydRow): HydRow | null {
  if (parentRow(card, row) || !row.pool) return null;
  return card.rows.find((r) => r !== row && !!r.pool && row.members.every((S) => {
    const m = mainRes(card.R[S]?.H ?? null, card.R[S]?.W ?? null);
    return !!m && m.roles.get(r.pool!.key)?.role === "lifted";
  })) ?? null;
}

/** page v36 scopeOf: whose count a pool is, in words (the store's badge; the entry's name from the pool's provenance). */
export function scopeOf(row: HydRow): string | null {
  const s = row.scope;
  if (!s) return null;
  if (s.of === "bc") return WORDS.BC;
  if (s.of === "region" || s.of === "area") return row.pool?.export.prov.entry_name || (s.of === "area" ? WORDS.THE_AREA : WORDS.THE_REGION_LC);
  return null;
}

/** page v36 narrowAll: the narrow rule binds every fish of the row.
 *
 *  PIPELINE NEED (T17 review 2026-10-21, owner rule): whether the narrowing rule binds every fish of the row is a
 *  rule-coverage answer, decided here from the rule's species sets; it picks the kicker ("All Region 2 streams" vs "Region
 *  2-wide"). The store must answer it: `rows_row.scope.narrow_all: bool` (rows.py `scope_of`; `elsewhere_of` already
 *  computes the same `all(_covers(nw, S) for S in row["members"])`). Until it ships this stays a faithful page v36 port,
 *  held by the parity; when it ships, `nar0` reads `row.scope.narrow_all` and this is deleted. */
export const narrowAll = (row: HydRow): boolean => !!row.narrow
  && row.members.every((S) => row.narrow!.species.includes(S) && !row.narrow!.speciesExcept.includes(S));
/** page v36 nar0: a stream (or lake) share of a regional total holds here and binds every fish of the row. */
export const nar0 = (row: HydRow): boolean => !!(row.pool && row.scope?.share && narrowAll(row));

/**
 * The row title's scope words as page v36's rowHtml draws them (before the prototype moves a tag): "Separate from your
 * 4 trout and char" (A0); else, for a pooled keep row, scopeBadge with its "· streams" form rewritten — "All Region 2
 * streams" when the narrow binds every fish (nar0), else "Region 2-wide"; "This water" for a water's own pool.
 */
export function scopeBadge(row: HydRow, card: CardModel, inp: RegsInputs): string | null {
  const a0 = separateFrom(card, row);
  if (a0) return `${WORDS.SEPARATE_FROM}${a0.daily} ${lc(rowTitle(a0, inp))}`;
  if (!row.pool || row.kind !== "keep") return null;
  if (!row.scope) return WORDS.SCOPE_MISSING;
  const s = scopeOf(row);
  if (s && row.scope.share && row.narrow) {
    const water = String(row.narrow.f.water ?? "");
    if (water === "stream") return nar0(row) ? WORDS.SCOPE_ALL_STREAMS(s) : WORDS.SCOPE_WIDE(s);
    return `${s}${WORDS.DOT_SEP}${water}${WORDS.PLURAL_S}`;
  }
  return s ? WORDS.SCOPE_WIDE(s) : WORDS.SCOPE_HERE;
}

/** prototype pagesOf: the printed pages of a rule's Rule book links. */
export const pagesOf = (r: RuleView | null): ReadonlySet<number> => new Set((r?.facts?.book ?? []).map(([p]) => p));

const tierOf = (t: TreeTier | TreeKind | undefined): TreeTier | null => (t && t.node === "tier" ? t : null);
const cap1 = (t: string | null | undefined): string => (t ? t[0]!.toUpperCase() + t.slice(1) : "");

export interface HeadCtx {
  readonly row: HydRow; readonly rowIx: number; readonly card: CardModel; readonly inp: RegsInputs;
  readonly g: GroupTotal; readonly title: string;
  /** the head kicker as page v36 drew it; `scope` is what stays in the head (null when it moved to the share) */
  readonly tag: string | null; readonly scope: string | null;
  readonly hdrRoot: TreeTier | null; readonly relTier: TreeTier | null;
  readonly moveTag: boolean; readonly tagShare: TreeTier | null;
  readonly rootBook: Line;                         // [] when none
  readonly rootPages: ReadonlySet<number>;         // printed pages the head shows (round 20b)
  readonly tierOrg: boolean;
  /** page v36 drew an origin line (the group's or a tier's), so the prototype has its "How to tell ›" */
  readonly howtell: boolean;
}

const hasTierOrigin = (ts: readonly (TreeTier | TreeKind)[]): boolean =>
  ts.some((t) => t.node === "tier" && (!!t.origin || hasTierOrigin(t.children)));

/** The head's context for card row `rowIx`, or null: no `group.headline` (not a g28 group). */
export function headCtx(rowIx: number, card: CardModel, inp: RegsInputs): HeadCtx | null {
  const row = card.rows[rowIx];
  const g = row?.raw.group;
  if (!row || !g || !g.headline) return null;
  const rt0 = tierOf(g.tree[0]);
  const tag = scopeBadge(row, card, inp);
  const moveTag = !!(rt0 && rt0.tier === "total" && !rt0.whose && / across /.test(rt0.says || "") && tag && !/-wide$/.test(tag));
  const tagShare = moveTag ? rt0!.children.map(tierOf).find((c) => !!c && c.tier === "share") ?? null : null;
  const hdrRoot = rt0 && g.header?.tier === rt0.id ? rt0 : null;
  const h = g.headline;
  const relTier = hdrRoot && hdrRoot.tier === "total" && h.n != null && h.n !== hdrRoot.n
    ? hdrRoot.children.map(tierOf).find((c) => !!c && c.n === h.n && (c.tier === "share" || c.tier === "own")) ?? null : null;
  const key = hdrRoot ? inp.rules.get(hdrRoot.by) : null;
  return {
    row, rowIx, card, inp, g, title: rowTitle(row, inp), tag, scope: moveTag ? null : tag, hdrRoot, relTier, moveTag, tagShare,
    rootBook: key && !key.missing ? ruleBookLine(key) : [], rootPages: key && !key.missing ? pagesOf(key) : new Set(),
    tierOrg: !g.origin && hasTierOrigin(g.tree),
    howtell: !!g.origin || hasTierOrigin(g.tree),
  };
}

/** prototype bigWords: the sentence split at its first whole-word `n` — [pre, " ", num, " ", post] with pre/post trimmed
 *  (no empty run), or null when the sentence does not say `n`. */
export function bigWords(says: string, n: number | null | undefined, size: "big" | "big2" = "big"): Run[] | null {
  if (n == null) return null;
  const m = new RegExp(`^(.*?)\\b${n}\\b(.*)$`).exec(says);
  if (!m) return null;
  const out: Run[] = [];
  if (m[1]!.trim()) out.push(text(nbsp(m[1]!.trim())), text(" "));
  out.push({ t: "num", s: String(n), size });
  if (m[2]!.trim()) out.push(text(" "), text(nbsp(m[2]!.trim())));
  return out;
}

/** prototype rootLine (tot a = the r20h owner pick, the default; tot b keeps the total first). */
export function groupHeadOf(ctx: HeadCtx, opts: { tot: "a" | "b" }): GroupHeadVM {
  const { g, hdrRoot, relTier } = ctx, h = g.headline;
  if (!hdrRoot) {
    // legacy head (no `group.header.tier`; no v38 state): prototype buildKeep `hm` / `num` — UNTESTED against a golden
    const hd = g.header?.says || "", hcut = hd.indexOf(" · "), hlead = hcut < 0 ? hd : hd.slice(0, hcut), hrest = hcut < 0 ? "" : hd.slice(hcut + 3);
    const hm = bigWords(hlead, h.n);
    const words: Run[] = hm ?? (h.n != null ? [{ t: "num", s: String(h.n), size: "big" }, text(" "), text(h.unit)] : [text(h.says)]);
    return { form: null, rootTier: null, lead: null, leadN: null, words, ofTotal: false,
      whyHere: hm && hrest && !g.tree.length ? cap1(hrest) : null, shareOf: null, terms: [], nolimit: !hm && h.n == null };
  }
  const words = bigWords(hdrRoot.says, hdrRoot.n);
  if (relTier && opts.tot === "a") {
    const lead = bigWords(h.says, h.n) ?? [{ t: "num", s: String(h.n), size: "big" } as Run, text(" "), text(nbsp(h.says))];
    return { form: "a", rootTier: hdrRoot.id, lead, leadN: h.n, words: words ?? [text(nbsp(hdrRoot.says))], ofTotal: true,
      whyHere: null, shareOf: null, terms: [], nolimit: false };
  }
  let shareOf: Run[] | null = null;
  if (relTier && opts.tot === "b") {
    const s = shortOf(relTier), m = /\b(\d+)\b/.exec(s);
    shareOf = [text(WORDS.SHARE_ARROW)];
    if (!m) shareOf.push(text(s));
    else {
      if (m.index) shareOf.push(text(s.slice(0, m.index)));
      shareOf.push({ t: "num", s: m[1]!, size: "big2" });
      if (s.slice(m.index + m[0].length)) shareOf.push(text(s.slice(m.index + m[0].length)));
    }
  }
  return { form: shareOf ? "b" : null, rootTier: hdrRoot.id, lead: null, leadN: null, words: words ?? [text(nbsp(hdrRoot.says))],
    ofTotal: false, whyHere: hdrRoot.why_here ? cap1(hdrRoot.why_here) : null, shareOf, terms: [], nolimit: !words };
}

/** The `.ghd.gsub` block (prototype orig / orig2): the group's origin words (its tag before " — "), "How to tell ›",
 *  the root's Rule book page; or the root's page alone when no tier carries the origin; null when nothing is drawn. */
export function originOf(ctx: HeadCtx): OriginVM | null {
  const { g, rootBook } = ctx;
  if (g.origin) {
    const ocut = g.origin.indexOf(" — ");
    if (g.header && !g.header.tag) return { says: [text(g.origin)], tag: null, howtell: ctx.howtell, book: rootBook };
    if (ocut > 0) return { says: [text(g.origin.slice(ocut + 3))], tag: g.header?.tag || g.origin.slice(0, ocut), howtell: ctx.howtell, book: rootBook };
    return { says: [text(g.origin)], tag: null, howtell: ctx.howtell, book: rootBook };
  }
  return rootBook.length && !ctx.tierOrg ? { says: [], tag: null, howtell: false, book: rootBook } : null;
}

const lc1w = (t: string): string => (t ? t[0]!.toLowerCase() + t.slice(1) : t);

/** prototype shortOf: one tier as one short line, from its own fields and the page's connecting words (the pin bar's,
 *  T28; form b's "↳ 2 of them from streams"). */
export function shortOf(t: TreeTier): string {
  const n = t.n, says = t.says || "";
  switch (t.tier) {
    case "total": {
      if (n == null) return says;
      const m = / across (.+?)(?:, (?:lakes|streams) included)?$/.exec(says);
      return m ? `${n}${WORDS.SHORT_ACROSS}${m[1]}` : t.whose ? `${t.whose}${WORDS.COLON_SP}${n}${WORDS.SHORT_DAY}` : `${n}${WORDS.SHORT_DAY}`;
    }
    case "share": {
      const w = /from (this stream|this lake|streams|lakes)/.exec(says)?.[1] || WORDS.SHORT_STREAMS;
      return /^Only /.test(says) ? `${n}${WORDS.SHORT_OF_THEM_FROM}${w}` : `${n}${WORDS.SHORT_DAY_FROM}${w}`;
    }
    case "own": {
      const k = t.children.find((c): c is TreeKind => c.node === "kind" && !!c.folded);
      const names = k ? / may be (.+?)(?: only —.*)?$/.exec(k.says)?.[1] || "" : "", whose = lc1w(t.whose || "");
      if (t.relation) return `${n}${names ? " " + names : ""}${WORDS.SHORT_DAY_UNDER}${whose}${WORDS.COMMA}${lc1w(t.relation.split(";")[0]!.trim())}`;
      return t.replaces ? `${WORDS.SHORT_PLUS}${n}${names ? " " + names : ""}${WORDS.SHORT_UNDER}${whose}`
        : `${n}${WORDS.SHORT_OF_THEM}${names ? `${WORDS.OPEN_PAREN}${names}${WORDS.CLOSE_PAREN}` : ""}${WORDS.SHORT_UNDER}${whose}`;
    }
    case "count": return `${WORDS.SHORT_OF_THOSE}${says.replace(/ \(part of the \d+\)$/, "")}`;
    case "size": {
      const cut = says.indexOf(" — "), tail = cut > 0 ? says.slice(cut + 3) : "";
      return `${WORDS.SHORT_OF_THOSE}${n == null ? "" : n + " "}${WORDS.SHORT_OVER}${t.over_cm}${WORDS.CM}${tail ? `${WORDS.OPEN_PAREN}${tail}${WORDS.CLOSE_PAREN}` : ""}`;
    }
  }
  return says;
}
