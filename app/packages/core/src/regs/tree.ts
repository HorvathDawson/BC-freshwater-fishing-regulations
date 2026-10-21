/**
 * T18 — the quota tree: each tier a line with its number big in its own words, the size box's second number, its Rule
 * book page once per branch, the moved scope kicker, the tier's origin line; each kind row = the data's sentence with its
 * number, the size chip, the qualifiers, the pictures FIRST, then also-counts, duties ("Steelhead stamp ›"), presence.
 *
 * Owner rule 2026-10-21: the tiers, kinds, numbers, sentences, size rules, duties and presence lines are the store's
 * (`rows_row.group.tree`, `rows_row.presence`); this file WORDS and LAYS THEM OUT (which number is big, which page link is
 * fresh on a branch, whether form a's share says anything the head did not), and decides no regulation.
 *
 * NOT ported: prototype `stampsFor`/`stampLine` (round UR7 — the licence documents a kind needs, e.g. "You need
 * Conservation Surcharge Stamp for Shuswap rainbow trout › only if you keep rainbow trout over 50 cm"). It decides which
 * document a fish needs from the licence answer and the kind's sizes — a regulation answer the pipeline must ship per kind.
 *
 * Ports (inventory names @ 7fb6809f): prototype buildKeep.{nbsp, boldN, sizeOf, dutyWords, kt, tierWords, pagesOf
 * (tierH, tnode), presOf, presP, pics}, fishBtn, fishName.
 */
import type { Presence, TreeKind, TreeTier } from "../answers.generated";
import type { CardModel } from "./card";
import type { FishManifest } from "./fish.generated";
import { bookLine, pagesOf, shortOf, type HeadCtx } from "./grouphead";
import type { RegsInputs } from "./inputs";
import { boldN, nbsp, text, type Line, type Run } from "./line";
import type { FishPicVM, KindVM, OriginVM, TierVM, TreeNodeVM } from "./model";
import { WORDS } from "./words";

export interface TreeEnv {
  readonly inp: RegsInputs; readonly card: CardModel; readonly fish: FishManifest;
  readonly drawn: ReadonlySet<number>; readonly stampST: boolean; readonly ctx: HeadCtx;
  readonly tot: "a" | "b";
}

const cap1 = (t: string | null | undefined): string => (t ? t[0]!.toUpperCase() + t.slice(1) : "");

/** prototype sizeOf, verbatim: the detail's size words (non-"any" preferred) and its other words (qualifiers). */
export function sizeOf(detail: string | null | undefined): { size: string; q: readonly string[] } {
  const sz: string[] = [], q: string[] = [];
  (detail || "").split(", ").filter(Boolean).forEach((x) => {
    const m = /^(.*?)((?:or )?(?:over |under |up to )?\d[\d–]* cm\b.*)$/.exec(x);
    if (m) { if (m[1]!.trim()) q.push(m[1]!.trim()); sz.push(m[2]!); }
    else if (/^any (size|length)$/.test(x)) sz.push(x);
    else q.push(x);
  });
  const real = sz.filter((x) => !/^any /.test(x));
  return { size: (real.length ? real : sz).join(", "), q };
}

/** prototype tierWords: boldN(says, n) big; a size box's `extra` makes the first number after " — " big too ("big2"). */
export function tierWords(t: TreeTier): Line {
  const w = boldN(t.says, t.n, "big");
  if (!(t.tier === "size" && t.extra)) return w;
  const out: Run[] = [];
  let seen = false, done = false;
  for (const r of w) {
    if (done || r.t !== "text") { out.push(r); continue; }
    let s = r.s, from = 0;
    if (!seen) { const c = s.indexOf(" — "); if (c < 0) { out.push(r); continue; } seen = true; from = c; }
    const m = /\b\d+\b/.exec(s.slice(from));
    if (!m) { out.push(r); continue; }
    const at = from + m.index;
    if (at) out.push(text(s.slice(0, at)));
    out.push({ t: "num", s: m[0], size: "big2" });
    s = s.slice(at + m[0].length);
    if (s) out.push(text(s));
    done = true;
  }
  return out;
}

/** prototype dutyWords (v33): cap1, and "this river / stream / lake / creek" becomes the water's own name. */
export function dutyWords(duty: string, waterName: string): string {
  return cap1(duty).replace(/\bthis (river|stream|lake|creek)\b/i, () => WORDS.THE_NAME(waterName));
}

const same = (a: readonly string[], b: readonly string[]): boolean => a.length === b.length && a.every((x) => b.includes(x));

/** prototype presOf: the kind's own presence, else that of a row page v36 drew no article for whose fish are the kind's. */
export function presOf(k: TreeKind, card: CardModel, drawn: ReadonlySet<number>): Presence | null {
  if (k.presence) return k.presence;
  const i = card.rows.findIndex((r, j) => !!r.raw.presence && !drawn.has(j) && same(r.members, k.species));
  return i < 0 ? null : card.rows[i]!.raw.presence!;
}

/** prototype presP: the presence sentence and its steelhead ⓘ. */
export const presenceLine = (p: Presence | null | undefined): Line | null =>
  p ? [text(p.says), text(" "), { t: "term", ids: ["steelhead"] }] : null;

/** prototype fishBtn per code: the FIRST manifest picture, captioned, opening its sheet; no picture: a silhouette. */
export function pictures(codes: readonly string[], fish: FishManifest, names: (c: string) => string): readonly FishPicVM[] {
  return codes.map((code): FishPicVM => {
    const f = fish[code]?.[0];
    const name = names(code) || f?.caption || code;
    return f ? { code, name, caption: WORDS.CAPTION(f.caption), open: `sp:${code}`, sil: false } : { code, name, sil: true };
  });
}

/** prototype fishName: the species' own name, else its picture's caption, else the code. */
export const fishNames = (inp: RegsInputs, fish: FishManifest) => (c: string): string =>
  inp.statics.exportSpecies.fish[c]?.name || fish[c]?.[0]?.caption || c;

/** prototype stampST (/Steelhead Conservation Surcharge Stamp/ over the drawn licence): a profile of the day's stored licence
 *  answer lists the steelhead stamp among its documents — the only place the page prints the stamp's name. A link target
 *  only: whether a stamp is needed is the licence answer's, never decided here. (A stamp the answer names only as exempt
 *  — x27p1_1010 — is not drawn, so it is no link.) */
export const stampSTOf = (inp: RegsInputs): boolean => (inp.frames?.licence.profiles ?? []).some((p) =>
  "documents" in p && p.documents.some((d) => d.doc === STAMP_DOC || (d.or ?? []).some((o) => o.need.includes(STAMP_DOC))));
const STAMP_DOC = "steelhead_stamp";

/** prototype kt: one kind row. */
export function kindRow(k: TreeKind, env: TreeEnv): KindVM {
  const { size: size0, q } = sizeOf(k.detail), zero = k.n === 0, size = k.size_rule || size0;
  let says: Run[] | null = null;
  if (!k.folded) {
    says = [...boldN(k.says, zero ? null : k.n, "inline")];
    if (k.species.includes("ST") && /\bhatchery steelhead only\b/.test(k.says)) says.push(text(" "), { t: "link", s: WORDS.HOW_TELL, open: "hatch" });
  }
  let duty: Run[] | null = null;
  if (k.duties.length) {
    duty = [text(k.duties.map((x) => dutyWords(x, env.inp.item?.name ?? env.inp.place?.name ?? "")).join(WORDS.DOT_SEP))];
    if (env.stampST && k.species.includes("ST")) duty.push(text(" "), { t: "link", s: WORDS.STEELHEAD_STAMP, open: { tab: "licence" } });
  }
  const sizeNote = (k as TreeKind & { size_note?: { says: string } | null }).size_note;
  return {
    t: "kind", kind: k.name, species: k.species, folded: !!k.folded, zero, says,
    size: size && !zero ? { text: nbsp(size), rule: !!k.size_rule } : null,
    qualifiers: q.length ? q.join(", ") : null,
    pictures: pictures(k.species, env.fish, fishNames(env.inp, env.fish)),
    also: [...(sizeNote ? [nbsp(cap1(sizeNote.says))] : []), ...(k.also_counts ?? []).map((a) => nbsp(cap1(a.says)))],
    duty, presence: presenceLine(presOf(k, env.card, env.drawn)),
  };
}

/** The relTier share that says no more than form a's lead ("Only 2 of the 4 may be from streams"): folded (int1). */
const foldsShare = (t: TreeTier, env: TreeEnv): boolean => t === env.ctx.relTier && env.tot === "a" && t.tier === "share"
  && t.n === env.ctx.g.headline?.n && /^Only \d+ of the \d+ may be from streams\.?$/.test(t.says || "");

/** prototype tierH: a tier's head — kicker, line (its fresh Rule book page), relation, sub, its origin line. */
export function tierNode(t: TreeTier, env: TreeEnv, root: boolean, seen: ReadonlySet<number>): TierVM {
  const key = env.inp.rules.get(t.by), hasKey = !key.missing;
  const pg = hasKey ? pagesOf(key) : new Set<number>(), fresh = [...pg].some((p) => !seen.has(p));
  const bk: Line = hasKey && fresh ? bookLine(key) : [];
  const g = env.ctx.g;
  const torg = (extra: Line): OriginVM | null => t.origin
    ? { says: [text(t.origin)], tag: null, howtell: env.ctx.howtell,
        book: [...(!g.origin ? env.ctx.rootBook : []), ...(!g.origin && env.ctx.rootBook.length && extra.length ? [text(" ")] : []), ...extra] }
    : null;
  const below = new Set([...seen, ...pg]);
  const children = t.children.map((c) => (c.node === "tier" ? tierNode(c, env, false, below) : kindRow(c, env)));
  const base = { t: "tier" as const, tier: t.tier, tid: t.id, band: t.tier === "size", short: shortOf(t), children };
  if (foldsShare(t, env)) {
    const origin = torg(bk);
    return { ...base, kicker: null, line: origin ? [] : bk, folded: true, relation: null, sub: null, origin };
  }
  const line: Run[] = [...tierWords(t)];
  if (bk.length) line.push(text(" "), ...bk);
  return { ...base, kicker: t === env.ctx.tagShare ? env.ctx.tag : null, line, folded: false,
    relation: t.relation ? nbsp(t.relation) : null,
    sub: t.tier === "own" && t.why_here ? nbsp(cap1(t.why_here)) : null, origin: torg([]) };
}

/** prototype tops/tree: the head's root's children (the head IS the root tier), else every tier at the top. */
export function treeOf(env: TreeEnv): readonly TreeNodeVM[] | null {
  const { g, hdrRoot, rootPages } = env.ctx;
  if (!g.tree.length) return null;
  const tops = hdrRoot ? hdrRoot.children : g.tree;
  return tops.map((t) => (t.node === "tier" ? tierNode(t, env, !hdrRoot, rootPages) : kindRow(t, env)));
}

/** The tree environment of one group (`drawn`: the rows page v36 draws an article for). */
export function treeEnv(ctx: HeadCtx, fish: FishManifest, drawn: ReadonlySet<number>, tot: "a" | "b" = "a"): TreeEnv {
  return { inp: ctx.inp, card: ctx.card, fish, drawn, stampST: stampSTOf(ctx.inp), ctx, tot };
}
