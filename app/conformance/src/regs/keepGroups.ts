/**
 * T17–T19 parity helpers: a golden state's Keep groups built by the core presenters over the state's inputs (until T21's
 * buildRegsPage composes them), and each VM projected to the golden's own shape (golden.mjs EXTRACT: `group`, `tierNode`,
 * `kind`, `otherFish`). A projection only reads the VM and words it the way the prototype's DOM reads (contracts §1:
 * runs concatenate as they are, separate elements one space); it never re-derives a value.
 */
import {
  buildModel, drawnRows, FISH, groupHeadOf, groupRows, headCtx, makePlace, notKeptOf, originOf, otherFishOf, plain, plainAll,
  quietOf, moreOf, settle, text, treeEnv, treeOf, workedOf, WORDS, type CardModel, type GroupHeadVM, type HeadCtx, type KindVM,
  type Line, type NotKeptVM, type OriginVM, type OtherFishRowVM, type RegsInputs, type TierVM, type TreeEnv, type TreeNodeVM,
} from "@app/core";
import { norm } from "./eq";

export interface KeepGroup { readonly ctx: HeadCtx; readonly env: TreeEnv }
export interface Keep { readonly card: CardModel; readonly drawn: ReadonlySet<number>; readonly groups: readonly KeepGroup[] }

const KEEP = new WeakMap<RegsInputs, Map<string, Keep>>();
/** The state's card model and its g28 groups (memoised per inputs and form). */
export function keepOf(inp: RegsInputs, tot: "a" | "b" = "a"): Keep {
  let m = KEEP.get(inp);
  if (!m) KEEP.set(inp, m = new Map());
  const hit = m.get(tot);
  if (hit) return hit;
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place)), drawn = drawnRows(card);
  const groups = groupRows(card, drawn).map((i) => { const ctx = headCtx(i, card, inp)!; return { ctx, env: treeEnv(ctx, FISH, drawn, tot) }; });
  const k = { card, drawn, groups };
  m.set(tot, k);
  return k;
}

const n = (s: string | null | undefined): string | null => (s == null ? null : norm(s));
const nl = (l: Line | null | undefined): string | null => (l ? norm(plain(l)) : null);
const nums = (l: Line) => l.filter((r) => r.t === "num").map((r) => ({ n: (r as { s: string }).s, second: (r as { size: string }).size === "big2" }));
const books = (l: Line | null | undefined) => (l ?? []).flatMap((r) => r.t === "book" ? [{ text: plain([r]), pdf: r.pdf }] : []);
const terms = (...ls: (Line | null | undefined)[]) => ls.flatMap((l) => (l ?? []).flatMap((r) => (r.t === "term" ? [...r.ids] : [])));
const hasLink = (l: Line | null | undefined, s: string) => (l ?? []).some((r) => r.t === "link" && r.s === s);

/** golden group.head */
export function headProj(h: GroupHeadVM) {
  const big = h.words.find((r) => r.t === "num") as { s: string } | undefined;
  const words = nl(h.words)!;
  return {
    form: h.form, nolimit: h.nolimit, rootTier: h.rootTier, lead: nl(h.lead),
    leadN: h.leadN == null ? null : String(h.leadN), big: big?.s ?? null,
    ofTotal: h.ofTotal ? norm(`${WORDS.OF_THE} ${words}`) : null, whyHere: n(h.whyHere), shareOf: nl(h.shareOf), terms: [...h.terms],
    text: norm(plainAll([h.lead, h.ofTotal ? [text(WORDS.OF_THE)] : null, h.words, h.whyHere ? [text(h.whyHere)] : null, h.shareOf])),
  };
}
/** golden group.origin (the .ghd.gsub block) */
export function originProj(o: OriginVM | null) {
  if (!o) return null;
  return { lines: [norm(plainAll([o.tag ? [text(o.tag)] : null, o.says, o.howtell ? [text(WORDS.HOW_TELL)] : null, o.book]))],
    tag: o.tag, howtell: o.howtell, pages: books(o.book) };
}
/** golden kind (minus `lines`: they carry the licence stamp lines this port does not draw — tree.ts) */
export function kindProj(k: KindVM) {
  const num = (k.says ?? []).find((r) => r.t === "num") as { s: string } | undefined;
  return { t: "kind", kind: k.kind, sp: [...k.species], folded: k.folded, zero: k.zero, says: nl(k.says), n: num?.s ?? null,
    howtell: hasLink(k.says, WORDS.HOW_TELL), size: n(k.size?.text), q: k.qualifiers,
    pics: k.pictures.flatMap((p) => (p.sil ? [] : [{ code: p.code, caption: n(p.caption) }])), sizeNote: null,
    also: k.also.map((a) => norm(a)), duty: nl(k.duty), presence: nl(k.presence), terms: terms(k.says, k.duty, k.presence) };
}
/** golden tier */
export function tierProj(t: TierVM): Record<string, unknown> {
  return { t: "tier", tier: t.tier, tid: t.tid, folded: t.folded, kicker: t.kicker, line: t.folded ? null : nl(t.line),
    relation: n(t.relation), big: t.folded ? [] : nums(t.line), pages: t.folded ? [] : books(t.line), terms: [],
    sub: n(t.relation ?? t.sub),
    origin: t.origin ? { text: norm(plainAll([t.origin.says, t.origin.howtell ? [text(WORDS.HOW_TELL)] : null, t.origin.book])),
      says: nl(t.origin.says), howtell: t.origin.howtell, pages: books(t.origin.book) } : null,
    children: t.children.map(nodeProj) };
}
export const nodeProj = (x: TreeNodeVM): Record<string, unknown> =>
  x.t === "tier" ? tierProj(x) : x.t === "kind" ? kindProj(x) : { t: "release", names: x.names, pics: x.pictures.flatMap((p) => (p.sil ? [] : [{ code: p.code, caption: p.caption }])) };

/** golden kind nodes without `lines` (see kindProj) */
export function goldenTree(nodes: unknown): unknown {
  return (nodes as Record<string, unknown>[] | null)?.map((x) => {
    const { lines: _l, ...rest } = x as { lines?: unknown; children?: unknown };
    return x.t === "tier" ? { ...rest, children: goldenTree((x as { children: unknown }).children) } : rest;
  }) ?? null;
}

export function notKeptProj(x: NotKeptVM) {
  return { status: x.status, says: x.says, whose: nl(x.whose), mark: x.mark, presence: nl(x.presence),
    pics: x.pictures.flatMap((p) => (p.sil ? [] : [{ code: p.code, caption: n(p.caption) }])), pages: books(x.whose) };
}
/** golden otherFish row: `name` fuses the weekday status (it is inside `.on`); `lines` is the button's one line. */
export function otherProj(o: OtherFishRowVM) {
  const on = o.name + (o.week ?? "");
  return { thumb: o.thumb, name: on, week: o.week, open: o.open,
    lines: [norm(plainAll([[text(on)], [text(o.value)], o.sizeHint ? [text(o.sizeHint)] : null]))] };
}

/** the group's whole head as the golden reads it (title, scope, head, headLines, origin) */
export function groupHeadProj(gr: KeepGroup, tot: "a" | "b" = "a") {
  const head = headProj(groupHeadOf(gr.ctx, { tot }));
  return { title: gr.ctx.title, scope: gr.ctx.scope, head, headLines: [gr.ctx.title + (gr.ctx.scope ?? ""), head.text],
    origin: originProj(originOf(gr.ctx)) };
}
export const treeProj = (gr: KeepGroup) => ({ tree: treeOf(gr.env)?.map(nodeProj) ?? null, treeRoot: gr.ctx.hdrRoot?.id ?? null });
export const tailProj = (gr: KeepGroup) => ({
  notKept: notKeptOf(gr.ctx.g, gr.env).map(notKeptProj), worked: (w => w && { says: w.says, lines: w.lines.map((l) => ({ ...l })) })(workedOf(gr.ctx.row, gr.ctx.g)),
  quiet: nl(quietOf(gr.ctx.row, gr.ctx.card)), more: moreOf(gr.ctx.rowIx).text,
});
export const othersProj = (inp: RegsInputs) => otherFishOf(keepOf(inp).card, inp, FISH)?.map(otherProj) ?? null;
/** golden open key `other:<state>|o<i>` → the app's `other:o<i>` (keys carry no state id, contracts §6 #1) */
export const appOpen = (k: string | null) => (k == null ? null : k.replace(/^other:.*\|/, "other:"));
