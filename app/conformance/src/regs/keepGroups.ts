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
import { keepLines } from "./exclusions";

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
/** the elements of one DOM line, one space between them (golden.mjs `gap`); null/empty parts drawn nothing */
const join = (...xs: (string | null | undefined)[]): string => norm(xs.filter((x) => !!x).join(" "));
const captions = (ps: readonly { sil: boolean; caption?: string }[]) => ps.flatMap((p) => (p.sil ? [] : [p.caption!]));
/** golden kind — `lines` the kind row's drawn lines in order: its sentence; the chip, qualifiers and pictures on one line;
 *  each also-count; the duty; each licence-document line (rows 31 stamps); the presence line (prototype kt; the golden's
 *  `lines(k)`) */
export function kindProj(k: KindVM) {
  const num = (k.says ?? []).find((r) => r.t === "num") as { s: string } | undefined;
  const says = nl(k.says), meta = join(k.size?.text, k.qualifiers, ...captions(k.pictures)), also = k.also.map((a) => norm(a));
  const duty = nl(k.duty), presence = nl(k.presence), stamps = k.stamps.map((l) => nl(l)!);
  return { t: "kind", kind: k.kind, sp: [...k.species], folded: k.folded, zero: k.zero, says, n: num?.s ?? null,
    howtell: hasLink(k.says, WORDS.HOW_TELL), size: n(k.size?.text), q: k.qualifiers,
    pics: k.pictures.flatMap((p) => (p.sil ? [] : [{ code: p.code, caption: n(p.caption) }])), sizeNote: null,
    also, duty, presence, terms: terms(k.says, k.duty, k.presence),
    lines: [says, meta, ...also, duty, ...stamps, presence].filter((l): l is string => !!l) };
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

/** the golden tree with the lines an exclusion drops (exclusions.ts `droppedLines`) removed from each kind's `lines` */
export function goldenTree(nodes: unknown, drop: readonly RegExp[] = []): unknown {
  return (nodes as Record<string, unknown>[] | null)?.map((x) => {
    if (x.t === "tier") return { ...x, children: goldenTree(x.children, drop) };
    return x.t === "kind" ? { ...x, lines: keepLines(x.lines as string[], drop) } : x;
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

/** one tree node's drawn lines in order (prototype tnode / tierH / kt / krel28; the golden's `lines(article)` over it): a tier
 *  its moved scope tag, its line (a folded share draws none, or its Rule book page alone), its relation, its sub, its origin;
 *  then its children */
function nodeLines(x: TreeNodeVM): string[] {
  if (x.t === "kind") return kindProj(x).lines;
  if (x.t === "release") return [join(x.names, ...captions(x.pictures))];
  const o = tierProj(x).origin as { text: string } | null;
  return [x.kicker, x.line.length ? nl(x.line) : null, n(x.relation), n(x.sub), o?.text ?? null]
    .filter((l): l is string => !!l).concat(x.children.flatMap(nodeLines));
}
/**
 * golden group `lines` — the whole group as drawn, line by line (prototype buildKeep's article: the pinned head, the origin
 * block, the tree, the not-kept lines, How it adds up, the quiet line, "See every regulation line ›" inline after the last
 * line; a released fish's licence-document lines under it). Read off the same VMs the field projections read; the draw
 * order and which parts share a line are what it proves.
 */
export function groupLinesProj(gr: KeepGroup, tot: "a" | "b" = "a"): string[] {
  const h = groupHeadProj(gr, tot), t = tailProj(gr), out: string[] = [...h.headLines.map((l) => norm(l))];
  if (h.origin) out.push(...h.origin.lines);
  out.push(...(treeOf(gr.env) ?? []).flatMap(nodeLines));
  for (const r of notKeptOf(gr.ctx.g, gr.env)) {
    const p = notKeptProj(r), tail = [...p.pics.map((x) => x.caption), p.mark];
    out.push(p.says, ...(p.presence ? [p.whose!, join(p.presence, ...tail)] : [join(p.whose, ...tail)]), ...r.stamps.map((l) => nl(l)!));
  }
  const w = workedOf(gr.ctx.row, gr.ctx.g);
  if (w) out.push(w.label, ...(w.says ? [norm(w.says)] : []), ...w.lines.map((l) => join(l.q, l.a)));
  if (t.quiet) out.push(t.quiet);
  if (t.more) out.push(join(out.pop(), t.more));
  return out.filter((l) => !!l);
}
