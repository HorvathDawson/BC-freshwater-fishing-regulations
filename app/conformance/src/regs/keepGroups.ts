/**
 * T17 parity helpers: a golden state's Keep groups built by the core presenters over the state's inputs (until T21's
 * buildRegsPage composes them), and each VM projected to the golden's own shape (golden.mjs EXTRACT: `group`). A projection only reads the VM and words it the way the prototype's DOM reads (contracts §1:
 * runs concatenate as they are, separate elements one space); it never re-derives a value.
 */
import {
  buildModel, drawnRows, groupHeadOf, groupRows, headCtx, makePlace, originOf, plain, plainAll, settle, text, WORDS,
  type CardModel, type GroupHeadVM, type HeadCtx, type Line, type OriginVM, type RegsInputs,
} from "@app/core";
import { norm } from "./eq";

export interface KeepGroup { readonly ctx: HeadCtx }
export interface Keep { readonly card: CardModel; readonly drawn: ReadonlySet<number>; readonly groups: readonly KeepGroup[] }

const KEEP = new WeakMap<RegsInputs, Map<string, Keep>>();
/** The state's card model and its g28 groups (memoised per inputs and form). */
export function keepOf(inp: RegsInputs, tot: "a" | "b" = "a"): Keep {
  let m = KEEP.get(inp);
  if (!m) KEEP.set(inp, m = new Map());
  const hit = m.get(tot);
  if (hit) return hit;
  const place = makePlace(inp), card = buildModel(inp, place, settle(inp, place)), drawn = drawnRows(card);
  const groups = groupRows(card, drawn).map((i) => ({ ctx: headCtx(i, card, inp)! }));
  const k = { card, drawn, groups };
  m.set(tot, k);
  return k;
}

const n = (s: string | null | undefined): string | null => (s == null ? null : norm(s));
const nl = (l: Line | null | undefined): string | null => (l ? norm(plain(l)) : null);
const books = (l: Line | null | undefined) => (l ?? []).flatMap((r) => r.t === "book" ? [{ text: plain([r]), pdf: r.pdf }] : []);

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
/** the group's whole head as the golden reads it (title, scope, head, headLines, origin) */
export function groupHeadProj(gr: KeepGroup, tot: "a" | "b" = "a") {
  const head = headProj(groupHeadOf(gr.ctx, { tot }));
  return { title: gr.ctx.title, scope: gr.ctx.scope, head, headLines: [gr.ctx.title + (gr.ctx.scope ?? ""), head.text],
    origin: originProj(originOf(gr.ctx)) };
}
