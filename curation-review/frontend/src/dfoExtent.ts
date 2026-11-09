/* T525 (owner 2026-11-09: "extents already defined get lost when confirming vs redo … feels like i will make bad data"):
   WHAT A DFO MARK DOES TO THE PLACE'S CURATED EXTENT, as pure functions — the review screen (`DfoReview`) renders them,
   and `curation-review/frontend/tests/dfoExtent.test.ts` runs them (node --test).

   The owner's call is one of four. Keep, Not sure and Wrong never carry an extent fix: the curated extent stays as it
   is. Change starts FROM the curated extent (`DfoExtentNow.pieces`), and its effect — keep (nothing differs), replace
   or add a piece — is said before the save. Every mark names the extent it was made on (`extent_before`); the server
   refuses one made on an extent that has moved since. */
import type { CheckAnswer, DfoExtentNow, ExtentFix, MarkPost, NewSplit } from "./types";

export type DfoCall = "keep" | "change" | "unsure" | "wrong";
export type DfoEffect = "keep" | "replace" | "add";

/** one piece as compared: its op, its cuts in order, the SET of waters it holds on, its area and tributary call */
const pieceKey = (f: ExtentFix) => JSON.stringify([f.op, f.points.map((p) => [p.kind, p.split_id ?? null, p.name ?? null,
  p.item_id ?? null, p.coord ?? null]), [...f.items].sort(), f.area ?? null, f.tributaries ?? null]);

/** WHAT A CHANGE DOES to the extent now: `keep` when the draft is the same pieces (a union has no order), `add` when it
 *  is every piece now plus more, `replace` otherwise (a draft of no pieces changes nothing: `keep`). */
export function effectOf(now: ExtentFix[], draft: ExtentFix[]): DfoEffect {
  if (!draft.length) return "keep";
  const have = now.map(pieceKey), want = draft.map(pieceKey);
  const same = (a: string[], b: string[]) => a.length === b.length && [...a].sort().join("\n") === [...b].sort().join("\n");
  if (same(have, want)) return "keep";
  return have.length && have.every((k) => want.includes(k)) ? "add" : "replace";
}

/** the pieces with piece `i` as `f` (a new last piece when `i` is past the end) */
export function withPiece(fixes: ExtentFix[], i: number, f: ExtentFix): ExtentFix[] {
  return i >= fixes.length ? [...fixes, f] : fixes.map((x, j) => (j === i ? f : x));
}
/** the pieces without piece `i` */
export function dropPiece(fixes: ExtentFix[], i: number): ExtentFix[] {
  return fixes.filter((_x, j) => j !== i);
}
/** ONE MORE PIECE (T524): on its own water, downstream of a cut to pick — on the tributary call the place's pieces
 *  already make (one call for the whole place, `marks.pieces_ok`; T524 review: never the title's flag after piece 1 was
 *  changed) */
export function addPiece(fixes: ExtentFix[], call: boolean | null): ExtentFix[] {
  return [...fixes, { scope: "entry", op: "downstream_of", points: [], items: [],
                      tributaries: fixes.length ? fixes[0].tributaries ?? null : call }];
}
/** what a piece's "preview on map" resolves: the piece alone when it is the only one, else the place's UNION with it */
export function previewOf(fixes: ExtentFix[], i: number, f: ExtentFix): ExtentFix | ExtentFix[] {
  const all = withPiece(fixes, i, f);
  return all.length > 1 ? all : f;
}

/** a change that proposes a cut not built yet (a NEW split, or a new point in a piece) cannot say Place y */
export function proposesCuts(fixes: ExtentFix[], newSplits: NewSplit[]): boolean {
  return newSplits.length > 0 || fixes.some((f) => f.points.some((p) => p.kind !== "split"));
}

/** THE PLACE ANSWER A CALL GIVES: keep y; change y (T210: the extent is set from existing cuts) or ? (a cut is still to
 *  build); not sure ?; wrong n */
export function placeAnswer(call: DfoCall, cuts: boolean): CheckAnswer {
  return call === "keep" ? "y" : call === "change" ? (cuts ? "?" : "y") : call === "unsure" ? "?" : "n";
}

/** why the save is held, or "" — the server's own rules, said before the save */
export function heldBy(a: { call: DfoCall | null; answers: Partial<Record<"T" | "R", CheckAnswer>>; problem: string;
                            note: string; effect: DfoEffect; cuts: boolean; cannotChange: string[];
                            extents: number }): string {
  if (!a.answers.T || !a.answers.R) return "answer T and R first (y n ?)";
  if (!a.call) return "pick your call: keep it, change it, not sure, or wrong";
  if (a.call === "keep" && !a.extents) return "there is no extent to keep — change it (set one), or say not sure / wrong";
  if (a.call === "change" && a.cannotChange.length) return `this extent cannot be changed here: ${a.cannotChange.join("; ")}`;
  if (a.call === "change" && a.effect === "keep" && !a.cuts) return "your change is the extent now — pick “Right, keep it”";
  const n = a.call === "wrong" || a.answers.T === "n" || a.answers.R === "n";
  return n && !(a.problem && a.note.trim()) ? "a wrong answer needs a problem (pick one) and a note saying what is wrong" : "";
}

/** the save button's words: what it will do to the extent */
export function saveWords(call: DfoCall | null, effect: DfoEffect): string {
  if (call === "change" && effect === "replace") return "Save: replace extent";
  if (call === "change" && effect === "add") return "Save: add a piece";
  if (call === "unsure") return "Save: not sure (keeps the extent)";
  if (call === "wrong") return "Save: wrong (keeps the extent)";
  return "Save: keep extent";
}

/** THE MARK A SAVE POSTS. An extent fix rides ONLY a change that changes something, new splits only a change; every
 *  mark names the extent it was made on (`extent_before`). A defer carries neither (it decides nothing). */
export function markBody(a: { target: string; call: DfoCall; answers: Partial<Record<"T" | "R", CheckAnswer>>;
                              problem: string; note: string; reviewer: string; seconds: number; fixes: ExtentFix[];
                              carried: NewSplit[]; now: DfoExtentNow; targets: string[]; defer?: boolean }): MarkPost {
  const change = a.call === "change" && !a.defer;
  const effect = change ? effectOf(a.now.pieces, a.fixes) : "keep";
  return {
    scope: "dfo", target: a.target, problem: a.problem || null, note: a.note, reviewer: a.reviewer, seconds: a.seconds,
    answers: a.defer ? { ...a.answers } : { ...a.answers, P: placeAnswer(a.call, proposesCuts(change ? a.fixes : [], a.carried)) },
    extent_before: a.now.seen,
    ...(change && effect !== "keep" ? { extent_fix: a.fixes } : {}),
    ...(change && a.carried.length ? { new_splits: a.carried } : {}),
    ...(a.defer ? { verdict: "deferred" as const } : {}),
    ...(a.targets.length > 1 ? { targets: a.targets } : {}),
  };
}

/** (owner 2026-11-09) what a reach line says of tributaries: a binding that walks them says so even when the slow
 *  walk is not drawn — never reads as if they were excluded */
export function tribWords(walks: boolean | undefined, drawn: boolean): string {
  if (walks === true) return drawn ? " (with tributaries)" : " (with tributaries — not drawn; tick Show tributaries to draw)";
  if (walks === false) return " (no tributaries)";
  return "";
}
