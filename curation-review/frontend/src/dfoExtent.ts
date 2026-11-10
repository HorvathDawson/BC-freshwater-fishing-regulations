/* T525 (owner 2026-11-09: "extents already defined get lost when confirming vs redo … feels like i will make bad data"):
   WHAT A DFO MARK DOES TO THE PLACE'S CURATED EXTENT, as pure functions — the review screen (`DfoReview`) renders them,
   and `curation-review/frontend/tests/dfoExtent.test.ts` runs them (node --test).

   The owner's call is one of four. Keep, Not sure and Wrong never carry an extent fix: the curated extent stays as it
   is. Change starts FROM the curated extent (`DfoExtentNow.pieces`), and its effect — keep (nothing differs), replace
   or add a piece — is said before the save. Every mark names the extent it was made on (`extent_before`); the server
   refuses one made on an extent that has moved since. */
import type { CheckAnswer, DfoExtentNow, DfoSaved, ExtentFix, FixPoint, GroupSave, MarkPost, NewSplit } from "./types";
import { OP_WORDS } from "./extentWords.ts";   // a runtime import: node runs this file too (tests/)

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

/** a point that says where it is: a cut by id, a map point by its coordinates, a confluence / lake end by its water */
const pointSet = (p: FixPoint | undefined) => !!p && (p.kind === "split" || p.kind === "new" ? !!p.split_id
  : p.kind === "coord" ? !!p.coord : !!(p.name || p.item_id));

/** WHAT EACH PIECE OF A CHANGE STILL LACKS, one line per incomplete piece — its points (as many as its op takes,
 *  `opPoints`, the server's own `marks.OP_POINTS`) and, for `within`, its area. The save is held until there is none:
 *  a piece is never saved half made (the server refuses one too, `marks.ExtentFix`). */
export function incompletePieces(fixes: ExtentFix[], opPoints: Record<string, number>): string[] {
  return fixes.flatMap((f, i) => {
    const want = opPoints[f.op];
    const what = `piece ${i + 1} (${OP_WORDS[f.op] ?? f.op})`;
    if (want === undefined) return [`${what}: not an op the server knows`];
    const set = Array.from({ length: want }, (_x, j) => f.points[j]).filter(pointSet).length;
    const lacks = [...(set < want ? [`${want - set} of its ${want} point${want === 1 ? "" : "s"} not picked`] : []),
                   ...(f.op === "within" && !f.area ? ["its area not picked"] : [])];
    return lacks.length ? [`${what}: ${lacks.join(", ")}`] : [];
  });
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
                            extents: number; incomplete: string[] }): string {
  if (!a.answers.T || !a.answers.R) return "answer T and R first (y n ?)";
  if (!a.call) return "pick your call: keep it, change it, not sure, or wrong";
  if (a.call === "keep" && !a.extents) return "there is no extent to keep — change it (set one), or say not sure / wrong";
  if (a.call === "change" && a.cannotChange.length) return `this extent cannot be changed here: ${a.cannotChange.join("; ")}`;
  if (a.call === "change" && a.effect !== "keep" && a.incomplete.length) {
    return `finish your change first — ${a.incomplete.join("; ")}`;
  }
  if (a.call === "change" && a.effect === "keep" && !a.cuts) return "your change is the extent now — pick “Right, keep it”";
  const n = a.call === "wrong" || a.answers.T === "n" || a.answers.R === "n";
  return n && !(a.problem && a.note.trim()) ? "a wrong answer needs a problem (pick one) and a note saying what is wrong" : "";
}

/** THE INLINE EDITOR'S ONE ACTION (T532 r9, owner 2026-11-10: "this is no right, keep it button…"): a message must
 *  never name a control that is not on screen, so when the draft IS the extent now the action offered is the confirm
 *  itself. -> save (the draft changes the extent) · confirm (the draft equals an extent the place has: one enabled
 *  button that confirms it as it is) · none (no extent and nothing drawn yet) */
export const CONFIRMED_AS_IS = "confirmed as it is by the owner from the checklist's editor (the draft equalled the extent)";
export function inlineAction(a: { changes: boolean; extents: number }): { kind: "save" | "confirm" | "none"; label: string } {
  if (a.changes) return { kind: "save", label: "Save this extent" };
  if (a.extents > 0) return { kind: "confirm", label: "Confirm it as it is (saves)" };
  return { kind: "none", label: "Nothing drawn yet (nothing to save)" };
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
                              carried: NewSplit[]; now: DfoExtentNow; group?: GroupSave | null; defer?: boolean }): MarkPost {
  const change = a.call === "change" && !a.defer;
  const effect = change ? effectOf(a.now.pieces, a.fixes) : "keep";
  return {
    scope: "dfo", target: a.target, problem: a.problem || null, note: a.note, reviewer: a.reviewer, seconds: a.seconds,
    answers: a.defer ? { ...a.answers } : { ...a.answers, P: placeAnswer(a.call, proposesCuts(change ? a.fixes : [], a.carried)) },
    extent_before: a.now.seen,
    ...(change && effect !== "keep" ? { extent_fix: a.fixes } : {}),
    ...(change && a.carried.length ? { new_splits: a.carried } : {}),
    ...(a.defer ? { verdict: "deferred" as const } : {}),
    // owner 2026-11-09: only the older wordings ticked "same place", each listed with the extent seen (`groupSave`)
    ...(a.group && a.group.targets.length > 1 ? { targets: a.group.targets, members: a.group.members } : {}),
  };
}

/** (owner 2026-11-09) what a reach line says of tributaries: a binding that walks them says so even when the slow
 *  walk is not drawn — never reads as if they were excluded */
export function tribWords(walks: boolean | undefined, drawn: boolean): string {
  if (walks === true) return drawn ? " (with tributaries)" : " (with tributaries — not drawn; tick Show tributaries to draw)";
  if (walks === false) return " (no tributaries)";
  return "";
}

/* T525 ROUND 3 (owner 2026-11-09: "still really bad, so confusing … multiple proposal streams … a save doesn't visibly
   update"): ONE proposal per place and ONE status line after a save. */

/** THE ONE PROPOSAL a place shows: Claude's fix proposal (from the owner's note) when there is one, else the suggested
 *  extent — never both as separate streams. It is not offered when it is the extent now, or while a save of the owner's
 *  is waiting to land (`pending`: that save's pieces). */
export function oneProposal<S>(fix: S | null | undefined, sugg: S | null | undefined):
    { from: "fix" | "suggestion"; s: S } | null {
  return fix ? { from: "fix", s: fix } : sugg ? { from: "suggestion", s: sugg } : null;
}
export function offerProposal(now: ExtentFix[], proposal: ExtentFix[], pending: ExtentFix[] | null): boolean {
  // a save still waiting to land IS the owner's answer: no second stream beside it (its new cuts carry other refs than
  // the proposal's, so comparing pieces would miss that they are the same)
  if (pending && pending.length) return false;
  return proposal.length > 0 && effectOf(now, proposal) !== "keep";
}

export type StatusTone = "ok" | "wait" | "bad";
const VERDICT_WORDS: Record<string, string> = { verified: "", unsure: " as not sure", wrong: " as wrong" };

/** THE ONE STATUS LINE of a place after a save, from the write-through's outcome (`outcome`, this session) and what the
 *  last save did (`saved`, `dfo.saved_status`): saving → refused (the reason) → changed since → deferred → waits for a
 *  cut → not applied → saved with the extent now. Null before any save. */
export function statusLine(a: { saved: DfoSaved | null | undefined; outcome?: { state: string; why?: string | null } | null;
                                saving: boolean; stale: boolean; nowText: string; cutName: (id: string) => string }):
    { tone: StatusTone; text: string } | null {
  const o = a.outcome;
  if (a.saving || o?.state === "saving") return { tone: "wait", text: "Saving…" };
  if (o?.state === "refused") return { tone: "bad", text: `Refused: ${o.why || "the server gave no reason"}` };
  const s = a.saved;
  if (!s) return null;
  if (a.stale) return { tone: "wait", text: `Saved ${s.date}, but the place changed since. Check it again.` };
  if (s.state === "deferred") return { tone: "wait", text: `Deferred ${s.date}. Nothing changed.` };
  if (s.state === "waits_cut") {
    const cuts = (s.cuts ?? []).map(a.cutName);
    return { tone: "wait", text: `Saved ${s.date}. Takes effect after the next rebuild builds the new cut`
      + `${cuts.length === 1 ? "" : "s"} ${cuts.join(" and ")}.` };
  }
  if (s.state === "not_landed") {
    return { tone: "wait", text: `Saved ${s.date}, not applied yet. Its cuts are built now: it lands with the next DFO save.` };
  }
  return { tone: "ok", text: `Saved ${s.date}${VERDICT_WORDS[s.verdict] ?? ""}. The extent now: ${a.nowText || "none"}.` };
}

/** the new cuts a change names that are not built yet, by name — said before the save ("takes effect after …") */
export function newCutsOf(fixes: ExtentFix[]): string[] {
  return [...new Set(fixes.flatMap((f) => f.points.filter((p) => p.kind === "new" && p.split_id)
    .map((p) => p.split_id as string)))];
}
