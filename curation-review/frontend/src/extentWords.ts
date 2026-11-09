/* AN EXTENT IN WORDS (owner 2026-11-09: the owner reviews by words) — a curated extent, an extent fix or a place's
   pieces as PARTS: plain words, and each cut as its label from the curated cuts file. A cut with no label is a part of
   its own (`{ cut }`), which the screen shows as an error, never as a bare id. Pure: `tests/dfoExtent.test.ts`. */
import type { DfoExtent, ExtentFix, FixPoint } from "./types";

export type Part = string | { cut: string };
/** a cut's label, or null when there is none */
export type CutLabel = (id: string) => string | null;

export const OP_WORDS: Record<string, string> = {
  whole: "the whole water", upstream_of: "upstream of", downstream_of: "downstream of",
  between: "between", within: "within an area", rest: "the rest", steelhead_waters: "steelhead waters",
};

/** one point of an extent fix in words — a cut by its name, a NEW one by its ref, a map point by its place */
export function pointWords(p: FixPoint, splitName: (id: string) => string): string {
  if (p.kind === "split") return p.split_id ? splitName(p.split_id) : "—";
  if (p.kind === "new") return p.split_id ? `NEW split ${p.split_id}` : "—";
  if (p.kind === "coord") return p.coord ? `${p.coord[1].toFixed(5)}, ${p.coord[0].toFixed(5)}` : "click the map";
  const what = p.kind === "confluence" ? "confluence of" : p.kind === "lake_inlet" ? "inlet of" : "outlet of";
  return `${what} ${p.name || p.item_id || "—"}`;
}

const cutPart = (id: string, label: CutLabel): Part => label(id) ?? { cut: id };
const joined = (xs: Part[][], sep: string): Part[] => xs.flatMap((x, i) => (i ? [sep, ...x] : x));

/** a curated extent: the op, its cuts by label, its area, the waters it holds on */
export function extentParts(e: DfoExtent, label: CutLabel, itemName: (id: string) => string): Part[] {
  const items = e.item_ids ?? (e.item_id ? [e.item_id] : []);
  const cuts = joined((e.splits ?? []).map((s) => [cutPart(s, label)]), " and ");
  return [OP_WORDS[e.op] ?? e.op, ...(cuts.length ? [" ", ...cuts] : []), ...(e.area_id ? [` ${e.area_id}`] : []),
          ...(items.length ? [` · on ${items.map(itemName).join(", ")}`] : [])];
}
/** an extent fix: the op, its points (a curated cut by label, a NEW one by its name), the waters it holds on, its
 *  tributary call */
export function fixParts(f: ExtentFix, label: CutLabel, itemName: (id: string) => string): Part[] {
  const pts = joined(f.points.map((p) => (p.kind === "split" && p.split_id ? [cutPart(p.split_id, label)]
    : p.kind === "new" && p.split_id ? ["NEW cut ", cutPart(p.split_id, label)] : [pointWords(p, (x) => x)])), " and ");
  return [OP_WORDS[f.op] ?? f.op, ...(pts.length ? [" ", ...pts] : []), ...(f.area ? [` ${f.area}`] : []),
          ` · ${f.items.length ? `on ${f.items.map(itemName).join(", ")}` : "on the place's waters"}`,
          ` · ${f.tributaries == null ? "tributaries as the place has them" : f.tributaries ? "with tributaries"
            : "without tributaries"}`];
}
/** several pieces, one after another ("piece 1: … + piece 2: …"); one piece reads as itself */
export function piecesParts<T>(xs: T[], one: (x: T) => Part[]): Part[] {
  return joined(xs.map((x, i) => [...(xs.length > 1 ? [`piece ${i + 1}: `] : []), ...one(x)]), " + ");
}
/** parts as one line of text (a notice): a cut with no label is SAID, never left as a bare id */
export function partsText(parts: Part[]): string {
  return parts.map((p) => (typeof p === "string" ? p : `[cut ${p.cut} has no label]`)).join("");
}
