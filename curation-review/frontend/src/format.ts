import type { Boundary, Extent, EntryReaches, ReachIdentity } from "./types";

export function boundaryLabel(id: string, boundaries: Boundary[]): string {
  const b = boundaries.find((x) => x.id === id);
  return b ? b.label || b.id : id;
}

// Human-readable text for one extent, e.g. "upstream of Foo Falls".
export function humanExtent(ex: Extent, boundaries: Boundary[]): string {
  const lbls = (ex.splits ?? []).map((s) => boundaryLabel(s, boundaries));
  switch (ex.op) {
    case "whole":
      return "whole reach";
    case "upstream_of":
      return `upstream of ${lbls[0] ?? "?"}`;
    case "downstream_of":
      return `downstream of ${lbls[0] ?? "?"}`;
    case "between":
      return `between ${lbls[0] ?? "?"} and ${lbls[1] ?? "?"}`;
    case "within":
      return `within ${ex.area_id ?? lbls.join(" & ") ?? "?"}`;
    default:
      return ex.op;
  }
}

// Raw form, e.g. "upstream_of[foo_falls]".
export function rawExtent(ex: Extent): string {
  const parts = [...(ex.splits ?? [])];
  if (ex.area_id) parts.push(`area=${ex.area_id}`);
  return `${ex.op}[${parts.join(", ")}]`;
}

// How many split ids an op requires (UI arity enforcement).
export function splitArity(op: Extent["op"]): number | null {
  switch (op) {
    case "whole":
      return 0;
    case "upstream_of":
    case "downstream_of":
      return 1;
    case "between":
      return 2;
    case "within":
      return null; // area-based; splits optional
    default:
      return null;
  }
}


// Distinct reaches get a short stable label so the rules list and the map call them the same thing.
// Several rules almost always share one reach — the Chilliwack's four gear/harvest rules below Vedder
// Crossing are one stretch of water — and seeing "B" on all of them is what makes that legible.
const REACH_COLORS = ["#2563eb", "#16a34a", "#b45309", "#7c3aed", "#0891b2", "#be123c", "#4d7c0f"];

export function reachIdentities(
  reaches: EntryReaches | null,
  ruleOrder: string[],
): Record<string, ReachIdentity> {
  const bySig = new Map<string, ReachIdentity>();
  const out: Record<string, ReachIdentity> = {};
  for (const rid of ruleOrder) {
    const per = reaches?.rules?.[rid];
    if (!per || per.length === 0) continue;
    const sections = [...new Set(per.flatMap((x) => x?.sections ?? []))].sort();
    if (sections.length === 0) continue;
    const sig = sections.join("|");
    let id = bySig.get(sig);
    if (!id) {
      const i = bySig.size;
      id = {
        key: String.fromCharCode(65 + (i % 26)) + (i >= 26 ? String(Math.floor(i / 26)) : ""),
        color: REACH_COLORS[i % REACH_COLORS.length],
        waters: [...new Set(per.flatMap((x) => x?.waters ?? []))],
        n: sections.length,
      };
      bySig.set(sig, id);
    }
    out[rid] = id;
  }
  return out;
}
