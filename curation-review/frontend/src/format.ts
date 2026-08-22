import type { Boundary, Extent } from "./types";

export function boundaryLabel(id: string, boundaries: Boundary[]): string {
  const b = boundaries.find((x) => x.id === id);
  return b ? b.label || b.id : id;
}

// Human-readable text for one extent, e.g. "upstream of Foo Falls".
export function humanExtent(ex: Extent, boundaries: Boundary[]): string {
  const lbls = ex.splits.map((s) => boundaryLabel(s, boundaries));
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
  const parts = [...ex.splits];
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
