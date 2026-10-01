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
    case "rest":
      return `the rest of the water, after ${(ex.siblings ?? []).join(", ") || "?"}`;
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
    case "rest":
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
    const per = reaches?.rules?.[rid] ?? [];
    // what the rule SHIPS with (the builder's verdict) when the backend sent it; the raw
    // per-extent resolve otherwise — it has no answer for `rest` and no tributaries
    const shippedSecs = reaches?.verdict?.[rid]?.sections;
    const sections = [...new Set(shippedSecs ?? per.flatMap((x) => x?.sections ?? []))].sort();
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

/** One reach-builder diagnostic as a sentence a reviewer can check against the book: what the
 *  builder decided about tributaries, `rest`, tidal water, confluence cuts and region clips. */
export function describeDiagnostic(d: { kind: string; [k: string]: unknown }): string {
  const n = (k: string) => Number(d[k] ?? 0).toLocaleString();
  switch (d.kind) {
    case "tributaries":
      return d.only
        ? `tributaries only: ${n("total")} section(s), the water itself left out`
        : `tributary walk: ${n("direct")} on the water + ${n("added")} added = ${n("total")}`;
    case "tributaries_pending":
      return "tributaries asked for, but the walk could not run (no seed) — INCOMPLETE";
    case "complement":
      return `rest of the water: ${n("water")} section(s) minus ${n("removed")} bound by its siblings`
        + ` = ${n("kept")}` + ((d.withheld as unknown[] | undefined)?.length
          ? `; ${(d.withheld as unknown[]).length} straddling piece(s) withheld` : "");
    case "tidal":
      return `tidal: ${n("removed")} section(s) taken out (federal tidal regulations apply), ${n("kept")} kept`;
    case "confluence_cut": {
      // `with_cut`: no row of its own, so it goes with the cut (`joined` sections added);
      // `included`: the rule's own words take it in; `in_reach`: it lies inside the reach anyway.
      // Only when none holds is it kept out — `own_row` says why.
      const at = `confluence cut at ${String(d.split)}: joining water ${String(d.water)} `;
      if (d.with_cut) return at + `goes WITH the cut (no row of its own) — ${n("joined")} section(s) joined`;
      if (d.included) return at + "is taken in by the rule's own words";
      if (d.in_reach) return at + "lies inside the reach";
      return at + `is left OUT (${n("kept_out")} section(s))` + ((d.own_row as unknown[] | undefined)?.length
        ? ` — it has its own row: ${(d.own_row as string[]).join(", ")}` : "");
    }
    case "region_clip":
      return `held to its region: ${n("removed")} section(s) outside removed`
        + (Number(d.removed_from_walk ?? 0) ? `, ${n("removed_from_walk")} from the walk` : "");
    case "unclassified":
      return `${((d.pieces as unknown[]) ?? []).length} piece(s) straddle the reach's end — `
        + (d.included ? "INCLUDED (closure)" : "left out");
    case "ambiguous_cut":
      return `ambiguous cut ${String(d.split_id)}: used ${String(d.used)} m, also cuts at ${
        ((d.also_at as unknown[]) ?? []).join(", ")} m`;
    case "watershed":
      return `watershed part: ${n("sections")} section(s), placed by FWA code position`
        + (Number(d.unplaced ?? 0) ? `; ${n("unplaced")} unplaced` : "");
    case "within_area":
      return "area rule: every water inside the area";
    case "feature_types":
      return `limited to ${((d.kinds as string[]) ?? []).join(", ")}: ${n("before")} → ${n("after")}`;
    case "outside_bc":
      return `${n("removed")} section(s) outside BC removed`;
    case "partial_extents":
      return "some of its extents did not resolve — PARTIAL";
    default: {
      const rest = Object.entries(d).filter(([k]) => k !== "kind")
        .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`);
      return `${d.kind}${rest.length ? `: ${rest.join(", ")}` : ""}`;
    }
  }
}
