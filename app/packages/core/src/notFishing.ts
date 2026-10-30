/**
 * T292 — NOT FISHING WATER, TAPPED: what the sheet says about the `not_fishing_water` tile layer (T152: the FWA's sewage
 * settling basins, mine tailing ponds and dumps — water no rule speaks of, drawn in a muted ground colour, never as a
 * lake). The tile carries one word, `kind` (pipeline `fishing_water.NOT_FISHING_KIND`); these are its words, whole, and a
 * test pins every finished line (AGENTS.md rule 33). Never a regulation, never a status: there is nothing to look up.
 */

/** The tile's `kind` values — the pipeline's `NOT_FISHING_KIND`, the same three. */
export const NOT_FISHING_KINDS = ["sewage", "tailings", "dump"] as const;
export type NotFishingKind = (typeof NOT_FISHING_KINDS)[number];

export const NOT_FISHING_WORDS = {
  /** the legend's and the Help page's word for the fill */
  legend: "Not a fishing water",
  /** the map's name for each kind, then what the Freshwater Atlas calls it */
  name: { sewage: "Sewage lagoon", tailings: "Mine tailings pond", dump: "Dump" } satisfies Record<NotFishingKind, string>,
  atlas: {
    sewage: "a sewage settling basin",
    tailings: "a mine tailings pond",
    dump: "a dump",
  } satisfies Record<NotFishingKind, string>,
  /** a tile without a `kind` (an older atlas): the three together */
  anyAtlas: "a sewage lagoon, mine tailings pond or dump",
  help: "Sewage lagoons, mine tailings ponds and dumps: man-made water nobody fishes. No regulations; tap one to see what it is.",
} as const;

export interface NotFishingSheetVM {
  readonly title: string;
  /** Whole paragraphs, in order. */
  readonly body: readonly string[];
}

/** The tile property as a kind, or null for anything else (absent on an older atlas, or a value this app does not know). */
export function notFishingKind(v: unknown): NotFishingKind | null {
  return typeof v === "string" && (NOT_FISHING_KINDS as readonly string[]).includes(v) ? (v as NotFishingKind) : null;
}

/** The sheet for a tap on not-fishing water: its kind named, and that it is no fishing water. */
export function notFishingSheet(kind: NotFishingKind | null): NotFishingSheetVM {
  const W = NOT_FISHING_WORDS;
  const what = kind ? W.atlas[kind] : W.anyAtlas;
  return {
    title: kind ? `${W.name[kind]} — not a fishing water` : W.legend,
    body: [`The Freshwater Atlas records this as ${what}: man-made, and not a place to fish. It has no fishing `
           + "regulations, and search does not list it."],
  };
}
