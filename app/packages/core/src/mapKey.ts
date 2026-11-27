/**
 * T300 — THE MAP KEY IN FEW WORDS. The legend overlays the map (T275), so every word in it is map hidden. These are
 * the short forms of the long labels the pages and sheets keep (`WATER_STATUS`, `METHOD_WORDS.legend`, `SALMON_WORDS.legend`,
 * `waterLegend`): same meanings, fewer and plainer words, one table for the strip, the Runs key and the obstacle row.
 * The long labels are not touched; they still head the rows of the sheets and the search.
 */
import { dayLabel } from "./day";
import type { GearMark } from "./method";
import type { SalmonMapStatus } from "./salmon";
import type { WaterStatus } from "./status";
import type { StockRecency } from "./lakeFacts";
import type { WaterColour } from "./layers";
import type { ObstacleIconKind } from "./obstacleIcons";
import type { RunState } from "./runTiming";

export const MAP_KEY = {
  // (T1211, owner 2026-11-26: "Its own rules / Region rules only") the same words as the status's own label (`WATER_STATUS`)
  status: { own: "Its own rules", base: "Region rules only", no_access: "No access" } satisfies Record<Exclude<WaterStatus, "closed">, string>,
  /** (T1211) the key floating over the map: its chip's word, what it says aloud, and what a press does */
  chip: { word: "Map key", label: "Map key", hintOpen: "Shows what the colours and lines mean", hintClose: "Hides the map key",
          /** (T1211) a class shown alone (T377) is said ON the chip, open or closed, so a grey map is never unexplained.
           *  A PREFIX, never a suffix: "Region rules only only", "Closed Oct 10 only" (closed ONLY on Oct 10) misstated the rules */
          showing: (label: string) => `Showing: ${label}`,
          /** what the chip says aloud while one class is shown alone */
          showingAloud: (label: string) => `Map key. Showing ${label} alone on the map`,
          showAll: "Show all", showAllHint: "Shows every class on the map again" },
  /** (T1208) the region boundary's entry: the dash-dot grey line with its band, the line between two regulation regions */
  region: "Region boundary",
  gear: { fly: "Fly only", bait: "Bait ban" } satisfies Record<GearMark, string>,
  notFishing: "Not fishable",
  /** (T759) the issue stripe's entry (Salmon key only): the banner on the water's page says what the rule is */
  issue: "Check rule",
  /** (T882, the owner's pick 2026-11-18 ~22:50: "Check DFO") the check colour is DFO's "To be determined" OR a DFO row the
   *  app could not read, and "Check DFO" is true of both; the long label (`SALMON_WORDS.legend.check`) names the two */
  salmon: { open: "Keep salmon", release: "Release only", closed: "Closed", check: "Check DFO" } satisfies Record<SalmonMapStatus, string>,
  stocked: { season: "This season", year: "This year", recent: "1–3 years ago", decade: "3–10 years ago", old: "Over 10 years ago" } satisfies Record<StockRecency, string>,
  stockedNone: "Not stocked",
  depth: { digitised: "Contours traced", sheet: "Sheet only", none: "Not surveyed" },
  /** the fish obstacles' marks, by icon kind */
  obstacle: { falls: "Falls", cascade: "Cascade", canyon: "Canyon", velocity: "Fast water", culvert: "Culvert", dam: "Dam", other: "Other" } satisfies Record<ObstacleIconKind, string>,
  /** the Runs map's vertical key: the colour a stream wears, top (most to say) to bottom, and what it says */
  runs: {
    title: "Fish runs",
    rows: [
      { state: "spawn", label: "In the water now" },
      { state: "pass", label: "Passing through" },
      { state: "off", label: "Run, not now" },
      { state: "none", label: "None recorded" },
    ] as const satisfies readonly { state: RunState | "none"; label: string }[],
    describe: "Key to the colours: in the water now, passing through, a run here but not now, none recorded",
  },
} as const;

/** The Closed row: "Closed" for today, "Closed Oct 10" for another day. */
export function closedKey(day: Date | null): string {
  return day ? `Closed ${dayLabel(day)}` : "Closed";
}

/** The water-colouring key's rows in few words (the lake measures; the regulations key is the status rows). */
export function waterKeyWords(drawn: WaterColour): readonly string[] {
  if (drawn === "stocked") return [...(Object.keys(MAP_KEY.stocked) as StockRecency[]).map((k) => MAP_KEY.stocked[k]), MAP_KEY.stockedNone];
  if (drawn === "depth") return [MAP_KEY.depth.digitised, MAP_KEY.depth.sheet, MAP_KEY.depth.none];
  return [];
}

/**
 * (T377) A TAPPABLE KEY: a tap on a regulations class shows that class alone on the map, a second tap (or a tap on the one
 * shown) shows them all again. What a reader of the screen hears for an entry, whole.
 */
export const KEY_ISOLATE_WORDS = {
  /** the entry's name, then what a tap does from here (`alone`: this class is the one shown) */
  entry: (label: string, alone: boolean) =>
    alone ? `${label}, shown alone on the map. Tap to show every class.` : `${label}. Tap to show only this on the map.`,
} as const;

/**
 * (T377) THE DAY WITH ONE CLASS ALONE: every other class's labels move to `none`, which the map paints its neutral
 * "not asked" grey (the profile modes' `missing`), and which the gear marks and the no-access dots never ride. `only`
 * null (or a class the day does not hold) returns the day unchanged. For the status day and the salmon day alike.
 */
export function isolateDay<K extends string>(day: Readonly<Record<K | "none", readonly string[]>>, only: NoInfer<K> | null):
    Readonly<Record<K | "none", readonly string[]>> {
  if (only === null || !(only in day) || only === "none") return day;
  const out = {} as Record<K | "none", readonly string[]>;
  const none: string[] = [...day.none];
  for (const k of Object.keys(day) as (K | "none")[]) {
    if (k === "none") continue;
    if (k === only) out[k] = day[k];
    else { out[k] = []; none.push(...day[k]); }
  }
  out.none = none;
  return out;
}
