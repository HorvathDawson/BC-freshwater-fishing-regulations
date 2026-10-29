/**
 * T173 — A FISH OBSTACLE, TAPPED: what the sheet says about one mark of the `obstacle` tile layer.
 *
 * The tile carries the slim record `pipeline/deliver/tiles/obstacles.py` writes (`obstacle_id`, `type`, `name`, `water`,
 * `height_m`, `source`, `year`, and on a PSCIS crossing `barrier`, `habitat`, `road`, `date`); this turns it into the sheet's words, whole, so a test can pin every finished line
 * (AGENTS.md rule 33). Nothing here is a regulation: an obstacle is context about the water, never part of its answer.
 *
 * The values arrive as the renderer hands them back — numbers may be strings on a phone, anything may be missing — so
 * every field is read defensively and a missing one is left out, never guessed.
 */

/** Every type a mark can have — held equal to `pipeline/deliver/tiles/obstacles.py` OBSTACLE_TYPES (obstacles.test.ts). */
export const OBSTACLE_TYPES = ["falls", "cascade", "canyon", "velocity", "culvert", "dam"] as const;
export type ObstacleType = (typeof OBSTACLE_TYPES)[number];
export type ObstacleSource = "fiss" | "wris" | "pscis";
/** A PSCIS crossing's current barrier result (the tile ships barrier, potential and unknown; passable is said if met). */
export const OBSTACLE_BARRIERS = ["barrier", "potential", "passable", "unknown"] as const;
export type ObstacleBarrier = (typeof OBSTACLE_BARRIERS)[number];
export type ObstacleHabitat = "high" | "medium" | "low";

export const OBSTACLE_WORDS = {
  types: {
    falls: "Falls", cascade: "Cascade", canyon: "Canyon", velocity: "Velocity barrier", culvert: "Culvert", dam: "Dam",
  } satisfies Record<ObstacleType, string>,
  /** A record whose type this app does not know (a newer tile): said plainly, never dropped. */
  unknownType: "Obstacle to fish",
  /** The Layers sheet key's word for the dot (the icon a type this app does not know, or none, wears). */
  otherKey: "Other",
  rows: {
    type: "Type", height: "Height", surveyed: "Surveyed", commissioned: "Commissioned", source: "Source",
    barrier: "Fish passage", habitat: "Habitat upstream", road: "Road", assessed: "Assessed",
  },
  /** A PSCIS crossing's barrier result, as the assessment states it. */
  barriers: {
    barrier: "Barrier", potential: "Potential barrier", passable: "Passable", unknown: "Not yet rated",
  } satisfies Record<ObstacleBarrier, string>,
  habitats: { high: "High value", medium: "Medium value", low: "Low value" } satisfies Record<ObstacleHabitat, string>,
  sources: {
    fiss: "Provincial Obstacles to Fish Passage",
    wris: "BC Dams (provincial dam register)",
    pscis: "Provincial Stream Crossing Inventory",
  } satisfies Record<ObstacleSource, string>,
  /** The record's own id, as the source names it — enough to look it up. */
  record: (source: ObstacleSource, id: string) => `${source.toUpperCase()} ${id}`,
  on: (water: string) => `On ${water}`,
  /** Under every sheet: what the mark is and is not. */
  note: "From provincial surveys, some decades old. Whether fish pass depends on the fish and the flow; "
    + "the regulations for this water are on its own page.",
  licence: "Contains information licensed under the Open Government Licence – British Columbia.",
} as const;

export interface ObstacleRow { readonly label: string; readonly value: string }

export interface ObstacleSheetVM {
  /** The dam's name, else its type ("Falls"). */
  readonly title: string;
  /** "On Cranberry River" — the water a surveyed obstacle is on — or null. */
  readonly sub: string | null;
  readonly rows: readonly ObstacleRow[];
  readonly note: string;
  readonly licence: string;
}

const text = (v: unknown): string | null => (typeof v === "string" && v.trim() ? v.trim() : null);
const num = (v: unknown): number | null => {
  const n = typeof v === "number" ? v : typeof v === "string" && v.trim() ? Number(v) : NaN;
  return Number.isFinite(n) ? n : null;
};
const isType = (v: unknown): v is ObstacleType => typeof v === "string" && (OBSTACLE_TYPES as readonly string[]).includes(v);
const isSource = (v: unknown): v is ObstacleSource => v === "fiss" || v === "wris" || v === "pscis";
const isBarrier = (v: unknown): v is ObstacleBarrier =>
  typeof v === "string" && (OBSTACLE_BARRIERS as readonly string[]).includes(v);
const isHabitat = (v: unknown): v is ObstacleHabitat => v === "high" || v === "medium" || v === "low";
const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"] as const;

/** "Sep 10, 2008" from "2008-09-10"; anything else, null. */
export function dateWords(v: unknown): string | null {
  const m = typeof v === "string" ? /^(\d{4})-(\d{2})-(\d{2})/.exec(v.trim()) : null;
  const mon = m ? MON[Number(m[2]) - 1] : undefined;
  return m && mon ? `${mon} ${Number(m[3])}, ${m[1]}` : null;
}

/** "4 m", "1.5 m" — a height as the survey gave it, to a tenth. */
export function heightWords(m: number): string {
  return `${Number.isInteger(m) ? m : Math.round(m * 10) / 10} m`;
}

/** The sheet for one tapped mark: its tile properties, as the renderer returned them. */
export function obstacleSheet(p: Readonly<Record<string, unknown>>): ObstacleSheetVM {
  const W = OBSTACLE_WORDS;
  const typeWord = isType(p.type) ? W.types[p.type] : W.unknownType;
  const name = text(p.name);
  const water = text(p.water);
  const height = num(p.height_m);
  const year = num(p.year);
  const source = isSource(p.source) ? p.source : null;
  const id = text(p.obstacle_id);
  const rows: ObstacleRow[] = [];
  if (name) rows.push({ label: W.rows.type, value: typeWord });
  if (height !== null && height > 0) rows.push({ label: W.rows.height, value: heightWords(height) });
  if (source === "pscis") {
    if (isBarrier(p.barrier)) rows.push({ label: W.rows.barrier, value: W.barriers[p.barrier] });
    if (isHabitat(p.habitat)) rows.push({ label: W.rows.habitat, value: W.habitats[p.habitat] });
    const road = text(p.road);
    if (road) rows.push({ label: W.rows.road, value: road });
    const assessed = dateWords(p.date) ?? (year !== null ? String(year) : null);
    if (assessed) rows.push({ label: W.rows.assessed, value: assessed });
  } else if (year !== null) {
    rows.push({ label: source === "wris" ? W.rows.commissioned : W.rows.surveyed, value: String(year) });
  }
  if (source) {
    const own = id && id.includes(":") ? id.slice(id.indexOf(":") + 1) : null;
    rows.push({ label: W.rows.source, value: own ? `${W.sources[source]} · ${W.record(source, own)}` : W.sources[source] });
  }
  return { title: name ?? typeWord, sub: water ? W.on(water) : null, rows, note: W.note, licence: W.licence };
}
