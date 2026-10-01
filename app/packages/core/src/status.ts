/**
 * A WATER'S STATUS — the one answer every surface colours by (AGENTS 23).
 *
 * Three states, one vocabulary, whichever surface draws them — the map line, a search row, a
 * row of "water near a town":
 *
 *   closed   no fishing here (on the day asked)                    `color.status.closed`
 *   own      the synopsis has regulations specific to this water   `color.status.own`
 *   base     only the regional (base) regulations apply            `color.status.base`
 *
 * The TOKENS are the map's own (`packages/map/style/tokens.json`), and the hexes live only
 * there: `status.closed` is the red every closure on the map already wears, and `status.own` /
 * `status.base` are amber and blue, chosen to stay apart under protanopia, deuteranopia and
 * tritanopia in every theme (tools/water-status-cvd.test.ts). They are NOT the plain map's
 * `water.mapped`, so recolouring a status never repaints the plain map. A surface resolves the
 * token through the map's theme (`waterStatusColour` in @app/map), so a row and the line
 * beside it cannot disagree.
 *
 * CLOSED IS NOT CARRIED BY COLOUR ALONE: it also names a `weight` token, the factor a closed
 * line is drawn wider by — on the map (`paintFor`) and on the legend swatch alike.
 *
 * TWO SOURCES, ONE VOCABULARY. The STATUS INDEX (`statusIndex.ts`, built by
 * `python -m pipeline.deliver.status_index` through the reference reader) answers closed / own /
 * base for a section or a water on a day; `statusOn` and `waterStatusOn` below are the only
 * readers of it, so the map line and the search row cannot disagree. `tidal` and `outside`
 * (past the border) are not freshwater statuses: they read null — drawn as nothing, never as
 * `base` (AGENTS 32). With no index (not loaded, or refused for another atlas) every answer is
 * null, "not asked".
 *
 * The regulation RECORDS are still not integrated (`regulations.ts`): `waterStatus(regs)` decides
 * from a water's records once they arrive, and returns null until then.
 */
import { REGULATIONS, type WaterRegulations } from "./regulations";
import type { SectionKey } from "./section";
import type { StatusCode, StatusIndex } from "./statusIndex";

export type WaterStatus = "closed" | "own" | "base";

export const WATER_STATUSES: readonly WaterStatus[] = ["closed", "own", "base"];

/**
 * What each status is called, the map token that paints it, and — for a status that must not
 * rest on colour alone — the token that widens its line.
 */
export const WATER_STATUS: Readonly<Record<WaterStatus,
    { label: string; token: string; weight?: string }>> = {
  closed: { label: "Closed", token: "color.status.closed", weight: "width.status.closed" },
  own: { label: "Has its own regulations", token: "color.status.own" },
  base: { label: "Base regulations only", token: "color.status.base" },
};

/**
 * THE STATUS OF ONE WATER, from its regulations. Null when there is nothing to decide from —
 * regulations not integrated, or not read for this water. `closed` needs the day and the
 * rules' closures, which arrive with the integration; until then no water is called closed.
 */
export function waterStatus(regs: WaterRegulations | null | undefined): WaterStatus | null {
  if ((REGULATIONS as string) === "not-integrated" || !regs) return null;
  return regs.rules.length || regs.licensing.length ? "own" : "base";
}

/** The file's code as a status: `tidal` and `outside` are not one, so they read null. */
export function statusOfCode(code: StatusCode): WaterStatus | null {
  return code === "closed" || code === "own" || code === "base" ? code : null;
}

/**
 * A SECTION'S STATUS ON A DAY — what the map colours a line or a lake by. `section` is the
 * tile's feature id (the section handle). Null when there is no index (not asked).
 */
export function statusOn(index: StatusIndex | null | undefined, section: SectionKey,
                         on: Date): WaterStatus | null {
  return index ? statusOfCode(index.codeOn(section, on)) : null;
}

/**
 * A WATER'S STATUS ON A DAY — what a search row or a "water near a town" row wears. Its parts
 * rolled up by the pipeline: closed only when every part is, own when any part has a row of its
 * own, base otherwise. Null when there is no index (not asked).
 */
export function waterStatusOn(index: StatusIndex | null | undefined, item: string,
                              on: Date): WaterStatus | null {
  return index ? statusOfCode(index.waterCodeOn(item, on)) : null;
}
