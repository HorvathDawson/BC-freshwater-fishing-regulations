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
 * COLOUR ALONE CARRIES IT. Closed used to name a `weight` token too and was drawn 1.6x wider;
 * that width was hard to use and was removed (2026-10-01), so every status draws at the
 * line's own width and the three hues are held apart for every reader by
 * tools/water-status-cvd.test.ts.
 *
 * TWO SOURCES, ONE VOCABULARY. The STATUS INDEX (`statusIndex.ts`, built by
 * `python -m pipeline.deliver.status_index` through the reference reader) answers closed / own /
 * base for a section or a water on a day; `statusOn` and `waterStatusOn` below are the only
 * readers of it, so the map line and the search row cannot disagree. `tidal` and `outside`
 * (past the border) are not freshwater statuses: they read null — drawn as nothing, never as
 * `base` (AGENTS 32). With no index (not loaded, or refused for another atlas) every answer is
 * null, "not asked".
 *
 * ONE DEFINITION. The index is the only thing that decides a status: a second reading of
 * own / base from a water's regulation records (`waterStatus(regs)`, dormant behind the
 * not-integrated flag) was removed on 2026-10-03 — two definitions of "own" is the AGENTS 23
 * failure waiting to happen.
 */
import { ACCESS_WORDS, noPublicAccess } from "./access";
import type { SectionKey } from "./section";
import type { StatusCode, StatusIndex } from "./statusIndex";

/**
 * THE MAP'S FOUR COLOURS. Three are a REGULATION answer (closed / own / base, the status index's); the fourth,
 * `no_access` (T54), is an ACCESS answer laid over them on the regulations map only: a section MOSTLY inside land the
 * public may not enter (`access.ts`, `noPublicAccess`). The index never says `no_access`, and neither does a water's
 * rollup (`waterStatusOn`, a search row): access is not regulation, and the section's page still answers its
 * regulations, under a banner.
 */
export type WaterStatus = "closed" | "own" | "base" | "no_access";

export const WATER_STATUSES: readonly WaterStatus[] = ["closed", "own", "base", "no_access"];

/** What each status is called, and the map token that paints it. */
export const WATER_STATUS: Readonly<Record<WaterStatus, { label: string; token: string }>> = {
  closed: { label: "Closed", token: "color.status.closed" },
  own: { label: "Has its own regulations", token: "color.status.own" },
  base: { label: "Base regulations only", token: "color.status.base" },
  no_access: { label: ACCESS_WORDS.legend, token: "color.status.no_access" },
};

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
 * THE MAP'S STATUS, SPLIT INTO WHAT NEVER CHANGES AND WHAT CHANGES WITH THE DAY (colour-latency study, AP).
 *
 * The index is base + deltas already: every listed section points at one of ~115 day-profiles, and only the profile's
 * code moves with the day. So the map stores each section's PROFILE once (`profileLabel`, as feature-state `p`) and
 * carries the day in its style (`dayProfiles`, one match over the labels). A day change is one paint property and no
 * per-section call; a newly loaded tile is coloured from a lookup with no React and no wait for `idle`.
 *
 * Both halves are HERE, beside `statusOfCode`, so the map's colour is still the one answer (AGENTS 23): a label is in
 * the `closed` set on a day exactly when `statusOn` says closed for every section wearing it. The renderer only applies
 * them. LABELS ARE STRINGS ("p17") because Android's `<Layer paint>` rejects numeric `match` labels, and a style
 * `match` takes one label type.
 */

/** The label of a section the index does not list — base every day, so it sits in every day's `base` set. */
export const UNLISTED_PROFILE = "b";
/**
 * The label of a section MOSTLY inside no-access land (T54, `noPublicAccess`): `no_access` every day, whatever its
 * regulations say — the fourth colour is drawn over every regulation answer, and the page carries the answer.
 */
export const NO_ACCESS_PROFILE = "n";

/**
 * A section's profile label: `NO_ACCESS_PROFILE` for one mostly inside no-access land (the index's `access`, decoded
 * from the same paint), else `p<n>` for the profile it reads, `UNLISTED_PROFILE` for one the index does not list.
 */
export function profileLabel(index: StatusIndex, section: SectionKey): string {
  if (noPublicAccess(index.access, section)) return NO_ACCESS_PROFILE;
  const p = index.profileOf(section);
  return p === null ? UNLISTED_PROFILE : `p${p}`;
}

/** Each status's profile labels on one day; `none` is a label whose code is not a status (tidal, outside B.C.). */
export type DayProfiles = Readonly<Record<WaterStatus | "none", readonly string[]>>;

/**
 * THE DAY, as label sets: which profile labels are closed, own, base and none on `on`. Every label the index can hand
 * out is in exactly one set, `UNLISTED_PROFILE` always in `base` (absent is base — the pipeline's claim, not a guess).
 * A label in no set (no state yet, or one from another release) is in none of them, and the map paints it "not asked".
 */
export function dayProfiles(index: StatusIndex, on: Date): DayProfiles {
  const out: Record<WaterStatus | "none", string[]> =
    { closed: [], own: [], base: [UNLISTED_PROFILE], no_access: [NO_ACCESS_PROFILE], none: [] };
  for (let p = 0; p < index.profiles; p++)
    out[statusOfCode(index.profileCodeOn(p, on)) ?? "none"].push(`p${p}`);
  return out;
}

/**
 * WHAT THE MAP DRAWS a section in on a day: `no_access` for one mostly inside no-access land, else its status. The
 * pushed-data map (`statusData`, a renderer with no per-tile event) reads this; the per-tile map reads the same answer
 * through `profileLabel` + `dayProfiles`, so the two cannot disagree. Null when there is no index (not asked).
 */
export function mapStatusOn(index: StatusIndex | null | undefined, section: SectionKey, on: Date): WaterStatus | null {
  if (!index) return null;
  return noPublicAccess(index.access, section) ? "no_access" : statusOn(index, section, on);
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
