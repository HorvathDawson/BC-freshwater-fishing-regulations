/**
 * A WATER'S STATUS — the one answer every surface colours by (AGENTS 23).
 *
 * Three states, one vocabulary, whichever surface draws them — the map line, a search row, a
 * row of "water near a town":
 *
 *   closed   no fishing here (on the day asked)                    `color.status.closed`
 *   own      the synopsis has regulations specific to this water   `color.water.mapped`
 *   base     only the regional (base) regulations apply            `color.water.unmapped`
 *
 * The TOKENS are the map's own (`packages/map/style/tokens.json`): `status.closed` is the
 * crimson every closure on the map already wears, and `water.mapped` / `water.unmapped` are the
 * coverage pair whose names say whether WE hold a water-specific record — never that a water
 * is unregulated (tokens.json `$naming`). A surface resolves the token through the map's theme
 * (`waterStatusColour` in @app/map), so a row and the line beside it cannot disagree.
 *
 * NOT INTEGRATED YET. The app reads no regulations (`regulations.ts`), so `waterStatus` has
 * nothing to decide from and returns null — "not asked", rendered as NOTHING (AGENTS 32), never
 * as `base`: "only the base regulations" is a claim about the data, and we have not read it.
 * When the integration lands, `regulationsFor` returns the water's records and this function
 * is where closed / own / base are decided, once, for the map and every list alike.
 */
import { REGULATIONS, type WaterRegulations } from "./regulations";

export type WaterStatus = "closed" | "own" | "base";

export const WATER_STATUSES: readonly WaterStatus[] = ["closed", "own", "base"];

/** What each status is called, and the map token that paints it. */
export const WATER_STATUS: Readonly<Record<WaterStatus, { label: string; token: string }>> = {
  closed: { label: "Closed", token: "color.status.closed" },
  own: { label: "Has its own regulations", token: "color.water.mapped" },
  base: { label: "Base regulations only", token: "color.water.unmapped" },
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
