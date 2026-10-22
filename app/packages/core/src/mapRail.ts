/**
 * THE MAP RAIL'S WORDS (owner 2026-10-21): the small stack on the side of the browse maps — Layers only since T48
 * (owner 2026-10-22: Spots is a bottom-bar tab again) — the Spots tab's words, and the sheet a long press on the map
 * raises. One copy, so every surface says the same.
 */

/** The rail's buttons: what each is called aloud (an icon alone is a guess, so the name is always said). */
export const MAP_RAIL = {
  /** The rail itself, as a group. */
  label: "Map tools",
  layers: "Layers",
  layersLabel: "Choose layers",
} as const;

/** The Spots tab (T48; the rail's Spots menu before it): your saved spots, and the ways to add one. */
export const SPOTS_MENU = {
  title: "Spots",
  /** The first action: a spot at the middle of the map as it is now. */
  addHere: "Add a spot here",
  addHereDetail: "At the middle of the map",
  /** The second: where the phone is — offered only when the reader has let the app know it, or will be asked on tap. */
  addMine: "At my location",
  addMineAsk: "Asks for your location first",
  /** The location could not be read (refused, timed out, no fix): said, and nothing is added. */
  noLocation: "Your location could not be read, so no spot was started.",
  /** A row's action besides opening it: the map goes to the spot. */
  show: "Show",
  showLabel: (title: string) => `Show ${title} on the map`,
  /** No spots yet: what a spot is, since this is the only place that says so. */
  emptyTitle: "No spots yet",
  emptyBody: "Pin an exact point on a stream and the app records the gauge, its standing against the record and " +
             "the weather that day. Notes and photographs go on top. Press and hold the map to drop one.",
  refresh: "Refresh",
  refreshing: "Refreshing…",
  refreshLabel: "Refresh spots",
} as const;

/** The sheet a long press on the browse map raises over the provisional pin. Cancel leaves nothing behind. */
export const SPOT_DROP = {
  title: "New spot",
  /** Where the pin is, said as numbers: the one thing every spot keeps. */
  where: (lat: number, lon: number) => `${lat.toFixed(4)}, ${lon.toFixed(4)}`,
  detail: "Next you pick the water it is on, then when you were there.",
  go: "Add a spot here",
  cancel: "Cancel",
} as const;

/** The spot-capture coach when the point is already chosen (a long press, the map's middle, your location). */
export const SPOT_CAPTURE_AT = {
  title: "Pick the water",
  detail: "Tap the stream or lake your spot is on, near the pin.",
} as const;
