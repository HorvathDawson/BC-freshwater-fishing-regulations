/**
 * THE MAP RAIL'S WORDS (owner 2026-10-21): the small stack on the side of the browse maps — Layers only since T48
 * (owner 2026-10-22: Spots is a bottom-bar tab again) — the Spots tab's words, and the sheet a long press on the map
 * raises. One copy, so every surface says the same.
 */

import { latLon } from "./grouped";

/** The rail's buttons: what each is called aloud (an icon alone is a guess, so the name is always said). */
export const MAP_RAIL = {
  /** The rail itself, as a group. */
  label: "Map tools",
  layers: "Layers",
  layersLabel: "Choose layers",
} as const;

/**
 * (T303) MY LOCATION, the rail's button under the zoom: the first tap asks (never before) and shows the blue dot with its
 * accuracy circle, centred; the next follows you; the next stops following. Foreground only; where you are never leaves
 * the device (the map draws it, nothing sends it).
 */
export const MY_LOCATION = {
  label: "My location",
  /** (T982, owner pick 2026-11-21) the one-line card on the map after the open's location ask was dismissed or refused: a tap asks again (or, refused for good, says how to turn it on) */
  offer: "Show my location",
  /** the button's label while the dot is shown and the map is, or is not, following it */
  labelOn: "Follow my location",
  labelFollowing: "Stop following my location",
  /** said when it was refused, or location is switched off: how to turn it on; the button stays usable to retry */
  denied: "Location is off for this app. Turn it on in your device settings, then tap again.",
  /**
   * (T1093; PROPOSAL, the owner picks the words) the phone's Location (or its accuracy mode) is off though the app holds the
   * permission: the app's own settings page would show Location already allowed, so no "Open Settings" action (not in
   * `needsSettings`), and no claim beyond what is known (Android without Google services refuses the same way with Location on).
   */
  deviceOff: "Your phone could not get a location. Check that Location is turned on in its settings, then tap again.",
  /** a fix did not arrive (no signal, timed out) */
  noFix: "Your location could not be found. Try again outside, or check location is on.",
  /**
   * (T864, owner 2026-11-18; PROPOSAL, the owner picks the words) the fix is outside B.C. and its ~50 km margin (`nearBC`):
   * not used — the map stays on B.C. — and said once per press (once per open for the open's own centring). Also the
   * Spots tab's "At my location" and a spot's "Parking: here", which start nothing from a far fix.
   */
  far: "You're outside British Columbia. This app only covers B.C. waters, so your location won't help here — the map " +
       "and regulations still work.",
} as const;

/** The Spots tab (T48; the rail's Spots menu before it): your saved spots, and the ways to add one. */
export const SPOTS_MENU = {
  title: "Spots",
  /** (T819 item 11) the header's square Add, always in sight: it opens the ways to add, at the top of the list */
  add: "Add", addLabel: "Add a spot", addClose: "Close the ways to add",
  /**
   * THE TWO WAYS TO ADD ONE (owner 2026-10-22: "where you are or select on map"; the map's-middle "Add a spot here" is
   * gone — you had to centre the map first). First where the phone is — offered only when the reader has let the app
   * know it, or will be asked on the tap itself.
   */
  addMine: "At my location",
  addMineAsk: "Asks for your location first",
  /** Your location cannot be offered: said, so the missing choice is not a mystery. Refused; none on this platform. */
  mineOff: "Location access is turned off for this app, so At my location is not offered.",
  mineNone: "Your location is not available here, so At my location is not offered.",
  /** Then: the Map tab in pick mode (`SPOT_PICK`); a tap places the pin. */
  addPick: "Pick on the map",
  addPickDetail: "Go to the map and tap the place",
  /** The location could not be read (refused, timed out, no fix): said, and nothing is added. */
  noLocation: "Your location could not be read, so no spot was started.",
  /** A row's action besides opening it: the map goes to the spot. */
  show: "Show",
  showLabel: (title: string) => `Show ${title} on the map`,
  /** No spots yet: what a spot is, since this is the only place that says so. */
  emptyTitle: "No spots yet",
  /** (T819 item 15) two plain lines, and where spots are kept (the web's notice says the same: not on the website yet) */
  emptyBody: "Save the places you fish: the water, the day, how the river and the weather were, and your notes and " +
             "photos.\nAdd one below, or press and hold the map.\nSaved on this phone and in its backup. Not on the " +
             "website yet.",
  refresh: "Refresh",
  refreshing: "Refreshing…",
  refreshLabel: "Refresh spots",
} as const;

/** The sheet a long press on the browse map raises over the provisional pin. Cancel leaves nothing behind. */
export const SPOT_DROP = {
  title: "New spot",
  /** Where the pin is, said as numbers: the one thing every spot keeps. */
  where: (lat: number, lon: number) => latLon({ lat, lon }),
  detail: "Next you pick the water it is on, then when you were there.",
  go: "Continue",
  cancel: "Cancel",
} as const;

/**
 * PICK ON THE MAP (owner 2026-10-22): the slim banner over the Map tab until a tap places the pin (which raises the drop
 * sheet). Cancel, back and Escape leave pick mode with nothing behind.
 */
export const SPOT_PICK = {
  banner: "Tap where your spot is",
  cancel: "Cancel",
} as const;

/** The spot-capture coach when the point is already chosen (a long press, a tap in pick mode, your location). */
export const SPOT_CAPTURE_AT = {
  title: "Pick the water",
  detail: "Tap the stream or lake your spot is on, near the pin.",
} as const;
