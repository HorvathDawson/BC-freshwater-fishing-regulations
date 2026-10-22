/**
 * Where the phone is, for the Spots menu's "At my location" — NOT YET on native: no location module is in the app
 * (expo-location would be one: a deps.md decision, with its Android permission, for the owner). Null, so the menu
 * does not offer it and nothing ever asks; a long press or "Add a spot here" (the map's middle) still drops a spot.
 */
import type { Locate } from "@app/ui";

export const locate: Locate | null = null;
