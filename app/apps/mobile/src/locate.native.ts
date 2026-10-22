/**
 * Where the phone is, for the Spots menu's "At my location" — NOT YET on native: no location module is in the app
 * (expo-location would be one: a deps.md decision, with its Android permission, for the owner). Null, so the menu
 * does not offer it (the Spots tab says it is not available here) and nothing ever asks; a long press or "Pick on the
 * map" still drops a spot.
 */
import type { Locate } from "@app/ui";

export const locate: Locate | null = null;
