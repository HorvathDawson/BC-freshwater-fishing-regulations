/**
 * Where the phone is, for the Spots menu's "At my location" (owner 2026-10-21; T74 adds the module): expo-location,
 * foreground only (deps.md).
 *
 * NEVER ASKS SILENTLY, as on the web (`locate.web.ts`). `permission()` reads the current grant, which raises no prompt:
 * granted → offered; not yet asked (or asked and the system will ask again) → offered with "Asks for your location
 * first", and the system prompt comes from the reader's own tap through `here()`; refused for good, or location
 * services off → not offered.
 */
import * as Location from "expo-location";
import type { Locate, LocatePermission } from "@app/ui";

export const locate: Locate | null = {
  permission: async (): Promise<LocatePermission> => {
    try {
      const p = await Location.getForegroundPermissionsAsync();
      if (p.granted) return "granted";
      return p.canAskAgain ? "prompt" : "denied";
    } catch {
      return "unavailable";
    }
  },
  here: async () => {
    const p = await Location.requestForegroundPermissionsAsync();
    if (!p.granted) throw new Error("location permission refused");
    // a recent fix is good enough to pin a spot by (as the web's maximumAge 60 s); otherwise a fresh one
    const last = await Location.getLastKnownPositionAsync({ maxAge: 60_000, requiredAccuracy: 100 });
    const fix = last ?? await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.High });
    return { lat: fix.coords.latitude, lon: fix.coords.longitude };
  },
};
