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
import { AppState } from "react-native";
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
    return { lat: fix.coords.latitude, lon: fix.coords.longitude, accuracy: fix.coords.accuracy ?? undefined };
  },
  // (T303) following: only after the reader's tap has been allowed (`here()`), foreground only — the updates stop while
  // the app is in the background and start again when it comes back; the returned function stops them for good
  watch: (on, failed) => {
    let sub: Location.LocationSubscription | null = null;
    let stopped = false;
    /** counts each start and pause: a subscription that arrives after a later pause or start is removed at once */
    let gen = 0;
    const pause = () => { gen += 1; sub?.remove(); sub = null; };
    const start = () => {
      if (stopped) return;
      pause();
      const mine = gen;
      Location.watchPositionAsync({ accuracy: Location.Accuracy.High, timeInterval: 2_000, distanceInterval: 2 },
        (p) => { if (mine === gen) on({ lat: p.coords.latitude, lon: p.coords.longitude, accuracy: p.coords.accuracy ?? undefined }); },
        (reason) => { if (mine === gen) failed(new Error(reason)); })
        .then((s) => { if (mine === gen) sub = s; else s.remove(); }, (e) => { if (mine === gen) failed(e); });
    };
    const app = AppState.addEventListener("change", (st) => { if (st === "active") start(); else if (st === "background") pause(); });
    start();
    return () => { stopped = true; pause(); app.remove(); };
  },
};
