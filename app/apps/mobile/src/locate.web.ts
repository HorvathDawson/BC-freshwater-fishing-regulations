/**
 * Where the device is, in a browser, for the Spots menu's "At my location" (owner 2026-10-21).
 *
 * NEVER ASKS SILENTLY. `permission()` reads the Permissions API, which raises no prompt; the menu offers the action when
 * it is granted, and when the browser would ask it says so on the button ("Asks for your location first") — the
 * prompt then comes from the reader's own tap, through `here()`. Refused, or no geolocation at all: not offered.
 */
import type { Locate, LocatePermission } from "@app/ui";

/** A fix good enough to pin a spot by, not so fussy that a phone in a canyon never answers. */
const OPTIONS: PositionOptions = { enableHighAccuracy: true, timeout: 15_000, maximumAge: 60_000 };

const geo = typeof navigator !== "undefined" ? navigator.geolocation : undefined;

export const locate: Locate | null = geo ? {
  permission: async (): Promise<LocatePermission> => {
    try {
      const s = await navigator.permissions.query({ name: "geolocation" });
      return s.state === "granted" ? "granted" : s.state === "prompt" ? "prompt" : "denied";
    } catch {
      // no Permissions API (older Safari): the browser asks on the tap, so say that it will
      return "prompt";
    }
  },
  here: () => new Promise((resolve, reject) => {
    geo.getCurrentPosition((p) => resolve({ lat: p.coords.latitude, lon: p.coords.longitude }), reject, OPTIONS);
  }),
} : null;
