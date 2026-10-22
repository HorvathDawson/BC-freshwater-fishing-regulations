/**
 * THE DEVICE'S WORD ON BEING ONLINE, in a browser: `navigator.onLine` and the window's online/offline events. Read by
 * `useImageryReach` (satellite is online-only, owner 2026-10-21). The web map reports a failed imagery tile itself, so
 * the probe need not recheck while satellite is up. See `connectivity.native.ts` for the phone (NetInfo).
 */
import type { Connectivity } from "@app/ui";

export const connectivity: Connectivity = {
  watches: true,
  probeWhileUp: false,
  online: () => (typeof navigator === "undefined" ? true : navigator.onLine !== false),
  subscribe(on) {
    if (typeof window === "undefined") return () => {};
    const up = () => on(true), down = () => on(false);
    window.addEventListener("online", up);
    window.addEventListener("offline", down);
    return () => { window.removeEventListener("online", up); window.removeEventListener("offline", down); };
  },
};
