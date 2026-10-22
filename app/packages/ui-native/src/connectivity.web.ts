/**
 * THE DEVICE'S WORD ON BEING ONLINE, in a browser: `navigator.onLine` and the window's online/offline events. Read by
 * `useImageryReach` (satellite is online-only, owner 2026-10-21). See `connectivity.native.ts` for the phone, which
 * has no such source.
 */
import type { Connectivity } from "@app/ui";

export const connectivity: Connectivity = {
  watches: true,
  online: () => (typeof navigator === "undefined" ? true : navigator.onLine !== false),
  subscribe(on) {
    if (typeof window === "undefined") return () => {};
    const up = () => on(true), down = () => on(false);
    window.addEventListener("online", up);
    window.addEventListener("offline", down);
    return () => { window.removeEventListener("online", up); window.removeEventListener("offline", down); };
  },
};
