/**
 * THE DEVICE'S WORD ON BEING ONLINE, in a browser: `navigator.onLine` and the window's online/offline events. Read by
 * `useImageryReach` (satellite is online-only, owner 2026-10-21). The web map reports a failed imagery tile itself, so
 * the probe need not recheck while satellite is up. See `connectivity.native.ts` for the phone (NetInfo). (T254) Also the
 * browser's word on the link's speed (`navigator.connection`), for low-bandwidth mode.
 */
import { connectionHints, type Connectivity } from "@app/ui";

type Nav = Parameters<typeof connectionHints>[0] & { connection?: { addEventListener?: (k: "change", f: () => void) => void;
                                                                    removeEventListener?: (k: "change", f: () => void) => void } };
const nav = (): Nav | undefined => (typeof navigator === "undefined" ? undefined : (navigator as unknown as Nav));

export const connectivity: Connectivity = {
  /** (T254) the browser's word on the link: Save-Data, effectiveType, downlink, rtt */
  hints: () => connectionHints(nav()),
  watchHints(on) {
    const c = nav()?.connection;
    c?.addEventListener?.("change", on);
    return () => c?.removeEventListener?.("change", on);
  },
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
