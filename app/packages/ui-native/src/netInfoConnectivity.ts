/**
 * THE PHONE'S WORD ON BEING ONLINE, read from `@react-native-community/netinfo` (owner 2026-10-21 late: "Add
 * network-status detection on phones (netinfo)"). This file names no module: `connectivity.native.ts` hands it the
 * loader, so the reading is tested here without NetInfo's native half (absent in jsdom).
 *
 * - Offline only on a DEFINITE no: `isConnected === false` (no network at all) or `isInternetReachable === false`
 *   (a network with no internet — the hotel wifi). An unknown (`null`, iOS's first report) counts as online and is
 *   left to the one-tile probe, which still runs when satellite is chosen and when the device comes back.
 * - Each CHANGE is said once: a switch from wifi to cellular is no news to `useImageryReach`.
 * - `probeWhileUp`: the phone's map binding reports no failed tile (Map.native.tsx), so the probe also rechecks,
 *   slower, while the imagery is up — the fallback for Esri being down while the phone is online.
 * - NetInfo's module not linked (its import throws: a development client built before it was added): no source, as
 *   before this dependency — the probe is then the phone's only signal. Never a crash for a switch.
 */
import type { Connectivity } from "@app/ui";

/** The part of NetInfo's state this reads. */
export interface NetInfoStateLike {
  isConnected: boolean | null;
  isInternetReachable?: boolean | null;
}

/** The part of NetInfo this calls: its listener, which reports the current state first and then every change. */
export interface NetInfoLike {
  addEventListener(on: (state: NetInfoStateLike) => void): () => void;
}

/** Online unless NetInfo says a definite no. */
export const onlineFrom = (s: NetInfoStateLike): boolean => s.isConnected !== false && s.isInternetReachable !== false;

/** No source: the phone never says, and the probe is its only signal. */
const NO_SOURCE: Connectivity = { watches: false, probeWhileUp: true, online: () => true, subscribe: () => () => {} };

export function netInfoConnectivity(load: () => NetInfoLike): Connectivity {
  let ni: NetInfoLike;
  try { ni = load(); } catch { return NO_SOURCE; }
  let last = true;
  return {
    watches: true,
    probeWhileUp: true,
    online: () => last,
    subscribe(on) {
      return ni.addEventListener((s) => {
        const now = onlineFrom(s);
        if (now === last) return;
        last = now;
        on(now);
      });
    },
  };
}
