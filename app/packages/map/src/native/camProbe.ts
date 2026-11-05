/**
 * (T312/T400/T245) THE CAMERA PROBE — test-bed measurement only, on the pan probe's switch (`globalThis.__panProbe`, set
 * by apps/mobile from `EXPO_PUBLIC_PAN_PROBE=1`); otherwise every call here is a no-op and nothing listens.
 *
 *   `[cam] {t, lon, lat, zoom, user}`   each settle of a map's camera (`onRegionDidChange`): where it came to rest and
 *                                        whether the reader moved it — a drag's and a fling's travel are read off two
 *   `[move] {t, why, round, h}`         each camera move the APP makes (a fit, a refine round, the copy's zoom probe)
 *
 * And THE CAMERA LINK: `canifishthis://testbed/camera?lon=…&lat=…&zoom=…` (`adb shell am start -d …`) puts the
 * top-most mounted map there at once — adb has no pinch, so this is how a scenario pans at z8 or z12 (T245).
 */
import { Linking } from "react-native";
import { now } from "@app/core";

const on = () => !!(globalThis as { __panProbe?: boolean }).__panProbe;

export interface CameraTo { lon: number; lat: number; zoom: number }

/** The camera link's target, or null for any other URL. */
export function parseCameraLink(url: string): CameraTo | null {
  const m = /^canifishthis:\/\/testbed\/camera\?(.*)$/.exec(url);
  if (!m) return null;
  const q = new URLSearchParams(m[1]);
  const lon = Number(q.get("lon")), lat = Number(q.get("lat")), zoom = Number(q.get("zoom"));
  return q.has("lon") && q.has("lat") && q.has("zoom") && [lon, lat, zoom].every(Number.isFinite) ? { lon, lat, zoom } : null;
}

/** The maps mounted, last on top: only the top one answers the link (a hidden tab's map must not move). */
const stack: ((c: CameraTo) => void)[] = [];
let listening = false;

export const camProbe = {
  settled(c: { lon: number; lat: number; zoom: number; user: boolean }): void {
    if (!on()) return;
    console.log(`[cam] ${JSON.stringify({ t: now(), lon: c.lon, lat: c.lat, zoom: c.zoom, user: c.user })}`);
  },
  /** `h`: the moving map's height (dp), which says which map it is (the browse map, a water's map view, …) */
  move(why: "fit" | "refine" | "probe", round: number, h = 0): void {
    if (!on()) return;
    console.log(`[move] ${JSON.stringify({ t: now(), why, round, h: Math.round(h) })}`);
  },
  /** A map's camera, for the link; returns its unmount. No-op (and nothing subscribed) outside the test bed. */
  mount(go: (c: CameraTo) => void): () => void {
    if (!on()) return () => {};
    stack.push(go);
    if (!listening) {
      listening = true;
      Linking.addEventListener("url", ({ url }) => {
        const c = parseCameraLink(url);
        if (c) { console.log(`[camlink] ${JSON.stringify(c)}`); stack.at(-1)?.(c); }
      });
    }
    return () => { const i = stack.lastIndexOf(go); if (i >= 0) stack.splice(i, 1); };
  },
};
