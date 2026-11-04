/**
 * (T303) MY LOCATION on the browse map: the state behind the rail's button.
 *
 * ASKS ONLY ON THE TAP. Nothing here reads a position until the reader presses the button (`locate.here()` is what may
 * raise the platform's prompt, as for "At my location"). Presses step through three states:
 *   off → on (the dot and its accuracy circle, the map centred on you)
 *   on → following (the map keeps you in view)
 *   following → on (stop following; the dot stays)
 * A refusal or no fix leaves it off with a line saying how to turn location on; the button stays usable to retry.
 * Foreground only, and nothing leaves the device: the fix goes to the map and nowhere else.
 *
 * (T384, owner 2026-11-04: "when we open the app default should be the persons current location") AND ON THE OPEN
 * (`useOpenAtMe`): an app opened with no start link goes to the reader by the SAME path a press takes (`open()`: the dot
 * on, the map centred — at `OPEN_ZOOM`, a town and its waters, not a press's street scale). Permission already given:
 * at once. Never asked: asked once on the first open (kept in `LocationAskStore`, written before the ask) — the map is
 * drawn at today's default meanwhile, never held for it. Refused, no fix, offline: the default stays, and the press's
 * line says why (a refused permission is the environment's: shown, never reported — core failure.ts).
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { MY_LOCATION, reportFailure, type LatLon } from "@app/core";
import type { Locate } from "./mapRail";
import type { Route } from "./route";

/** (T384) The zoom the open's centre goes to: a town and the waters around it (HOME is 10; a press goes to 15). */
export const OPEN_ZOOM = 12;

export type Fix = LatLon & { accuracy?: number };

export interface MyLocation {
  /** the dot is shown */
  on: boolean;
  following: boolean;
  /** the last fix: both maps draw the dot and its circle from it, and centre / follow on it */
  fix: Fix | null;
  /** counts up each time the map should centre on `fix` */
  centre: number;
  /** (T384) the zoom that centre goes to; null: a press's (street scale, or nearer if the map is already) */
  zoom: number | null;
  /** a line when location was refused or not found, else null */
  notice: string | null;
  /** waiting on the platform (the prompt, the first fix) */
  busy: boolean;
  press(): void;
  /** (T384) the open's centre: as a press from off, at `OPEN_ZOOM`; a refusal or no fix is said as a press's is */
  open(): void;
  /** the reader moved the map by hand: following stops (the dot stays) */
  release(): void;
  dismiss(): void;
}

export const refusedLine = (e: unknown): string =>
  e instanceof Error && /refused|denied|permission/i.test(e.message) ? MY_LOCATION.denied
  : typeof e === "object" && e !== null && (e as { code?: number }).code === 1 ? MY_LOCATION.denied
  : MY_LOCATION.noFix;

export function useMyLocation(locate: Locate | null): MyLocation | null {
  const [fix, setFix] = useState<Fix | null>(null);
  const [on, setOn] = useState(false);
  const [following, setFollowing] = useState(false);
  const [centre, setCentre] = useState(0);
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [zoom, setZoom] = useState<number | null>(null);
  const stop = useRef<(() => void) | null>(null);
  const alive = useRef(true);
  useEffect(() => () => { alive.current = false; stop.current?.(); }, []);

  const unwatch = () => { stop.current?.(); stop.current = null; };
  const watchNow = () => {
    unwatch();
    stop.current = locate?.watch?.((f) => { if (alive.current) setFix(f); },
                                   // a missed reading while following is not worth a line (the dot stays at the last one); a refusal is
                                   (e) => { if (alive.current && refusedLine(e) === MY_LOCATION.denied) setNotice(MY_LOCATION.denied); }) ?? null;
  };

  /** off → on: read the fix (the platform may ask), centre on it at `z` (null: a press's zoom), then keep the dot moving */
  const start = (z: number | null) => {
    if (!locate) return;
    setBusy(true);
    locate.here().then((f) => {
      if (!alive.current) return;
      setFix(f); setOn(true); setZoom(z); setCentre((n) => n + 1); setBusy(false);
      watchNow(); // (T308) the dot keeps moving with every fix while shown, following or not
    }, (e) => {
      if (!alive.current) return;
      setOn(false); setBusy(false); setNotice(refusedLine(e));
    });
  };

  const press = useCallback(() => {
    if (!locate || busy) return;
    setNotice(null);
    if (on && !following) { setFollowing(true); setZoom(null); setCentre((n) => n + 1); return; }
    if (on && following) { setFollowing(false); return; }
    start(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [locate, busy, on, following]);

  const open = useCallback(() => {
    if (!locate || busy || on) return;
    start(OPEN_ZOOM);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [locate, busy, on]);

  const release = useCallback(() => { setFollowing(false); }, []);
  const dismiss = useCallback(() => setNotice(null), []);
  return locate ? { on, following, fix, centre, zoom, notice, busy, press, open, release, dismiss } : null;
}

/** (T384) Where "asked on the first open" is kept (`@app/data` `openLocationAskStore`): "asked", or nothing yet. */
export interface LocationAskStore { read(): string | null; write(text: string): void }
const ASKED = "asked";

/**
 * (T384) Does this open go to the reader? Only a plain open: no start link took the app somewhere (a water, a place, an
 * open spot, a capture), and no camera held from before (Android recreating the activity puts the map back where it
 * was). `initial` is the route the app opens on (absent: the Map tab).
 */
export function opensAtMe(initial: Route | undefined, arrive: unknown, heldCamera: boolean): boolean {
  if (arrive || heldCamera) return false;
  if (!initial) return true;
  return initial.mode === "browse" && initial.spot === null && initial.adding === null && !initial.search;
}

/**
 * (T384) THE OPEN AT MY LOCATION: once, when `ready` (nothing else is up first — the first-run notice), if `opens`.
 * Granted: `me.open()`. Not yet asked and never asked on an open before: the store says so first, then `me.open()` (the
 * platform's prompt). Refused for good, no location here, or asked on an earlier open: nothing — today's default.
 * No store: never asks by itself (a test, a tool); a permission already given still centres.
 */
export function useOpenAtMe(me: MyLocation | null, locate: Locate | null, store: LocationAskStore | null,
                            opens: boolean, ready: boolean): void {
  const done = useRef(false);
  const meNow = useRef(me);
  meNow.current = me;
  useEffect(() => {
    if (done.current || !ready) return;
    done.current = true;
    if (!opens || !locate || !meNow.current) return;
    // not cancelled by a later render: asked once, answered once (the hook's own `alive` guards an unmounted map)
    locate.permission().then((p) => {
      if (p === "granted") { meNow.current?.open(); return; }
      if (p !== "prompt" || !store || store.read() === ASKED) return;
      store.write(ASKED);
      meNow.current?.open();
    }, reportFailure);
  }, [ready, opens, locate, store]);
}
