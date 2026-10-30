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
 */
import { useCallback, useEffect, useRef, useState } from "react";
import { MY_LOCATION, type LatLon } from "@app/core";
import type { Locate } from "./mapRail";

export type Fix = LatLon & { accuracy?: number };

export interface MyLocation {
  /** the dot is shown */
  on: boolean;
  following: boolean;
  /** the last fix: both maps draw the dot and its circle from it, and centre / follow on it */
  fix: Fix | null;
  /** counts up each time the map should centre on `fix` */
  centre: number;
  /** a line when location was refused or not found, else null */
  notice: string | null;
  /** waiting on the platform (the prompt, the first fix) */
  busy: boolean;
  press(): void;
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

  const press = useCallback(() => {
    if (!locate || busy) return;
    setNotice(null);
    if (on && !following) { setFollowing(true); setCentre((n) => n + 1); watchNow(); return; }
    if (on && following) { setFollowing(false); unwatch(); return; }
    setBusy(true);
    locate.here().then((f) => {
      if (!alive.current) return;
      setFix(f); setOn(true); setCentre((n) => n + 1); setBusy(false);
    }, (e) => {
      if (!alive.current) return;
      setOn(false); setBusy(false); setNotice(refusedLine(e));
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [locate, busy, on, following]);

  const release = useCallback(() => { setFollowing(false); unwatch(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, []);
  const dismiss = useCallback(() => setNotice(null), []);
  return locate ? { on, following, fix, centre, notice, busy, press, release, dismiss } : null;
}
