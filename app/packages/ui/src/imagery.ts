/**
 * CAN THE SATELLITE IMAGERY BE REACHED — the hook half of "satellite is online-only" (owner 2026-10-21). The rule and
 * the words are in @app/core (`imagery.ts`); `mapModes` turns this answer and the reader's choice into the ground drawn.
 *
 * Three signals, any one down is down:
 *   - the device's own word (`Connectivity`): the web's `navigator.onLine` and its online/offline events; a phone's
 *     NetInfo (owner 2026-10-21 late). Where NetInfo's module is not linked the phone has no source: `watches` is
 *     false and it never says;
 *   - an imagery tile failing on the web map (`tileFailed`, from `MapProps.onImageryError`): a 404 is a hole Esri has
 *     no imagery for, not an outage (`isImageryOutage`);
 *   - a PROBE of one z0 tile, run when satellite is chosen and then every `recheckMs` while it is down — which is how
 *     it comes back by itself where nothing would say so. On a phone, whose map reports no failed tile
 *     (`probeWhileUp`), the probe also rechecks (4× slower) while it is up: the fallback for what NetInfo cannot see.
 * Nothing is asked while satellite is not chosen, and nothing while the device says it is offline: its online event
 * brings satellite back.
 */
import { useCallback, useEffect, useReducer, useRef } from "react";
import { IMAGERY_START, imageryReach, imageryUp, isImageryOutage } from "@app/core";

export interface Connectivity {
  /** Can this platform say whether the device is online at all. */
  watches: boolean;
  /** This platform's map reports no failed imagery tile (the phone's binding), so the probe also rechecks while up. */
  probeWhileUp: boolean;
  /** The device's word now; `true` where it cannot say. */
  online(): boolean;
  /** Every change of the device's word; returns the unsubscribe. Never called where it cannot say. */
  subscribe(on: (online: boolean) => void): () => void;
  /**
   * (T231) A METERED connection now (cellular, or one the system calls expensive): what holds a large release download
   * for Wi-Fi. Absent: the platform cannot say (the web), read as not metered.
   */
  metered?(): boolean;
  /** (T231) Every change of `metered`; returns the unsubscribe. */
  watchMetered?(on: (metered: boolean) => void): () => void;
}

export interface ImageryReachInput {
  /** Satellite is the reader's choice — the only time anything is asked. */
  wanted: boolean;
  connectivity: Connectivity;
  /** One imagery tile: does it answer? A throw is a no. */
  probe: () => Promise<boolean>;
  /** How often a down imagery is asked again. */
  recheckMs?: number;
}

export interface ImageryReachVM {
  up: boolean;
  /** An imagery tile failed with this HTTP status (undefined: no response at all). */
  tileFailed: (status: number | undefined) => void;
}

export function useImageryReach({ wanted, connectivity, probe, recheckMs = 30_000 }: ImageryReachInput): ImageryReachVM {
  const [s, dispatch] = useReducer(imageryReach, IMAGERY_START,
    (start) => ({ ...start, online: connectivity.online() }));
  useEffect(() => connectivity.subscribe((on) => dispatch({ type: on ? "online" : "offline" })), [connectivity]);

  const probeNow = useRef(probe);
  probeNow.current = probe;
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  const check = useCallback(() => {
    void probeNow.current().catch(() => false).then((ok) => {
      if (alive.current) dispatch({ type: ok ? "reached" : "failed" });
    });
  }, []);

  const asks = wanted && (!connectivity.watches || s.online);
  // the first word on choosing satellite (and on the device coming back): one probe
  useEffect(() => { if (asks) check(); }, [asks, check]);
  // and again while it is down; a phone, whose map never says a tile failed, also while it is up
  useEffect(() => {
    if (!asks || (!s.failed && !connectivity.probeWhileUp)) return;
    const id = setInterval(check, s.failed ? recheckMs : recheckMs * 4);
    return () => clearInterval(id);
  }, [asks, s.failed, connectivity.probeWhileUp, recheckMs, check]);

  const tileFailed = useCallback((status: number | undefined) => {
    if (isImageryOutage(status)) dispatch({ type: "failed" });
  }, []);
  return { up: imageryUp(s), tileFailed };
}
