/**
 * Dawn/dusk tracking hook — ported verbatim (same gating logic) from
 * `webapp/src/hooks/useDawnDusk.ts`. Pure TS, no DOM dependency, so it works
 * identically on-device.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import * as SunCalc from 'suncalc';

export interface DawnDuskTimes {
  // null at extreme latitudes where the event doesn't occur (polar day/night)
  // — not expected in practice for BC, but the underlying suncalc types
  // allow it, so callers must handle it.
  dawn: Date | null;
  dusk: Date | null;
}

// Sun times barely change over a short pan — only recompute once the map
// center has moved further than this, so we're not re-running SunCalc on
// every pixel of a drag gesture.
const RECOMPUTE_DISTANCE_KM = 5;
// Coarse safety-net poll so a long-open session picks up a date rollover
// (e.g. left open overnight) without requiring the user to pan the map.
const DATE_CHECK_INTERVAL_MS = 5 * 60 * 1000;

function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) * Math.cos((lat2 * Math.PI) / 180) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

/**
 * Tracks civil dawn/dusk for a caller-supplied position (the current map
 * view center), recomputed only when the position moves past a distance
 * threshold or the calendar date changes — not on every render/pan tick.
 *
 * The caller (App.tsx, via MapView's onRegionDidChange) is responsible for
 * calling `updatePosition`; this hook owns only the threshold/date-change
 * gating and the periodic date-rollover check.
 */
export function useDawnDusk(): {
  times: DawnDuskTimes | null;
  updatePosition: (lat: number, lng: number) => void;
} {
  const [times, setTimes] = useState<DawnDuskTimes | null>(null);
  const lastRef = useRef<{ lat: number; lng: number; dateStr: string } | null>(null);

  const recompute = useCallback((lat: number, lng: number) => {
    const sunTimes = SunCalc.getTimes(new Date(), lat, lng);
    lastRef.current = { lat, lng, dateStr: new Date().toDateString() };
    setTimes({ dawn: sunTimes.dawn, dusk: sunTimes.dusk });
  }, []);

  const updatePosition = useCallback(
    (lat: number, lng: number) => {
      const last = lastRef.current;
      const dateStr = new Date().toDateString();
      if (last) {
        const moved = haversineKm(last.lat, last.lng, lat, lng);
        if (moved < RECOMPUTE_DISTANCE_KM && last.dateStr === dateStr) {
          return;
        }
      }
      recompute(lat, lng);
    },
    [recompute],
  );

  useEffect(() => {
    const interval = setInterval(() => {
      const last = lastRef.current;
      if (last && last.dateStr !== new Date().toDateString()) {
        recompute(last.lat, last.lng);
      }
    }, DATE_CHECK_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [recompute]);

  return { times, updatePosition };
}
