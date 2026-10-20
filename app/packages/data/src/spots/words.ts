/**
 * The sentences a spot is shown with (P12b).
 *
 * They were composed inside `SpotScreen` and `SpotsScreen`, where a test could only match a fragment it had itself
 * supplied (AGENTS 33). Here each one is a whole string a test pins, and the screens draw it.
 */
import { localDate, localDateTime, percentileLabel } from "@app/core";
import type { Spot, WeatherWindow } from "./model";

/**
 * What changed across the three hours around the visit, in words; null when it cannot be said.
 *
 * Thresholds are the width of the models' own disagreement, so a change below one is noise rather than a trend: 10
 * points of cloud, 1 hPa of pressure. Below both, the honest sentence is that it was holding — which is itself
 * information. Each half is said only when both ends of the window know it.
 */
export function weatherTrend(win: WeatherWindow): string | null {
  const b = win.before, a = win.after;
  if (!b || !a) return null;
  const bits: string[] = [];
  if (b.cloudPct != null && a.cloudPct != null) {
    const d = a.cloudPct - b.cloudPct;
    bits.push(d >= 10 ? "clouding over" : d <= -10 ? "clearing" : "cloud holding");
  }
  if (b.pressureHpa != null && a.pressureHpa != null) {
    const d = a.pressureHpa - b.pressureHpa;
    bits.push(d >= 1 ? "pressure rising" : d <= -1 ? "pressure falling" : "pressure steady");
  }
  if (!bits.length) return null;
  return `${bits.join(" · ")} — across the hour either side`;
}

/** When a frozen value was observed, by whom, and whether it was filled in after the fact. */
export function observedLine(at: string | null, backfilled?: boolean, station?: string | null): string {
  return [
    station ?? null,
    at ? `observed ${at.replace("T", " ").slice(0, 16)}` : "no observation time recorded",
    backfilled ? "filled in afterwards" : null,
  ].filter(Boolean).join(" · ");
}

/** "2026-08-30 at 09:00 · 49.0974, -121.9675" — the visit, in the reader's own time, and the place. */
export function spotHeadline(spot: Pick<Spot, "visitedAt" | "createdAt" | "lat" | "lon">): string {
  return `${localDateTime(spot.visitedAt ?? spot.createdAt)} · ${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`;
}

/**
 * The two lines under a spot's name in the list: the LOCAL day of the visit with what was recorded, then where.
 *
 * The visit, not the record: spots are typed up on the drive home or the week after, and the list used to date
 * them by `createdAt` in UTC — a Sunday evening's fishing read as Monday's.
 */
export function spotRowLines(spot: Spot): [string, string] {
  const g = spot.reading, w = spot.weather;
  const gauge = g?.discharge != null
    ? `${g.discharge} m³/s${g.percentile != null ? ` · ${percentileLabel(g.percentile)}` : ""}`
    : "no gauge";
  const weather = w?.tempC != null ? `${w.tempC}°C${w.backfilled ? " (filled in)" : ""}` : "no weather";
  return [`${localDate(spot.visitedAt ?? spot.createdAt)} · ${gauge} · ${weather}`,
          `${spot.lat.toFixed(4)}, ${spot.lon.toFixed(4)}`];
}
