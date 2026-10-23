/**
 * T131 — YOUR SPOTS ON A MAP, the half both renderers share (`MapProps.spots`): what a tap on a mark does, and what
 * makes a map regroup. Which marks there are at a zoom, and their words, are `@app/core` `spotMarkers` / `spotPress`;
 * how a face looks is the app's (`@app/ui-native` `SpotPin`, passed in as `MapSpots.face`). Platform-free, so it is a
 * test (`spots.test.ts`).
 */
import { spotPress, type SpotMarker, type SpotPoint } from "@app/core";
import type { MapSpots } from "./map-props";

/** How long the camera takes into a group (the fit's own 450 ms). */
export const SPOT_EASE_MS = 450;

/** A mark tapped: a spot opens; a group eases in to where it parts; a group no zoom parts goes to the list. */
export function pressSpot(m: SpotMarker, spots: Pick<MapSpots, "onOpen" | "onList">,
                          ease: (c: { lat: number; lon: number; zoom: number }, o: { duration: number }) => void): void {
  const p = spotPress(m);
  if (p.kind === "open") spots.onOpen(p.id);
  else if (p.kind === "zoom") ease(p.camera, { duration: SPOT_EASE_MS });
  else spots.onList(p.ids);
}

/** What the marks are made of: every spot's id, place and name, and the selected one. "" for none. */
export function spotsKey(points: readonly SpotPoint[] | undefined, selected: string | null): string {
  return points ? JSON.stringify([points.map((p) => [p.id, p.lat, p.lon, p.title]), selected]) : "";
}
