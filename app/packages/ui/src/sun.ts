/**
 * (T382, owner 2026-11-04: "we should also add sunrise/set time somewhere using bc time for the location") THE PAGE'S
 * SUN: sunrise and sunset on THE day, at the water, on the B.C. clock — core builds the line (`sunLine`), a component
 * draws it (web and native: one path).
 *
 * The point: the frame's tapped point when it has one (an unnamed reach's own point is its tap), else the water's own
 * point (`WaterSource.waterPoint`: on its line, never the map's centre). No point known — an unnamed reach opened with
 * no tap, a source with no geometry — draws nothing: there is no place to give the sun for. A failed point read is a
 * failed `useAsync`, reported there.
 */
import { useMemo } from "react";
import { daylightAt, sunLine, type DaylightAt, type SunDay } from "@app/core";
import type { ItemId, WaterSource } from "@app/data";
import { useAsync } from "./async";
import type { LatLon } from "./route";

/** The point the day's sun is given for (the frame's tap, else the water's own point), or null: none known. */
export function useSunPoint(source: Pick<WaterSource, "waterPoint">, item: ItemId | null, at: LatLon | null): LatLon | null {
  const asks = !at && item !== null && !!source.waterPoint;
  const own = useAsync(async () => (asks ? source.waterPoint!(item!) : null), `water-point:${asks ? item : ""}`, asks);
  return at ?? own.value ?? null;
}

/** The day's sun at the water (T878: "05:07–21:52 PT", aria "Sunrise 5:07, sunset 21:52 PT"), or null with no point (or no
 *  rise/set). */
export function useSunLine(source: Pick<WaterSource, "waterPoint">, item: ItemId | null, at: LatLon | null,
                           day: { y: number; m: number; d: number }): SunDay | null {
  const p = useSunPoint(source, item, at);
  return useMemo(() => (p ? sunLine(p.lat, p.lon, day) : null), [p?.lat, p?.lon, day.y, day.m, day.d]);   // eslint-disable-line react-hooks/exhaustive-deps -- the point and the day
}

/** (T473) The hours a daylight-only closure leaves open on the day at the water ("07:04–18:37 PT") and (T898) the hours it
 *  closes ("18:21", "7:23"), given the rule's own offsets (minutes closed after sunset, before sunrise), at the same point
 *  and on the same day as `useSunLine`; null: no point known (the rule is then said without hours). A new day, a new value. */
export function useDaylight(source: Pick<WaterSource, "waterPoint">, item: ItemId | null, at: LatLon | null,
                            day: { y: number; m: number; d: number }): DaylightAt | null {
  const p = useSunPoint(source, item, at);
  return useMemo(() => (p ? daylightAt(p.lat, p.lon, day) : null),
    [p?.lat, p?.lon, day.y, day.m, day.d]);   // eslint-disable-line react-hooks/exhaustive-deps -- the point and the day
}
