/**
 * Assembling a spot from what the app can find out, right now.
 *
 * Platform-free and side-effect-free: it is handed the sources, so the same function runs
 * in a test with fakes and on a phone with a network.
 *
 * NOTHING HERE INVENTS A VALUE. Each source may fail or be absent, and each failure lands
 * as a null on the record rather than a zero or a guess — the screen then says "not known",
 * which is the truth, and a later backfill can fill it in and mark itself as backfilled.
 */
import type { GaugeTrace, PlainDate, SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "../index";
import type { Spot, SpotReading, SpotWeather } from "./model";

/**
 * Weather, from wherever weather comes from.
 *
 * A PORT, not an implementation. There is no weather service wired into this app yet, so
 * the default returns null and the spot records that it does not know. Naming the shape now
 * means the screen, the record and the backfill are all written against the real thing.
 */
export interface WeatherSource {
  at(lat: number, lon: number, when: Date): Promise<SpotWeather | null>;
}

/** The honest default: no service, so no weather, said out loud. */
export const noWeather: WeatherSource = { at: async () => null };

export interface CaptureInput {
  source: RegsSource;
  weather?: WeatherSource;
  at: { lat: number; lon: number };
  item: ItemId | null;
  section: SectionId | null;
  waterName: string | null;
  group: SpeciesGroup;
  title: string;
  /**
   * When the person was at the water. Defaults to now.
   *
   * THE ONLY DATE THIS FUNCTION TAKES, and it replaced a separate `on: PlainDate`. Two date
   * inputs is two dates that can disagree — and they did: the test that caught this passed
   * `on: 2026-08-30` alongside a `now` in August 2025, and nothing complained, because each
   * value was used for something different. Weather, gauge reading and regulation are all
   * claims about one instant, so they take one instant.
   */
  visitedAt?: number;
  now?: number;
  id?: () => string;
}

export async function captureSpot(input: CaptureInput): Promise<Spot> {
  const { source, at, item, section, waterName, group } = input;
  const now = input.now ?? Date.now();
  const visitedAt = input.visitedAt ?? now;
  // The date the CONTENTS are about. `input.on` is the day the app was showing; if the
  // person then said they were there on the 12th, the 12th is what the record must answer
  // for — otherwise a spot from a closed weekend reads as open because it was typed up on
  // the Tuesday after the closure lifted.
  const v = new Date(visitedAt);
  const on: PlainDate = { year: v.getUTCFullYear(), month: v.getUTCMonth() + 1,
                          day: v.getUTCDate() };

  // Everything in parallel: this runs while a person is looking at a "saving" spinner, and
  // three round trips in series is three times as long to look at it.
  const [reading, trace, regulation, weather] = await Promise.all([
    readingFor(source, section),
    traceFor(source, section),
    regulationFor(source, section, on, group),
    (input.weather ?? noWeather).at(at.lat, at.lon, new Date(visitedAt)),
  ]);

  return {
    id: input.id?.() ?? `spot_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now, updatedAt: now, visitedAt,
    lat: at.lat, lon: at.lon,
    item, section, waterName,
    // May be "". A spot without a title is a perfectly good spot — it has a place, a
    // date and what the water was doing — and "Untitled spot" is a placeholder pretending
    // to be a name. The list falls back to the water or the coordinates, which are true.
    title: input.title || waterName || "",
    notes: "", photos: [],
    reading, weather, trace, regulation,
  };
}

async function readingFor(source: RegsSource, section: SectionId | null):
  Promise<SpotReading | null> {
  if (!section) return null;
  const link = await source.gaugeForSection(section);
  // NO LINK IS THE REFUSAL. A station that drains far too much to describe this water gets
  // no `section_gauge` row, so the link is null — recording a number anyway would freeze a
  // wrong answer into a saved spot permanently, and a spot is forever.
  if (!link) return null;
  const now = await source.gaugeNow(link.station);
  if (!now) return { station: link.station, discharge: null, level: null,
                     percentile: null, at: null };
  return {
    station: link.station,
    discharge: now.value.discharge,
    level: now.value.level,
    percentile: now.value.percentile ?? null,
    at: now.value.at ?? null,
  };
}

async function traceFor(source: RegsSource, section: SectionId | null):
  Promise<GaugeTrace | null> {
  if (!section) return null;
  const link = await source.gaugeForSection(section);
  if (!link)
    return { station: null, stationName: null, trust: null, path: [],
             metres: null, reachMagnitude: null, gaugeMagnitude: null,
             lon: null, lat: null };
  return {
    station: link.station, stationName: link.name, trust: link.trust,
    path: await source.traceToGauge(section),
    metres: null,
    reachMagnitude: link.reachMagnitude || null,
    gaugeMagnitude: link.gaugeMagnitude || null,
    // FROZEN WITH THE REST OF THE SPOT. The station does not move, but a spot pinned
    // before this field existed carries null and draws no route — which is the truth about
    // what was recorded, not a failure to load.
    lon: link.lon,
    lat: link.lat,
  };
}

async function regulationFor(source: RegsSource, section: SectionId | null,
                             on: PlainDate, group: SpeciesGroup) {
  if (!section) return null;
  const statuses = await source.statusFor([section], on, group);
  const s = statuses.get(section);
  if (!s) return null;
  // The DATE is stored with it. Half of these regulations are seasonal, so an outcome with
  // no date is a sentence that stops being true and never says when it did.
  return { outcome: s.outcome, provenance: s.provenance,
           on: `${on.year}-${String(on.month).padStart(2, "0")}-${String(on.day).padStart(2, "0")}` };
}
