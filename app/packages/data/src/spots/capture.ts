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
import { freezeTrace, leadUpDays, localDate, type FrozenTrace } from "@app/core";
import { answerFrom, type PanelAnswer } from "../panel";
import { sheetLead, speakerFor, speaks, type Speaker } from "../standing";
import type { ItemId, WaterSource, SectionId, StationId } from "../index";
import type { DailyWeather, LeadUp, Spot, SpotReading, SpotWeather } from "./model";
import { stripRoutes } from "./migrate";
import { leadUpFor } from "./leadup";
import { capturePanelOnDay, fillDayFlow, type DayFlowPorts } from "./dayFlow";
import type { WeatherAhead } from "./likeVisit";

/**
 * Weather, from wherever weather comes from.
 *
 * A PORT, not an implementation. There is no weather service wired into this app yet, so
 * the default returns null and the spot records that it does not know. Naming the shape now
 * means the screen, the record and the backfill are all written against the real thing.
 */
export interface WeatherSource {
  at(lat: number, lon: number, when: Date): Promise<SpotWeather | null>;
  /**
   * S1: the DAILY weather for these local days ("2026-08-23" …), for the week before a visit. Null when it could not
   * be fetched. Optional: a source without it records no lead-up weather, said as unknown.
   */
  days?(lat: number, lon: number, days: readonly string[]): Promise<DailyWeather[] | null>;
  /**
   * T51: the forecast for the coming days (with the past days the rain window needs), for "Like your visit?". Null
   * when it could not be fetched. Optional: a source without it scores the coming days on flow alone.
   */
  ahead?(lat: number, lon: number): Promise<WeatherAhead | null>;
}

/** The honest default: no service, so no weather, said out loud. */
export const noWeather: WeatherSource = { at: async () => null };

export interface CaptureInput {
  source: WaterSource;
  weather?: WeatherSource;
  at: { lat: number; lon: number };
  item: ItemId | null;
  /**
   * The reach tapped, used ONLY to look up the reading, the trace and the estimate while
   * capturing. It is never stored on the spot (AGENTS 5).
   */
  section: SectionId | null;
  waterName: string | null;
  title: string;
  /**
   * When the person was at the water. Defaults to now.
   *
   * THE ONLY DATE THIS FUNCTION TAKES, and it replaced a separate `on: PlainDate`. Two date
   * inputs is two dates that can disagree — and they did: the test that caught this passed
   * `on: 2026-08-30` alongside a `now` in August 2025, and nothing complained, because each
   * value was used for something different. Weather and gauge reading are claims about one
   * instant, so they take one instant.
   */
  visitedAt?: number;
  /** The live index, so the estimate can be frozen as the reader saw it. */
  feed?: { index(): Promise<Parameters<typeof answerFrom>[1]> };
  /**
   * Q161: HYDAT and the device's online word, for a trip on an EARLIER day than today — its gauges are read from that
   * day's record (`flowOnDay`), never from the live feed. Absent: the feed's own daily series only; older days wait.
   */
  history?: Omit<DayFlowPorts, "source">;
  now?: number;
  id?: () => string;
}

/** The estimate as the app had it, or null when there is no feed or no panel. */
async function panelFor(source: WaterSource, section: SectionId | null,
                        feed: CaptureInput["feed"]): Promise<PanelAnswer | null> {
  if (!feed || !section) return null;
  try {
    const [panels, idx] = await Promise.all([source.panelsFor([section]), feed.index()]);
    const got = answerFrom(panels.get(section), idx, "discharge");
    // Only a real answer is worth freezing: "no panel" is not a fact about that day, it is
    // a fact about the app, and it would read as one on a spot opened a year later.
    return got.answer.ok || got.rows.length ? stripRoutes(got) : null;
  } catch {
    // A spot must save. A missing estimate is a smaller loss than a lost record.
    return null;
  }
}

export async function captureSpot(input: CaptureInput): Promise<Spot> {
  const { source, at, item, section, waterName } = input;
  const now = input.now ?? Date.now();
  const visitedAt = input.visitedAt ?? now;
  /*
   * THE TRIP'S DAY, NOT THE SAVE'S (owner 2026-10-24, Q161: "it should save the date that you set as the time of trip
   * in the add a spot"). A trip on an earlier day than today is recorded from THAT day's record — the lead gauge's
   * daily mean (`fillDayFlow`) and the section's panel with every member's daily mean that day (`capturePanelOnDay`),
   * both through the one history read (`flowOnDay`) — and never from the live feed, which is today's river. Offline or
   * the day not over: saved as pending, filled by a refresh. A trip today is read live, exactly as before.
   */
  const earlier = localDate(visitedAt) < localDate(now);
  const ports: DayFlowPorts = { source, ...input.history };

  // In parallel where nothing depends on anything: this runs while a person is looking at a "saving" spinner, and
  // round trips in series are longer to look at.
  const sky = (input.weather ?? noWeather).at(at.lat, at.lon, new Date(visitedAt));
  const [speaker, panelled] = await Promise.all([
    // WHO SPEAKS (T105): the speaker the map colours by and the sheet reads, `speakerFor`. A failed lookup fails the
    // capture (a spot must not freeze "no gauge speaks here" because a read failed).
    speakerOf(source, section),
    // THE ANSWER THE APP WAS SHOWING, through the same function the map and the sheet use.
    // Without a feed there is nothing to freeze and the spot keeps only the reading.
    // An earlier trip day: the same panel, each member's daily mean that day (Q161).
    earlier ? panelOnTripDay(source, section, localDate(visitedAt), ports)
            : panelFor(source, section, input.feed).then((p): Pick<Spot, "panel" | "panelDay"> => ({ panel: p })),
  ]);
  const { panel, ...onDay } = panelled;     // `panelDay` only on an earlier trip day (Q161)
  // THE SHEET'S STATION (`sheetLead`): never the station `section_gauge` matched to the reach unless it speaks here, so
  // a saved spot cannot quote a gauge the sheet refused (T105 review).
  const lead = sheetLead(speaker, panel?.rows ?? [], null);
  const [reading, trace, weather, leadUp] = await Promise.all([
    // an earlier trip day holds no live reading: its flow is that day's record (`dayFlow`, below)
    earlier ? Promise.resolve(lead ? { station: lead, discharge: null, level: null, percentile: null, at: null } : null)
            : readingFor(source, lead),
    traceFor(source, section, speaker),
    sky,
    // S1: the seven days before the visit — daily weather and the station's daily means.
    leadUpFor(source, input.weather, at, lead, leadUpDays(visitedAt), input.feed),
  ]);

  const spot: Spot = {
    panel,
    ...onDay,
    id: input.id?.() ?? `spot_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    createdAt: now, updatedAt: now, visitedAt,
    lat: at.lat, lon: at.lon,
    item, waterName,
    // May be "". A spot without a title is a perfectly good spot — it has a place, a
    // date and what the water was doing — and "Untitled spot" is a placeholder pretending
    // to be a name. The list falls back to the water or the coordinates, which are true.
    title: input.title || waterName || "",
    notes: "", photos: [],
    reading, weather, trace, leadUp,
  };
  // the trip day's own flow, through the one fill a refresh and "Get the flow for that day" use; offline: asked later
  return earlier ? (await fillDayFlow(spot, ports, now)).spot : spot;
}

/** The section's panel with each member's daily mean on the trip day (Q161); a failed lookup freezes no panel. */
async function panelOnTripDay(source: WaterSource, section: SectionId | null, date: string, ports: DayFlowPorts):
  Promise<Pick<Spot, "panel" | "panelDay">> {
  if (!section) return { panel: null };
  try {
    const panels = await source.panelsFor([section]);
    return await capturePanelOnDay(panels.get(section), date, ports);
  } catch {
    // A spot must save. A missing estimate is a smaller loss than a lost record.
    return { panel: null };
  }
}

/** Who speaks for the reach (`speakerFor`, T105): its panel, else the station in its lake, else nobody. */
async function speakerOf(source: WaterSource, section: SectionId | null): Promise<Speaker | null> {
  if (!section) return null;
  const [panels, lakes] = await Promise.all([source.panelsFor([section]), source.lakeStationsFor([section])]);
  return speakerFor(panels.get(section), lakes.get(section));
}

/** The lead station's reading. No station (nothing speaks for the water) is the refusal: no reading at all. */
async function readingFor(source: WaterSource, station: StationId | null):
  Promise<SpotReading | null> {
  // NO SPEAKER IS THE REFUSAL. Recording a number anyway would freeze a wrong answer into a saved spot permanently,
  // and a spot is forever.
  if (!station) return null;
  const now = await source.gaugeNow(station);
  if (!now) return { station, discharge: null, level: null,
                     percentile: null, at: null };
  return {
    station,
    discharge: now.value.discharge,
    level: now.value.level,
    parameter: now.value.parameter,
    percentile: now.value.percentile ?? null,
    at: now.value.at ?? null,
  };
}

async function traceFor(source: WaterSource, section: SectionId | null, speaker: Speaker | null):
  Promise<FrozenTrace | null> {
  if (!section) return null;
  const link = await source.gaugeForSection(section);
  // THE MATCH RECORD, only for a station that speaks here — as the sheet shows it (T105).
  if (!link || !speaks(speaker, link.station))
    return { station: null, stationName: null, trust: null, reaches: 0,
             metres: null, reachMagnitude: null, gaugeMagnitude: null,
             lon: null, lat: null };
  // FROZEN WITHOUT ITS HANDLES: the walk's section ids name reaches of this atlas only.
  return freezeTrace({
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
  });
}
