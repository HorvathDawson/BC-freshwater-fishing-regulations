/**
 * WHAT THE WATER IS DOING, finished for the conditions panel (`useConditionsView`): the speaker, the donor answer with
 * its routes, the route map's colouring, the lead gauge's reading or its quiet head, and the gauge switcher.
 */
import { useCallback, useMemo, useRef, useState } from "react";
import { speaks, leadOver, throughLakeStations, type DonorRow, type Horizon, type PanelAnswer, type Parameter, type Quantity,
         type WaterSource, type SectionId, type StationId } from "@app/data";
import { aheadDays, CONDITIONS_WORDS, donorLastReport, FORECAST_DAY_WORDS, forecastDayMark, forecastDayName,
         forecastFigureLine, donorReads, donorSilent, donorState, leadGapLine, LEVEL_ONLY_LEAD, freshness, gaugeCountLine, noGaugeSentence,
         readingAgeLine, readingHeading, readingIn, readingStanding, shownParameter, stageLine, stateAt,
         type GaugeTrace, type SectionKey, type Standing, bcMidnight, shiftDay, wallIso, now } from "@app/core";
import { useGaugeTrace, useStationReading } from "../hooks";
import { useBcToday } from "../today";
import { routeLabels, routeValues, type RouteColourVM } from "../routeColour";
import { useLastReports, type PanelFeed } from "./feed";
import { useJoinedRoutes, usePanelAsk, usePanelStandings, useRoutes, useSpeaker } from "./standings";
import { gaugeChoiceGroups, gaugeChoices, useGaugeSides, type GaugeChoiceVM, type GaugeGroupVM,
         type GaugeSides } from "./donor";

/** The lead station's own reading, finished. */
export interface ReadingVM {
  heading: string;
  /** (T367) the gauge's name as sourced: the gauge card's title */
  title: string;
  figure: number;
  unit: string;
  /** Which quantity `figure` is. */
  quantity: Parameter;
  /** The stage beside a discharge, where the station has both. */
  stage: string | null;
  /** (T567) Every quantity the station reported, so the card can show the one the chart is about. */
  figures: Readonly<Partial<Record<Parameter, number>>>;
  /** (T567) On a forecast day, whose this figure is: "Today's reading" (the day asked has its own line, `forecast`); else null. */
  scope: string | null;
  /** (T567) On a forecast day, the lead gauge's forecast for the day asked, finished; null on Now / Change or without a row. */
  forecast: string | null;
  standing: string;
  /** (T367) the reading's percentile, 0–1, for the standing's dot in the map's colour; null without a record */
  percentile: number | null;
  age: string;
}

/**
 * (T581, T580 F7) THE ONE CURRENT VALUE of the lead gauge in a quantity: what the card prints and what the alert editor
 * prefills and says is "now". Null where the station did not report that quantity.
 */
export function currentFigure(reading: Pick<ReadingVM, "figures"> | null, quantity: Parameter): number | null {
  return reading?.figures[quantity] ?? null;
}

/** A quiet donor at the head of the chart, in core's words. */
export interface QuietVM { heading: string; title: string; reads: string; lastReport: string }

/**
 * THE QUIET HEAD, at the horizon the sheet is on: a day ahead with no forecast is a missing forecast, as the silent
 * line under the table says it — never "Not reporting today" over a gauge that reported last night (review
 * 2026-10-22). `zone` is for tests.
 */
export function quietHead(name: string, row: DonorRow, ref: number, horizon: Horizon, zone?: string): QuietVM {
  return { heading: readingHeading(name), title: name, reads: donorReads(row, { ref, horizon }),
           lastReport: donorLastReport(row.observedAt, zone) };
}

/** A gauged lake's own station with no reading: whose it is, that it is quiet, when it last reported (core words). */
export function lakeQuietHead(name: string, observedAt: string | null, ref: number, horizon: Horizon,
                              zone?: string): QuietVM {
  return { heading: readingHeading(name), title: name, reads: donorReads({ percentile: null, observedAt }, { ref, horizon }),
           lastReport: donorLastReport(observedAt, zone) };
}

export interface ConditionsVM {
  /** Who speaks for the section (`useSpeaker`) is still being looked up. */
  loading: boolean;
  /** The day everything is for — the caller's, or today when it chose none. */
  day: Date;
  /** The donor answer with its routes, for `DonorPanel`. */
  panel: PanelAnswer;
  /** The reaches between here and every donor. */
  chain: readonly SectionKey[];
  /**
   * Each of those reaches coloured by its own percentile, for the route map — through the colouring engine
   * (`standing`); see `routeColour.ts`.
   */
  route: RouteColourVM;
  /**
   * (T508) Every first answer this view draws is in (who speaks, the panel, its routes, the lead's reading): what is
   * shown now is what stays — a held pane's "ready" (`ConditionsPanel`). Refreshes after it (a feed tick) change values
   * in place.
   */
  settled: boolean;
  /** The station the chart and the reading are about. */
  lead: StationId | null;
  select: (station: StationId) => void;
  /** Null when the lead gauge has no reading: draw `quiet` if it is set, else `noGauge`. */
  reading: ReadingVM | null;
  /**
   * The lead is one of the panel's donors and has no reading (a quiet gauge picked in the switch, or every donor
   * quiet): whose it is, what it reads today and when it last reported, from core. NOT the no-gauge refusal — the
   * panel's gauges speak for this water, one of them is only silent today (review 2026-10-21).
   */
  quiet: QuietVM | null;
  /** True where nothing speaks for the section (`speakerFor` null): draw `noGauge`, the refusal. */
  refuse: boolean;
  noGauge: string;
  /** The single-station trace, only where the panel could not answer. */
  trace: GaugeTrace | null;
  /** The chart's gauge switcher — see `gaugeChoices`. */
  gauges: readonly GaugeChoiceVM[];
  /** (T168) The switcher grouped (`gaugeChoiceGroups`); null: draw `gauges` as one run. */
  gaugeGroups: readonly GaugeGroupVM<GaugeChoiceVM>[] | null;
  /** (T568/T567) The forecast day asked, for the chart's mark beside today's: its clock instant and words; null on Now / Change. */
  ahead: { at: number; label: string } | null;
  /** (owner 2026-11-10) No gauge of this water answers in flow that day: the lead was chosen on level; the card says so. */
  leadNote: string | null;
  /** (T168) Each station's side, for the donor panel; null until read. */
  sides: GaugeSides;
  /** (T168) "2 gauges on this water · 5 estimating from other waters · 6 not reporting" (core `gaugeCountLine`). */
  count: string | null;
}

/**
 * WHAT THE WATER IS DOING, finished for the conditions panel.
 *
 * `day` is the chosen day (null: today, read once per mount); `horizon` is how far past it.
 * `opts` carries the live feed and the quantity the map is painting, so the sheet answers
 * the same question the colour does.
 */
export function useConditionsView(
  source: WaterSource, section: SectionId | null, day: Date | null, horizon: Horizon,
  opts: { feed: PanelFeed | undefined; quantity: Quantity },
): ConditionsVM {
  const { feed, quantity } = opts;
  const spoken = useSpeaker(source, section);
  const speaker = spoken.state === "ready" ? spoken.value.speaker : null;
  const traced = useGaugeTrace(source, section);
  // (T296) the routes first: a member whose route crosses a lake is weighed as such once they are walked (the answer
  // shows at once without them, and is refreshed in place)
  const routes = useRoutes(source, section);
  const lakes = useMemo(() => throughLakeStations(routes), [routes]);
  const asked = usePanelAsk(source, feed, section, quantity, horizon, lakes);
  const panel = useLastReports(feed, useJoinedRoutes(asked.value, routes));
  const nowDay = useBcToday();                  // (T551) rolls over at B.C. midnight
  const today = day ?? nowDay;
  // (T551, T580 F15) THE INSTANT THE DAY IS ASKED AT: a picked day is B.C.'s midnight of it, today is now (read once, again at
  // the roll-over) -- never the day's device-local fields read back as an instant
  // eslint-disable-next-line react-hooks/exhaustive-deps -- re-read when the day changes, not on every render
  const ref = useMemo(() => (day ? bcMidnight(wallIso(day)) : now()), [day, nowDay]);
  // THE ROUTES OF THE MEMBERS THAT SPEAK — the route map's own chain (`donorView`): a silent member has no pin, so its
  // route is not coloured either.
  const chain = useMemo<readonly SectionKey[]>(
    () => [...new Set(panel.rows.filter((r) => !donorSilent(r, { ref, horizon }))
                                .flatMap((r) => r.route?.path ?? []))], [panel.rows, today, horizon]);
  const standings = usePanelStandings(source, feed, chain as readonly SectionId[], quantity,
                                      horizon);
  // THE ROUTE AS THE ENGINE'S CLASSES, kept while the route is. A horizon change re-asks the panel, which answers no rows
  // while it loads and then the same rows as new objects: the chain goes empty and comes back new-but-equal. New labels
  // would make the engine clear the route map twice; the last route of THIS section is kept through the gap instead,
  // and the values (null while the chain is empty: the engine leaves the old colours) relabel when they come.
  const steady = useRef<{ section: SectionId | null; chain: readonly SectionKey[] }>({ section, chain });
  if (chain.length || steady.current.section !== section) steady.current = { section, chain };
  const steadyKey = `${section ?? ""}:${steady.current.chain.join(",")}`;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const labels = useMemo(() => routeLabels(steady.current.chain as readonly SectionId[]), [steadyKey]);
  const route = useMemo<RouteColourVM>(() => ({ standing: { labels, values: routeValues(labels, standings, horizon) } }),
                                         [labels, standings, horizon]);

  /*
   * THE CHART FOLLOWS THE SPEAKER (T105), the one the map colours by (`speakerFor`): a panel's picked donor, else its
   * heaviest reporting one, any donor, its first member; a lake's own station; nothing where nothing speaks
   * (`sheetLead`). The station `section_gauge` matched to this reach is never read for a reading: for 54,386 sections
   * with no panel it quoted a gauge the map left uncoloured, and a gauged lake (no `section_gauge` row) read nothing.
   */
  // (T584, T580 F6) THE GAUGE PICK IS THE SECTION'S: it is dropped when the section changes (a part step, a new water), so
  // every way into a part leads with the same gauge; the reader's span and model are kept apart (`useChartPrefs`)
  const [pick, setPick] = useState<{ section: SectionId | null; station: StationId } | null>(null);
  const picked = pick && pick.section === section ? pick.station : null;
  const select = useCallback((station: StationId) => setPick({ section, station }), [section]);
  // (owner 2026-11-10) ONE LEAD per water and day, chosen in FLOW whatever the chart shows (`leadOver`); level ranks it
  // only where no gauge answers in flow. The two asks are the same ones the rows come from (cached; one of them is `asked`).
  const flowAsk = usePanelAsk(source, feed, section, "discharge", horizon, lakes);
  const levelAsk = usePanelAsk(source, feed, section, "level", horizon, lakes);
  const { lead, levelOnly } = leadOver(speaker, { discharge: flowAsk.value.rows, level: levelAsk.value.rows }, picked);
  const got = useStationReading(source, lead);
  const c = got.state === "ready" ? got.value : null;
  // (T168) on this water or a donor: every station that speaks for the section — the panel's members, a lake's own
  const stationKey = speaker?.kind === "panel" ? speaker.panel.members.map((m) => m.station).join(",")
    : speaker?.kind === "lake" ? speaker.station : "";
  const stations = useMemo(() => (stationKey ? stationKey.split(",") as StationId[] : []), [stationKey]);
  const sides = useGaugeSides(source, section, stations);
  const ask = { ref, horizon };
  const count = !sides ? null
    : speaker?.kind === "lake"
      ? (got.state === "ready" ? gaugeCountLine([{ side: sides.get(speaker.station) ?? "donor",
                                                   state: stateAt({ percentile: c?.percentile ?? null,
                                                                    observedAt: c?.observedAt ?? null }, ask) }],
                                                 horizon) : null)
      : gaugeCountLine(panel.rows.map((r) => ({ side: sides.get(r.station) ?? "donor", state: stateAt(r, ask) })),
                       horizon);
  const name = panel.rows.find((r) => r.station === lead)?.route?.name
    ?? (speaker?.kind === "lake" && spoken.state === "ready" ? spoken.value.name : null)
    ?? lead ?? "";

  const dayName = forecastDayName(today, horizon);
  const leadRow = panel.rows.find((r) => r.station === lead);
  const ahead = dayName
    ? { at: bcMidnight(wallIso(shiftDay(today, aheadDays(horizon)))) + 12 * 3_600_000,
        label: forecastDayMark(dayName) } : null;
  let reading: ReadingVM | null = null;
  // THE DECLARED QUANTITY (T106): the publisher's `parameter`, the one its percentile is about. 08ME030 reports both
  // and declares level; reading whichever value was present printed its discharge beside the level percentile.
  const shown: Parameter | null = c ? shownParameter(c) : null;
  const figure = c && shown ? readingIn(c, shown) : null;
  if (c && shown && figure !== null) {
    reading = {
      heading: readingHeading(name),
      title: name,
      figure,
      unit: CONDITIONS_WORDS.unit[shown],
      quantity: shown,
      stage: shown === "discharge" && c.level != null ? stageLine(c.level) : null,
      figures: { ...(c.discharge != null ? { discharge: c.discharge } : {}), ...(c.level != null ? { level: c.level } : {}) },
      scope: dayName ? FORECAST_DAY_WORDS.today : null,
      forecast: dayName && leadRow
        ? (quantity === "level" && donorState(leadRow, ask).kind === "flow-only" ? leadGapLine(dayName)
           : forecastFigureLine(dayName, donorReads(leadRow, ask))) : null,
      // (T585 F8/F14) a figure the index did not rank says which reading was ranked and which this is; a kept one says so
      standing: readingStanding({ percentile: c.percentile, standing: c.standing as Standing | null,
                                  ...(c.rankedAt !== undefined ? { rankedAt: c.rankedAt } : {}), observedAt: c.observedAt }),
      percentile: c.percentile ?? null,
      age: readingAgeLine(freshness(c.fetchedAt, now()), c.observedAt, undefined, !!c.kept),
    };
  }

  const row = reading ? undefined : panel.rows.find((r) => r.station === lead);
  // Said once the reading has been asked for and is not there — not while it is on its way. A gauged lake with nothing
  // to say today is its own gauge gone quiet (the map's sentinel), never the no-gauge refusal (T105 review).
  const quiet: QuietVM | null = got.state === "loading" || reading ? null
    : row ? quietHead(name, row, ref, horizon)
    : speaker?.kind === "lake" ? lakeQuietHead(name, c?.observedAt ?? null, ref, horizon)
    : null;

  return {
    loading: spoken.state === "loading",
    // (T508) every first answer in: who speaks, the panel and its routes (the route map's chain), the lead's reading
    settled: spoken.state !== "loading" && (speaker?.kind !== "panel" || (!asked.loading && routes !== null))
      && (lead === null || got.state !== "loading"),
    // A LAKE IS READ AT ITS OWN GAUGE: the donor panel says so, not "no gauge on this water" (answerFrom's "no-station"
    // for a section with no panel), which the lake's own reading under it would contradict (T105 review).
    day: today, panel: speaker?.kind === "lake" ? { ...panel, answer: { ok: false, why: "own-gauge" } } : panel,
    chain, route, lead, select, reading, quiet,
    // THE REFUSAL ONLY WHERE NOTHING SPEAKS (`speakerFor` null): a panel offline or a lake gone quiet has a gauge that
    // speaks for it, and "No gauge is entitled to speak for this water" over it would be false (T105 review).
    refuse: spoken.state !== "loading" && speaker === null,
    noGauge: noGaugeSentence(),
    // THE MATCH RECORD, only for a station that speaks here: where the panel could not answer, how this reach reaches
    // the one of its donors it was matched to. Never a trace to a gauge the map does not colour by.
    // (T568, shot 2026-11-10) ONLY where the panel has no gauges to draw (`no-station`, `offline`): a refusal that is about
    // the day or quantity asked has its donor map, and this trace's own map stood above it, moving it down the page.
    trace: !panel.answer.ok && (panel.answer.why === "no-station" || panel.answer.why === "offline") && traced.state === "ready" && speaks(speaker, traced.value.station)
      ? traced.value : null,
    gauges: gaugeChoices(panel.rows, lead, { ref, horizon }),
    gaugeGroups: gaugeChoiceGroups(panel.rows, lead, ask, sides),
    ahead, sides, count, leadNote: levelOnly ? LEVEL_ONLY_LEAD : null,
  };
}
