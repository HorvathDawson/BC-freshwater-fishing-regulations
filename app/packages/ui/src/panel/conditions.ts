/**
 * WHAT THE WATER IS DOING, finished for the conditions panel (`useConditionsView`): the speaker, the donor answer with
 * its routes, the route map's colouring, the lead gauge's reading or its quiet head, and the gauge switcher.
 */
import { useMemo, useRef, useState } from "react";
import { speaks, sheetLead, type DonorRow, type Horizon, type PanelAnswer, type Parameter, type Quantity,
         type WaterSource, type SectionId, type StationId } from "@app/data";
import { CONDITIONS_WORDS, donorLastReport, donorReads, donorSilent, freshness, gaugeCountLine, noGaugeSentence,
         readingAgeLine, readingHeading, readingIn, readingStandingLine, shownParameter, stageLine, stateAt,
         type GaugeTrace, type SectionKey, type Standing } from "@app/core";
import { useGaugeTrace, useStationReading } from "../hooks";
import { pushedRoute, routeLabels, routeValues, type RouteColourVM } from "../routeColour";
import { useLastReports, type PanelFeed } from "./feed";
import { usePanel, usePanelRoutes, usePanelStandings, useSpeaker } from "./standings";
import { gaugeChoiceGroups, gaugeChoices, useGaugeSides, type GaugeChoiceVM, type GaugeGroupVM,
         type GaugeSides } from "./donor";

/** The lead station's own reading, finished. */
export interface ReadingVM {
  heading: string;
  figure: number;
  unit: string;
  /** Which quantity `figure` is. */
  quantity: Parameter;
  /** The stage beside a discharge, where the station has both. */
  stage: string | null;
  standing: string;
  age: string;
}

/** A quiet donor at the head of the chart, in core's words. */
export interface QuietVM { heading: string; reads: string; lastReport: string }

/**
 * THE QUIET HEAD, at the horizon the sheet is on: a day ahead with no forecast is a missing forecast, as the silent
 * line under the table says it — never "Not reporting today" over a gauge that reported last night (review
 * 2026-10-22). `zone` is for tests.
 */
export function quietHead(name: string, row: DonorRow, ref: number, horizon: Horizon, zone?: string): QuietVM {
  return { heading: readingHeading(name), reads: donorReads(row, { ref, horizon }),
           lastReport: donorLastReport(row.observedAt, zone) };
}

/** A gauged lake's own station with no reading: whose it is, that it is quiet, when it last reported (core words). */
export function lakeQuietHead(name: string, observedAt: string | null, ref: number, horizon: Horizon,
                              zone?: string): QuietVM {
  return { heading: readingHeading(name), reads: donorReads({ percentile: null, observedAt }, { ref, horizon }),
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
   * Each of those reaches coloured by its own percentile, for the route map — through the colouring engine on a
   * per-tile map (`standing`), pushed elsewhere (`data`); see `routeColour.ts`.
   */
  route: RouteColourVM;
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
  opts: { feed: PanelFeed | undefined; quantity: Quantity;
          /** `coloursEachTile(Platform.OS)`: the route map colours through the engine. Omitted: pushed. */
          perTile?: boolean },
): ConditionsVM {
  const { feed, quantity } = opts;
  const perTile = opts.perTile ?? false;
  const spoken = useSpeaker(source, section);
  const speaker = spoken.state === "ready" ? spoken.value.speaker : null;
  const traced = useGaugeTrace(source, section);
  const panel = useLastReports(feed, usePanelRoutes(source, section,
                                                    usePanel(source, feed, section, quantity, horizon)));
  const today = useMemo(() => day ?? new Date(), [day]);
  // THE ROUTES OF THE MEMBERS THAT SPEAK — the route map's own chain (`donorView`): a silent member has no pin, so its
  // route is not coloured either.
  const chain = useMemo<readonly SectionKey[]>(
    () => [...new Set(panel.rows.filter((r) => !donorSilent(r, { ref: today.getTime(), horizon }))
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
  const route = useMemo<RouteColourVM>(() => (perTile
    ? { standing: { labels, values: routeValues(labels, standings, horizon) }, data: {} }
    : { standing: null, data: pushedRoute(standings, horizon) }), [perTile, labels, standings, horizon]);

  /*
   * THE CHART FOLLOWS THE SPEAKER (T105), the one the map colours by (`speakerFor`): a panel's picked donor, else its
   * heaviest reporting one, any donor, its first member; a lake's own station; nothing where nothing speaks
   * (`sheetLead`). The station `section_gauge` matched to this reach is never read for a reading: for 54,386 sections
   * with no panel it quoted a gauge the map left uncoloured, and a gauged lake (no `section_gauge` row) read nothing.
   */
  const [picked, select] = useState<StationId | null>(null);
  const lead: StationId | null = sheetLead(speaker, panel.rows, picked);
  const got = useStationReading(source, lead);
  const c = got.state === "ready" ? got.value : null;
  // (T168) on this water or a donor: every station that speaks for the section — the panel's members, a lake's own
  const stationKey = speaker?.kind === "panel" ? speaker.panel.members.map((m) => m.station).join(",")
    : speaker?.kind === "lake" ? speaker.station : "";
  const stations = useMemo(() => (stationKey ? stationKey.split(",") as StationId[] : []), [stationKey]);
  const sides = useGaugeSides(source, section, stations);
  const ask = { ref: today.getTime(), horizon };
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

  let reading: ReadingVM | null = null;
  // THE DECLARED QUANTITY (T106): the publisher's `parameter`, the one its percentile is about. 08ME030 reports both
  // and declares level; reading whichever value was present printed its discharge beside the level percentile.
  const shown: Parameter | null = c ? shownParameter(c) : null;
  const figure = c && shown ? readingIn(c, shown) : null;
  if (c && shown && figure !== null) {
    reading = {
      heading: readingHeading(name),
      figure,
      unit: CONDITIONS_WORDS.unit[shown],
      quantity: shown,
      stage: shown === "discharge" && c.level != null ? stageLine(c.level) : null,
      standing: readingStandingLine(c.percentile, c.standing as Standing | null),
      age: readingAgeLine(freshness(c.fetchedAt, Date.now()), c.observedAt),
    };
  }

  const row = reading ? undefined : panel.rows.find((r) => r.station === lead);
  // Said once the reading has been asked for and is not there — not while it is on its way. A gauged lake with nothing
  // to say today is its own gauge gone quiet (the map's sentinel), never the no-gauge refusal (T105 review).
  const quiet: QuietVM | null = got.state === "loading" || reading ? null
    : row ? quietHead(name, row, today.getTime(), horizon)
    : speaker?.kind === "lake" ? lakeQuietHead(name, c?.observedAt ?? null, today.getTime(), horizon)
    : null;

  return {
    loading: spoken.state === "loading",
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
    trace: !panel.answer.ok && traced.state === "ready" && speaks(speaker, traced.value.station)
      ? traced.value : null,
    gauges: gaugeChoices(panel.rows, lead, { ref: today.getTime(), horizon }),
    gaugeGroups: gaugeChoiceGroups(panel.rows, lead, ask, sides),
    sides, count,
  };
}
