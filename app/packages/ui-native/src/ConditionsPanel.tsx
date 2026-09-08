/**
 * WHAT THE WATER IS DOING. One implementation, every surface.
 *
 * There were two. Tapping a river on the Map tab opened the water sheet's "conditions"
 * face; tapping the same river on the Conditions tab opened a different screen — different
 * layout, different wording, different controls, and one of them had the chart toggles and
 * the other did not. Two answers to one question, in one app, which is exactly the drift
 * AGENTS rule 23 exists to stop. This file is that answer; both surfaces render it.
 *
 * It decides nothing. Every number arrives from a hook in @app/ui (rule 25) — which is what
 * lets the desktop build its own layout over the same three questions without either side
 * being able to disagree about the answer.
 */
import { useMemo, useState } from "react";
import { ScrollView, Text, View } from "react-native";
import { percentileOrdinal, standingWord, type Standing } from "@app/core";
import type { Parameter, RegsSource, SectionId, StationId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useConditions, useGaugeParameters, useGaugeTrace, useHydrograph, usePanel,
         usePanelRoutes, usePanelStandings, useSeries, useStationReading,
         type Horizon } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { Credits } from "./Credits";
import { FishSpinner } from "./FishSpinner";
import { DonorPanel } from "./DonorPanel";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** core's own word for "a reading, but nothing to compare it against". */
const NO_RECORD: Standing = "no-record";

export const UNIT: Record<Parameter, string> = { discharge: "m³/s", level: "m" };

export type Span = "72h" | "year";

export function ConditionsPanel({ source, section, palette, tiles, theme, colour,
                                  parameter, onParameter, from, feed, scroll = true,
                                  footer, credits, horizon = 0, onHorizon }: {
  source: RegsSource; section: SectionId | null; palette: Palette;
  /** The live index, for the donor panel. Absent offline — it then says so. */
  feed?: { index(): Promise<Parameters<typeof usePanel>[1] extends undefined ? never : any> };
  /** Present, the route panel draws the chain of reaches down to the station. */
  tiles?: TileEndpoints; theme?: string;
  /** The trace colour, so a sheet can tint the chart to its own outcome. */
  colour?: string;
  /**
   * The quantity, when the CALLER owns it.
   *
   * The Conditions tab colours the map by flow or by level, and the chart under a tap has
   * to be about the same thing the map is showing — a reader who switched the map to level
   * and then opened a discharge chart has been handed two different questions. Left
   * undefined, the panel owns the choice itself.
   */
  parameter?: Parameter; onParameter?: (p: Parameter) => void;
  /**
   * Data credits, for the foot of the screen.
   *
   * The route map here is deliberately bare — no zoom stack, no scale, no attribution
   * button — so the credit it would have carried has to appear on the page instead. These
   * are the app's own list, passed down rather than restated: the Province's forecast
   * notice is required verbatim, and a second copy is a second thing to get wrong.
   */
  credits?: readonly string[];
  /** Where the reader tapped, so the route map can mark where they are standing. */
  from?: { lat: number; lon: number } | null;
  scroll?: boolean;
  footer?: React.ReactNode;
  /**
   * How far ahead the estimate is for — 0 is now.
   *
   * Kept in step with the map's own horizon control, for the same reason `parameter` is:
   * a reader who set the map to Friday and then tapped a river would otherwise be handed
   * today's answer under Friday's colouring, with nothing on screen saying so.
   */
  horizon?: Horizon;
  /** Set the horizon from the sheet — see DonorPanel. Absent, the chips are not offered. */
  onHorizon?: (d: Horizon) => void;
}) {
  const conditions = useConditions(source, section);
  const trace = useGaugeTrace(source, section);
  /*
   * EVERY GAUGE THAT CAN SPEAK, not only the nearest one.
   *
   * `useGaugeTrace` above answers "which single station was matched to this reach, and how
   * do I walk to it" — the design that answers for 9.7% of the water. This is the panel:
   * the set that each know something, weighted, with the working shown. They sit together
   * on purpose, because the trace is how a reader checks the panel's claim about direction
   * and distance against the map.
   */
  /*
   * THE SAME QUANTITY THE MAP IS PAINTING — including "both".
   *
   * This asked for "discharge" whenever the map was on "both", so a reach the map coloured
   * from its station's own level was answered here from a discharge it may not have. One
   * arithmetic, two questions, and the reader sees a colour and a number that disagree.
   * `parameter` is undefined exactly when the caller is on "both", and "both" is a real
   * value of this argument now.
   */
  const panel = usePanelRoutes(
    source, section,
    usePanel(source, feed, section, parameter ?? "both", horizon));
  /*
   * THE WATER BETWEEN HERE AND EACH GAUGE, COLOURED BY WHAT IT IS DOING.
   *
   * The route map drew the chain in one flat highlight colour, which says "this is the
   * path" and nothing else — and the path is not the interesting part. What a reader wants
   * to know is how the water they are standing in relates to the water at the gauge: 
   * whether the whole river is low, or only this end of it. So the reaches on the route are
   * coloured by their OWN percentile, at their own widths, from the same arithmetic the big
   * map uses, and every other reach is left as unmeasured grey.
   */
  const chain = useMemo(
    () => [...new Set(panel.rows.flatMap((r) => r.route?.path ?? []))],
    [panel.rows]);
  const routeStandings = usePanelStandings(
    source, feed as Parameters<typeof usePanelStandings>[1], chain,
    parameter ?? "both", horizon);
  const routeData = useMemo(() => {
    const values = Object.fromEntries(
      [...routeStandings].map(([sec, p]) => [sec, { standing: p * 100 }]));
    // Both layers get the same values: `standing` is keyed by SECTION, and a lake section
    // is a section. A lake with no reading is simply absent from the map, as it should be.
    return { stream: values, lake: values };
  }, [routeStandings]);

  const matched = conditions.state === "ready" ? conditions.value : null;
  /*
   * THE CHART IS ABOUT A GAUGE THE READER CAN SEE IN THE LIST.
   *
   * It used to be about `section_gauge` — the ONE station matched to this reach — while the
   * table beneath listed the panel's donors, which need not include it. The Similkameen
   * near Hedley charted above four gauges at Princeton, Keremeos and Coalmont, with nothing
   * on screen to say why. So the chart follows the panel: it opens on the donor carrying
   * the most of the answer (rows are already in weight order) and any row can be tapped to
   * move it. The matched station is the fallback for water that has no panel at all.
   */
  const [pickedStation, setPickedStation] = useState<StationId | null>(null);
  const lead: StationId | null =
    pickedStation
    ?? panel.rows.find((r) => r.percentile !== null)?.station
    ?? panel.rows[0]?.station
    ?? matched?.station
    ?? null;
  const reading = useStationReading(source, lead);
  const live = reading.state === "ready" ? reading.value : null;
  // The station's NAME comes from the donor that named it; the matched link names the
  // fallback. One name per station, from whichever of the two knows it.
  const leadName = panel.rows.find((r) => r.station === lead)?.route?.name
    ?? (lead === matched?.station ? matched?.stationName : null)
    ?? null;
  // Everything below reads the SELECTED station's numbers. `matched` is kept only for the
  // no-panel fallback and for the reach-to-gauge trace, which is about a relationship.
  const c = lead === matched?.station && !live?.fetchedAt ? matched : live;
  const station = lead;

  // WHICH QUANTITIES THIS STATION CAN ANSWER IN. Asked of the bundle rather than assumed:
  // 237 BC stations measure stage and never discharge, and offering a discharge chart for
  // one of them would produce an empty frame with no explanation.
  const params = useGaugeParameters(source, station);
  const available = params.state === "ready" ? params.value : [];
  const [own, setOwn] = useState<Parameter | null>(null);
  const [span, setSpan] = useState<Span>("72h");
  const picked = parameter ?? own;
  const pick = onParameter ?? setOwn;
  const param: Parameter | undefined =
    picked && available.includes(picked) ? picked : undefined;
  const shown: Parameter = param
    ?? (c?.discharge !== null && c?.discharge !== undefined ? "discharge" : "level");
  // WHICH MODEL. Left alone the freshest run is chosen, and that is a UI default rather
  // than a judgement: CLEVER asks how HIGH over ten days and ELF how LOW over thirty, and
  // which one matters depends on the month and on why you are asking.
  const [model, setModel] = useState<string | null>(null);
  const chart = useHydrograph(source, station, span, param, model ?? undefined);
  const series = useSeries(source, station, span, param, model ?? undefined);
  const tint = colour ?? palette.live;
  const forecast = chart.state === "ready" ? chart.value?.forecast : null;
  const runs = series.state === "ready" ? series.value?.forecasts ?? [] : [];
  const run = (series.state === "ready" ? series.value?.forecast : null) ?? null;

  if (conditions.state === "loading")
    return (
      <View style={{ paddingVertical: 48, alignItems: "center" }}>
        <FishSpinner palette={palette} size={110} label="Reading the water" />
      </View>
    );

  const body = (
    <>
      {/*
        THE ANSWER FIRST, THE EVIDENCE UNDER IT.
        
        The raw gauge reading used to open this screen — a number from a station that is
        often not on this water and, once the panel existed, often not even in the list
        below it. A reader arriving at "5.2 m³/s · BELOW NORMAL · SIMILKAMEEN NEAR HEDLEY"
        reasonably concludes that is what the water they tapped is doing. It is what one
        gauge somewhere in the watershed is doing. The estimate for the spot is the answer
        to the question that was asked, so it goes first, and the gauge below is the
        working.
      */}
      <DonorPanel palette={palette} value={panel} at={tiles} theme={theme} from={from}
                  selected={lead} onSelect={setPickedStation} horizon={horizon}
                  chain={chain} data={routeData} onHorizon={onHorizon} />

      {c?.discharge != null || c?.level != null ? (
        <View style={{ gap: 6 }}>
          {/* WHOSE READING THIS IS, before the number rather than under it. The line under
              the figure said the station's name in grey at 11.5px, which is not where a
              reader looks to find out what a big teal number is about. */}
          <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                         color: palette.faint }}>
            AT {(leadName ?? station ?? "").toUpperCase()}
          </Text>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, fontSize: 34, color: tint }}>
              {c.discharge ?? c.level}
            </Text>
            <Text style={{ ...TYPE.figure, color: palette.sub }}>
              {/* From UNIT, not spelled again — the table above is the one place this
                  app decides what a quantity is measured in. */}
              {UNIT[c.discharge != null ? "discharge" : "level"]}
            </Text>
            {/* BOTH NUMBERS WHERE THERE ARE BOTH. A station measuring stage and discharge
                has two readings a person may want, and hiding one behind the chart toggle
                makes the sheet answer a question it was not asked. */}
            {c.discharge != null && c.level != null && (
              <Text style={{ ...TYPE.figure, fontSize: 13, color: palette.faint }}>
                {c.level} m stage
              </Text>
            )}
          </View>
          <Text style={{ ...TYPE.body, color: palette.ink }}>
            {c.percentile != null
              // The word comes from core, so this panel and the map agree about what "low"
              // means rather than each deciding. `standing` is null when nothing computed
              // one; core's own vocabulary calls that "no-record" rather than leaving it
              // unsaid.
              ? `${standingWord((c.standing ?? NO_RECORD) as Standing)} — ` +
                `${percentileOrdinal(c.percentile)} percentile for the date`
              : "There is a reading here, but no record to compare it against."}
          </Text>
          <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
            This gauge's own reading
            {c.fetchedAt ? ` · checked ${new Date(c.fetchedAt).toISOString()
              .replace("T", " ").slice(0, 16)}` : ""}
          </Text>
        </View>
      ) : (
        <Text style={{ ...TYPE.body, color: palette.sub }}>
          No gauge is entitled to speak for this water. A station draining a far larger
          watershed would have given a number, and the number would have been wrong.
        </Text>
      )}

      {station && (
        <View style={{ gap: 10 }}>
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10 }}>
            <ChartControls<Parameter> palette={palette} value={shown} label="Quantity"
                                      onPick={pick}
                                      options={available.map((p) =>
                                        [p, p === "discharge" ? "Flow" : "Level"] as const)} />
            <ChartControls<Span> palette={palette} value={span} label="Span"
                                 onPick={setSpan}
                                 options={[["72h", "Last 72 hours"],
                                           ["year", "This year"]] as const} />
            {/* THE MODEL, when more than one is running. Both are shown rather than the
                app picking: they answer different questions, and in the shoulder months
                both are in season and disagree. */}
            <ChartControls<string> palette={palette} label="Forecast model"
                                   value={run?.model ?? ""} onPick={setModel}
                                   options={runs.map((f) =>
                                     [f.model, `${f.model} · ${f.horizonDays}d`] as const)} />
          </View>
          {chart.state === "loading" && (
            <View style={{ alignItems: "center", paddingVertical: 22 }}>
              <FishSpinner palette={palette} size={72} label="Loading the record" />
            </View>
          )}
          {chart.state === "ready" && chart.value && (
            <Hydrograph shape={chart.value} palette={palette} colour={tint}
                        unit={UNIT[shown]}
                        disclaimer={run?.disclaimer ?? null}
                        forecastLabel={run && forecast
                          ? `${run.model} · ${run.horizonDays} days` : undefined}
                        label={span === "72h" ? "the last 72 hours"
                                              : "the whole year against its record"}
                        caption={span === "year"
                          ? "Bands are this station's whole record for each five-day " +
                            "period. The bold line is this year to date, which the feed " +
                            "accumulates day by day — it starts a month long and grows, " +
                            "because the published daily record lags a year behind. The " +
                            "thin lines are the last complete years out of that record. " +
                            "The frame ends a month past today, where the longest " +
                            "forecast does."
                          : undefined} />
          )}
          {chart.state === "ready" && !chart.value && (
            <Text style={{ ...TYPE.small, color: palette.sub }}>
              {span === "year"
                ? "This station has no envelope in this quantity, so there is no season " +
                  "to draw it against."
                : "This station published no recent readings."}
            </Text>
          )}
          {/* WHEN the run was made, beside what it says. A forecast with no issue time is
              a claim with no age, and these are reissued once or twice a day. */}
          {run?.series && run.issuedAt && (
            <Text style={{ ...TYPE.small, fontSize: 11, color: palette.faint }}>
              {run.model} run issued {run.issuedAt.replace("T", " ").slice(0, 16)} ·
              {" "}{run.horizonDays}-day outlook
            </Text>
          )}
        </View>
      )}

      {/* THE SINGLE-STATION TRACE IS THE FALLBACK, not the companion: shown only where the
          panel could not answer, so water with no panel still gets the older, narrower
          explanation rather than an empty space. `GaugeTrace` is also still right for a
          saved spot, which carries one frozen trace and no panel at all. */}
      {!panel.answer.ok && trace.state === "ready" && trace.value.station && (
        <GaugeTrace trace={trace.value} palette={palette} at={tiles} theme={theme}
                    from={from} />
      )}
      {footer}
      <Credits palette={palette} lines={credits} />
    </>
  );

  return scroll
    ? <ScrollView contentContainerStyle={{ padding: 18, gap: 22, paddingBottom: 40 }}>
        {body}
      </ScrollView>
    : <View style={{ gap: 22 }}>{body}</View>;
}
