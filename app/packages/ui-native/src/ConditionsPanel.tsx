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
import { useState } from "react";
import { ScrollView, Text, View } from "react-native";
import { standingWord, type Standing } from "@app/core";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useConditions, useGaugeParameters, useGaugeTrace, useHydrograph,
         useSeries } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { FishSpinner } from "./FishSpinner";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** core's own word for "a reading, but nothing to compare it against". */
const NO_RECORD: Standing = "no-record";

export const UNIT: Record<Parameter, string> = { discharge: "m³/s", level: "m" };

export type Span = "72h" | "year";

export function ConditionsPanel({ source, section, palette, tiles, theme, colour,
                                  parameter, onParameter, from, scroll = true, footer }: {
  source: RegsSource; section: SectionId | null; palette: Palette;
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
  /** Where the reader tapped, so the route map can mark where they are standing. */
  from?: { lat: number; lon: number } | null;
  scroll?: boolean;
  footer?: React.ReactNode;
}) {
  const conditions = useConditions(source, section);
  const trace = useGaugeTrace(source, section);
  const c = conditions.state === "ready" ? conditions.value : null;
  const station = c?.station ?? null;

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
      {c?.discharge != null || c?.level != null ? (
        <View style={{ gap: 6 }}>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                         flexWrap: "wrap" }}>
            <Text style={{ ...TYPE.figureBig, fontSize: 34, color: tint }}>
              {c.discharge ?? c.level}
            </Text>
            <Text style={{ ...TYPE.figure, color: palette.sub }}>
              {c.discharge != null ? "m³/s" : "m"}
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
                `${Math.round(c.percentile * 100)}th percentile for the date`
              : "There is a reading here, but no record to compare it against."}
          </Text>
          <Text style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>
            {c.stationName ?? c.station}
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

      {trace.state === "ready" && (
        <GaugeTrace trace={trace.value} palette={palette} at={tiles} theme={theme}
                    from={from} />
      )}
      {footer}
    </>
  );

  return scroll
    ? <ScrollView contentContainerStyle={{ padding: 18, gap: 22, paddingBottom: 40 }}>
        {body}
      </ScrollView>
    : <View style={{ gap: 22 }}>{body}</View>;
}
