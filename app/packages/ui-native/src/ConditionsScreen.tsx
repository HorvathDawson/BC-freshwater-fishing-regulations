/**
 * What one reach is doing right now — the Conditions counterpart to the rules sheet.
 *
 * TWO TABS, TWO QUESTIONS. Tapping a river on the Map tab asks "may I fish here"; tapping
 * it on Conditions asks "what is the water doing". They were one screen, and the answer to
 * the second was buried under the answer to the first.
 *
 * The trace panel is the SAME component a saved spot uses, fed live instead of frozen. A
 * gauge's representativeness is the most misread number in this app and may not have two
 * explanations — see `packages/core/src/trace.ts`.
 */
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import type { Parameter, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { standingWord, type Standing } from "@app/core";

const NO_RECORD: Standing = "no-record";
import { useConditions, useGaugeParameters, useGaugeTrace, useHydrograph } from "@app/ui";
import { ChartControls } from "./ChartControls";
import { FishSpinner } from "./FishSpinner";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import type { Palette } from "./theme";

type Span = "72h" | "year";

const UNIT: Record<Parameter, string> = { discharge: "m³/s", level: "m" };

export function ConditionsScreen({ source, section, palette, onBack, tiles, theme }: {
  source: RegsSource; section: SectionId; palette: Palette; onBack: () => void;
  /** Present, the route panel draws the chain of reaches down to the station. */
  tiles?: TileEndpoints; theme?: string;
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
  const [pick, setPick] = useState<Parameter | null>(null);
  const [span, setSpan] = useState<Span>("72h");
  // The reader's choice, unless the station cannot answer in it — then whatever the reading
  // itself is about, which is what the percentile was computed against.
  const param: Parameter | undefined =
    pick && available.includes(pick) ? pick : undefined;
  const shown: Parameter = param
    ?? (c?.discharge !== null && c?.discharge !== undefined ? "discharge" : "level");
  const chart = useHydrograph(source, station, span, param);

  if (conditions.state === "loading")
    return (
      <View style={{ flex: 1, backgroundColor: palette.card, alignItems: "center",
                     justifyContent: "center" }}>
        <FishSpinner palette={palette} size={110} label="Reading the water" />
      </View>
    );

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14 }}>
        <Pressable onPress={onBack} accessibilityRole="button" accessibilityLabel="Back">
          <Text style={{ ...TYPE.micro, fontSize: 13, color: palette.accent }}>
            ‹  Conditions
          </Text>
        </Pressable>
      </View>

      <ScrollView contentContainerStyle={{ padding: 18, gap: 22, paddingBottom: 40 }}>
        {c?.discharge != null || c?.level != null ? (
          <View style={{ gap: 6 }}>
            <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                           flexWrap: "wrap" }}>
              <Text style={{ ...TYPE.figureBig, fontSize: 34, color: palette.live }}>
                {c.discharge ?? c.level}
              </Text>
              <Text style={{ ...TYPE.figure, color: palette.sub }}>
                {c.discharge != null ? "m³/s" : "m"}
              </Text>
              {/* BOTH NUMBERS WHERE THERE ARE BOTH. A station measuring stage and discharge
                  has two readings a person may want, and hiding one behind the chart
                  toggle makes the sheet answer a question it was not asked. */}
              {c.discharge != null && c.level != null && (
                <Text style={{ ...TYPE.figure, fontSize: 13, color: palette.faint }}>
                  {c.level} m stage
                </Text>
              )}
            </View>
            <Text style={{ ...TYPE.body, color: palette.ink }}>
              {c.percentile != null
                // The word comes from core, so this screen and the map agree about what
                // "low" means rather than each deciding.
                // `standing` is null when nothing computed one; core's own vocabulary
                // calls that "no-record" rather than leaving it unsaid.
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
                                        onPick={setPick}
                                        options={available.map((p) =>
                                          [p, p === "discharge" ? "Flow" : "Level"] as const)} />
              <ChartControls<Span> palette={palette} value={span} label="Span"
                                   onPick={setSpan}
                                   options={[["72h", "Last 72 hours"],
                                             ["year", "Whole year"]] as const} />
            </View>
            {chart.state === "loading" && (
              <View style={{ alignItems: "center", paddingVertical: 22 }}>
                <FishSpinner palette={palette} size={72} label="Loading the record" />
              </View>
            )}
            {chart.state === "ready" && chart.value && (
              <Hydrograph shape={chart.value} palette={palette} colour={palette.live}
                          unit={UNIT[shown]}
                          label={span === "72h" ? "the last 72 hours"
                                                : "the whole year against its record"}
                          caption={span === "year"
                            ? "Bands are this station's whole record for each five-day " +
                              "period of the year — the middle half, then the 10th to " +
                              "90th. The dot is today's reading. There is no line for " +
                              "this year: the daily record is not published until the " +
                              "next HYDAT release."
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
          </View>
        )}

        {trace.state === "ready" && (
          <GaugeTrace trace={trace.value} palette={palette} at={tiles} theme={theme} />
        )}
      </ScrollView>
    </View>
  );
}
