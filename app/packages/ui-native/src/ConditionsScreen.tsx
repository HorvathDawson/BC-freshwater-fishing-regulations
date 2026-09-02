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
import { Pressable, ScrollView, Text, View } from "react-native";
import type { RegsSource, SectionId } from "@app/data";
import { standingWord, type Standing } from "@app/core";

const NO_RECORD: Standing = "no-record";
import { useConditions, useGaugeTrace, useHydrograph } from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function ConditionsScreen({ source, section, palette, onBack }: {
  source: RegsSource; section: SectionId; palette: Palette; onBack: () => void;
}) {
  const conditions = useConditions(source, section);
  const trace = useGaugeTrace(source, section);
  const c = conditions.state === "ready" ? conditions.value : null;
  const chart = useHydrograph(source, c?.station ?? null, "72h");

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

        {chart.state === "ready" && chart.value && (
          <Hydrograph shape={chart.value} palette={palette} colour={palette.live}
                      label="the last 72 hours" />
        )}

        {trace.state === "ready" && (
          <GaugeTrace trace={trace.value} palette={palette} />
        )}
      </ScrollView>
    </View>
  );
}
