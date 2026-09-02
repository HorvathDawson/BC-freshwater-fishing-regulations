/**
 * One water's sheet. Drawn to `design/riffle.html`.
 *
 * Every value here arrived from a hook in @app/ui (rule 25). Nothing on this screen decides
 * whether a stretch is open, what a flow reading means, or whether a gauge may speak for
 * this water — read it as a list of the questions the app knows how to ask.
 */
import { Pressable, ScrollView, Text, View } from "react-native";
import type { PlainDate, SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { useConditions, useGaugeTrace, useHydrograph, useWaterGauge, useWaterSheet }
  from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { GaugeBadge } from "./GaugeBadge";
import { GaugeTrace } from "./GaugeTrace";
import { Hydrograph } from "./Hydrograph";
import { StatusPill } from "./StatusPill";
import { ordinal } from "./StatusChip";
import { TYPE } from "./type";
import { outcomeColour, type Palette } from "./theme";

export function WaterScreen({ source, item, on, group, palette, onBack }: {
  source: RegsSource; item: ItemId; on: PlainDate; group: SpeciesGroup;
  palette: Palette; onBack?: () => void;
}) {
  const sheet = useWaterSheet(source, item, on, group);
  // Reaches run mouth -> source; conditions belong to the first, which is the stretch a
  // gauge is most likely to be entitled to speak for.
  const first: SectionId | null =
    sheet.state === "ready" && sheet.value ? sheet.value.reaches[0]?.section ?? null : null;
  const conditions = useConditions(source, first);
  const station = conditions.state === "ready" ? conditions.value.station : null;
  const chart = useHydrograph(source, station, "72h");
  const trace = useGaugeTrace(source, first);
  // Asked of the WATER, not of `first`: most of a well-gauged river has no station on the
  // stretch you happen to be looking at, and answering "ungauged" there would be false.
  const gauged = useWaterGauge(source, item);

  if (sheet.state === "loading") {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card,
                     alignItems: "center", justifyContent: "center" }}>
        <FishSpinner palette={palette} size={110} label="Loading this water" />
      </View>
    );
  }
  if (sheet.state === "failed" || !sheet.value) {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card, padding: 20, gap: 10 }}>
        {onBack && <Back palette={palette} onPress={onBack} />}
        <Text style={{ ...TYPE.body, color: palette.closed }}>
          {sheet.state === "failed" ? String(sheet.error) : "No such water in this bundle."}
        </Text>
      </View>
    );
  }

  const s = sheet.value;
  const c = conditions.state === "ready" ? conditions.value : null;

  return (
    <ScrollView style={{ flex: 1, backgroundColor: palette.card }}
                contentContainerStyle={{ paddingBottom: 32 }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14, gap: 10 }}>
        {onBack && <Back palette={palette} onPress={onBack} />}
        <Text style={{ ...TYPE.title, color: palette.ink }}>{s.name}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          {s.reaches.length} {s.reaches.length === 1 ? "stretch" : "stretches"}
          {" · "}{s.rules.length} written {s.rules.length === 1 ? "rule" : "rules"}
        </Text>
        {/* Before any reading: is there anything measuring this at all. Rendered while
            still loading as nothing rather than as "not measured" — an unanswered
            question must never be shown as a negative answer. */}
        {gauged.state === "ready" && (
          <View style={{ paddingTop: 4 }}>
            <GaugeBadge gauge={gauged.value} palette={palette} waterName={s.name} />
          </View>
        )}
      </View>

      <Section palette={palette} title="Stretches" />
      {s.reaches.map((r) => (
        <View key={r.section}
              style={{ paddingHorizontal: 18, paddingVertical: 14, gap: 9,
                       borderTopWidth: 1, borderTopColor: palette.line,
                       flexDirection: "row" }}>
          {/* colour is the outcome; it never carries the whole message alone */}
          <View style={{ width: 4, borderRadius: 2, alignSelf: "stretch",
                         backgroundColor: outcomeColour(palette, r.status.outcome) }} />
          <View style={{ flex: 1, gap: 8 }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
              {r.lowerLabel ?? "the mouth"} → {r.upperLabel ?? "the source"}
            </Text>
            <View style={{ flexDirection: "row" }}>
              <StatusPill status={r.status} palette={palette} />
            </View>
            {r.status.from.length > 0 && (
              <Text style={{ ...TYPE.small, color: palette.sub }}>
                {r.status.from.map((rule) => rule.kind.replace(/_/g, " ")).join(" · ")}
              </Text>
            )}
          </View>
        </View>
      ))}

      <Section palette={palette} title="Conditions" />
      <View style={{ paddingHorizontal: 18, paddingTop: 12, gap: 14 }}>
        {c?.station ? (
          <>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{c.stationName}</Text>
            <View style={{ flexDirection: "row", flexWrap: "wrap", columnGap: 26, rowGap: 12 }}>
              <Figure palette={palette} label="flow" tone={palette.live}
                      value={c.discharge === null ? "—" : `${c.discharge}`}
                      unit={c.discharge === null ? undefined : "m³/s"} />
              <Figure palette={palette} label="against record"
                      value={c.percentile === null ? "—" : ordinal(c.percentile)} />
              <Figure palette={palette} label="standing"
                      value={(c.standing ?? "—").replace(/-/g, " ")} />
            </View>
            {chart.state === "ready" && chart.value && (
              <Hydrograph shape={chart.value} palette={palette}
                          colour={outcomeColour(palette, s.reaches[0]!.status.outcome)}
                          label={`Flow on ${s.name} over three days`} />
            )}
            {chart.state === "loading" && (
              <View style={{ alignItems: "center", paddingVertical: 22 }}>
                <FishSpinner palette={palette} size={72} label="Loading flow" />
              </View>
            )}
            {/* The SAME panel a saved spot shows, from the same component. The sentence
                about what a gauge can and cannot speak for is written once, in core. */}
            {trace.state === "ready" && (
              <View style={{ marginTop: 4 }}>
                <GaugeTrace trace={trace.value} palette={palette}
                            title="How this water reaches the gauge" />
              </View>
            )}
          </>
        ) : (
          <Text style={{ ...TYPE.small, color: palette.sub }}>
            No gauge is entitled to speak for this water. A station draining a far larger
            watershed would give a number, and the number would be wrong.
          </Text>
        )}
      </View>
    </ScrollView>
  );
}

function Section({ palette, title }: { palette: Palette; title: string }) {
  return (
    <Text style={{ ...TYPE.section, color: palette.faint,
                   paddingHorizontal: 18, paddingTop: 22, paddingBottom: 2 }}>
      {title}
    </Text>
  );
}

function Back({ palette, onPress }: { palette: Palette; onPress: () => void }) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel="Back">
      <Text style={{ ...TYPE.micro, color: palette.accent }}>‹  Back</Text>
    </Pressable>
  );
}

function Figure({ palette, label, value, unit, tone }: {
  palette: Palette; label: string; value: string; unit?: string; tone?: string;
}) {
  return (
    <View style={{ gap: 3 }}>
      <Text style={{ ...TYPE.section, fontSize: 10, color: palette.faint }}>{label}</Text>
      <View style={{ flexDirection: "row", alignItems: "baseline", gap: 4 }}>
        <Text style={{ ...TYPE.figureBig, color: tone ?? palette.ink }}>{value}</Text>
        {unit && <Text style={{ ...TYPE.figure, fontSize: 12, color: palette.sub }}>{unit}</Text>}
      </View>
    </View>
  );
}
