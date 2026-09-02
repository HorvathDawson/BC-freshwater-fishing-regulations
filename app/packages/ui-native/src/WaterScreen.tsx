/**
 * One water's sheet. Drawn to `design/riffle.html`.
 *
 * TWO FACES ON ONE SHEET, exactly as the design has it: **Conditions** and **Regulations**,
 * switched by a control under the title. They are not two screens because they are not two
 * subjects — a person looking at a river wants "may I fish here" and "what is the water
 * doing" about the SAME water, and making them two destinations means navigating away to
 * answer half of one question. They are not one scroll either: stacked, the conditions sat
 * below however many stretches the river happened to have, so a well-documented river hid
 * its own gauge behind sixty rules.
 *
 * Which face opens first is the caller's, because the tab you came from is the question you
 * were asking. Tapping a river on the Conditions tab opens Conditions; on the Map tab,
 * Regulations.
 *
 * Every value here arrived from a hook in @app/ui (rule 25). Nothing on this screen decides
 * whether a stretch is open, what a flow reading means, or whether a gauge may speak for
 * this water — read it as a list of the questions the app knows how to ask.
 */
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { statusWord, type PlainDate, type SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useConditions, useWaterGauge, useWaterSheet } from "@app/ui";
import { ConditionsPanel } from "./ConditionsPanel";
import { FishSpinner } from "./FishSpinner";
import { GaugeBadge } from "./GaugeBadge";
import { StatusPill } from "./StatusPill";
import { ordinal } from "./StatusChip";
import { TYPE } from "./type";
import { outcomeColour, type Palette } from "./theme";

export type Face = "conditions" | "regulations";

export function WaterScreen({ source, item, on, group, palette, onBack,
                              face: initialFace = "regulations", tiles, theme }: {
  source: RegsSource; item: ItemId; on: PlainDate; group: SpeciesGroup;
  palette: Palette; onBack?: () => void;
  /** Which question the reader arrived with. */
  face?: Face;
  /** Present, the Conditions face can draw the route down to the gauge. */
  tiles?: TileEndpoints; theme?: string;
}) {
  const [face, setFace] = useState<Face>(initialFace);
  const sheet = useWaterSheet(source, item, on, group);
  // Reaches run mouth -> source; conditions belong to the first, which is the stretch a
  // gauge is most likely to be entitled to speak for.
  const first: SectionId | null =
    sheet.state === "ready" && sheet.value ? sheet.value.reaches[0]?.section ?? null : null;
  // Only for the one line the Regulations face shows about the water's condition; the
  // Conditions face asks its own questions inside ConditionsPanel.
  const conditions = useConditions(source, first);
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
        <Faces palette={palette} face={face} onFace={setFace} />
      </View>

      {face === "conditions" ? (
        // THE SAME PANEL THE CONDITIONS TAB RENDERS. This face used to be its own layout
        // with its own wording and no chart controls, so the app answered "what is the
        // water doing" two different ways depending on which tab you came from.
        <View style={{ paddingHorizontal: 18, paddingTop: 18 }}>
          <ConditionsPanel source={source} section={first} palette={palette}
                           tiles={tiles} theme={theme} scroll={false}
                           colour={s.reaches[0]
                             ? outcomeColour(palette, s.reaches[0].status.outcome)
                             : undefined}
                           footer={
                             // The other face's answer, in one line — a reader on
                             // Conditions still has to be told the water is closed, and
                             // telling them to go and look is not telling them.
                             <CrossLink palette={palette} onPress={() => setFace("regulations")}
                                        label="and the regulations"
                                        value={s.reaches[0]
                                          ? statusWord(s.reaches[0].status)
                                          : "nothing written here"}
                                        tone={s.reaches[0]
                                          ? outcomeColour(palette, s.reaches[0].status.outcome)
                                          : palette.sub} />
                           } />
        </View>
      ) : (
        <>
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
          <View style={{ paddingHorizontal: 18, paddingTop: 20 }}>
            <CrossLink palette={palette} onPress={() => setFace("conditions")}
                       label="and the conditions"
                       value={c?.percentile != null
                         ? `${ordinal(c.percentile)} percentile for the date`
                         : c?.station
                           ? "a reading, but no record to compare it against"
                           : "no gauge speaks for this water"}
                       tone={palette.live} />
          </View>
        </>
      )}
    </ScrollView>
  );
}

/**
 * The face switch. Two buttons, both always visible, the current one pressed.
 *
 * NOT a tab bar and not a segmented control that hides the other label: which two questions
 * this sheet answers is itself information, and a reader who does not know Conditions exists
 * will never go looking for it.
 */
function Faces({ palette, face, onFace }: {
  palette: Palette; face: Face; onFace: (f: Face) => void;
}) {
  const items: [Face, string][] = [["conditions", "Conditions"], ["regulations", "Regulations"]];
  return (
    <View style={{ flexDirection: "row", gap: 8, paddingTop: 6 }}>
      {items.map(([id, label]) => {
        const on = face === id;
        return (
          <Pressable key={id} onPress={() => onFace(id)} accessibilityRole="button"
                     accessibilityState={{ selected: on }} accessibilityLabel={label}
                     style={{ paddingVertical: 8, paddingHorizontal: 16, borderRadius: 999,
                              borderWidth: 1,
                              borderColor: on ? palette.accent : palette.line,
                              backgroundColor: on ? palette.tint : "transparent" }}>
            <Text style={{ ...TYPE.micro, fontSize: 12,
                           color: on ? palette.accent : palette.sub }}>{label}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}

/** The other face's answer, in one line, and a way to get there. */
function CrossLink({ palette, label, value, tone, onPress }: {
  palette: Palette; label: string; value: string; tone: string; onPress: () => void;
}) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button"
               accessibilityLabel={`${label}: ${value}`}
               style={{ borderTopWidth: 1, borderTopColor: palette.line, paddingTop: 14,
                        gap: 5 }}>
      <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                     color: palette.faint }}>{label.toUpperCase()}</Text>
      <Text style={{ ...TYPE.body, color: tone }}>{value}</Text>
    </Pressable>
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
