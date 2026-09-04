/**
 * One water's sheet. Drawn to `design/riffle.html`.
 *
 * THE RULES, AND A DOOR TO THE CONDITIONS. The switch under the title shows both questions
 * a water answers — which is worth showing, because a reader who does not know Conditions
 * exists will never go looking for it — but pressing the other one LEAVES.
 *
 * This sheet used to render conditions in place. Sharing `ConditionsPanel` made the two
 * agree on the numbers and on nothing else: the Conditions tab's screen carries the reach
 * you actually tapped, the coordinate that puts "you are here" on the route map, and the
 * map's own flow/level choice, and a sheet opened from a search result knows none of those.
 * Two surfaces answering one question, with one of them permanently the poorer, is exactly
 * the drift AGENTS rule 23 exists to stop.
 *
 * Every value here arrived from a hook in @app/ui (rule 25). Nothing on this screen decides
 * whether a stretch is open, what a flow reading means, or whether a gauge may speak for
 * this water — read it as a list of the questions the app knows how to ask.
 */
import { Pressable, ScrollView, Text, View } from "react-native";
import { NO_GAUGE, NO_READING_HERE, type PlainDate,
         type SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useConditions, useWaterGauge, useWaterSheet } from "@app/ui";
import { FaceBar } from "./Faces";
import { FishSpinner } from "./FishSpinner";
import { GaugeBadge } from "./GaugeBadge";
import { StatusPill } from "./StatusPill";
import { ordinal } from "./StatusChip";
import { TYPE } from "./type";
import { outcomeColour, type Palette } from "./theme";
import { plural } from "./format";

export type Face = "conditions" | "regulations";

export function WaterScreen({ source, item, on, group, palette, onBack, onConditions }: {
  source: RegsSource; item: ItemId; on: PlainDate; group: SpeciesGroup;
  palette: Palette; onBack?: () => void;
  /**
   * Leave for the Conditions screen, on this water's first reach.
   *
   * A CALLBACK RATHER THAN A FACE. Rendering conditions here as well gave the app two
   * surfaces answering one question, and only one of them could have the things that make
   * it useful — the reach you tapped, where on it you tapped, and the map's own quantity.
   */
  onConditions?: (section: SectionId) => void;
}) {
  const sheet = useWaterSheet(source, item, on, group);
  // Reaches run mouth -> source; conditions belong to the first, which is the stretch a
  // gauge is most likely to be entitled to speak for.
  const first: SectionId | null =
    sheet.state === "ready" && sheet.value ? sheet.value.reaches[0]?.section ?? null : null;
  // ONE LINE ONLY. This sheet says what the water is doing in a sentence and offers a way
  // to the screen that says it properly; it does not answer the question itself.
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
        {/* THE SAME ROW, IN THE SAME PLACE, on both screens — see FaceBar. */}
        <FaceBar palette={palette} face="regulations" onBack={onBack} backLabel="Back"
                 onFace={(f) => { if (f === "conditions" && first) onConditions?.(first); }} />
        <Text style={{ ...TYPE.title, color: palette.ink }}>{s.name}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          {plural(s.reaches.length, "stretch", "stretches")}
          {" \u00b7 "}{plural(s.rules.length, "written rule", "written rules")}
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

      {/* ONE CONDITIONS SCREEN, REACHED ONE WAY.

           This used to render `ConditionsPanel` in place, which meant the app still had two
           conditions surfaces: the tab's, opened by tapping a reach, and this one, opened by
           toggling here. Sharing the panel made them agree on the numbers and not on
           anything else — the tab's carries the reach you actually tapped, the tap
           coordinate that puts "you are here" on the route map, and the map's own flow/level
           choice, none of which a sheet opened from a list can know.

           So the toggle NAVIGATES. `Faces` is still two buttons because which two questions
           exist about a water is itself worth showing, but pressing Conditions leaves for
           the one screen that answers it. */}
      <Section palette={palette} title="Stretches" />
      {s.reaches.map((r, i) => (
        <View key={r.section}
              style={{ paddingHorizontal: 18, paddingVertical: 14, gap: 9,
                       borderTopWidth: 1, borderTopColor: palette.line,
                       flexDirection: "row" }}>
          {/* colour is the outcome; it never carries the whole message alone */}
          <View style={{ width: 4, borderRadius: 2, alignSelf: "stretch",
                         backgroundColor: outcomeColour(palette, r.status.outcome) }} />
          <View style={{ flex: 1, gap: 8 }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>
              {stretchLabel(r, i, s.reaches.length)}
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
            {/*
              THE BADGE AND THIS LINE MUST NOT CONTRADICT EACH OTHER, and they did.

              They ask different questions on purpose — the badge asks of the WATER
              (`useWaterGauge`), this asks of its FIRST reach (`useConditions`) — so a river
              whose first stretch has no station showed "Gauged nearby · FRASER RIVER AT
              MISSION" three lines above "no gauge speaks for this water". Both were true of
              their own question and the screen was still wrong.

              So the absence has two different wordings, and which one is used depends on
              what the badge just said. Both come from core, because a sentence written
              twice is a sentence that drifts (rule 23) — `NO_GAUGE` was exported and
              imported by nobody while this file spelled it differently.
            */}
            <CrossLink palette={palette}
                       onPress={() => { if (first) onConditions?.(first); }}
                       label="and the conditions"
                       value={c?.percentile != null
                         ? `${ordinal(c.percentile)} percentile for the date`
                         : c?.station
                           ? "a reading, but no record to compare it against"
                           : gauged.state === "ready" && gauged.value
                             ? NO_READING_HERE
                             : NO_GAUGE}
                       tone={palette.live} />
      </View>
    </ScrollView>
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

/** The plain back link, for the states where there is no sheet to put a FaceBar over. */
function Back({ palette, onPress }: { palette: Palette; onPress: () => void }) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel="Back">
      <Text style={{ ...TYPE.micro, color: palette.accent }}>‹  Back</Text>
    </Pressable>
  );
}

/**
 * What one stretch is called in the list.
 *
 * `lowerLabel`/`upperLabel` come from a rule's extent, and the bundle does not carry rules
 * yet — so on the province build EVERY reach had both null and the fallback rendered the
 * same string for all of them. The Fraser River showed 201 rows each reading
 * "the mouth → the source", which is not a list, and 6,521 waters have more than one reach.
 *
 * Unlabelled reaches are still distinct pieces of river with their own status; what is
 * missing is a name for their ENDS. So number them, which distinguishes without inventing
 * geography. The mouth-to-source wording survives only where it is true: a water that is
 * one reach really does run the whole way.
 */
function stretchLabel(r: { lowerLabel?: string | null; upperLabel?: string | null },
                      i: number, n: number): string {
  if (r.lowerLabel || r.upperLabel)
    return `${r.lowerLabel ?? "the mouth"} \u2192 ${r.upperLabel ?? "the source"}`;
  return n === 1 ? "the mouth \u2192 the source" : `Stretch ${i + 1} of ${n}`;
}
