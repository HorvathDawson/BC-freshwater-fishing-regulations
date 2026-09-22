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
import { stretchLabel, type PlainDate, type SpeciesGroup } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { TileEndpoints } from "@app/map";
import { useWaterSheet } from "@app/ui";
import { FaceBar } from "./Faces";
import { FishSpinner } from "./FishSpinner";
import { RulesPlaceholder } from "./RulesPlaceholder";
import { StatusPill } from "./StatusPill";
import { TYPE } from "./type";
import { outcomeColour, type Palette } from "./theme";
import { plural } from "./format";

export type Face = "conditions" | "regulations";

export function WaterScreen({ source, item, on, group, palette, onBack,
                              onConditions }: {
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
  /*
   * NO CONDITIONS HOOK RUNS HERE, deliberately.
   *
   * When the conditions block came out of this screen its DATA did not: `useConditions`,
   * `usePanel` and `useWaterGauge` all kept running, so opening a water from a search result
   * still read the gauge index, walked the donor panel and resolved the water's station, and
   * then rendered none of it. Invisible work is the cheapest kind of drift to reintroduce —
   * the next person to want a number here finds three hooks already wired and no reason not
   * to print one, and the two surfaces diverge again. The reach is all this screen needs;
   * the Conditions screen asks the questions.
   */

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

  return (
    <ScrollView style={{ flex: 1, backgroundColor: palette.card }}
                contentContainerStyle={{ paddingBottom: 32 }}>
      <View style={{ paddingHorizontal: 18, paddingTop: 14, gap: 10 }}>
        {/* THE SAME ROW, IN THE SAME PLACE, on both screens — see FaceBar. */}
        <FaceBar palette={palette} face="regulations" onBack={onBack} backLabel="Back"
                 onFace={(f) => { if (f === "conditions" && first) onConditions?.(first); }} />
        <Text style={{ ...TYPE.title, color: palette.ink }}>{s.name}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>
          {plural(s.reaches.length, "set of rules", "sets of rules")}
          {" \u00b7 "}{plural(s.rules.length, "written rule", "written rules")}
        </Text>
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
      {/*
          THE RULE REGIMES, NOT THE ATLAS'S CUTS.

          This listed one row per section, and the atlas cuts a river at confluences, lake
          outlets, gauge matches and a 25 km cap — none of which is a reason a REGULATION
          changes. The Fraser rendered 248 rows, every one of them reading the same, and the
          stretch where the rules actually change was indistinguishable from its neighbours.

          The bundle already interned the rule sets, so "which sections answer identically"
          is a number it hands over rather than something to work out here. The Fraser is 16
          rows now. A regime that applies in more than one place says so, because "Fraser
          River, closed" in three separate pieces is not one stretch and must not read as
          one. */}
      <Section palette={palette} title="Rules along this water" />
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
              {stretchLabel({ lower: r.lowerLabel, upper: r.upperLabel }, i,
                            s.reaches.length)}
            </Text>
            {r.pieces > 1 && (
              <Text style={{ ...TYPE.small, color: palette.faint }}>
                {plural(r.pieces, "separate stretch", "separate stretches")} of this water
              </Text>
            )}
            <View style={{ flexDirection: "row" }}>
              <StatusPill status={r.status} palette={palette} />
            </View>
            {/* THE TABLE GOES HERE — see RulesPlaceholder, and
                `pipeline/docs/06-ui-data-contract.md`. What stood here joined each rule's
                generated label with dots, which names the shape of a rule and never its
                content; a reader could neither act on it nor check it against the book. */}
            <RulesPlaceholder palette={palette} count={r.status.from.length} />
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
                       /*
                        * A DOOR, NOT A PREVIEW.
                        *
                        * This quoted a percentile — from `section_gauge`'s single matched
                        * station, while the screen one tap away quoted the donor panel. The
                        * same water carried two different numbers depending on which door
                        * you came through, which is the failure this app keeps producing
                        * and the one hardest to notice, because both look right alone.
                        *
                        * The fix is not to make them agree. It is that THIS SCREEN IS THE
                        * REGULATIONS FACE and conditions are not its subject: there is a
                        * screen for that, it is one tap away, and it has the reach you
                        * tapped, the coordinate for "you are here", the donor panel and the
                        * forecast horizons. A summary here can only ever be a worse copy.
                        */
                       value="what the water is doing"
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

