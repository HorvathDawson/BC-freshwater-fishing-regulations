/**
 * One water's sheet. Drawn to `design/riffle.html`.
 *
 * THE REGULATIONS FACE, AND A DOOR TO THE CONDITIONS. The switch under the title shows both
 * questions a water answers — which is worth showing, because a reader who does not know
 * Conditions exists will never go looking for it — but pressing the other one LEAVES.
 *
 * REGULATIONS ARE NOT INTEGRATED. The face keeps its place and shows `RegulationsPlaceholder`
 * where the regulations will go; see `regulations.ts` in @app/core.
 *
 * This sheet used to render conditions in place. Sharing `ConditionsPanel` made the two
 * agree on the numbers and on nothing else: the Conditions tab's screen carries the reach
 * you actually tapped, the coordinate that puts "you are here" on the route map, and the
 * map's own flow/level choice, and a sheet opened from a search result knows none of those.
 * Two surfaces answering one question, with one of them permanently the poorer, is exactly
 * the drift AGENTS rule 23 exists to stop.
 *
 * Every value here arrived from a hook in @app/ui (rule 25). Nothing on this screen decides
 * what a flow reading means or whether a gauge may speak for this water.
 */
import { Pressable, ScrollView, Text, View } from "react-native";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { useWater } from "@app/ui";
import { FaceBar } from "./Faces";
import { FishSpinner } from "./FishSpinner";
import { RegulationsPlaceholder } from "./RegulationsPlaceholder";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export type Face = "conditions" | "regulations";

export function WaterScreen({ source, item, palette, onBack, onConditions }: {
  source: RegsSource; item: ItemId;
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
  const sheet = useWater(source, item);
  // Sections run mouth -> source; conditions belong to the first, which is the reach a
  // gauge is most likely to be entitled to speak for.
  const first: SectionId | null =
    sheet.state === "ready" && sheet.value ? sheet.value.sections[0] ?? null : null;
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
      </View>

      {/* ONE CONDITIONS SCREEN, REACHED ONE WAY. The toggle above NAVIGATES rather than
           rendering conditions here: the Conditions tab's screen carries the reach you
           actually tapped, the tap coordinate that puts "you are here" on the route map, and
           the map's own flow/level choice, none of which a sheet opened from a list can know. */}
      <Section palette={palette} title="Regulations" />
      <View style={{ paddingHorizontal: 18, paddingTop: 8 }}>
        <RegulationsPlaceholder palette={palette} />
      </View>
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

