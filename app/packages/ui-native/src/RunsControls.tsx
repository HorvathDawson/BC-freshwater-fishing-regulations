/**
 * (T261) THE RUNS TAB'S CONTROLS (owner 2026-10-29: "really nice"): the SPECIES chips over the map — several at once,
 * each with the fish the regulations already draw (`FISH_IMAGES`, by `RUN_SPECIES_CODE`; a species with no picture is
 * its name alone) — and the DATE SLIDER under it, which scrubs the year by pentad (five days), marks today, plays
 * through the year, and wears the season at a glance behind its track (the share of watershed groups with these runs
 * in the water, `RunTimingFacts.season`). Every word is core's (`RUN_WORDS`); every colour a palette or map token.
 */
import { FISH, MONTH_PENTADS, RUN_SPECIES_CODE, RUN_WORDS, TAB_TOUR_WORDS, pentadLabel, sliderKey, type RunSpecies } from "@app/core";
import { runStateColour } from "@app/map/colours";
import { useMemo, useRef, useState } from "react";
import { Image, PanResponder, Pressable, ScrollView, Text, View } from "react-native";
import { useCoachTarget } from "./CoachMarks";
import { Icon } from "./icons";
import { hitProps } from "./regs/hit";
import type { FishImages } from "./regs/types";
import { LEGEND_TEXT, TYPE } from "./type";
import { round, type Palette } from "./theme";

const CHIP_HIT = hitProps({ w: 72, h: 40 });

/** The species chips: a row of checkboxes, the fish and its name; the last one chosen cannot be cleared. */
export function RunSpeciesChips({ species, chosen, onToggle, fishImages, palette }: {
  species: readonly RunSpecies[]; chosen: readonly RunSpecies[]; onToggle: (name: string) => void;
  fishImages?: FishImages; palette: Palette;
}) {
  return (
    // a phone is narrower than six species: the row scrolls sideways rather than run under the map's buttons
    <ScrollView horizontal showsHorizontalScrollIndicator={false} style={{ width: "100%", flexGrow: 0 }}
                contentContainerStyle={{ gap: 6, paddingRight: 8 }}>
      <View testID="run-species" accessibilityRole="toolbar" accessibilityLabel={RUN_WORDS.speciesLabel}
            style={{ flexDirection: "row", gap: 6 }}>
        {species.map((s) => {
          const on = chosen.some((c) => c.name === s.name);
          // (T367, owner 2026-11-03) the run's own look: the spawning picture, where the species has one
          const code = RUN_SPECIES_CODE[s.name] ?? "";
          const art = fishImages?.[code]?.[Math.max(0, (FISH[code] ?? []).findIndex((l) => l.phase === "spawning"))];
          return (
            <Pressable key={s.name} testID={`run-species-${s.name}`} onPress={() => onToggle(s.name)}
                       accessibilityRole="checkbox" aria-checked={on} accessibilityLabel={s.name} {...CHIP_HIT}>
              <View style={{ flexDirection: "row", alignItems: "center", gap: 6, height: 40, paddingLeft: art ? 6 : 14,
                             paddingRight: 12, borderRadius: palette.r.pill, borderWidth: on ? 1.5 : 1,
                             borderColor: on ? palette.ink : palette.line2,
                             backgroundColor: on ? palette.wash : palette.card, ...palette.lift }}>
                {art && (
                  <Image source={art} resizeMode="contain" accessibilityIgnoresInvertColors
                         style={{ width: 40, height: 20, opacity: on ? 1 : 0.55 }} />
                )}
                <Text style={{ ...TYPE.micro, fontSize: 15, lineHeight: 19, fontWeight: on ? "700" : "500",
                               color: on ? palette.ink : palette.sub }}>{s.name}</Text>
              </View>
            </Pressable>
          );
        })}
      </View>
    </ScrollView>
  );
}

const MONTH_LETTER = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"];
const PLAY_HIT = hitProps({ w: 48, h: 48 });
/** (T276) a phone-sized slider: a 48 px touch row, a 28 px thumb, a 5 px track */
const TRACK_H = 48, THUMB = 28, LINE = 5;

/**
 * THE DATE SLIDER: 73 pentads on one track. A press or a drag anywhere on it moves the day; the arrow keys and a
 * screen reader's adjust step it by one. Today is a mark on the track and a button back to it.
 */
export function RunDaySlider({ pentad, today, playing, onPentad, onPlay, season, theme, palette }: {
  pentad: number; today: number; playing: boolean; onPentad: (p: number) => void; onPlay: (on: boolean) => void;
  season: readonly { spawn: number; pass: number }[]; theme: string; palette: Palette;
}) {
  const target = useCoachTarget("runs:slider");
  const width = useRef(0);
  const start = useRef(0);
  const set = useRef(onPentad);
  set.current = onPentad;
  const toP = (x: number) => Math.round(Math.max(0, Math.min(1, width.current ? x / width.current : 0)) * 72);
  const pan = useMemo(() => PanResponder.create({
    onStartShouldSetPanResponder: () => true,
    onMoveShouldSetPanResponder: () => true,
    onPanResponderTerminationRequest: () => false,
    onPanResponderGrant: (e) => { start.current = e.nativeEvent.locationX; set.current(toP(start.current)); },
    onPanResponderMove: (_e, g) => set.current(toP(start.current + g.dx)),
  // eslint-disable-next-line react-hooks/exhaustive-deps -- reads refs only
  }), []);
  const [w, setW] = useState(0);
  const x = (p: number) => (p / 72) * w;
  const spawn = runStateColour(theme, "spawn"), pass = runStateColour(theme, "pass");
  // the season's SHAPE: scaled to its own busiest pentad, so a species in few groups still reads as a curve
  const peak = Math.max(0.0001, ...season.map((s) => s.spawn + s.pass));
  const h = (v: number) => Math.round((v / peak) * 20);
  const step = (d: number) => onPentad(Math.max(0, Math.min(72, pentad + d)));
  const keys = { onKeyDown: (e: { key: string; preventDefault?: () => void }) => {
    const to = sliderKey(e.key, pentad);
    if (to !== null) { e.preventDefault?.(); onPentad(to); }
  } } as object;
  return (
    <View testID="run-slider" ref={target} collapsable={false} style={{ flexDirection: "row", alignItems: "center", gap: 12 }}>
      <Pressable testID="run-play" onPress={() => onPlay(!playing)} accessibilityRole="button"
                 accessibilityLabel={playing ? RUN_WORDS.slider.pause : RUN_WORDS.slider.play} aria-pressed={playing}
                 {...PLAY_HIT}
                 style={{ width: 48, height: 48, borderRadius: round(48), alignItems: "center", justifyContent: "center",
                          backgroundColor: palette.accent }}>
        <Icon name={playing ? "pause" : "play"} colour={palette.onAccent} size={22} />
      </Pressable>
      <View style={{ flex: 1 }}>
        <View testID="run-slider-track" {...pan.panHandlers} {...keys} focusable
              accessibilityRole="adjustable" accessibilityLabel={RUN_WORDS.slider.label}
              accessibilityValue={{ min: 0, max: 72, now: pentad, text: RUN_WORDS.slider.value(pentad, pentad === today) }}
              aria-valuemin={0} aria-valuemax={72} aria-valuenow={pentad}
              aria-valuetext={RUN_WORDS.slider.value(pentad, pentad === today)}
              accessibilityActions={[{ name: "increment" }, { name: "decrement" }]}
              onAccessibilityAction={(e) => step(e.nativeEvent.actionName === "increment" ? 1 : -1)}
              onLayout={(e) => { width.current = e.nativeEvent.layout.width; setW(e.nativeEvent.layout.width); }}
              style={{ height: TRACK_H, justifyContent: "center", cursor: "pointer" } as object}>
          {/* the season behind the track: spawning up from the line, passing above it, both at the map's colours,
              scaled to the busiest pentad */}
          <View pointerEvents="none" style={{ position: "absolute", left: 0, right: 0, bottom: TRACK_H / 2 - 2, height: 20,
                                               flexDirection: "row", alignItems: "flex-end" }}>
            {season.map((s, i) => (
              <View key={i} style={{ flex: 1, marginHorizontal: 0.25 }}>
                <View style={{ height: h(s.pass), backgroundColor: pass, opacity: 0.45 }} />
                <View style={{ height: h(s.spawn), backgroundColor: spawn, opacity: 0.7 }} />
              </View>
            ))}
          </View>
          <View pointerEvents="none" style={{ height: LINE, borderRadius: palette.r.chip, backgroundColor: palette.line2 }} />
          {w > 0 && (
            <>
              {/* today: a short ink tick through the track */}
              <View pointerEvents="none" testID="run-slider-today"
                    style={{ position: "absolute", left: x(today) - 1.5, top: TRACK_H / 2 - 12, width: 3, height: 24,
                             backgroundColor: palette.ink }} />
              <View pointerEvents="none"
                    style={{ position: "absolute", left: x(pentad) - THUMB / 2, top: (TRACK_H - THUMB) / 2, width: THUMB, height: THUMB, borderRadius: round(THUMB),
                             backgroundColor: palette.card, borderWidth: 4, borderColor: palette.accent, ...palette.lift }} />
            </>
          )}
        </View>
        <View pointerEvents="none" style={{ height: 16 }}>
          {w > 0 && MONTH_PENTADS.map((p, m) => (
            <Text key={m} style={{ ...TYPE.month, fontSize: 12, lineHeight: 15, position: "absolute", left: x(p), color: palette.sub }}>
              {MONTH_LETTER[m]}
            </Text>
          ))}
        </View>
      </View>
    </View>
  );
}

/** The slider's caption: the days shown, big, and a way back to today when it has moved. */
export function RunDayCaption({ pentad, today, title, onToday, onHelp, palette }: {
  pentad: number; today: number; title: string | null; onToday: () => void; palette: Palette;
  /** (T276) the "?" that replays the Runs tutorial */
  onHelp?: () => void;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
      <View style={{ flex: 1 }}>
        <Text testID="run-day" style={{ ...TYPE.bodyStrong, fontSize: 17, color: palette.ink }}>
          {RUN_WORDS.slider.value(pentad, pentad === today)}
        </Text>
        {title && (
          <Text testID="runs-legend-title" numberOfLines={1} style={{ ...TYPE.small, fontSize: LEGEND_TEXT, color: palette.sub }}>
            {title}
          </Text>
        )}
      </View>
      {pentad !== today && (
        <Pressable testID="run-today" onPress={onToday} accessibilityRole="button"
                   accessibilityLabel={`${RUN_WORDS.slider.today}: ${pentadLabel(today)}`} {...hitProps({ w: 64, h: 36 })}
                   style={{ height: 36, paddingHorizontal: 14, borderRadius: palette.r.pill, borderWidth: 1,
                            borderColor: palette.line2, justifyContent: "center" }}>
          <Text style={{ ...TYPE.micro, fontSize: 14, fontWeight: "600", color: palette.ink }}>{RUN_WORDS.slider.today}</Text>
        </Pressable>
      )}
      {onHelp && (
        <Pressable testID="run-help" onPress={onHelp} accessibilityRole="button" accessibilityLabel={TAB_TOUR_WORDS.label.runs}
                   {...hitProps({ w: 44, h: 44 })}
                   style={{ width: 32, height: 32, borderRadius: round(32), borderWidth: 1.5, borderColor: palette.line2,
                            alignItems: "center", justifyContent: "center" }}>
          <Text style={{ ...TYPE.bodyStrong, fontSize: 16, color: palette.ink }}>?</Text>
        </Pressable>
      )}
    </View>
  );
}
