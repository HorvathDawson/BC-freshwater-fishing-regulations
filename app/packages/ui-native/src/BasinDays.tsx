/**
 * (T377 item 1, owner idea T367 round 3 A2) THE FORECAST DAYS OF ONE WATERSHED GROUP — a compact card: the group's name
 * ("Chilliwack River watershed"), what the colours rank, then one button per day (Now, then each forecast day the
 * horizon chips offer), each in the map's own ramp colour for that day's standing (`flowRampStops` → `rampAt`, the
 * colours the legend's ramp draws) with its words; a day no gauge in the group reads wears the ungauged colour and a
 * dash, said in the key. A tap on a day is the horizon chips' own pick (`onPick`): the map and the view show that day.
 *
 * Two arrangements of one card (the VM is @app/ui `useBasinDays` → core `basinDaysVM`):
 *   strip  the Conditions map's bottom bar (under the legend's ramp): the days side by side, the day shown in words
 *   rows   a water's Conditions view (with its other cards): a row per day, each with its words
 */
import { BASIN_DAYS_WORDS, type DayHorizon, type Horizon } from "@app/core";
import type { BasinDaysState } from "@app/ui";
import { ScrollView, View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { PartPending } from "./regs/NetWaiting";
import { rampAt } from "./DonorPanel";
import { flowRampStops, SPACE, ungaugedColour, type Palette } from "./theme";
import { TYPE } from "./type";
import { hitProps, MIN_HIT } from "./regs/hit";

/** a day's width in the strip: the press floor, so eleven days fit a desktop bar and scroll on a phone */
const DAY_W = MIN_HIT;

export function BasinDays({ state, palette, theme, onPick, layout, testID = "basin-days" }: {
  state: BasinDaysState;
  palette: Palette; theme: string;
  /** the horizon chips' pick (`col.pickDay`) */
  onPick?: (d: Horizon) => void;
  layout: "strip" | "rows";
  testID?: string;
}) {
  const card = { gap: SPACE.s2, padding: SPACE.s3, borderWidth: 1, borderColor: palette.line2,
                 borderRadius: palette.r.box, backgroundColor: palette.card } as const;
  if (state.state === "loading" || state.state === "failed")
    // (T368) the one face for a part that is coming or cannot come: still loading, offline, or couldn't load
    return <PartPending palette={palette} what={BASIN_DAYS_WORDS.part} state={state.state === "loading" ? "loading" : "failed"}
                        testID={`${testID}-${state.state}`} />;
  if (state.state === "none")
    return (
      <View testID={`${testID}-none`} style={card}>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{state.line}</Text>
      </View>
    );
  const { vm } = state;
  const stops = flowRampStops(theme);
  const none = ungaugedColour(theme);
  const fill = (p: number | null) => (p === null ? none : rampAt(stops, p));
  const lit = vm.days.find((d) => d.on) ?? null;
  const pick = (d: DayHorizon) => onPick?.(d);
  const buttons = vm.days.map((d) => (
    <Pressable key={d.day} testID={`${testID}-day-${d.day}`} accessibilityRole="button" accessibilityLabel={d.label}
               aria-pressed={d.on} onPress={() => pick(d.day)}
               // every day a full 44 × 44 press of its own (no slop: the days sit side by side)
               {...hitProps(layout === "strip" ? { w: DAY_W, h: MIN_HIT } : { w: 200, h: MIN_HIT })}
               style={layout === "strip"
                 ? { width: DAY_W, minHeight: MIN_HIT, alignItems: "center", justifyContent: "center", gap: 2,
                     borderWidth: 1.5, borderColor: d.on ? palette.ink : "transparent" }
                 : { flexDirection: "row", alignItems: "center", gap: SPACE.s2, minHeight: MIN_HIT,
                     paddingHorizontal: SPACE.s2, borderWidth: 1.5,
                     borderColor: d.on ? palette.ink : "transparent",
                     backgroundColor: d.on ? palette.tint : "transparent" }}>
      {layout === "strip" ? (
        <>
          <Text numberOfLines={1} style={{ ...TYPE.caption, fontSize: 11, lineHeight: 14,
                                           color: d.on ? palette.ink : palette.sub }}>{d.chip}</Text>
          <View style={{ alignSelf: "stretch", height: 12, backgroundColor: fill(d.percentile),
                         alignItems: "center", justifyContent: "center" }}>
            {d.standing === null && <Text style={{ ...TYPE.caption, fontSize: 10, lineHeight: 12, color: palette.ink }}>—</Text>}
          </View>
        </>
      ) : (
        <>
          <Text style={{ ...TYPE.small, width: 56, color: d.on ? palette.ink : palette.sub }}>{d.chip}</Text>
          <View style={{ width: 22, height: 12, backgroundColor: fill(d.percentile),
                         alignItems: "center", justifyContent: "center" }}>
            {d.standing === null && <Text style={{ ...TYPE.caption, fontSize: 10, lineHeight: 12, color: palette.ink }}>—</Text>}
          </View>
          <Text style={{ ...TYPE.small, flex: 1, color: d.standing === null ? palette.sub : palette.ink }}>{d.word}</Text>
        </>
      )}
    </Pressable>
  ));
  return (
    <View testID={testID} style={card}>
      <View style={{ gap: 1 }}>
        <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, fontSize: 13, lineHeight: 18, color: palette.ink }}>{vm.title}</Text>
        <Text style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{vm.scale}</Text>
      </View>
      {/* ONE LINE in the bottom bar: the eleven days scroll sideways where the bar is narrower than they are (a phone) */}
      {layout === "strip" ? (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} role="group" accessibilityLabel={vm.list}
                    contentContainerStyle={{ gap: 2 }}>
          {buttons}
        </ScrollView>
      ) : (
        <View role="group" accessibilityLabel={vm.list} style={{ gap: 2 }}>{buttons}</View>
      )}
      {/* the strip says the day shown in words (each day's words are its button's name); the rows say every day's */}
      {layout === "strip" && lit && (
        <Text testID={`${testID}-shown`} style={{ ...TYPE.caption, lineHeight: 15, color: palette.ink }}>
          {`${lit.chip}: ${lit.word}`}
        </Text>
      )}
      {vm.noneKey && <Text style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{vm.noneKey}</Text>}
    </View>
  );
}
