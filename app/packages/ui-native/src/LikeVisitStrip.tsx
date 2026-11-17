/**
 * "The next few days" (T51, restructured T284; T819 item 9: one compact row a day) — how alike each of the coming days
 * looks to the day of the visit: a row per day in date order with the day, its single biggest difference from the visit
 * and its score; a tap on the row opens its reasons ("Air temperature: 4° cooler") and why flow is out, if it is. The
 * day that looks most like the visit carries a mark and a heavier left edge (colour is not the only cue). A visit made
 * today has no "Today" row: it would only compare the visit with itself.
 *
 * Draws `LikeVisitView` (@app/ui) and nothing else: every string is core's.
 */
import { useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import type { LikeVisitView } from "@app/ui";
import { TYPE } from "./type";
import { useCoachTarget } from "./CoachMarks";
import type { Palette } from "./theme";
import { Icon } from "./icons";
import { hitProps } from "./regs/hit";

export function LikeVisitStrip({ view, palette }: { view: LikeVisitView; palette: Palette }) {
  const small = (testID: string, s: string, colour = palette.sub) => (
    <Text testID={testID} style={{ ...TYPE.small, color: colour }}>{s}</Text>
  );
  // (T293) the walkthrough points at the coming days, then at the day closest to the visit
  const coach = useCoachTarget("spot:days"), best = useCoachTarget("spot:best");
  const [open, setOpen] = useState<string | null>(null);
  return (
    <View testID="like-visit" ref={coach} collapsable={false} style={{ gap: 12 }}>
      {small("like-intro", view.intro)}
      {view.days ? (
        <>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8, paddingVertical: 8, paddingHorizontal: 12,
                         borderRadius: palette.r.box, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{view.visit.top}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub, flex: 1, minWidth: 0 }}>{view.visit.sub}</Text>
          </View>
          <View>
            {view.days.map((d, i) => {
              const on = open === d.date;
              return (
                <View key={d.date} testID={`like-day-${d.date}`} ref={d.best ? best : undefined} collapsable={false}
                      style={{ borderTopWidth: i === 0 ? 0 : 1, borderTopColor: palette.line,
                               borderLeftWidth: d.best ? 3 : 0, borderLeftColor: palette.ink, paddingLeft: d.best ? 9 : 0 }}>
                  <Pressable {...hitProps({ w: 300, h: 48 })} onPress={() => setOpen(on ? null : d.date)}
                             accessibilityRole="button" aria-expanded={on}
                             accessibilityLabel={d.label}
                             style={{ minHeight: 48, flexDirection: "row", alignItems: "center", gap: 10, paddingVertical: 8 }}>
                    <View style={{ width: 92 }}>
                      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{d.top}</Text>
                      {d.sub ? <Text style={{ ...TYPE.caption, color: palette.faint }}>{d.sub}</Text> : null}
                    </View>
                    <View style={{ flex: 1, minWidth: 0 }}>
                      {d.best ? <Text style={{ ...TYPE.section, fontSize: 10.5, color: palette.accent }}>{view.best.toUpperCase()}</Text> : null}
                      {d.lead ? <Text testID={`like-lead-${d.date}`} style={{ ...TYPE.small, color: palette.sub }}>{d.lead}</Text> : null}
                    </View>
                    <View style={{ alignItems: "flex-end" }}>
                      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{d.score}</Text>
                      {d.read ? <Text style={{ ...TYPE.caption, color: palette.sub }}>{d.read}</Text> : null}
                    </View>
                    <Icon name={on ? "collapse" : "expand"} colour={palette.faint} size={16} />
                  </Pressable>
                  {on ? (
                    <View testID={`like-why-${d.date}`} style={{ gap: 2, paddingBottom: 10, paddingLeft: 102 }}>
                      {d.basis ? <Text style={{ ...TYPE.caption, color: palette.faint }}>{d.basis}</Text> : null}
                      {d.reasons.map((w) => <Text key={w} style={{ ...TYPE.small, color: palette.sub }}>{w}</Text>)}
                    </View>
                  ) : null}
                </View>
              );
            })}
          </View>
        </>
      ) : view.note ? small("like-note", view.note) : null}
      {view.hint ? small("like-hint", view.hint) : null}
      <Text testID="like-age" style={{ ...TYPE.caption, color: palette.faint }}>{view.age}</Text>
    </View>
  );
}
