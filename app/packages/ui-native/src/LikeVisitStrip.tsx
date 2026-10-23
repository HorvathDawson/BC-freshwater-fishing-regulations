/**
 * "Like your visit?" (T51) — a small forecast-looking strip: the visit's own day, then today and the next four, each
 * with how alike it looks to the visit and a one-word read. Tapping a day shows what drives its score.
 *
 * Draws `LikeVisitView` (@app/ui) and nothing else: every string is core's. Which day is open is this strip's own
 * state; back closes it first (`useBackLayer`, "back everywhere").
 */
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { useBackLayer, type LikeVisitView } from "@app/ui";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The app's hit area, px. */
const TARGET = 44;

export function LikeVisitStrip({ view, palette }: { view: LikeVisitView; palette: Palette }) {
  const [open, setOpen] = useState<string | null>(null);
  useBackLayer(open !== null, () => setOpen(null));
  const shown = view.days?.find((d) => d.date === open) ?? null;
  const small = (testID: string, s: string, colour = palette.sub) => (
    <Text testID={testID} style={{ ...TYPE.small, color: colour }}>{s}</Text>
  );
  return (
    <View testID="like-visit" style={{ gap: 10 }}>
      {small("like-intro", view.intro)}
      {view.days ? (
        <View style={{ flexDirection: "row", gap: 4 }}>
          <View accessible accessibilityLabel={view.visit.label}
                style={{ flex: 1, alignItems: "center", gap: 2, paddingVertical: 6, borderRadius: palette.r.box,
                         backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.ink }}>{view.visit.top}</Text>
            <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{view.visit.sub}</Text>
          </View>
          {view.days.map((d) => {
            const on = d.date === open;
            return (
              <Pressable key={d.date} onPress={() => setOpen(on ? null : d.date)} accessibilityRole="button"
                         accessibilityLabel={d.label} aria-selected={on} aria-expanded={on}
                         style={{ flex: 1, minHeight: TARGET, alignItems: "center", gap: 2, paddingVertical: 6,
                                  borderBottomWidth: 2, borderBottomColor: on ? palette.ink : "transparent" }}>
                <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.ink }}>{d.top}</Text>
                <Text style={{ ...TYPE.micro, fontSize: 10, color: palette.faint }}>{d.sub}</Text>
                <Text style={{ ...TYPE.figure, fontSize: 15, color: palette.ink }}>{d.score}</Text>
                {d.read ? <Text style={{ ...TYPE.small, fontSize: 10.5, color: palette.sub }}>{d.read}</Text> : null}
                {d.basis ? <Text style={{ ...TYPE.micro, fontSize: 9.5, color: palette.faint }}>{d.basis}</Text> : null}
              </Pressable>
            );
          })}
        </View>
      ) : view.note ? small("like-note", view.note) : null}
      {shown ? (
        <View testID="like-why" style={{ gap: 4, padding: 12, borderRadius: palette.r.box, borderWidth: 1,
                                         borderColor: palette.line }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{shown.label}</Text>
          {shown.why.map((w) => <Text key={w} style={{ ...TYPE.small, color: palette.sub }}>{w}</Text>)}
        </View>
      ) : null}
      <Text testID="like-age" style={{ ...TYPE.small, fontSize: 11.5, color: palette.faint }}>{view.age}</Text>
    </View>
  );
}
