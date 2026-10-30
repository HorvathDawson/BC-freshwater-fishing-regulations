/**
 * "The next few days" (T51, restructured T284) — how alike each of the coming days looks to the day of the visit, one
 * card per day in date order: the day, its score and one-word read, and the plain reasons under it ("Air
 * temperature: 4° cooler", always shown — nothing is behind a tap). The day that looks most like the visit carries a mark and a heavier
 * border (colour is not the only cue). The cards sit one to a row on a phone and two to a row where there is room.
 *
 * Draws `LikeVisitView` (@app/ui) and nothing else: every string is core's.
 */
import { Text, View } from "react-native";
import type { LikeVisitView } from "@app/ui";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function LikeVisitStrip({ view, palette }: { view: LikeVisitView; palette: Palette }) {
  const small = (testID: string, s: string, colour = palette.sub) => (
    <Text testID={testID} style={{ ...TYPE.small, color: colour }}>{s}</Text>
  );
  return (
    <View testID="like-visit" style={{ gap: 12 }}>
      {small("like-intro", view.intro)}
      {view.days ? (
        <>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: 8, paddingVertical: 8, paddingHorizontal: 12,
                         borderRadius: palette.r.box, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{view.visit.top}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{view.visit.sub}</Text>
          </View>
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10 }}>
            {view.days.map((d) => (
              <View key={d.date} testID={`like-day-${d.date}`}
                    style={{ flexGrow: 1, flexBasis: 280, minWidth: 0, gap: 6, padding: 12, borderRadius: palette.r.box,
                             borderWidth: d.best ? 2 : 1, borderColor: d.best ? palette.ink : palette.line }}>
                <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
                  <View style={{ flex: 1, minWidth: 0 }}>
                    <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{d.top}</Text>
                    <Text style={{ ...TYPE.small, color: palette.faint }}>{d.sub}</Text>
                  </View>
                  <View style={{ alignItems: "flex-end" }}>
                    <Text style={{ ...TYPE.name, color: palette.ink }}>{d.score}</Text>
                    {d.read ? <Text style={{ ...TYPE.small, color: palette.sub }}>{d.read}</Text> : null}
                  </View>
                </View>
                {d.best ? <Text style={{ ...TYPE.section, color: palette.accent }}>{view.best.toUpperCase()}</Text> : null}
                {d.basis ? <Text style={{ ...TYPE.caption, color: palette.faint }}>{d.basis}</Text> : null}
                {d.reasons.length ? (
                  <View style={{ gap: 2, paddingTop: 4, borderTopWidth: 1, borderTopColor: palette.line }}>
                    {d.reasons.map((w) => <Text key={w} style={{ ...TYPE.small, color: palette.sub }}>{w}</Text>)}
                  </View>
                ) : null}
              </View>
            ))}
          </View>
        </>
      ) : view.note ? small("like-note", view.note) : null}
      {view.hint ? small("like-hint", view.hint) : null}
      <Text testID="like-age" style={{ ...TYPE.caption, color: palette.faint }}>{view.age}</Text>
    </View>
  );
}
