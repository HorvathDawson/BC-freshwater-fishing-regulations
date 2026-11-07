/**
 * A water's header: the way back, and its views as ONE strip — identical on every screen a water is read on.
 *
 *   ‹ Back-to-where-you-came-from                              share  ×   (× leaves every water: owner 2026-10-22 late;
 *                                                                          share sends its web link: T385)
 *   Regulations   Conditions   Map         (the selected view underscored in ink: the regs header's tabs, v11)
 *
 * ONE ORDER, LEFT TO RIGHT, and it is the swipe's (`@app/core` `WATER_VIEWS`): a finger moving left brings in the
 * view written to the right. The strip and the swipe can never disagree because they read the same list.
 *
 * IT DOES NOT MOVE. The row sits in the same place on Regulations, Conditions and Map, so the control you just used
 * is where you left it: it is drawn by the water frame ONLY (`WaterFrame`, T44), never by a view.
 *
 * TWO SOURCES, ONE DRAWING. In water mode, and on a Conditions-tab reach, the Shell provides `WaterViewsContext`: the
 * views on offer, the one on screen and where back goes. Without it (a screen mounted alone) the row falls back to the
 * `face` / `onFace` props, offering Regulations and Conditions. The header's swipe is the frame's (it wraps this row
 * and the name row under it).
 *
 * Both labels stay visible rather than showing only the alternative: which questions this water answers is itself
 * information, and a reader who does not know Conditions exists will never go looking for it.
 */
import { useContext } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { TO_REGULATIONS, WATER_BACK, WATER_VIEWS, waterBackName, WATER_VIEWS_LABEL, WATER_VIEW_LABEL, type WaterView } from "@app/core";
import { WaterViewsContext } from "./ViewSwipe";
import { useCoachRefs, useCoachTarget } from "./CoachMarks";
import { Icon, IconText } from "./icons";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

export type Face = "conditions" | "regulations";

const toView = (f: Face): WaterView => (f === "regulations" ? "regs" : "conditions");

export function FaceBar({ palette, face, onFace, onBack, backLabel }: {
  palette: Palette; face: Face; onFace: (f: Face) => void;
  onBack?: () => void;
  /** Where back goes, in the reader's words — "Conditions", "Map", "Search". */
  backLabel?: string;
}) {
  const ctx = useContext(WaterViewsContext);
  const views = ctx?.views ?? (["regs", "conditions"] as const);
  const on = ctx?.on ?? toView(face);
  const pick = ctx?.onView ?? ((v: WaterView) => { if (v !== "map") onFace(v === "regs" ? "regulations" : "conditions"); });
  const back = ctx?.onBack ?? onBack;
  const label = ctx?.backLabel ?? backLabel ?? WATER_BACK;
  const close = ctx?.close ?? null;
  const share = ctx?.share ?? null;
  const backRef = useCoachTarget("water:back");   // (T456) the walkthrough's "Back to the map" step rings it

  return (
    <View style={{ gap: SPACE.s1 }}>
      {(back || close) && (
        // ‹ at the left, × at the right of the same 32-tall row: the row's height does not change
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", minHeight: 32 }}>
          {back ? (
            // (T40) 44 × 44 to the touch: the row is 32 tall and "‹  Map" (the shortest place back goes) about 36 wide
            <Pressable ref={backRef} onPress={back} accessibilityRole="button" accessibilityLabel={waterBackName(label)} testID="water-back"
                       {...hitProps({ w: 36, h: 32 })} style={{ minHeight: 32, justifyContent: "center" }}>
              <IconText name="back" text={label} style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }} />
            </Pressable>
          ) : <View />}
          {/* (T385) the right end: what Share said, Share, and × */}
          <View style={{ flexDirection: "row", alignItems: "center", gap: 4, flexShrink: 1 }}>
            {share?.said && (
              // (T385) "Link copied", or why it could not be shared: in the row, beside the control it answers
              <Text testID="water-share-said" accessibilityLiveRegion="polite" numberOfLines={2}
                    style={{ ...TYPE.meta, flexShrink: 1, textAlign: "right", color: palette.sub }}>{share.said}</Text>
            )}
            {share && (
              // (T385) the phone's share sheet (the web's, else the link copied): 32 drawn, 44 × 44 to the touch
              <Pressable onPress={share.press} accessibilityRole="button" accessibilityLabel={share.label}
                         aria-disabled={share.busy} disabled={share.busy}
                         testID="water-share" {...hitProps({ w: 32, h: 32 })}
                         style={{ width: 32, height: 32, alignItems: "center", justifyContent: "center" }}>
                <Icon name="share" colour={palette.accent} size={18} />
              </Pressable>
            )}
            {close && (
              // (owner 2026-10-22 late) one tap out of every water, to the tab the first was opened from; 32 drawn,
              // 44 × 44 to the touch, at the row's right edge where a sheet's × sits
              <Pressable onPress={close} accessibilityRole="button" accessibilityLabel={ctx!.closeLabel}
                         testID="water-close" {...hitProps({ w: 32, h: 32 })}
                         style={{ width: 32, height: 32, alignItems: "center", justifyContent: "center" }}>
                <Icon name="close" colour={palette.sub} size={20} />
              </Pressable>
            )}
          </View>
        </View>
      )}
      <ViewTabs palette={palette} views={views} on={on} onView={pick} />
    </View>
  );
}

/**
 * The view strip: the regs header's tabs (v11 `.segm[role=tablist]`: 700 --t4, 22 px apart, the selected one
 * underscored 3 px in ink, the rest in `sub`), with a hairline under the whole row so it reads as the header's edge.
 */
export function ViewTabs({ palette, views, on, onView }: {
  palette: Palette; views: readonly WaterView[]; on: WaterView; onView: (v: WaterView) => void;
}) {
  const shown = WATER_VIEWS.filter((v) => views.includes(v));
  const coach = useCoachRefs();   // (T293) the walkthrough points at Regulations and Conditions (`view:<view>`)
  return (
    <View accessibilityRole="tablist" accessibilityLabel={WATER_VIEWS_LABEL}
          style={{ flexDirection: "row", gap: 22, borderBottomWidth: 1, borderBottomColor: palette.line }}>
      {shown.map((v) => {
        const sel = v === on;
        return (
          <Pressable key={v} ref={coach(`view:${v}`)} onPress={() => { if (!sel) onView(v); }}
                     accessibilityRole="tab" aria-selected={sel} accessibilityLabel={WATER_VIEW_LABEL[v]} testID={`water-view-${v}`}
                     accessibilityHint={v === "regs" ? TO_REGULATIONS.hint : undefined}
                     // (T40) the strip's row is 38 tall and "Map" (14 px bold) about 30 wide: grown to 44 × 44 inside the 22 px gap
                     {...hitProps({ w: 28, h: 38 })}
                     style={{ minHeight: 38, justifyContent: "flex-end", paddingTop: 2, marginBottom: -1,
                              borderBottomWidth: 3, borderBottomColor: sel ? palette.ink : "transparent" }}>
            <Text style={{ ...TYPE.regsTab, fontSize: 14, lineHeight: 18, paddingBottom: 6,
                           color: sel ? palette.ink : palette.sub }}>{WATER_VIEW_LABEL[v]}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
