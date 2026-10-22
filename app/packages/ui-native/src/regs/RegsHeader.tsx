/**
 * T23 — the Regulations page's header (prototype regs v38 `hdr`, `placeRow`, `placeGo`, `fitLabel`; tokens.md §5
 * "RegsHeader"): the water's name, the date pill (status dot, "Today ▾" / weekday over the date), the Map (or Parts)
 * button, the part's place label on two lines, and the four tabs. Fixed above the page's one scroll — never inside it
 * (header-one-shape).
 *
 * Draws `HeaderVM` (core header.ts) and nothing else: no logic beyond the label fit (`pickPlace`, layout only). The tab
 * row reads the selected tab from the pin store (T29) in its own child, so a scroll re-renders the tabs and not the
 * header.
 *
 * THE BACK IS NOT HERE: in the app it is the water's header row above (FaceBar, drawn by the water frame — the same row
 * on every view of the water), so the name and the place label start at the gutter, not after a 44 px back (the
 * prototype's 56). The name row itself is the frame's `WaterTitle` (T44), the box every view's name sits in.
 */
import { useState, useSyncExternalStore } from "react";
import { Pressable, Text, View } from "react-native";
import Svg, { Path } from "react-native-svg";
import { WORDS, type HeaderVM, type Tab } from "@app/core";
import { TYPE } from "../type";
import { WaterTitle } from "../WaterFrame";
import { GUTTER, type Palette } from "../theme";

/** The selected tab, from the pin store (T29) or any source with the same two methods. */
export interface TabSource { subscribe(fn: () => void): () => void; getTab(): Tab }

/** The place row's line height (metaSemi) — fitLabel's measure. */
export const PLACE_LH = 16.22;

/**
 * prototype fitLabel: the full label when it lays out in at most two lines (+1 px), else the short one. `measuredHeight`
 * is the full label's laid-out height; null (not laid out yet) or 0 (jsdom has no layout) → the full label.
 */
export function pickPlace(place: { full: string; short: string }, measuredHeight: number | null, lineHeight: number): string {
  if (!measuredHeight) return place.full;
  return measuredHeight <= lineHeight * 2 + 1 ? place.full : place.short;
}

/** The prototype's own glyphs (placeGo): a folded map; the Parts list. */
const GLYPH = {
  map: ["M2.5 5.5 8 3l7 2.5 5.5-2.5v15L15 20.5 8 18l-5.5 2.5Z", "M8 3v15", "M15 5.5v15"],
  parts: ["M4 6h15M4 11.5h15M4 17h15"],
} as const;

export function RegsHeader({ header: h, palette, tabs, onDate, onAction, onTab }: {
  header: HeaderVM; palette: Palette;
  /** where the selected tab is read from (the pin store) */
  tabs: TabSource;
  onDate: () => void;
  /** Map / Parts pressed. Absent → the slot stays (44 px, so the name does not reflow between waters) with nothing in it. */
  onAction?: (kind: "map" | "parts") => void;
  onTab: (t: Tab) => void;
}) {
  const dot = h.dot === "open" ? palette.open : h.dot === "closed" ? palette.closed : palette.sub;
  return (
    <View testID="regs-header" style={{ backgroundColor: palette.card, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      {/* THE NAME ROW is the water frame's (T44): the same box on every view of the water, the pill and Map in it */}
      <WaterTitle name={h.name} palette={palette}>
        <Pressable testID="regs-date" onPress={onDate} accessibilityRole="button" accessibilityLabel={h.dateAria}
                   style={{ minHeight: 44, paddingHorizontal: 6, gap: 6, flexDirection: "row", alignItems: "center" }}>
          <View testID="regs-dot" style={{ width: 8, height: 8, backgroundColor: dot }} />
          <View>
            <Text style={[TYPE.metaHeavy, { color: palette.accent }]}>
              {h.dayWord}<Text style={{ fontSize: 10 }}>{WORDS.DATE_CARET}</Text>
            </Text>
            <Text style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.sub }]}>{h.date}</Text>
          </View>
        </Pressable>
        <View style={{ minWidth: 44, minHeight: 44 }}>
          {h.action && onAction && (
            <Pressable testID="regs-action" onPress={() => onAction(h.action!.kind)} accessibilityRole="button"
                       accessibilityLabel={h.action.aria}
                       style={{ minWidth: 44, minHeight: 44, paddingHorizontal: 4, gap: 1, alignItems: "center", justifyContent: "center" }}>
              <Svg width={20} height={20} viewBox="0 0 23 23" fill="none">
                {GLYPH[h.action.kind].map((d) => (
                  <Path key={d} d={d} stroke={palette.accent} strokeWidth={1.7} strokeLinejoin="round" />
                ))}
              </Svg>
              <Text style={[TYPE.metaBold, { lineHeight: 13.73, color: palette.accent }]}>{h.action.label}</Text>
            </Pressable>
          )}
        </View>
      </WaterTitle>
      <Place place={h.place} palette={palette} />
      <TabRow h={h} tabs={tabs} palette={palette} onTab={onTab} />
    </View>
  );
}

/** The place row: ALWAYS present (an empty slot of the same height when there is no label — the header is one size). */
function Place({ place, palette }: { place: HeaderVM["place"]; palette: Palette }) {
  const [measured, setMeasured] = useState<number | null>(null);
  const shown = place ? pickPlace(place, measured, PLACE_LH) : "";
  return (
    <View style={{ paddingLeft: GUTTER, paddingRight: 12, paddingBottom: 6, marginTop: -2 }}>
      <Text testID="regs-place" numberOfLines={2} accessibilityLabel={place?.full}
            style={[TYPE.metaSemi, { height: PLACE_LH * 2, color: palette.sub }]}>{shown}</Text>
      {place && (
        // the measuring twin: the full label at the same width, invisible (react-native-web has no onTextLayout)
        <Text aria-hidden pointerEvents="none" onLayout={(e) => setMeasured(e.nativeEvent.layout.height)}
              style={[TYPE.metaSemi, { position: "absolute", left: GUTTER, right: 12, top: 0, opacity: 0 }]}>{place.full}</Text>
      )}
    </View>
  );
}

function TabRow({ h, tabs, palette, onTab }: { h: HeaderVM; tabs: TabSource; palette: Palette; onTab: (t: Tab) => void }) {
  const selected = useSyncExternalStore(tabs.subscribe, tabs.getTab);
  return (
    <View accessibilityRole="tablist" accessibilityLabel={WORDS.TABS_LABEL}
          style={{ paddingHorizontal: GUTTER, gap: 16, flexDirection: "row" }}>
      {h.tabs.map((t) => {
        const on = t.tab === selected;
        return (
          <Pressable key={t.tab} onPress={() => onTab(t.tab)} accessibilityRole="tab" aria-selected={on} hitSlop={{ top: 5, bottom: 5 }}
                     style={{ minHeight: 34, paddingTop: 2, justifyContent: "center", borderBottomWidth: 3, borderBottomColor: on ? palette.ink : "transparent" }}>
            <Text style={[TYPE.regsTab, { color: on ? palette.ink : palette.sub }]}>{t.label}</Text>
          </Pressable>
        );
      })}
    </View>
  );
}
