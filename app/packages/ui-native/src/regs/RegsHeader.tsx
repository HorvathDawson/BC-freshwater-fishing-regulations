/**
 * T23 — the Regulations page's header (prototype regs v38 `hdr`, `placeRow`, `placeGo`, `fitLabel`; tokens.md §5
 * "RegsHeader"): the water's name, the date pill (status dot, "Today ▾" / weekday over the date), the part's place
 * label on two lines, and the four tabs (no Map or Parts button: owner call 3, T71 / T150). Fixed above the page's one scroll — never inside it
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
import { View } from "react-native";
import { Text } from "../Text";
import { WORDS, type ClassBadgeVM, type HeaderVM, type SunDay, type Tab } from "@app/core";
import { CAPPED_SCALE, TYPE } from "../type";
import { useCappedScale } from "../textScale";
import { WaterTitle } from "../WaterFrame";
import { GUTTER, type Palette } from "../theme";
import { RegsPress } from "./RegsPress";
import { Icon } from "../icons";
import { CoachSpot, useCoachTarget } from "../CoachMarks";

/** The selected tab, from the pin store (T29) or any source with the same two methods. */
export interface TabSource { subscribe(fn: () => void): () => void; getTab(): Tab }

/** about how many characters of the place row fit one line at a phone's width (the guess before it is measured) */
const ONE_LINE_CHARS = 44;

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

/**
 * NO ACTION IN THE NAME ROW (owner call 3): T71 dropped the Map button (the view strip is the one Map entry), T150 the
 * Parts button (the water's Map view lists the parts). The place label below keeps the part's name.
 */
export function RegsHeader({ header: h, palette, tabs, onDate, onTab, onTerm, side = false, nameSource = null, sun = null }: {
  header: HeaderVM; palette: Palette;
  /** (T841) the classified-water badge's press: opens its glossary term (`gl:<id>`); absent, the badge opens nothing */
  onTerm?: (id: string) => void;
  /** (T224) where the water's name came from when it is not the official one (`WaterTitle`'s `source`) */
  nameSource?: string | null;
  /** where the selected tab is read from (the pin store) */
  tabs: TabSource;
  onDate: () => void;
  onTab: (t: Tab) => void;
  /**
   * (T45) the desktop arrangement: the date pill and the tabs are drawn in the side column (`RegsSide`), so the header
   * keeps the name row and the place label. false (a phone, a tablet): the header as it always was.
   */
  side?: boolean;
  /** (T878) the picked day's sun at the water (`useSunLine`), a small line under the date pill (`DayControl`; on the
   *  desktop the side column's); null: nothing drawn */
  sun?: SunDay | null;
}) {
  return (
    <View testID="regs-header" style={{ backgroundColor: palette.card, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      {/* THE NAME ROW is the water frame's (T44): the same box on every view of the water, the pill in it */}
      <WaterTitle name={h.name} palette={palette} source={nameSource}>
        {h.classed && <ClassBadge b={h.classed} palette={palette} onTerm={onTerm} />}
        {!side && <DayControl h={h} palette={palette} onDate={onDate} sun={sun} align="end" />}
      </WaterTitle>
      {/* (T878) no place label (a water of one part, Babine Lake — Main Body): no empty slot under the name */}
      {!!h.place && <Place place={h.place} palette={palette} />}
      {!side && <TabRow h={h} tabs={tabs} palette={palette} onTab={onTab} />}
    </View>
  );
}

/**
 * (T841) THE CLASSIFIED-WATER BADGE beside the name: "Class II" in the header's chip style (an outlined chip, the keep
 * box's tag), the accent while a designation is in force; (look B) the period under it and, out of period, the badge in
 * the secondary colour. A press opens the classified-waters term when the release has one. Draws `ClassBadgeVM` only.
 */
export function ClassBadge({ b, palette, onTerm }: { b: ClassBadgeVM; palette: Palette; onTerm?: (id: string) => void }) {
  const c = b.inForce ? palette.accent : palette.sub;
  const chip = (
    <View style={{ borderWidth: 1, borderColor: c, paddingHorizontal: 5, alignItems: "center" }}>
      <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.chip, { color: c }]}>{b.label}</Text>
      {!!b.period && <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.sub, marginTop: -2, paddingBottom: 2 }]}>{b.period}</Text>}
    </View>
  );
  const term = b.term;
  return term && onTerm ? (
    <RegsPress testID="regs-class" onPress={() => onTerm(term)} accessibilityRole="button" accessibilityLabel={b.aria} box={{ w: 44, h: 44 }}
               style={{ minHeight: 44, justifyContent: "center" }}>{chip}</RegsPress>
  ) : (
    <View testID="regs-class" accessible accessibilityLabel={b.aria} style={{ minHeight: 44, justifyContent: "center" }}>{chip}</View>
  );
}

/**
 * (T878, owner 2026-11-18: "There seems to be a ton of space used by the sunset/rise could we maybe move it to top of
 * something?", picked "Under the date") THE DAY CONTROL: the date pill and, under it, the picked day's sun at the water
 * (`SunDay`, core's words: the sun icon and "05:07–21:52 PT", read "Sunrise 5:07, sunset 21:52 PT"), its right edge on the
 * date's (`align` "end", the name row) or its left on the pill's (`align` "start", the desktop side column). No sun: the pill
 * alone, as before.
 */
export function DayControl({ h, palette, onDate, sun, align }: {
  h: HeaderVM; palette: Palette; onDate: () => void; sun: SunDay | null; align: "start" | "end";
}) {
  const pill = <DatePill h={h} palette={palette} onDate={onDate} />;
  if (!sun) return pill;
  return (
    <View style={{ alignItems: align === "end" ? "flex-end" : "flex-start" }}>
      {pill}
      {/* tucked into the pill's own slack under the date (its 44 px box is taller than its two lines) */}
      <View testID="regs-sun" accessible accessibilityLabel={sun.aria}
            style={{ flexDirection: "row", alignItems: "center", gap: 3, marginTop: -6, paddingHorizontal: 6 }}>
        <Icon name="sun" colour={palette.sub} size={11} />
        <Text maxFontSizeMultiplier={CAPPED_SCALE} numberOfLines={1} style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.sub }]}>{sun.text}</Text>
      </View>
    </View>
  );
}

/**
 * The date pill: the status dot, "Today" / the weekday and the set's chevron (`expand`, T186: the typed ▾ it replaced
 * was a glyph in a word) over the date; a press asks "Which day?" (T49).
 */
export function DatePill({ h, palette, onDate }: { h: HeaderVM; palette: Palette; onDate: () => void }) {
  const dot = h.dot === "open" ? palette.open : h.dot === "closed" ? palette.closed : palette.sub;
  return (
    <CoachSpot k="regs:date">
    <RegsPress testID="regs-date" onPress={onDate} accessibilityRole="button" accessibilityLabel={h.dateAria} box={{ w: 44, h: 44 }}
               style={{ minHeight: 44, paddingHorizontal: 6, gap: 6, flexDirection: "row", alignItems: "center" }}>
      <View testID="regs-dot" style={{ width: 8, height: 8, backgroundColor: dot }} />
      <View>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 2 }}>
          <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.metaHeavy, { color: palette.accent }]}>{h.dayWord}</Text>
          <Icon name="expand" colour={palette.accent} size={12} />
        </View>
        <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.metaSemi, { lineHeight: 14.98, color: palette.sub }]}>{h.date}</Text>
      </View>
    </RegsPress>
    </CoachSpot>
  );
}

/**
 * The place row: one line, two when the label wraps (T878: the header draws it only when there is a label; the Conditions
 * view too). At the reader's text size (T40: the slot and the fit's line height grow with the scale, so a label at 2× is not
 * cut to one line); the measuring twin lays out again at the new size and the fit re-runs on its new height.
 */
/** The part's place row; (T367) also the Conditions view's, under its name, so both views say where alike. */
export function Place({ place, palette, coach: coachKey = "regs:place" }: { place: HeaderVM["place"]; palette: Palette;
                                                                          /** the walkthrough's key (null: none) */
                                                                          coach?: string | null }) {
  const [measured, setMeasured] = useState<number | null>(null);
  const [measuredLabel, setMeasuredLabel] = useState<number | null>(null);
  const scale = useCappedScale();
  const lh = PLACE_LH * scale;
  const shown = place ? pickPlace(place, measured, lh) : "";
  // (owner 2026-10-27) one line when the name fits, more only when it wraps. Before the measuring twin has laid out, a
  // guess from the text's length (no jump for the common short name); the measure then settles it. (T517) A part's
  // name is its two ends' words and may be long ("Mouth → Cable car crossing (200 m downstream of Vedder Crossing
  // bridge)"): the row takes as many lines as the name shown needs, so nothing is ever cut (T40 at 200 % text)
  const labelHeight = place && shown === place.full ? measured : measuredLabel;
  const lines = !place ? 1 : labelHeight ? Math.max(1, Math.round(labelHeight / lh))
    : Math.max(1, Math.ceil((shown.length * scale) / ONE_LINE_CHARS));
  const coach = useCoachTarget(coachKey ?? undefined);   // (T293) the walkthrough: which part of the water this is
  return (
    <View ref={coach} collapsable={false} style={{ paddingLeft: GUTTER, paddingRight: 12, paddingBottom: 6, marginTop: -2 }}>
      <Text testID="regs-place" numberOfLines={lines} accessibilityLabel={place?.full} maxFontSizeMultiplier={CAPPED_SCALE}
            style={[TYPE.metaSemi, { height: lh * lines, color: palette.sub }]}>{shown}</Text>
      {place && (
        // the measuring twin: the full label at the same width, invisible (react-native-web has no onTextLayout)
        <Text aria-hidden pointerEvents="none" onLayout={(e) => setMeasured(e.nativeEvent.layout.height)} maxFontSizeMultiplier={CAPPED_SCALE}
              style={[TYPE.metaSemi, { position: "absolute", left: GUTTER, right: 12, top: 0, opacity: 0 }]}>{place.full}</Text>
      )}
      {place && shown !== place.full && (
        // and the label shown, when it is the short one: the lines the row takes
        <Text aria-hidden pointerEvents="none" onLayout={(e) => setMeasuredLabel(e.nativeEvent.layout.height)} maxFontSizeMultiplier={CAPPED_SCALE}
              style={[TYPE.metaSemi, { position: "absolute", left: GUTTER, right: 12, top: 0, opacity: 0 }]}>{shown}</Text>
      )}
    </View>
  );
}

/**
 * (T45) The tabs as the desktop side column's section list: the same four tabs (one tablist, the same labels, the
 * selected one read from the pin store), one under another, the selected one underlined in ink as in the tab row, its
 * words at the gutter. A press scrolls the page to its section, as the tab row's does.
 */
export function SectionList({ h, tabs, palette, onTab }: { h: HeaderVM; tabs: TabSource; palette: Palette; onTab: (t: Tab) => void }) {
  const selected = useSyncExternalStore(tabs.subscribe, tabs.getTab, tabs.getTab);
  const coach = useCoachTarget("regs:tabs");
  return (
    <View testID="regs-section-list" ref={coach} collapsable={false} accessibilityRole="tablist" accessibilityLabel={WORDS.TABS_LABEL} aria-orientation="vertical"
          style={{ gap: 4 }}>
      {h.tabs.map((t) => {
        const on = t.tab === selected;
        return (
          <RegsPress key={t.tab} onPress={() => onTab(t.tab)} accessibilityRole="tab" aria-selected={on} box={TAB_BOX_SIDE}
                     style={{ minHeight: 36, alignSelf: "flex-start", justifyContent: "center", borderBottomWidth: 3,
                              borderBottomColor: on ? palette.ink : "transparent" }}>
            <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.regsTab, { color: on ? palette.ink : palette.sub }]}>{t.label}</Text>
          </RegsPress>
        );
      })}
    </View>
  );
}

/**
 * (T40) A tab's known box: its row (34 px; the side list's 36) by the shortest label's run ("Gear", 12.48 px bold, about
 * 30 px; 28 is the floor). The slop (5 above and below, 8 each side) stays inside the row's 16 px gap.
 */
export const TAB_BOX = { w: 28, h: 34 } as const, TAB_BOX_SIDE = { w: 28, h: 36 } as const;

function TabRow({ h, tabs, palette, onTab }: { h: HeaderVM; tabs: TabSource; palette: Palette; onTab: (t: Tab) => void }) {
  const selected = useSyncExternalStore(tabs.subscribe, tabs.getTab, tabs.getTab);
  const coach = useCoachTarget("regs:tabs");   // (T293) the walkthrough: Gear, Methods, Licence
  return (
    // (T40) the row wraps: at 2× text the four tabs are wider than a phone (one row at 1×, as before)
    <View ref={coach} collapsable={false} accessibilityRole="tablist" accessibilityLabel={WORDS.TABS_LABEL}
          style={{ paddingHorizontal: GUTTER, columnGap: 16, flexDirection: "row", flexWrap: "wrap" }}>
      {h.tabs.map((t) => {
        const on = t.tab === selected;
        return (
          <RegsPress key={t.tab} onPress={() => onTab(t.tab)} accessibilityRole="tab" aria-selected={on} box={TAB_BOX}
                     style={{ minHeight: 34, paddingTop: 2, justifyContent: "center", borderBottomWidth: 3, borderBottomColor: on ? palette.ink : "transparent" }}>
            <Text maxFontSizeMultiplier={CAPPED_SCALE} style={[TYPE.regsTab, { color: on ? palette.ink : palette.sub }]}>{t.label}</Text>
          </RegsPress>
        );
      })}
    </View>
  );
}
