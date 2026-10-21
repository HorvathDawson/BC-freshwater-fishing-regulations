/**
 * T23 — the Regulations view of one tap (prototype regs v38 page layout): the water's header row (FaceBar: back and
 * the view strip, the same row on every view of the water) and the regs header (name, date pill, Map, place label, the
 * four tabs) FIXED above ONE ScrollView that holds the four tab sections — "What you can keep", then the Gear / Methods
 * / Licence slots — and the pin bar over the scroll's top edge.
 *
 * Owns the layout state only: the day sheet's open flag, the pin store (scroll offset → pinned lines + tab in view, off
 * React's render path) and the anchors. The tabs are not a pager (D1 a): a press scrolls to its section
 * (`tabScrollY`), and the scroll moves the selected tab (`tabInView`). No words of its own and no regulation logic:
 * every string is the RegsPage's.
 *
 * Owner rule "one water screen": the Map button frames THIS water's map view through the water's own view strip
 * (WaterScreen hands `onMap`); it is not another way into a water.
 */
import { useContext, useEffect, useMemo, useRef, useState } from "react";
import { AccessibilityInfo, ScrollView, Text, View } from "react-native";
import { tabScrollY, WORDS, type RegsPage, type Tab } from "@app/core";
import { FaceBar } from "../Faces";
import { TYPE } from "../type";
import { ViewSwipe, WaterViewsContext } from "../ViewSwipe";
import { GUTTER, SPACE, type Palette } from "../theme";
import { DaySheet } from "./DaySheet";
import { KeepTab } from "./KeepTab";
import type { KeepActions } from "./LineText";
import { createPinStore, type PinStore } from "./pinStore";
import { RegsHeader } from "./RegsHeader";
import type { FishImages } from "./types";

export interface RegsScreenProps {
  page: RegsPage;
  palette: Palette;
  images?: FishImages;
  /** what a Keep link does (the tab links are this screen's: they scroll to the section) */
  actions: Omit<KeepActions, "onTab">;
  /** THE ONE DAY (the Shell's `mapDay`; null = today) and its setter — the date pill's sheet moves it for the map too (D7) */
  day: Date | null;
  onDay: (d: Date | null) => void;
  /** back, drawn in the water's header row (FaceBar) */
  onBack?: () => void;
  /** the Map button: this water's map view. Absent → the button is not drawn (its slot stays). */
  onMap?: () => void;
  /** the Parts button (T37's sheet). Absent → not drawn. */
  onParts?: () => void;
  /** Gear / Methods / Licence (T34/T35); absent → that section is not drawn, its tab is still listed */
  gear?: React.ReactNode; methods?: React.ReactNode; licence?: React.ReactNode;
  /** the last child of the scroll */
  footer?: React.ReactNode;
  /** a test injects its store (golden anchors); the app makes its own */
  store?: PinStore;
}

/** A tab section whose presenter the app does not draw yet: says so, and where to look instead (never a blank). */
export function TabPending({ label, palette }: { label: string; palette: Palette }) {
  return (
    <View testID="regs-tab-pending" style={{ margin: GUTTER, padding: 14, borderWidth: 1, borderStyle: "dashed", borderColor: palette.line2 }}>
      <Text style={[TYPE.regsBody, { color: palette.sub }]}>{WORDS.TAB_PENDING(label)}</Text>
    </View>
  );
}

export function RegsScreen({ page, palette, images, actions, day, onDay, onBack, onMap, onParts,
                             gear, methods, licence, footer, store: injected }: RegsScreenProps) {
  const store = useMemo(() => injected ?? createPinStore(), [injected]);
  const scroll = useRef<ScrollView>(null);
  const [dayOpen, setDayOpen] = useState(false);
  // the sideways swipe between the water's views (Regulations · Conditions · Map): the fixed header and the scroll's
  // content each listen (siblings, never nested — nested detectors race); see WaterViews in ViewSwipe.tsx
  const swipe = useContext(WaterViewsContext)?.bodySwipe ?? null;
  const still = useRef(false);
  useEffect(() => {
    let live = true;
    void AccessibilityInfo.isReduceMotionEnabled().then((on) => { if (live) still.current = on; });
    return () => { live = false; };
  }, []);

  // prototype 2000: the pressed tab is selected at once and the page scrolls to its section (Keep → the top)
  const toTab = (t: Tab) => {
    store.selectTab(t);
    scroll.current?.scrollTo({ y: tabScrollY(t, store.getSections()), animated: !still.current });
  };
  const keepActions: KeepActions = { ...actions, onTab: toTab };
  const action = page.header.action;
  const onAction = action?.kind === "map" ? onMap && (() => onMap())
    : action?.kind === "parts" ? onParts && (() => onParts()) : undefined;

  const head = (
    <View>
      {/* THE SAME INSETS AS THE CONDITIONS AND MAP VIEWS, so the water's header row does not move between them */}
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s2 }}>
        <FaceBar palette={palette} face="regulations" onBack={onBack} backLabel="Back" onFace={() => {}} />
      </View>
      <RegsHeader header={page.header} palette={palette} tabs={store} onDate={() => setDayOpen(true)}
                  onAction={onAction} onTab={toTab} />
    </View>
  );
  const sec = (tab: "gear" | "methods" | "licence", node: React.ReactNode) =>
    node ? <View nativeID={`regs-${tab}`} testID={`regs-${tab}`}>{node}</View> : null;
  const body = (
    <View testID="regs-content">
      <KeepTab keep={page.keep} dateLabel={page.header.date} palette={palette} actions={keepActions} images={images} />
      {sec("gear", gear)}
      {sec("methods", methods)}
      {sec("licence", licence)}
      {footer}
    </View>
  );

  return (
    <View testID="regs-view" style={{ flex: 1, backgroundColor: palette.card }}>
      {swipe ? <ViewSwipe swipe={swipe} fill={false} testId="view-swipe-regs-header">{head}</ViewSwipe> : head}
      <View style={{ flex: 1 }}>
        <ScrollView ref={scroll} testID="regs-scroll" scrollEventThrottle={16} contentContainerStyle={{ flexGrow: 1, paddingBottom: 32 }}
                    onScroll={(e) => store.setScroll(e.nativeEvent.contentOffset.y)}
                    onLayout={(e) => store.setViewport(e.nativeEvent.layout.height)}
                    onContentSizeChange={(_w, h) => store.setContentHeight(h)}>
          {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{body}</ViewSwipe> : body}
        </ScrollView>
      </View>
      <DaySheet open={dayOpen} onClose={() => setDayOpen(false)} palette={palette} day={day} onDay={onDay} />
    </View>
  );
}
