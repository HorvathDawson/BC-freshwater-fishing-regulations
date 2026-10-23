/**
 * T23 + T29 — the Regulations view of one tap (prototype regs v38 page layout), drawn inside the water frame (T44
 * `WaterFrame`: the back and the view strip, the same row on every view of the water, are the frame's — this screen
 * draws no header row, gutter or width of its own): the regs header (name, date pill, Parts, place label, the four tabs)
 * FIXED above ONE ScrollView that holds the four tab sections — "What you can keep", then the Gear / Methods
 * / Licence slots — and the pin bar over the scroll's top edge.
 *
 * Owns the layout state only: the open regs sheet (T37 SheetHost), the pin store (scroll offset → pinned lines + tab in view, off
 * React's render path) and the anchors. The tabs are not a pager (D1 a): a press scrolls to its section
 * (`tabScrollY`), and the scroll moves the selected tab (`tabInView`). No words of its own and no regulation logic:
 * every string is the RegsPage's.
 *
 * (T45) ON A WIDE SCREEN (`layout`, the water screen's `useLayoutClass`) the same page is arranged by `RegsWide`: the
 * page and its pin bar in a reading column of about 70 characters (tablet), and beside it a side column holding the date
 * pill, the sections as a list and the pinned lines (desktop). A phone is drawn exactly as before.
 *
 * ONE MAP ENTRY (T71, owner call 3, 2026-10-22): the regs header has no Map button; the water's Map view is offered once,
 * by the view strip the frame draws.
 */
import { useContext, useEffect, useMemo, useRef, useState } from "react";
import { AccessibilityInfo, Platform, ScrollView, Text, View } from "react-native";
import { creditsFor, parseSheetKey, tabScrollY, WORDS, type AccessNotice, type RegsPage, type SheetKey, type Tab } from "@app/core";
import { AccessNoticeBlock } from "./AccessNoticeBlock";
import { Credits } from "../Credits";
import { FreshDot } from "../menu/Fresh";
import { WaterInset } from "../WaterFrame";
import { ReportLink } from "../menu/ReportLink";
import { TYPE } from "../type";
import { ViewSwipe, WaterViewsContext } from "../ViewSwipe";
import { type Palette } from "../theme";
import { KeepTab } from "./KeepTab";
import { TermTitles, type KeepActions } from "./LineText";
import { createPinStore, type PinStore } from "./pinStore";
import { RegsHeader } from "./RegsHeader";
import { PinOver, ReadingColumn, RegsSide } from "./RegsWide";
import { SheetHost } from "./SheetHost";
import type { LayoutClass, RegsStart } from "@app/ui";
import { AnchorContext, useAnchors } from "./useAnchors";
import type { AdiposeImages, FishImages } from "./types";

declare const __DEV__: boolean | undefined;

/**
 * (T41) THE BROWSER WALK'S HOOK — in a development build on the web only (`app/tools/regs-walk.mjs`): open any sheet of
 * this page by its key without a reload, and read the pin bar's lines. Never in a release build, never on native.
 */
export interface RegsDebug { openSheet(k: string): boolean; pinLines(): string[] }
const debugOn = () => typeof __DEV__ !== "undefined" && __DEV__ && Platform.OS === "web";

/** A section slot: a node, or a node drawn with this screen's actions. */
export type Slot = React.ReactNode | ((actions: KeepActions) => React.ReactNode);

export interface RegsScreenProps {
  page: RegsPage;
  palette: Palette;
  images?: FishImages;
  /** (T38) the hatch sheet's two drawings (apps/mobile `ADIPOSE_IMAGES`); absent → an empty ground of their size */
  adipose?: AdiposeImages;
  /** what a Keep link does (the tab links are this screen's: they scroll to the section) */
  actions: Omit<KeepActions, "onTab">;
  /**
   * The date pill: opens "Which day?" (T49). The sheet is the water screen's, not this one's: a new day reloads the page
   * (this screen remounts on the next day's answer), and a sheet kept here closed under the reader at every step.
   */
  onDate: () => void;
  /** the Parts button; absent → it opens the page's own Parts sheet (T37). */
  onParts?: () => void;
  /**
   * (T58) A row of the Parts sheet pressed: read that part (the water screen makes it the frame's part — a move within
   * the water, no back layer). The sheet closes. Absent → the rows read nothing.
   */
  onPart?: (partIx: number) => void;
  /** Gear / Methods / Licence (T34/T35); absent → that section is not drawn, its tab is still listed. A function is handed
   *  this screen's actions (its tab links scroll to the section, like the Keep tab's). */
  gear?: Slot; methods?: Slot; licence?: Slot;
  /** the last child of the scroll */
  footer?: React.ReactNode;
  /**
   * (T45) how much room there is (@app/ui `useLayoutClass`, handed down by the water screen; this screen never reads the
   * window). "phone" (the default): the page as it always was. "tablet": the page and its pin bar held to the reading
   * column. "desktop": that, beside the side column (the date pill, the sections, the pinned lines) — `RegsWide`.
   */
  layout?: LayoutClass;
  /** a test injects its store (golden anchors); the app makes its own */
  store?: PinStore;
  /**
   * (T41) where the page starts — the web start link's (`@app/ui` `parseStartLink`), a test affordance: the sheet is open
   * at once; the scroll jumps to the tab's section (+ `scroll` px) once that section is measured. `onStarted` is called
   * when done, so the host hands the start to this page once (a new day remounts the page).
   */
  start?: RegsStart | null;
  onStarted?: () => void;
  /**
   * The release's edition has lapsed (core `regsLapsedNote`: past `regulations.valid_until`): its sentence, drawn first
   * in the scroll with the out-of-date dot (`FreshDot`, the Data block's). Null/absent: in force, or no stamp — nothing.
   */
  lapsed?: string | null;
  /**
   * (T54) The section's access notice (core `accessNotice`, through `@app/ui` `useAccessNotice`): the no-access banner or
   * the gentle note, drawn first in the scroll after the lapsed line. Null/absent: nothing to say.
   */
  access?: AccessNotice | null;
  /** (T54) the notice's link: show the area on the water's map. Absent → no link. */
  onAccessMap?: () => void;
}

export function RegsScreen({ page, palette, images, adipose, actions, onDate, onParts, onPart,
                             gear, methods, licence, footer, store: injected, layout = "phone", start = null, onStarted,
                             lapsed = null, access = null, onAccessMap }: RegsScreenProps) {
  const store = useMemo(() => injected ?? createPinStore(), [injected]);
  const scroll = useRef<ScrollView>(null);
  const content = useRef<View>(null);
  const { sink, section, remeasure } = useAnchors(page, content, store);
  // the sideways swipe between the water's views (Regulations · Conditions · Map): the frame's header, the regs header and
  // the scroll's content each listen (siblings, never nested — nested detectors race); see WaterViews in ViewSwipe.tsx.
  // Only the scroll's content follows the finger: the regs header listens `still` (it is one page with the frame's rows)
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
  // (T37) the one sheet over the page: every sheet key a press opens — in any section — lands here (the caller hears it too)
  const [sheet, setSheet] = useState<SheetKey | null>(start?.sheet ?? null);
  // (T41) the start link's scroll: polled until the tab's section has a measured top (images and fonts move it), once
  useEffect(() => {
    if (!start) return;
    if (start.tab === "keep" && start.scroll === 0) { onStarted?.(); return; }
    const timer = setInterval(() => {
      if (start.tab !== "keep" && store.getSections()[start.tab] == null) return;
      clearInterval(timer);
      store.selectTab(start.tab);
      scroll.current?.scrollTo({ y: tabScrollY(start.tab, store.getSections()) + start.scroll, animated: false });
      onStarted?.();
    }, 100);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per mount: a start is applied to the page it opened
  }, []);
  const openSheet = (k: SheetKey) => { actions.onSheet(k); setSheet(k); };
  useEffect(() => {
    if (!debugOn()) return;
    const g = globalThis as { __regsDebug?: RegsDebug };
    const hook: RegsDebug = {
      openSheet: (k) => { const key = parseSheetKey(k); if (key) setSheet(key); return key !== null; },
      pinLines: () => store.getLines().map((l) => (l.sub ? `${l.text} ${l.sub}` : l.text)),
    };
    g.__regsDebug = hook;
    return () => { if (g.__regsDebug === hook) delete g.__regsDebug; };
  }, [store]);
  const keepActions: KeepActions = { ...actions, onSheet: openSheet, onTab: toTab };
  const action = page.header.action;
  const onAction = action?.kind === "parts" ? (onParts ? () => onParts() : () => openSheet("parts" as SheetKey)) : undefined;

  // (T43) the pictures this page can show, credited at its foot (the app's whole list is the menu's Data sources page)
  const credits = useMemo(() => creditsFor(page), [page]);

  const head = <RegsHeader header={page.header} palette={palette} tabs={store} onDate={onDate} onAction={onAction} onTab={toTab}
                           side={layout === "desktop"} />;
  const sec = (tab: "gear" | "methods" | "licence", slot: Slot) => {
    const node = typeof slot === "function" ? slot(keepActions) : slot;
    if (!node) return null;
    const s = section(tab);
    return (
      <View ref={s.ref} onLayout={s.onLayout} collapsable={false} nativeID={`regs-${tab}`} testID={`regs-${tab}`}>
        {node}
      </View>
    );
  };
  const body = (
    <View ref={content} collapsable={false} testID="regs-content">
      {lapsed && (
        <WaterInset testID="regs-lapsed" style={{ paddingTop: 12, flexDirection: "row", alignItems: "flex-start", gap: 8 }}>
          <View style={{ paddingTop: 4 }}><FreshDot palette={palette} kind="stale" /></View>
          <Text accessibilityRole="alert" style={{ ...TYPE.metaBold, color: palette.ink, flex: 1 }}>{lapsed}</Text>
        </WaterInset>
      )}
      {access && <AccessNoticeBlock notice={access} palette={palette} onMap={onAccessMap} />}
      <AnchorContext.Provider value={sink}>
        <KeepTab keep={page.keep} dateLabel={page.header.date} palette={palette} actions={keepActions} images={images} />
      </AnchorContext.Provider>
      {sec("gear", gear)}
      {sec("methods", methods)}
      {sec("licence", licence)}
      {/* (T41 Q3) once, at the page's foot: the report is about the whole page (it names no tab) */}
      <ReportLink page={page} palette={palette} />
      {credits.length > 0 && (
        <WaterInset style={{ paddingTop: 20 }} testID="regs-fish-credits">
          <Credits palette={palette} ink={palette.sub} lines={credits} label={WORDS.FISH_PICTURES} />
        </WaterInset>
      )}
      {footer}
    </View>
  );

  // (T45) the page in the reading column (a phone: as it is); on a desktop, beside the side column
  const read = <ReadingColumn layout={layout} testID="regs-reading">{body}</ReadingColumn>;
  const pageScroll = (
    <View style={{ flex: 1 }}>
      <ScrollView ref={scroll} testID="regs-scroll" scrollEventThrottle={16} contentContainerStyle={{ flexGrow: 1, paddingBottom: 32 }}
                  onScroll={(e) => store.setScroll(e.nativeEvent.contentOffset.y)}
                  onLayout={(e) => store.setViewport(e.nativeEvent.layout.height)}
                  onContentSizeChange={(_w, h) => { store.setContentHeight(h); remeasure(); }}>
        {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{read}</ViewSwipe> : read}
      </ScrollView>
      {layout !== "desktop" && <PinOver layout={layout} store={store} palette={palette} />}
    </View>
  );

  // (T40) every ⓘ under the page — its sheets too — names its word for a screen reader from the page's glossary titles
  return (
    <TermTitles.Provider value={page.terms}>
    <View testID="regs-view" style={{ flex: 1, backgroundColor: palette.card }}>
      {swipe ? <ViewSwipe swipe={swipe} fill={false} still testId="view-swipe-regs-header">{head}</ViewSwipe> : head}
      {layout === "desktop" ? (
        <View testID="regs-columns" style={{ flex: 1, flexDirection: "row" }}>
          <RegsSide header={page.header} store={store} palette={palette} onDate={onDate} onTab={toTab} />
          {pageScroll}
        </View>
      ) : pageScroll}
      <SheetHost page={page} open={sheet} onOpen={openSheet} onClose={() => setSheet(null)} palette={palette} fish={images} adipose={adipose}
                 onTab={(t) => { setSheet(null); toTab(t); }} onBook={actions.onBook} layout={layout}
                 onPart={onPart && ((i) => { setSheet(null); onPart(i); })} />
    </View>
    </TermTitles.Provider>
  );
}
