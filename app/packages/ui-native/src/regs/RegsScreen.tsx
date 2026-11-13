/**
 * T23 + T29 — the Regulations view of one tap (prototype regs v38 page layout), drawn inside the water frame (T44
 * `WaterFrame`: the back and the view strip, the same row on every view of the water, are the frame's — this screen
 * draws no header row, gutter or width of its own): the regs header (name, date pill, place label, the four tabs; no Parts since T150)
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
import { useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { openProbe } from "@app/data";
import { AccessibilityInfo, Animated, Platform, ScrollView, View, type ScrollViewInstance, type ViewInstance } from "react-native";
import { Text } from "../Text";
import { LEGAL_WORDS, parseSheetKey, tabScrollY, type AccessNotice, type GlanceRowVM, type InSeasonBannerVM, type Line, type RegsPage, type SalmonStripVM, type SheetKey, type Tab } from "@app/core";
import { AccessNoticeBlock } from "./AccessNoticeBlock";
import { FreshDot } from "../menu/Fresh";
import { WaterInset } from "../WaterFrame";
import { ReportLink } from "../menu/ReportLink";
import { TYPE } from "../type";
import { ViewSwipe, WaterViewsContext } from "../ViewSwipe";
import { type Palette } from "../theme";
import { ReferenceFold } from "./ReferenceFold";
import { KeepTab } from "./KeepTab";
import { LineText, TermTitles, type KeepActions } from "./LineText";
import { createPinStore, type PinStore } from "./pinStore";
import { RegsHeader } from "./RegsHeader";
import { PinOver, ReadingColumn, RegsSide } from "./RegsWide";
import { SheetHost } from "./SheetHost";
import { recallRegsScroll, rememberRegsScroll } from "./scrollMemory";
import type { LayoutClass, RegsStart } from "@app/ui";
import { AnchorContext, StickyContext, useAnchors } from "./useAnchors";
import { InSeasonBanners } from "./InSeasonBanner";
import type { AdiposeImages, FishImages } from "./types";
import { CoachScrollContext, useCoachScroller } from "../CoachMarks";

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
  /** (T224) where the water's name came from when it is not the official one ("City of Abbotsford stream layer") */
  nameSource?: string | null;
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
  /** Gear / Methods / Licence (T34/T35); absent → that section is not drawn, its tab is still listed. A function is handed
   *  this screen's actions (its tab links scroll to the section, like the Keep tab's). */
  gear?: Slot; methods?: Slot; licence?: Slot;
  /** (T516) the Reference fold's first state: closed; a render test opens it to read the folded lines */
  referenceOpen?: boolean;
  /** the last child of the scroll */
  footer?: React.ReactNode;
  /** (T211) the "Salmon · DFO" card, drawn ABOVE the Keep tab's provincial rules (owner 2026-10-28); absent → none. A
   *  function is handed this screen's actions and the fish pictures (its species open their fish sheets). */
  salmon?: React.ReactNode | ((a: { actions: KeepActions; images?: FishImages }) => React.ReactNode);
  /**
   * (T221, owner 2026-10-28 — a try) THE AT-A-GLANCE TABLE: the Keep tab's species list carries the quota groups and the
   * Salmon card as folded rows (`KeepTab` `glance`). `salmon` is then that card's row (`@app/core` `salmonGlance`), the
   * card drawn when it is opened, not above the Keep tab. Absent: the page as it was (the cards in full).
   */
  glance?: { salmon: GlanceRowVM | null } | null;
  /** (T227) DFO's salmon year, drawn under the provincial year bar at the top of the Keep tab (`@app/core` `salmonStripOf`) */
  salmonStrip?: SalmonStripVM | null;
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
   * (T54, T387) The page's PART's access notices (core `pageAccessNotices`, through `@app/ui` `useAccessNotices`; the
   * same for every tap on the part): the no-access banner and/or the gentle note, (T367) pinned
   * as slim strips under the header, outside the scroll. Empty/absent: nothing to say.
   */
  access?: readonly AccessNotice[];
  /** (T230) the provincial in-season changes on this water / part on the page's day (`useInSeasonBanners`), drawn
   *  as one collapsed line above the species table (T472); informational, they change no answer. */
  inSeason?: readonly InSeasonBannerVM[];
  /**
   * (T236) how current this page's sources are (core `asOfLine`: "Regulations as of … · salmon feed … · in-season …"),
   * drawn at the page's foot under the "not legal advice" line (`LEGAL_WORDS.footer`, always drawn). Absent: that line only.
   */
  asOf?: string | null;
  /** (T382) the day's sunrise and sunset at the water (`useSunLine`), a small line in the page's header; absent: none. */
  sun?: string | null;
  /** (T473) a daylight-only closure's open hours on the day at the water, from its own offsets; null: no point known */
  /** (T474) the water's own entry in the rule book (core `waterBookOf`): "This water in the rule book p.N ↗"; empty: none */
  waterBook?: Line;
  daylight?: ((after: number, before: number) => string | null) | null;
  /**
   * (T50) the water as opened (`regsScrollKey`): the page remembers its scroll under it and, mounted again without a
   * start link (a view or tab switch, back / forward, a new day, an Android recreation), returns there once the page is
   * tall enough to hold it. Absent: the page opens at the top, as before.
   */
  scrollKey?: string | null;
}

export function RegsScreen({ page, palette, images, adipose, actions, onDate,
                             gear, methods, licence, referenceOpen = false, footer, salmon, store: injected, layout = "phone", start = null, onStarted,
                             lapsed = null, access = [], inSeason = [], glance = null, salmonStrip = null,
                             nameSource = null, asOf = null, scrollKey = null, sun = null, daylight = null, waterBook = [] }: RegsScreenProps) {
  // (T286) the open probe (test bed only): the page's first render, and its first layout (mounted, before paint)
  openProbe.mark("regsRender");
  useLayoutEffect(() => { openProbe.mark("layout"); }, []);
  const store = useMemo(() => injected ?? createPinStore(), [injected]);
  const scroll = useRef<ScrollViewInstance>(null);
  const content = useRef<ViewInstance>(null);
  const { sink, section, remeasure } = useAnchors(page, content, store);
  const scrollY = useRef(new Animated.Value(0)).current;
  const sticky = useMemo(() => ({ store, overlay: layout !== "desktop", scrollY }), [store, layout, scrollY]);
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
  // (T50) back where the reader was: the remembered scroll, once the content is tall enough to hold it (polled: images
  // and fonts grow it), or after ~3 s as far as it goes. A start link wins (it says where to go).
  // Until then the page's own first scroll events (a layout settling at 0) do not overwrite the memory.
  const contentH = useRef(0), viewH = useRef(0);
  const restoring = useRef(!start && !!scrollKey && (recallRegsScroll(scrollKey)?.y ?? 0) > 0);
  useEffect(() => {
    if (!restoring.current || !scrollKey) return;
    const at = recallRegsScroll(scrollKey)!;
    let tries = 0;
    const timer = setInterval(() => {
      tries++;
      if (!restoring.current) { clearInterval(timer); return; }   // the reader took the scroll: leave it with them
      if (contentH.current < at.y + viewH.current && tries < 30) return;
      clearInterval(timer);
      restoring.current = false;
      scroll.current?.scrollTo({ y: at.y, animated: false });
    }, 100);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per mount, like the start link
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

  // (T227) a tab whose section is not drawn (Gear / Methods on a day nothing may be fished) is not offered
  const header = useMemo(() => ({ ...page.header, tabs: page.header.tabs.filter((t) =>
    (t.tab !== "gear" || !!gear) && (t.tab !== "methods" || !!methods)) }), [page.header, gear, methods]);
  const head = <RegsHeader header={header} palette={palette} tabs={store} onDate={onDate} onTab={toTab}
                           side={layout === "desktop"} nameSource={nameSource} sun={sun} />;
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
      {waterBook.length > 0 && (
        <WaterInset testID="regs-water-book" style={{ paddingTop: 10 }}>
          <LineText line={waterBook} style={TYPE.metaBold} color={palette.accent} palette={palette} actions={keepActions} />
        </WaterInset>
      )}
      {/* (T565, owner 2026-11-09) an in-season change is B.C. Fish and Wildlife's notice for the WATER, not a salmon or a
          species thing: first on the page, above the salmon card and every calendar and species block (T472: collapsed to one line) */}
      <InSeasonBanners banners={inSeason} palette={palette} />
      {/* (T395) the vessel restrictions are no longer here: their own section under Methods (`MethodsTab` boats) */}
      {/* (T387 review) the glance folds the card into its table only when it has the card's row: a card still coming, or
          failed, is drawn here in its place (its pending / failed face) — never a silent gap */}
      {(!glance || !glance.salmon) && (typeof salmon === "function" ? salmon({ actions: keepActions, images }) : salmon)}
      <AnchorContext.Provider value={sink}>
        <StickyContext.Provider value={sticky}>
          <KeepTab keep={page.keep} daylight={daylight} dateLabel={page.header.date} palette={palette} actions={keepActions} images={images} salmonStrip={salmonStrip}
                   glance={glance ? { salmon: glance.salmon && salmon
                     ? { row: glance.salmon, card: () => (typeof salmon === "function" ? salmon({ actions: keepActions, images }) : salmon) }
                     : null } : null} />
        </StickyContext.Provider>
      </AnchorContext.Provider>
      {sec("gear", gear)}
      {sec("methods", methods)}
      {sec("licence", licence)}
      {/* (T516, owner 2026-11-08) the "Handling fish" reference closes the page, after Licence: it is background reading,
          not one of the tabs (null on a closed day, as before) */}
      {methods && page.methods?.reference && (
        <WaterInset testID="regs-reference">
          <ReferenceFold vm={page.methods.reference} palette={palette} actions={keepActions} initiallyOpen={referenceOpen} />
        </WaterInset>
      )}
      {/* (T236) every page's foot: not legal advice, and how current its sources are — (owner 2026-11-04) as fine print
          under a hairline, so it does not read as part of the page */}
      <WaterInset testID="regs-legal" style={{ marginTop: 20, paddingTop: 10, gap: 2, borderTopWidth: 1, borderTopColor: palette.line }}>
        <Text style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{LEGAL_WORDS.footer}</Text>
        {asOf && <Text testID="regs-as-of" style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{asOf}</Text>}
      </WaterInset>
      {/* (T41 Q3) once, at the page's foot: the report is about the whole page (it names no tab) */}
      <ReportLink page={page} palette={palette} />
      {/* (owner 2026-10-29) the fish pictures are credited on More → Sources and licences only, not at the page's foot */}
      {footer}
    </View>
  );

  // (T45) the page in the reading column (a phone: as it is); on a desktop, beside the side column
  const read = <ReadingColumn layout={layout} testID="regs-reading">{body}</ReadingColumn>;
  const coachScroll = useCoachScroller();
  const pageScroll = (
    <View style={{ flex: 1 }}>
      <ScrollView ref={(n) => { (scroll as { current: ScrollViewInstance | null }).current = n; coachScroll.ref(n); }} testID="regs-scroll" scrollEventThrottle={16} onScrollBeginDrag={() => { restoring.current = false; }} contentContainerStyle={{ flexGrow: 1, paddingBottom: 32 }}
                  onScroll={(e) => { const y = e.nativeEvent.contentOffset.y; scrollY.setValue(y); store.setScroll(y); coachScroll.track(y);
                                     if (scrollKey && !restoring.current) rememberRegsScroll(scrollKey, y); }}
                  onLayout={(e) => { viewH.current = e.nativeEvent.layout.height; store.setViewport(e.nativeEvent.layout.height); }}
                  onContentSizeChange={(_w, h) => { contentH.current = h; store.setContentHeight(h); remeasure(); }}>
        {/* (T456) the walkthrough brings a step's control on this page into view */}
        <CoachScrollContext.Provider value={coachScroll.scroller}>
          {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{read}</ViewSwipe> : read}
        </CoachScrollContext.Provider>
      </ScrollView>
      {layout !== "desktop" && <PinOver layout={layout} store={store} palette={palette} />}
    </View>
  );

  // (T40) every ⓘ under the page — its sheets too — names its word for a screen reader from the page's glossary titles
  return (
    <TermTitles.Provider value={page.terms}>
    <View testID="regs-view" style={{ flex: 1, backgroundColor: palette.card }}>
      {swipe ? <ViewSwipe swipe={swipe} fill={false} still testId="view-swipe-regs-header">{head}</ViewSwipe> : head}
      {/* (T367) the access facts are pinned under the header, outside the scroll: one slim strip each */}
      <AccessNoticeBlock notices={access} palette={palette} />
      {layout === "desktop" ? (
        <View testID="regs-columns" style={{ flex: 1, flexDirection: "row" }}>
          <RegsSide header={header} store={store} palette={palette} onDate={onDate} onTab={toTab} />
          {pageScroll}
        </View>
      ) : pageScroll}
      <SheetHost page={page} open={sheet} onOpen={openSheet} onClose={() => setSheet(null)} palette={palette} fish={images} adipose={adipose}
                 onTab={(t) => { setSheet(null); toTab(t); }} onBook={actions.onBook} layout={layout} />
    </View>
    </TermTitles.Provider>
  );
}
