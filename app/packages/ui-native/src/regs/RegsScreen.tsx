/**
 * T23 + T29 — the Regulations view of one tap (prototype regs v38 page layout), drawn inside the water frame (T44
 * `WaterFrame`: the back and the view strip, the same row on every view of the water, are the frame's — this screen
 * draws no header row, gutter or width of its own): the regs header (name, date pill, place label, the four tabs; no Parts since T150)
 * FIXED above ONE ScrollView that holds the four tab sections — "What you can keep", then the Gear / Methods
 * / Licence slots. (T810 cutover, owner 2026-11-17 23:29 "Remove it (Recommended)") The pin bar is gone.
 *
 * Owns the layout state only: the open regs sheet (T37 SheetHost), the tab store (scroll offset → tab in view, off React's
 * render path) and the section anchors. The tabs are not a pager (D1 a): a press scrolls to its section
 * (`tabScrollY`), and the scroll moves the selected tab (`tabInView`). No words of its own and no regulation logic:
 * every string is the RegsPage's.
 *
 * (T45) ON A WIDE SCREEN (`layout`, the water screen's `useLayoutClass`) the same page is arranged by `RegsWide`: the
 * page in a reading column of about 70 characters (tablet), and beside it a side column holding the date pill and the
 * sections as a list (desktop). A phone is drawn exactly as before.
 *
 * ONE MAP ENTRY (T71, owner call 3, 2026-10-22): the regs header has no Map button; the water's Map view is offered once,
 * by the view strip the frame draws.
 */
import { useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { openProbe } from "@app/data";
import { Platform, ScrollView, View, type ScrollViewInstance, type ViewInstance } from "react-native";
import { Text } from "../Text";
import { LEGAL_WORDS, parseSheetKey, withInSeason, withSalmonSheets, type SalmonCardVM, tabScrollY, type AccessNotice, type GlanceRowVM, type InSeasonBannerVM, type RegsPage, type TabSourcesVM, type SalmonStripVM, type SheetKey, type SunDay, type Tab, type DaylightAt } from "@app/core";
import { AccessNoticeBlock } from "./AccessNoticeBlock";
import { FreshDot } from "../menu/Fresh";
import { NO_SNIPPET, WaterInset } from "../WaterFrame";
import { ReportLink } from "../menu/ReportLink";
import { TYPE } from "../type";
import { ViewSwipe, WaterViewsContext } from "../ViewSwipe";
import { type Palette } from "../theme";
import { ReferenceFold } from "./ReferenceFold";
import { KeepTab } from "./KeepTab";
import { TermTitles, type KeepActions } from "./LineText";
import { createTabStore, type TabStore } from "./tabStore";
import { RegsHeader } from "./RegsHeader";
import { ReadingColumn, RegsSide } from "./RegsWide";
import { SheetHost } from "./SheetHost";
import { SourceRow } from "../SourceRow";
import { recallRegsScroll, rememberRegsScroll } from "./scrollMemory";
import type { LayoutClass, RegsStart } from "@app/ui";
import { AnchorContext, useAnchors } from "./useAnchors";
import { InSeasonBanners } from "./InSeasonBanner";
import type { FishImages } from "./types";
import { CoachScrollContext, useCoachScroller } from "../CoachMarks";

declare const __DEV__: boolean | undefined;

/**
 * (T41) THE BROWSER WALK'S HOOK — in a development build on the web only (`app/tools/regs-walk.mjs`): open any sheet of
 * this page by its key without a reload, and read the pin bar's lines. Never in a release build, never on native.
 */
export interface RegsDebug { openSheet(k: string): boolean }
const debugOn = () => typeof __DEV__ !== "undefined" && __DEV__ && Platform.OS === "web";

/** A section slot: a node, or a node drawn with this screen's actions. */
export type Slot = React.ReactNode | ((actions: KeepActions) => React.ReactNode);

export interface RegsScreenProps {
  page: RegsPage;
  palette: Palette;
  /** (T224) where the water's name came from when it is not the official one ("City of Abbotsford stream layer") */
  nameSource?: string | null;
  images?: FishImages;
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
  /** (T227) DFO's salmon year, drawn at the top of the Keep tab, above the provincial year bar (owner 2026-11-12) (`@app/core` `salmonStripOf`) */
  salmonStrip?: SalmonStripVM | null;
  /**
   * (T45) how much room there is (@app/ui `useLayoutClass`, handed down by the water screen; this screen never reads the
   * window). "phone" (the default): the page as it always was. "tablet": the page and its pin bar held to the reading
   * column. "desktop": that, beside the side column (the date pill, the sections, the pinned lines) — `RegsWide`.
   */
  layout?: LayoutClass;
  /** a test injects its store (golden anchors); the app makes its own */
  store?: TabStore;
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
  /** (T382) the day's sunrise and sunset at the water (`useSunLine`); (T878) a small line under the date pill; absent: none. */
  sun?: SunDay | null;
  /** (T991 round 3) the tab's Source row (core `regsSourcesOf`: every synopsis page the page cites — T474: the water's own
   *  among them — the in-season changes and DFO where shown, "All sources ›"). Required: the host computes it from the tabs it draws. */
  sources: TabSourcesVM;
  /** (T1031 item 3) the trout table's Source line (core `troutSourcesOf`), null where the page draws no trout table.
   *  Required: the host computes it with the foot row, which cites its pages too. */
  troutSources: TabSourcesVM | null;
  /** (T1031) the page's salmon card (core `salmonCard`): its species' sheets, Identification | Sources, join the page's
   *  (`withSalmonSheets`). Absent/null: no salmon card, the page's sheets alone. */
  salmonCard?: SalmonCardVM | null;
  /** (T473, T898) a daylight-only closure's open and closed hours on the day at the water (`useDaylight`), from its own
   *  offsets; null: no point known */
  daylight?: DaylightAt | null;
  /**
   * (T50) the water as opened (`regsScrollKey`): the page remembers its scroll under it and, mounted again without a
   * start link (a view or tab switch, back / forward, a new day, an Android recreation), returns there once the page is
   * tall enough to hold it. Absent: the page opens at the top, as before.
   */
  scrollKey?: string | null;
}

/** (T1049) ticks (100 ms) a start's target must hold before the start is done; ticks it is followed at most (~2 s) */
const START_STEADY = 2, START_FOLLOW = 20;

export function RegsScreen({ page, palette, images, actions, onDate,
                             gear, methods, licence, referenceOpen = false, footer, salmon, store: injected, layout = "phone", start = null, onStarted,
                             lapsed = null, access = [], inSeason = [], glance = null, salmonStrip = null,
                             nameSource = null, asOf = null, scrollKey = null, sun = null, daylight = null, sources, troutSources, salmonCard = null }: RegsScreenProps) {
  // (T286) the open probe (test bed only): the page's first render, and its first layout (mounted, before paint)
  openProbe.mark("regsRender");
  useLayoutEffect(() => { openProbe.mark("layout"); }, []);
  const store = useMemo(() => injected ?? createTabStore(), [injected]);
  const scroll = useRef<ScrollViewInstance>(null);
  const content = useRef<ViewInstance>(null);
  const { sink, section, remeasure } = useAnchors(page, content, store);
  // the sideways swipe between the water's views (Regulations · Conditions · Map): the frame's header, the regs header and
  // the scroll's content each listen (siblings, never nested — nested detectors race); see WaterViews in ViewSwipe.tsx.
  // Only the scroll's content follows the finger: the regs header listens `still` (it is one page with the frame's rows)
  const swipe = useContext(WaterViewsContext)?.bodySwipe ?? null;

  // prototype 2000: the pressed tab is selected at once and the page scrolls to its section (Keep → the top)
  // (T1025) a JUMP, never an animated pass: an animation from "What you can keep" to "Licence" scrolled through Gear and
  // Methods, and the strip's highlight (it follows the scroll) flickered through each one on the way
  const toTab = (t: Tab) => {
    store.selectTab(t);
    scroll.current?.scrollTo({ y: tabScrollY(t, store.getSections()), animated: false });
  };
  // (T37) the one sheet over the page: every sheet key a press opens — in any section — lands here (the caller hears it too)
  const [sheet, setSheet] = useState<SheetKey | null>(start?.sheet ?? null);
  // (T41) the start link's scroll: polled until the tab's section has a measured top (images and fonts move it), once
  // (T1038) per START, not per mount: a phone link to the water already on screen hands this page a new one, applied as a
  // fresh page applies its first — its sheet (none: shut), its tab and offset (Keep at 0 is the top)
  // `first`: the start the page MOUNTED with (none at mount: every later one is a new start on an open page)
  const firstStart = useRef(true);
  // (T1049) and FOLLOWED: the first measured top is not the last (on staging the Keep tab grows 903 → 1104 px ~35 ms after
  // the first layout, so a jump to it landed short: Licence on Methods). The jump is made again while the target moves,
  // and the start is done once it has held for START_STEADY ticks, or after START_FOLLOW ticks of a page still moving.
  useEffect(() => {
    const first = firstStart.current;
    firstStart.current = false;
    if (!start) return;
    if (!first) { setSheet(start.sheet); restoring.current = false; }
    if (first && start.tab === "keep" && start.scroll === 0) { onStarted?.(); return; }
    let at: number | null = null, held = 0, followed = 0;
    const timer = setInterval(() => {
      if (start.tab !== "keep" && store.getSections()[start.tab] == null) return;
      const y = tabScrollY(start.tab, store.getSections()) + start.scroll;
      if (y !== at && followed < START_FOLLOW) {
        at = y; held = 0; followed += 1;
        store.selectTab(start.tab);
        scroll.current?.scrollTo({ y, animated: false });
        return;
      }
      if (++held < START_STEADY && followed < START_FOLLOW) { followed += 1; return; }
      clearInterval(timer);
      onStarted?.();
    }, 100);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per start: a start is applied to the page it is handed to
  }, [start]);
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
    };
    g.__regsDebug = hook;
    return () => { if (g.__regsDebug === hook) delete g.__regsDebug; };
  }, [store]);
  const keepActions: KeepActions = { ...actions, onSheet: openSheet, onTab: toTab };

  // (T227) a tab whose section is not drawn (Gear / Methods on a day nothing may be fished) is not offered
  const header = useMemo(() => ({ ...page.header, tabs: page.header.tabs.filter((t) =>
    (t.tab !== "gear" || !!gear) && (t.tab !== "methods" || !!methods)) }), [page.header, gear, methods]);
  // (T990) `src:all` also lists the Province's in-season changes this water shows (the screen's, not the page's)
  // (T1031) and the salmon species' sheets (the screen's card, not the page's)
  const hostPage = useMemo(() => ({ ...page, sheet: withSalmonSheets(withInSeason(page.sheet, inSeason), salmonCard) }), [page, inSeason, salmonCard]);
  const head = <RegsHeader header={header} palette={palette} tabs={store} onDate={onDate} onTab={toTab} onTerm={(id) => openSheet(`gl:${id}`)}
                           side={layout === "desktop"} nameSource={nameSource} sun={sun} />;
  const sec = (tab: "gear" | "methods" | "licence", slot: Slot) => {
    const node = typeof slot === "function" ? slot(keepActions) : slot;
    if (!node) return null;
    const s = section(tab);
    return (
      <View ref={s.ref} onLayout={s.onLayout} collapsable={false} nativeID={`regs-${tab}`} testID={`regs-${tab}`} {...(tab === "licence" ? NO_SNIPPET : {})}>
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
      {/* (T565, owner 2026-11-09) an in-season change is B.C. Fish and Wildlife's notice for the WATER, not a salmon or a
          species thing: first on the page, above the salmon card and every calendar and species block (T472: collapsed to one line) */}
      <InSeasonBanners banners={inSeason} palette={palette} />
      {/* (T395) the vessel restrictions are no longer here: their own section under Methods (`MethodsTab` boats) */}
      {/* (T387 review) the glance folds the card into its table only when it has the card's row: a card still coming, or
          failed, is drawn here in its place (its pending / failed face) — never a silent gap */}
      {(!glance || !glance.salmon) && (typeof salmon === "function" ? salmon({ actions: keepActions, images }) : salmon)}
      <AnchorContext.Provider value={sink}>
          <KeepTab keep={page.keep} troutSources={troutSources} daylight={daylight} dateLabel={page.header.date} palette={palette} actions={keepActions} images={images} salmonStrip={salmonStrip}
                   glance={glance ? { salmon: glance.salmon && salmon
                     ? { row: glance.salmon, card: () => (typeof salmon === "function" ? salmon({ actions: keepActions, images }) : salmon) }
                     : null } : null} />
      </AnchorContext.Provider>
      {sec("gear", gear)}
      {sec("methods", methods)}
      {sec("licence", licence)}
      {/* (T516, owner 2026-11-08) the "Handling fish" reference closes the page, after Licence: it is background reading,
          not one of the tabs (null on a closed day, as before) */}
      {methods && page.methods?.reference && (
        <WaterInset testID="regs-reference" noSnippet>
          <ReferenceFold vm={page.methods.reference} palette={palette} actions={keepActions} initiallyOpen={referenceOpen} />
        </WaterInset>
      )}
      {/* (T236) every page's foot: not legal advice, and how current its sources are — (owner 2026-11-04) as fine print
          under a hairline, so it does not read as part of the page */}
      <WaterInset testID="regs-legal" noSnippet style={{ marginTop: 20, paddingTop: 10, gap: 2, borderTopWidth: 1, borderTopColor: palette.line }}>
        <Text style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{LEGAL_WORDS.footer}</Text>
        {asOf && <Text testID="regs-as-of" style={{ ...TYPE.caption, lineHeight: 15, color: palette.sub }}>{asOf}</Text>}
      </WaterInset>
      {/* (T990, owner 2026-11-21: "It will be at bottom of page of regs"; T991 round 3, owner 2026-11-22: one Source row per
          tab) the last thing at the end of the scrolled page, same place on every water, just above "Report a problem": the
          authorities this page cites — each synopsis page it cites (T474: the water's own row among them, so the water's
          page is always linked, and once), the in-season changes, DFO — then "All sources ›", the breakdown sheet */}
      <WaterInset testID="regs-all-sources" style={{ marginTop: 4 }}>
        <SourceRow vm={sources} palette={palette} onBook={actions.onBook} onSheet={openSheet} testID="regs-sources" sheetTestID="regs-all-sources-button" />
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
  // (T842) one ref for the page's life: the walkthrough's highlight is tied to this scroll view's native scroll event
  const pageRef = useCallback((n: ScrollViewInstance | null) => {
    (scroll as { current: ScrollViewInstance | null }).current = n; coachScroll.ref(n);
  }, [scroll, coachScroll]);
  const pageScroll = (
    <View style={{ flex: 1 }}>
      <ScrollView ref={pageRef} testID="regs-scroll" scrollEventThrottle={16} onScrollBeginDrag={() => { restoring.current = false; }} contentContainerStyle={{ flexGrow: 1, paddingBottom: 32 }}
                  onScroll={(e) => { const y = e.nativeEvent.contentOffset.y; store.setScroll(y); coachScroll.track(y);
                                     if (scrollKey && !restoring.current) rememberRegsScroll(scrollKey, y); }}
                  onLayout={(e) => { viewH.current = e.nativeEvent.layout.height; store.setViewport(e.nativeEvent.layout.height); }}
                  onContentSizeChange={(_w, h) => { contentH.current = h; store.setContentHeight(h); remeasure(); }}>
        {/* (T456) the walkthrough brings a step's control on this page into view */}
        <CoachScrollContext.Provider value={coachScroll.scroller}>
          {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{read}</ViewSwipe> : read}
        </CoachScrollContext.Provider>
      </ScrollView>
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
          <RegsSide header={header} store={store} palette={palette} onDate={onDate} onTab={toTab} sun={sun} />
          {pageScroll}
        </View>
      ) : pageScroll}
      <SheetHost page={hostPage} open={sheet} onOpen={openSheet} onClose={() => setSheet(null)} palette={palette} fish={images}
                 onTab={(t) => { setSheet(null); toTab(t); }} onBook={actions.onBook} layout={layout} />
    </View>
    </TermTitles.Provider>
  );
}
