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
import { AppState, Platform, ScrollView, View, type ScrollViewInstance, type ViewInstance } from "react-native";
import { Text } from "../Text";
import { LEGAL_WORDS, OPEN, now as clockNow, environmentError, parseSheetKey, reportFailure, startEndSentence, withInSeason, withSalmonSheets, type SalmonCardVM, tabScrollY, isStartOwnScroll, startEndOf, startTick, START_FOLLOW_0, START_TICK_MS, type StartFollow, type AccessNotice, type GlanceRowVM, type InSeasonBannerVM, type RegsPage, type TabSourcesVM, type SalmonStripVM, type SheetKey, type SunDay, type Tab, type DaylightAt } from "@app/core";
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
import { recallRegsScroll, regsScrollItem, rememberRegsScroll } from "./scrollMemory";
import type { LayoutClass, RegsStart } from "@app/ui";
import { AnchorContext, useAnchors } from "./useAnchors";
import { InSeasonBanners } from "./InSeasonBanner";
import { startLog } from "./startProbe";
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
  onStarted?: (spent: RegsStart) => void;
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
  /**
   * (T1049 cold start) a part of the page drawn ABOVE its sections is still coming (the salmon card, waiting on its feed):
   * a start link is not landed while it is, however still the page looks (the host's `salmonExpected` and still loading).
   */
  coming?: boolean;
  /** (T1140) the header's line to the other parts pressed: the water's Map, its list of parts open (`RegsHeader`) */
  onParts?: () => void;
}

export function RegsScreen({ page, palette, images, actions, onDate,
                             gear, methods, licence, referenceOpen = false, footer, salmon, store: injected, layout = "phone", start = null, onStarted,
                             lapsed = null, access = [], inSeason = [], glance = null, salmonStrip = null,
                             nameSource = null, asOf = null, scrollKey = null, sun = null, daylight = null, sources, troutSources, salmonCard = null,
                             coming = false, onParts }: RegsScreenProps) {
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
    startLog("tab", { t });
    startTaken.current = true;   // (T1049) the reader's press: a start still following its section stops here
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
  /** (T1049) the reader took the page (a drag, a tab press, a scroll not the start's own): the start stops following */
  const startTaken = useRef(false);
  /** (T1049) every y the start asked the page for: a scroll event at one of them (or at the page's end, for a y beyond it)
   *  is the start's own, whenever it arrives (`isStartOwnScroll`; never a time window: a slow phone's event comes late) */
  const startAsked = useRef<number[]>([]);
  /** (T1049 cold start) every page end (content - view) seen while the start follows: a jump asked beyond one lands there */
  const startEnds = useRef<number[]>([]);
  /** (T1049 cold start) a start is following (its ends are kept only then) */
  const startOn = useRef(false);
  /** (T1049 review) the tab the start following is for (its section must be drawn before a page end is kept) */
  const startTab = useRef<Tab>("keep");
  const recordStartEnd = (h: number) => {
    const end = startEndOf(h, viewH.current, startTab.current === "keep" || store.getSections()[startTab.current] != null);
    if (end != null) startEnds.current.push(end);
  };
  /** (T1049 cold start) `coming`, read by the follow's timer (a prop change does not restart a start) */
  const comingNow = useRef(coming);
  comingNow.current = coming;
  /** (T1049 review) the water this page shows (its scroll key's item): a start is for the water it was handed with */
  const waterNow = useRef(regsScrollItem(scrollKey));
  waterNow.current = regsScrollItem(scrollKey);
  /** (T1049 review) the app in front: time in the background is not the follow's (core `startTick`, `active`) */
  const activeNow = useRef(AppState.currentState !== "background" && AppState.currentState !== "inactive");
  useEffect(() => {
    const sub = AppState.addEventListener("change", (st) => { activeNow.current = st !== "background" && st !== "inactive"; });
    return () => sub.remove();
  }, []);
  // (T1049) and FOLLOWED until the page has SETTLED (core `startTick`): the first measured top is not the last. On staging
  // the Keep tab grew 903 → 1104 px ~35 ms after the first layout; on a slow phone's cold start (0.9.0 device pass) the
  // salmon card, waiting on its feed, landed 201 dp above the sections seconds after a follow that had already stopped
  // (~2 s), so Gear and Licence were left short. The start jumps again whenever the page is not where it wants it, and is
  // done once target, content and scroll hold still with nothing still `coming`. Bounded twice (core): a page still moving
  // with nothing to come is said as a defect; a part still on its way past the line's own give-up drops the start as the
  // line's. The reader's drag, tab press, sheet or own scroll ends it at once.
  // (T1049 review) A start is SPENT once, for the page it was handed to: landed or ended, and also when the follow is torn
  // down unfinished (the page left for another water, unmounted): `onStarted(start)` then too, so the host never hands it
  // to the next page (the web: another water opened at this link's tab; the phone: Back re-applying it).
  useEffect(() => {
    const first = firstStart.current;
    firstStart.current = false;
    if (!start) return;
    if (!first) { setSheet(start.sheet); restoring.current = false; }
    if (first && start.tab === "keep" && start.scroll === 0) { onStarted?.(start); return; }
    let s: StartFollow = START_FOLLOW_0;
    const water = waterNow.current;
    startTaken.current = false; startAsked.current = []; startEnds.current = []; startOn.current = true; startTab.current = start.tab;
    startLog("begin", { tab: start.tab, scroll: start.scroll, first });
    const end = (why: string) => {
      clearInterval(timer);
      startOn.current = false;
      startLog("done", { why, at: s.at, drawn: s.drawn, tops: store.getSections(), ch: contentH.current, sy: scrollY.current });
      onStarted?.(start);
    };
    const timer = setInterval(() => {
      // another water on this page: the start was for the one it was handed with — spent, never landed here
      if (waterNow.current !== water) { end("moved"); return; }
      const top = start.tab === "keep" ? 0 : store.getSections()[start.tab];
      const [next, step] = startTick(s, { now: clockNow(), active: activeNow.current,
        target: top == null ? null : tabScrollY(start.tab, store.getSections()) + start.scroll,
        contentH: contentH.current, viewH: viewH.current, scrollY: scrollY.current, loading: comingNow.current, taken: startTaken.current });
      s = next;
      if (step.do === "wait") return;
      if (step.do === "jump") {
        store.selectTab(start.tab);
        startAsked.current.push(step.y);
        startLog("jump", { y: step.y, drawn: s.drawn, tops: store.getSections(), ch: contentH.current, vh: viewH.current, sy: scrollY.current, coming: comingNow.current });
        scroll.current?.scrollTo({ y: step.y, animated: false });
        return;
      }
      // (T1049) a tab this page never draws (Gear on a closed day): said, and the start is done — never polled for ever
      if (step.why === "no-section") reportFailure(new Error(`start link: tab=${start.tab} has no section on this page: the page opens at its top`));
      // (T1049 review) a page that never held still with nothing to come: a defect, said; a part still on its way past the
      // line's own give-up: the line's (core `environmentError`: shown by the part's own pending face, never a defect report)
      const secs = Math.round((s.clock - (s.drawn ?? s.clock)) / 1000);
      if (step.why === "cap") reportFailure(new Error(startEndSentence(start.tab, "cap", secs)));
      if (step.why === "coming-cap") reportFailure(environmentError(new Error(startEndSentence(start.tab, "coming-cap", secs))));
      end(step.why);
    }, START_TICK_MS);
    return () => {
      clearInterval(timer);
      // torn down unfinished (unmounted, or a newer start handed over): this start is spent all the same
      if (startOn.current) { startOn.current = false; startLog("done", { why: "torn-down" }); onStarted?.(start); }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per start: a start is applied to the page it is handed to
  }, [start]);
  // (T50) back where the reader was: the remembered scroll, once the content is tall enough to hold it (polled: images
  // and fonts grow it), or after ~3 s as far as it goes. A start link wins (it says where to go).
  // Until then the page's own first scroll events (a layout settling at 0) do not overwrite the memory.
  const contentH = useRef(0), viewH = useRef(0), scrollY = useRef(0);
  // (T1049) the start probe (test bed only): the page's tops, content height and scroll as they change, 60 s from mount
  useEffect(() => {
    if (!openProbe.on()) return;
    startLog("mount", { start, salmon: typeof salmon, inSeason: inSeason.length, glance: !!glance, salmonCard: !!salmonCard });
    let last = "", n = 0;
    const timer = setInterval(() => {
      const s = { tops: store.getSections(), ch: contentH.current, vh: viewH.current, sy: scrollY.current };
      const k = JSON.stringify(s);
      if (k !== last) { last = k; startLog("tops", s); }
      if (++n >= 600) clearInterval(timer);
    }, 100);
    return () => { clearInterval(timer); startLog("unmount"); };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- once per mount
  }, []);
  useEffect(() => { startLog("props", { salmon: typeof salmon, inSeason: inSeason.length, glance: !!glance, salmonCard: !!salmonCard, troutSources: !!troutSources, gear: !!gear, methods: !!methods, licence: !!licence }); },
    [salmon, inSeason, glance, salmonCard, troutSources, gear, methods, licence]);
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
  // (T1049 review) a sheet the reader opens is the reader acting: a start still following stops there
  const openSheet = (k: SheetKey) => { startTaken.current = true; actions.onSheet(k); setSheet(k); };
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
  const head = <RegsHeader header={header} palette={palette} tabs={store} onDate={onDate} onTab={toTab} onTerm={(id) => openSheet(OPEN.terms([id]))}
                           side={layout === "desktop"} nameSource={nameSource} sun={sun} onParts={onParts} />;
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
      {/* (T1052) a closed banner that says the salmon's closure too (core `closedWithSalmon`) is the card's one statement */}
      {(!glance || !glance.salmon) && !page.keep.banner?.salmon && (typeof salmon === "function" ? salmon({ actions: keepActions, images }) : salmon)}
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
      <ScrollView ref={pageRef} testID="regs-scroll" scrollEventThrottle={16} onScrollBeginDrag={() => { startLog("drag"); restoring.current = false; startTaken.current = true; }} contentContainerStyle={{ flexGrow: 1, paddingBottom: 32 }}
                  onScroll={(e) => { const ev = e.nativeEvent, y = ev.contentOffset.y; store.setScroll(y); coachScroll.track(y); scrollY.current = y;
                                     // (T1049) a scroll that is not the start's own jump is the reader's (a wheel, keys, a
                                     // web drag: react-native-web fires no onScrollBeginDrag for those)
                                     // (by position: the y the start asked for, or the page's end for a y beyond it)
                                     const maxY = ev.contentSize && ev.layoutMeasurement ? ev.contentSize.height - ev.layoutMeasurement.height : null;
                                     // (T1049 cold start) or an end the page had while the start followed (it grew since)
                                     const own = isStartOwnScroll(y, startAsked.current, maxY != null && maxY > 0 ? maxY : null, startEnds.current);
                                     startLog("scroll", { y, ch: ev.contentSize?.height, vh: ev.layoutMeasurement?.height, own, asked: startAsked.current.length });
                                     if (!own) startTaken.current = true;
                                     if (scrollKey && !restoring.current) rememberRegsScroll(scrollKey, y); }}
                  onLayout={(e) => { viewH.current = e.nativeEvent.layout.height; store.setViewport(e.nativeEvent.layout.height); }}
                  onContentSizeChange={(_w, h) => { startLog("size", { h }); contentH.current = h; store.setContentHeight(h); remeasure();
                                                 if (startOn.current) recordStartEnd(h); }}>
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
