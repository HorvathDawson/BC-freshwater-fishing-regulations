/**
 * One water's Regulations view, drawn inside the water frame (T44 `WaterFrame`: the back, the view strip and the name
 * row are the frame's, at the same place on every view; this view sets no gutter, width or header of its own).
 *
 * THE REGULATIONS VIEW OF A WATER, one of three (Regulations · Conditions · Map — the strip in the header, and a
 * sideways swipe, move between them; the Shell draws the other two). It used to carry a door to the conditions ("and
 * the conditions: what the water is doing", opening the water's FIRST reach): the strip is that door now, and it opens
 * the reach the reader tapped, not `sections[0]`.
 *
 * THE REGULATIONS (T21): the store answers for the tapped part on THE ONE DAY (`useRegsStore` → `useRegsTarget` →
 * `useRegulations` → `useRegsPage`, @app/ui), and `RegsScreen` (T23/T29) draws the page under the frame's header: the
 * regs header fixed above ONE scroll of the four tab sections — What you can keep, Gear, Methods, Licence (T21, T34, T35;
 * the Licence tab's angler is `useAnglerProfile`'s, D8 a) — the pin bar over it. With no release loader (a test that
 * mounts the screen alone; the app always has one), while the store loads, or when it cannot answer, the view draws the
 * name row under the frame's header, then the spinner / why ("Regulations unavailable: …", never a blank).
 *
 * AN UNNAMED REACH (T53, `item` null): the same view, answered by the tapped section (`RegsTarget` kind "section" —
 * the store's region base plus whatever override reaches it, as the pipeline answered it). Its name is the store's
 * ("Unnamed stream · Region 1"), and the header's label slot names the water it flows into when the flow pointers reach
 * one ("Tributary of Eve River": `useDownstreamWater`, worded by core). No water sheet is read for it.
 *
 * Every value here arrived from a hook in @app/ui (rule 25). NO CONDITIONS HOOK RUNS HERE: a number on this view
 * would be a second, poorer copy of the Conditions view one swipe away (AGENTS rule 23).
 */
import { ErrorState } from "./ErrorState";
import { useContext, useEffect, useMemo, useRef, useState } from "react";
import { ScrollView } from "react-native";
import { NET_WORDS, asOfLine, dayRange, gearIfFished, gearTabsFor, licenceIfFished, licenceTabFor, regsLapsedNote, waterBookOf, salmonGlance, salmonStripOf, WATER_BACK, WATER_SCREEN_WORDS, WORDS, type RegsTarget, bcNow, now as clockNow } from "@app/core";
import { openProbe, type ItemId, type WaterSource, type SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { InSeasonContext, mapDayOf, regsDay, useAccessNotices, useInSeasonBanners, useReleaseLines, useAnglerProfile, useDownstreamWater, useLayoutClass, useRegsPage, useRegsStore,
         useSalmonCard, useNet, usePageSubject,
         useDaylight, useFrameView, useInSeasonPage, useMapEntries, useRegsTarget, useRegulations, useSunLine, useWater, type LatLon, type RegsStart } from "@app/ui";
import { LoadingState, useDelayedShow } from "./LoadingState";
import { ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import type { AdiposeImages, FishImages } from "./regs/types";
import { DaySheet } from "./regs/DaySheet";
import { RegsScreen } from "./regs/RegsScreen";
import { regsScrollKey } from "./regs/scrollMemory";
import { SalmonCard } from "./regs/SalmonCard";
import { GearTab } from "./regs/GearTab";
import { LicenceTab } from "./regs/LicenceTab";
import { MethodsTab } from "./regs/MethodsTab";
import { openBook } from "./regs/openBook";
import { PartPending, RegsWaiting } from "./regs/NetWaiting";
import { SPACE, type Palette } from "./theme";
import { WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";
import { usePaneReady } from "./shell/paneReveal";

/** Why the page says the regulations are unavailable when no release loader was handed in. */

/**
 * (T254) THE VIEW, REMOUNTED TO TRY AGAIN: a page that failed on a dead line (not a refusal: the loader forgets a failed
 * fetch) is asked again when the reader presses Try again, and by itself when the device comes back online.
 */
export function WaterScreen(p: Parameters<typeof WaterScreenBody>[0]) {
  const [n, setN] = useState(0);
  return <WaterScreenBody key={n} {...p} onRetry={() => setN((x) => x + 1)} />;
}

function WaterScreenBody({ source, item, palette, onBack, onRetry,
                              loader = null, day = null, onDay, section = null, part = null, at = null, fishImages, adiposeImages,
                              start = null, onStarted, glance = false }: {
  /** The water; null for an unnamed reach (T53), which `section` then names — its regulations are that section's. */
  source: WaterSource; item: ItemId | null;
  /**
   * T01: everything the Regulations page reads (T21 draws it: `useRegsStore(loader)` opens the store on the first water).
   *
   * The release loader (P11): the page's water / regs-<region> / regs-sid files come through it (README §0, D22), never
   * a `/regs.sqlite` of their own. null on the dev-bundle path, which holds no regulations.
   */
  loader?: ReleaseLoader | null;
  /** THE ONE DAY: the map's day (null = today). The page's date pill moves it for the map too (D7). */
  day?: Date | null;
  onDay?: (d: Date | null) => void;
  /** The section the reader tapped to open this water (null: from search, a list or a flow) — picks the part (T09). */
  section?: SectionId | null;
  /** The part the water was opened at (a flow into it names one), else null. */
  part?: number | null;
  /** (T382) the frame's tapped point, else null: where the page's sunrise and sunset are for (else the water's point). */
  at?: LatLon | null;
  palette: Palette; onBack?: () => void;
  /**
   * The fish pictures by species code (T20), for the regulations panel (the app registers the assets; T21 draws them).
   */
  fishImages?: FishImages;
  /** (T38) the hatch sheet's two drawings, registered by the app. */
  adiposeImages?: AdiposeImages;
  /** (T41) where the page starts (the web start link, a test affordance), handed on to the page once */
  start?: RegsStart | null;
  onStarted?: () => void;
  /** (T221, a try) the Keep tab's at-a-glance table: the quota groups and Salmon folded into the species list. */
  glance?: boolean;
  /** (T254) ask the whole page again (the wrapper remounts it) */
  onRetry?: () => void;
}) {
  const sheet = useWater(source, item);
  // THE REGULATIONS: the store, the part (the one the frame names, else the tapped section's — T369: the same reading
  // of a frame `useFrameView` makes, `targetFor`), the day's inputs, the page
  const store = useRegsStore(loader);
  const found = useRegsTarget(store.value ?? null, item, part === null ? section : null);
  const target: RegsTarget | null = useMemo(
    () => (part !== null && item !== null ? { kind: "part", item, partIx: part } : found.value ?? null), [part, item, found.value]);
  const inputs = useRegulations(store.value ?? null, target, day);
  const [who, setWho] = useAnglerProfile();
  const d = regsDay(day);
  // (T53) an unnamed reach: the named water down its flow, for the header's "Tributary of …" (none for a named water)
  const down = useDownstreamWater(source, item, section);
  // (T382) the chosen day's sunrise and sunset at the water (the tapped point, else the water's own)
  const sunDay = { y: d.calendarYear, m: d.day.month, d: d.day.day };
  // (T584, T580 F5) the frame's own point (`useFrameView`: the tap, else the part's middle), so a step, a search and a reload
  // of one part say the same sunrise as a tap on it; the water's own point only where the frame has none
  const fv = useFrameView(store.value ?? null, { item, part, tapSid: section, at });
  const sunAt = fv.value?.here ?? at;
  const sun = useSunLine(source, item, sunAt, sunDay);
  const daylight = useDaylight(source, item, sunAt, sunDay);   // (T473) the hours a daylight-only closure leaves open
  // (T517) the header and the Parts sheet name a part as the water's map does: the same group, the same live salmon
  const entries = useMapEntries(loader, store.value ?? null, item);
  const regs = useRegsPage(inputs.value ?? null, who, d.today, down.value?.name ?? null, entries?.names ?? null);
  const waterBook = useMemo(() => (inputs.value ? waterBookOf(inputs.value, regs) : undefined), [inputs.value, regs]);   // (T474; T60: not a page the lines already link)
  const regsError = store.error ?? found.error ?? inputs.error;
  // (T254) a refusal (a release that cannot answer) is not a dead line: no Try again for it
  const regsRetry = !!regsError && regsError.name !== "RegsStoreError" && !!onRetry;
  const regsErrorNow = useRef(false);
  regsErrorNow.current = !!regsError || sheet.state === "failed";
  // (T257) the waits, on the one timing: no spinner for a quick load, and one shown stays up its short hold
  const sheetWait = useDelayedShow(item !== null && sheet.state === "loading");
  const regsWait = useDelayedShow(!!loader && !regsError && !regs);
  // (T54, T147, T367) the access notices: the no-access banner and the gentle note; (T387) THE PART'S — asked by the
  // page's subject (its part; an unnamed reach's section), never the tapped section, so every tap on a part says the same
  const pageItem = inputs.value?.item ?? null;
  const subject = usePageSubject(inputs.value ?? null);
  const access = useAccessNotices(loader, store.value ?? null, subject);
  // (T211) the Salmon card: DFO's salmon for (T387) the page's part, on THE day — the same card for every tap on it
  // (T692, owner 2026-11-12 "tidal card only") a TIDAL page asks no salmon: DFO's tidal rules are not in this feed, so the
  // card and bar read "Closed" over the tidal card for a water DFO does not list
  const tidalPage = regs?.status === "tidal";
  const salmonPage = useMemo(() => (subject && !tidalPage ? { subject, akey: inputs.value?.key?.akey ?? null } : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the page's identity
    [subject, inputs.value?.key?.akey, tidalPage]);
  // (T254) a failed Salmon card is asked again on Try again, and when the line comes back
  const { mode: netMode } = useNet();
  const [salmonTry, setSalmonTry] = useState(0);
  const salmon = useSalmonCard(loader, store.value ?? null, salmonPage, day, salmonTry);
  const wasOffline = useRef(netMode.offline);
  useEffect(() => {
    const back = wasOffline.current && !netMode.offline;
    wasOffline.current = netMode.offline;
    if (!back) return;
    if (salmon.state === "failed") setSalmonTry((x) => x + 1);
    if (regsErrorNow.current) onRetry?.();
  }, [netMode.offline]);   // eslint-disable-line react-hooks/exhaustive-deps -- only the line coming back
  // (T254) a release that ships salmon draws a card on every page: until it is in, its place says why
  const salmonExpected = !!loader?.manifest?.files?.salmon && !tidalPage;
  // (T230) the provincial in-season changes matched to this water / part, in effect on THE day (informational)
  // (T517) the page covers the map entry holding its part (`inSeasonPageOf`)
  const inSeasonPage = useInSeasonPage(pageItem, inputs.value?.partIx ?? null, entries);
  // (T520) where each lands: core's `inSeasonCover` over the water's section lines (read only for a part-covering change)
  const inSeasonLines = useReleaseLines(loader ?? null);
  const inSeason = useInSeasonBanners(store.value ?? null, inSeasonLines, inSeasonPage, day);
  // (T236) the page's foot: how current the regulations, the salmon feed and the in-season changes are
  const inSeasonAt = useContext(InSeasonContext).data?.feed.fetchedAt ?? null;
  // (T227) the day's book gear as if open, for a day only salmon may be fished (core `gearIfFished`)
  const gearAnyway = useMemo(() => (inputs.value ? gearIfFished(inputs.value, d.today) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the inputs and the day
    [inputs.value, d.today.month, d.today.day]);
  // (T670 G5) the open day's Licence tab, for a day game fish are closed and salmon is not (core `licenceTabFor`)
  const licenceAnyway = useMemo(() => (inputs.value ? licenceIfFished(inputs.value, who) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the inputs, the day and the angler
    [inputs.value, d.today.month, d.today.day, who]);
  // (T45) how much room the window gives (the app shell's width; never read here): the page arranges itself for it
  const layout = useLayoutClass();
  // the sideways swipe between the water's views, anywhere on this one (see WaterViews: inside the ScrollView)
  const views = useContext(WaterViewsContext);
  const swipe = views?.bodySwipe ?? null;
  /*
   * (T49) "WHICH DAY?" — the date pill's sheet, the same `DaySheet` the map's calendar button opens, on THE day. Kept
   * HERE, not in `RegsScreen`: a new day reloads the page, so the page draws its loading frame and then remounts on the
   * next day's answer — a sheet inside it closed under the reader at every step. Here it stays open across the reload
   * (drawn beside whichever frame is up), and back closes it first (a `<Sheet>`).
   */
  const [dayOpen, setDayOpen] = useState(false);
  const daySheet = loader && (
    <DaySheet open={dayOpen} onClose={() => setDayOpen(false)} palette={palette} day={day} onDay={(x) => onDay?.(x)}
              // the same days the map's date sheet offers: this calendar year, cut at the release's last day
              range={dayRange(bcNow(), loader.manifest?.regulations?.valid_until ?? null)} />
  );

  // (T286) the open probe (test bed only): when each stage first arrives, and when the page is drawn
  const drawn = !(item !== null && (sheet.state !== "ready" || sheetWait.pending || !sheet.value))
    && !!loader && !!regs && !regsError && !regsWait.pending;
  if (openProbe.on()) {
    openProbe.mark("screen");
    if (sheet.state === "ready") openProbe.mark("sheet");
    if (store.value) openProbe.mark("store");
    if (found.value) openProbe.mark("target");
    if (inputs.value) openProbe.mark("inputs");
    if (regs) openProbe.mark("page");
    if (sheetWait.shown || regsWait.shown) openProbe.mark("spinner");
    if (salmon.state !== "loading") openProbe.mark("salmon");
    if (access.state !== "loading") openProbe.mark("access");
  }
  useEffect(() => { openProbe.mark("shell"); }, []);
  // (T498) DRAWN WHOLE: the page with its Salmon card (or the card's failed face) and its access notices in, or the
  // why it cannot be drawn — what the Shell waits for before it shows a pane opened over the map (`shell/paneReveal.ts`)
  const failedWhole = (item !== null && sheet.state === "failed") || !loader || !!regsError;
  usePaneReady(failedWhole
    || (drawn && access.state !== "loading" && !(salmonExpected && salmon.state === "loading")));
  useEffect(() => {
    if (!drawn || !openProbe.on()) return;
    openProbe.mark("commit");
    openProbe.endOnFrame("frame");
  }, [drawn]);

  // EVERY STATE IS DRAWN IN THE WATER FRAME (T44): the header (back, the view strip) and the name row sit where they sit
  // on the Conditions and Map views; this view draws only its content.
  const frame = (title: React.ReactNode, body: React.ReactNode) => (
    <WaterFrame palette={palette} face="regulations" onBack={onBack} backLabel={WATER_BACK} title={title}>{body}</WaterFrame>
  );

  // an unnamed reach has no water sheet: its name is the store's, on the page
  if (item !== null && (sheet.state === "loading" || sheetWait.pending)) {
    return frame(<WaterTitle name={null} palette={palette} />, (
      <LoadingState palette={palette} size={110} label={WATER_SCREEN_WORDS.loading} shown={sheetWait.shown}
                    style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
        <RegsWaiting palette={palette} />
      </LoadingState>
    ));
  }
  if (item !== null && (sheet.state === "failed" || !sheet.value)) {
    return frame(<WaterTitle name={null} palette={palette} />, (
      <WaterInset style={{ paddingTop: SPACE.s4 }}>
        {/* (T254) a dead line: ask again (and by itself when the device comes back online) */}
        <ErrorState palette={palette} size="body"
                    message={sheet.state === "failed" ? String(sheet.error) : WATER_SCREEN_WORDS.noSuchWater}
                    onRetry={sheet.state === "failed" ? onRetry : undefined} retryLabel={NET_WORDS.part.retry} retryTestID="regs-retry" />
      </WaterInset>
    ));
  }

  const s = sheet.value;   // null only for an unnamed reach

  // THE REGULATIONS PAGE (T23): the regs header (its name row is the frame's WaterTitle) fixed above ONE scroll
  if (loader && regs && !regsError && !regsWait.pending) {
    // (T227) DFO's salmon year under the provincial bar, at the top (none where DFO has no salmon rules here); the Gear /
    // Methods tabs' closed line names its exception
    const salmonStrip = salmon.value ? salmonStripOf(salmon.value, regs.keep.strip) : null;
    // (T227, owner 2026-10-28) Gear / Methods state no closure: shown only where game fish or salmon may be fished
    const tabs = gearTabsFor(regs, gearAnyway, salmonStrip, salmon.value ?? null);
    const licence = licenceTabFor(regs, licenceAnyway, salmonStrip);
    return (
      <>
      {frame(null, (
      <RegsScreen page={regs} palette={palette} nameSource={s?.nameSource ?? null} images={fishImages} adipose={adiposeImages} onDate={() => setDayOpen(true)} layout={layout} sun={sun} daylight={daylight} waterBook={waterBook}
                  start={start} onStarted={onStarted} scrollKey={regsScrollKey(item, part, section)}
                  // past the release's last day in force, the page leads with core's out-of-date sentence (none unstamped)
                  lapsed={regsLapsedNote(loader.manifest?.regulations?.valid_until, clockNow())}
                  // (T54, T367) the access notices: pinned strips under the header
                  access={access.value ?? []}
                  inSeason={inSeason}
                  asOf={asOfLine({ release: loader.manifest ?? null, inSeasonAt, now: clockNow(),
                                   salmonAt: salmon.value ? salmon.value.feedAt ?? null : undefined })}
                  salmon={salmon.value ? ({ actions, images }) => <SalmonCard card={salmon.value!} palette={palette} actions={actions} images={images} />
                    // (T254) the card is still coming (it waits behind the regulations on a slow line) or could not come:
                    // said in its place, never a silent gap
                    // (T387 review) a release that ships salmon draws the card or its face: a card that came back
                    // empty is said as failed, never a silent gap
                    : salmonExpected
                      ? <PartPending palette={palette} what={NET_WORDS.part.salmon}
                                     state={salmon.state === "loading" ? "loading" : "failed"} testID="salmon-pending"
                                     onRetry={() => setSalmonTry((x) => x + 1)} />
                      : undefined}
                  glance={glance ? { salmon: salmon.value ? salmonGlance(salmon.value) : null } : null}
                  salmonStrip={salmonStrip}
                  actions={{
                    onSheet: () => {},                                   // RegsScreen opens the sheet itself (T37 SheetHost)
                    onDay: (x) => onDay?.(mapDayOf(x, d.calendarYear)),
                    onBook: (pdf, box) => { void (box ? openBook(pdf, box) : openBook(pdf)); },   // D6 a: native opens the bare PDF (T43 openBook)
                  }}
                  // Gear / Methods / Licence (T34/T35); each is null only outside B.C. (T42: no key, so the page has
                  // none and the Keep tab's foot line is the page)
                  gear={tabs.gear ? (a) => <GearTab vm={tabs.gear!} palette={palette} actions={a} salmonGear={tabs.salmonGear} /> : undefined}
                  methods={tabs.methods ? (a) => <MethodsTab vm={tabs.methods!} palette={palette} actions={a} boats={regs.boats} /> : undefined}
                  licence={licence ? (a) => <LicenceTab vm={licence} who={regs.who} palette={palette} actions={a} onWho={setWho} />
                    : undefined} />
      ))}
      {daySheet}
      </>
    );
  }

  // No "Regulations" heading: the strip's selected view already says it.
  // (T367, owner 2026-11-03) the wait in the middle of the pane, not in its top corner; a failure keeps the top
  const waiting = !(!loader || regsError);
  const page = (
    <WaterInset style={waiting ? { flex: 1, alignItems: "center", justifyContent: "center", paddingBottom: 32 }
                               : { paddingTop: SPACE.s4, paddingBottom: 32 }}>
      {!loader || regsError
        ? <>
            {/* (T254) a dead line, not a refusal: ask again (and by itself when the device comes back online) */}
            <ErrorState palette={palette} size="body"
                        message={WORDS.REGS_UNAVAILABLE(regsError?.message ?? WATER_SCREEN_WORDS.noRelease)}
                        onRetry={regsRetry ? onRetry : undefined} retryLabel={NET_WORDS.part.retry} retryTestID="regs-retry" />
          </>
        : <>
            <LoadingState palette={palette} size={96} label={WORDS.REGS_LOADING} shown={regsWait.shown} />
            {regsWait.shown && <RegsWaiting palette={palette} align="center" />}
          </>}
    </WaterInset>
  );
  // the day sheet in the SAME place as beside the page above, so a reload's loading frame keeps it (it does not remount)
  return (
    <>
    {frame(<WaterTitle name={s?.name ?? null} palette={palette} source={s?.nameSource ?? null} />, (
      <ScrollView style={{ flex: 1 }} contentContainerStyle={{ flexGrow: 1 }}>
        {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{page}</ViewSwipe> : page}
      </ScrollView>
    ))}
    {daySheet}
    </>
  );
}
