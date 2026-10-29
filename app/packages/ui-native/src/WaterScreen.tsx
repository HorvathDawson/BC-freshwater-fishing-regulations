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
import { useContext, useMemo, useState } from "react";
import { ScrollView, Text, View } from "react-native";
import { asOfLine, dayRange, gearIfFished, gearTabsFor, regsLapsedNote, salmonGlance, salmonStripOf, WATER_BACK, WATER_SCREEN_WORDS, WORDS, type RegsTarget } from "@app/core";
import type { ItemId, WaterSource, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { InSeasonContext, mapDayOf, regsDay, useAccessNotice, useInSeasonBanners, useAnglerProfile, useDownstreamWater, useLayoutClass, useRegsPage, useRegsStore,
         useSalmonCard,
         useRegsTarget, useRegulations, useWater, type RegsStart } from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import type { AdiposeImages, FishImages } from "./regs/types";
import { DaySheet } from "./regs/DaySheet";
import { RegsScreen } from "./regs/RegsScreen";
import { SalmonCard } from "./regs/SalmonCard";
import { GearTab } from "./regs/GearTab";
import { LicenceTab } from "./regs/LicenceTab";
import { MethodsTab } from "./regs/MethodsTab";
import { openBook } from "./regs/openBook";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";
import { WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";

/** Why the page says the regulations are unavailable when no release loader was handed in. */

export function WaterScreen({ source, item, palette, onBack,
                              loader = null, day = null, onDay, section = null, part = null, fishImages, adiposeImages,
                              start = null, onStarted, onPart, glance = false }: {
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
  /** (T58) a Parts sheet row: read that part of this water (the caller makes it the frame's part). */
  onPart?: (partIx: number) => void;
  /** (T221, a try) the Keep tab's at-a-glance table: the quota groups and Salmon folded into the species list. */
  glance?: boolean;
}) {
  const sheet = useWater(source, item);
  // THE REGULATIONS: the store, the part (the one the frame names, else the tapped section's), the day's inputs, the page
  const store = useRegsStore(loader);
  const found = useRegsTarget(store.value ?? null, item, part === null ? section : null);
  const target: RegsTarget | null = useMemo(
    () => (part !== null && item !== null ? { kind: "part", item, partIx: part } : found.value ?? null), [part, item, found.value]);
  const inputs = useRegulations(store.value ?? null, target, day);
  const [who, setWho] = useAnglerProfile();
  const d = regsDay(day);
  // (T53) an unnamed reach: the named water down its flow, for the header's "Tributary of …" (none for a named water)
  const down = useDownstreamWater(source, item, section);
  const regs = useRegsPage(inputs.value ?? null, who, d.today, down.value?.name ?? null);
  const regsError = store.error ?? found.error ?? inputs.error;
  // (T54) the tapped section's access: the no-access banner or the gentle note; (T147) opened with no section, the
  // page's own part's no-access banner (a water opened from search must still lead with the fact); (Q140) a tapped
  // section wholly outside, on a part that runs through no-access land, leads with the part's banner
  const pageItem = inputs.value?.item ?? null;
  const accessPage = useMemo(() => (pageItem ? { ord: pageItem.ord, partIx: inputs.value!.partIx, parts: pageItem.parts.length } : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the page's identity is its item, part and part count
    [pageItem?.ord, inputs.value?.partIx, pageItem?.parts.length]);
  const access = useAccessNotice(loader, section, store.value ?? null, accessPage);
  // (T211) the Salmon card: DFO's salmon for the tapped section, else the page's part, on THE day
  const salmonPage = useMemo(() => (inputs.value
    ? { section: part === null ? section : null, item: pageItem?.id ?? null, ord: pageItem?.ord ?? null,
        partIx: inputs.value.partIx, akey: inputs.value.key?.akey ?? null }
    : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the page's identity
    [inputs.value?.partIx, pageItem?.id, pageItem?.ord, part, section, inputs.value?.key?.akey]);
  const salmon = useSalmonCard(loader, store.value ?? null, salmonPage, day);
  // (T230) the provincial in-season changes matched to this water / part, in effect on THE day (informational)
  const inSeasonPage = useMemo(() => (pageItem && inputs.value
    ? { item: pageItem.id, ord: pageItem.ord, partIx: inputs.value.partIx, parts: pageItem.parts.length } : null),
    // the page's identity
    [pageItem?.id, pageItem?.ord, inputs.value?.partIx, pageItem?.parts.length]);
  const inSeason = useInSeasonBanners(store.value ?? null, inSeasonPage, day);
  // (T236) the page's foot: how current the regulations, the salmon feed and the in-season changes are
  const inSeasonAt = useContext(InSeasonContext).data?.feed.fetchedAt ?? null;
  // (T227) the day's book gear as if open, for a day only salmon may be fished (core `gearIfFished`)
  const gearAnyway = useMemo(() => (inputs.value ? gearIfFished(inputs.value, d.today) : null),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the inputs and the day
    [inputs.value, d.today.month, d.today.day]);
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
              range={dayRange(new Date(), loader.manifest?.regulations?.valid_until ?? null)} />
  );

  // EVERY STATE IS DRAWN IN THE WATER FRAME (T44): the header (back, the view strip) and the name row sit where they sit
  // on the Conditions and Map views; this view draws only its content.
  const frame = (title: React.ReactNode, body: React.ReactNode) => (
    <WaterFrame palette={palette} face="regulations" onBack={onBack} backLabel={WATER_BACK} title={title}>{body}</WaterFrame>
  );

  // an unnamed reach has no water sheet: its name is the store's, on the page
  if (item !== null && sheet.state === "loading") {
    return frame(<WaterTitle name={null} palette={palette} />, (
      <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
        <FishSpinner palette={palette} size={110} label={WATER_SCREEN_WORDS.loading} />
      </View>
    ));
  }
  if (item !== null && (sheet.state === "failed" || !sheet.value)) {
    return frame(<WaterTitle name={null} palette={palette} />, (
      <WaterInset style={{ paddingTop: SPACE.s4 }}>
        <Text style={{ ...TYPE.body, color: palette.closed }}>
          {sheet.state === "failed" ? String(sheet.error) : WATER_SCREEN_WORDS.noSuchWater}
        </Text>
      </WaterInset>
    ));
  }

  const s = sheet.value;   // null only for an unnamed reach

  // THE REGULATIONS PAGE (T23): the regs header (its name row is the frame's WaterTitle) fixed above ONE scroll
  if (loader && regs && !regsError) {
    // (T227) DFO's salmon year under the provincial bar, at the top (none where DFO has no salmon rules here); the Gear /
    // Methods tabs' closed line names its exception
    const salmonStrip = salmon.value ? salmonStripOf(salmon.value, regs.keep.strip) : null;
    // (T227, owner 2026-10-28) Gear / Methods state no closure: shown only where game fish or salmon may be fished
    const tabs = gearTabsFor(regs, gearAnyway, salmonStrip, salmon.value ?? null);
    return (
      <>
      {frame(null, (
      <RegsScreen page={regs} palette={palette} nameSource={s?.nameSource ?? null} images={fishImages} adipose={adiposeImages} onDate={() => setDayOpen(true)} layout={layout}
                  start={start} onStarted={onStarted} onPart={onPart}
                  // past the release's last day in force, the page leads with core's out-of-date sentence (none unstamped)
                  lapsed={regsLapsedNote(loader.manifest?.regulations?.valid_until, Date.now())}
                  // (T54) the access notice; its link moves to the water's Map view, where the area is drawn
                  access={access.value ?? null}
                  inSeason={inSeason}
                  asOf={asOfLine({ release: loader.manifest ?? null, inSeasonAt, now: Date.now(),
                                   salmonAt: salmon.value ? salmon.value.feedAt ?? null : undefined })}
                  salmon={salmon.value ? ({ actions, images }) => <SalmonCard card={salmon.value!} palette={palette} actions={actions} images={images} /> : undefined}
                  glance={glance ? { salmon: salmon.value ? salmonGlance(salmon.value) : null } : null}
                  salmonStrip={salmonStrip}
                  onAccessMap={views?.views.includes("map") ? () => views.onView("map") : undefined}
                  actions={{
                    onSheet: () => {},                                   // RegsScreen opens the sheet itself (T37 SheetHost)
                    onDay: (x) => onDay?.(mapDayOf(x, d.calendarYear)),
                    onBook: (pdf, box) => { void (box ? openBook(pdf, box) : openBook(pdf)); },   // D6 a: native opens the bare PDF (T43 openBook)
                  }}
                  // Gear / Methods / Licence (T34/T35); each is null only outside B.C. (T42: no key, so the page has
                  // none and the Keep tab's foot line is the page)
                  gear={tabs.gear ? (a) => <GearTab vm={tabs.gear!} palette={palette} actions={a} salmonGear={tabs.salmonGear} /> : undefined}
                  methods={tabs.methods ? (a) => <MethodsTab vm={tabs.methods!} palette={palette} actions={a} /> : undefined}
                  licence={regs.licence ? (a) => <LicenceTab vm={regs.licence!} who={regs.who} palette={palette} actions={a} onWho={setWho} />
                    : undefined} />
      ))}
      {daySheet}
      </>
    );
  }

  // No "Regulations" heading: the strip's selected view already says it.
  const page = (
    <WaterInset style={{ paddingTop: SPACE.s4, paddingBottom: 32 }}>
      {!loader || regsError
        ? <Text style={{ ...TYPE.body, color: palette.closed }}>{WORDS.REGS_UNAVAILABLE(regsError?.message ?? WATER_SCREEN_WORDS.noRelease)}</Text>
        : <FishSpinner palette={palette} size={64} label={WORDS.REGS_LOADING} />}
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
