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
 * the Licence tab's angler is `useAnglerProfile`'s, D8 a) — the pin bar over it. With no release loader (the dev bundle
 * holds no regulations), while the store loads, or when it cannot answer, the view draws the name row under
 * the frame's header, then the placeholder / spinner / why.
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
import { dayRange, regsLapsedNote, WATER_BACK, WORDS, type RegsTarget } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { mapDayOf, regsDay, useAnglerProfile, useDownstreamWater, useLayoutClass, useRegsPage, useRegsStore, useRegsTarget,
         useRegulations, useWater, type RegsStart } from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { RegulationsPlaceholder } from "./RegulationsPlaceholder";
import { ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import type { AdiposeImages, FishImages } from "./regs/types";
import { DaySheet } from "./regs/DaySheet";
import { RegsScreen } from "./regs/RegsScreen";
import { GearTab } from "./regs/GearTab";
import { LicenceTab } from "./regs/LicenceTab";
import { MethodsTab } from "./regs/MethodsTab";
import { openBook } from "./regs/openBook";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";
import { WaterFrame, WaterInset, WaterTitle } from "./WaterFrame";

export function WaterScreen({ source, item, palette, onBack,
                              loader = null, day = null, onDay, section = null, part = null, fishImages, adiposeImages,
                              start = null, onStarted, onPart }: {
  /** The water; null for an unnamed reach (T53), which `section` then names — its regulations are that section's. */
  source: RegsSource; item: ItemId | null;
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
        <FishSpinner palette={palette} size={110} label="Loading this water" />
      </View>
    ));
  }
  if (item !== null && (sheet.state === "failed" || !sheet.value)) {
    return frame(<WaterTitle name={null} palette={palette} />, (
      <WaterInset style={{ paddingTop: SPACE.s4 }}>
        <Text style={{ ...TYPE.body, color: palette.closed }}>
          {sheet.state === "failed" ? String(sheet.error) : "No such water in this bundle."}
        </Text>
      </WaterInset>
    ));
  }

  const s = sheet.value;   // null only for an unnamed reach

  // THE REGULATIONS PAGE (T23): the regs header (its name row is the frame's WaterTitle) fixed above ONE scroll
  if (loader && regs && !regsError) {
    return (
      <>
      {frame(null, (
      <RegsScreen page={regs} palette={palette} images={fishImages} adipose={adiposeImages} onDate={() => setDayOpen(true)} layout={layout}
                  start={start} onStarted={onStarted} onPart={onPart}
                  // past the release's last day in force, the page leads with core's out-of-date sentence (none unstamped)
                  lapsed={regsLapsedNote(loader.manifest?.regulations?.valid_until, Date.now())}
                  actions={{
                    onSheet: () => {},                                   // RegsScreen opens the sheet itself (T37 SheetHost)
                    onDay: (x) => onDay?.(mapDayOf(x, d.calendarYear)),
                    onBook: (pdf) => { void openBook(pdf); },   // D6 a: native opens the bare PDF (T43 openBook)
                  }}
                  // Gear / Methods / Licence (T34/T35); each is null only outside B.C. (T42: no key, so the page has
                  // none and the Keep tab's foot line is the page)
                  gear={regs.gear ? (a) => <GearTab vm={regs.gear!} palette={palette} actions={a} /> : undefined}
                  methods={regs.methods ? (a) => <MethodsTab vm={regs.methods!} palette={palette} actions={a} /> : undefined}
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
      {!loader ? <RegulationsPlaceholder palette={palette} />
        : regsError ? <Text style={{ ...TYPE.body, color: palette.closed }}>{WORDS.REGS_UNAVAILABLE(regsError.message)}</Text>
        : <FishSpinner palette={palette} size={64} label={WORDS.REGS_LOADING} />}
    </WaterInset>
  );
  // the day sheet in the SAME place as beside the page above, so a reload's loading frame keeps it (it does not remount)
  return (
    <>
    {frame(<WaterTitle name={s?.name ?? null} palette={palette} />, (
      <ScrollView style={{ flex: 1 }} contentContainerStyle={{ flexGrow: 1 }}>
        {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{page}</ViewSwipe> : page}
      </ScrollView>
    ))}
    {daySheet}
    </>
  );
}
