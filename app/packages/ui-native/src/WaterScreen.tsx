/**
 * One water's Regulations view. Drawn to `design/riffle.html`; the header is the water's (`FaceBar`).
 *
 * THE REGULATIONS VIEW OF A WATER, one of three (Regulations · Conditions · Map — the strip in the header, and a
 * sideways swipe, move between them; the Shell draws the other two). It used to carry a door to the conditions ("and
 * the conditions: what the water is doing", opening the water's FIRST reach): the strip is that door now, and it opens
 * the reach the reader tapped, not `sections[0]`.
 *
 * THE REGULATIONS (T21): the store answers for the tapped part on THE ONE DAY (`useRegsStore` → `useRegsTarget` →
 * `useRegulations` → `useRegsPage`, @app/ui), and `RegsScreen` (T23/T29) draws the page: the water's header row and the
 * regs header fixed above ONE scroll of the four tab sections — What you can keep, Gear, Methods, Licence (T21, T34, T35;
 * the Licence tab's angler is `useAnglerProfile`'s, D8 a) — the pin bar over it. With no release loader (the dev bundle
 * holds no regulations), while the store loads, or when it cannot answer, the view keeps its old frame (the header row,
 * the name, then the placeholder / spinner / why).
 *
 * Every value here arrived from a hook in @app/ui (rule 25). NO CONDITIONS HOOK RUNS HERE: a number on this view
 * would be a second, poorer copy of the Conditions view one swipe away (AGENTS rule 23).
 */
import { useContext, useMemo, useState } from "react";
import { Linking, Platform, Pressable, ScrollView, Text, View } from "react-native";
import { bookUrl, dayRange, WORDS, type RegsTarget } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { mapDayOf, regsDay, useAnglerProfile, useRegsPage, useRegsStore, useRegsTarget, useRegulations, useWater } from "@app/ui";
import { FaceBar } from "./Faces";
import { FishSpinner } from "./FishSpinner";
import { RegulationsPlaceholder } from "./RegulationsPlaceholder";
import { ViewSwipe, WaterViewsContext } from "./ViewSwipe";
import type { FishImages } from "./regs/types";
import { DaySheet } from "./regs/DaySheet";
import { RegsScreen, TabPending } from "./regs/RegsScreen";
import { GearTab } from "./regs/GearTab";
import { LicenceTab } from "./regs/LicenceTab";
import { MethodsTab } from "./regs/MethodsTab";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

export function WaterScreen({ source, item, palette, onBack,
                              loader = null, day = null, onDay, section = null, part = null, fishImages }: {
  source: RegsSource; item: ItemId;
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
}) {
  const sheet = useWater(source, item);
  // THE REGULATIONS: the store, the part (the one the frame names, else the tapped section's), the day's inputs, the page
  const store = useRegsStore(loader);
  const found = useRegsTarget(store.value ?? null, item, part === null ? section : null);
  const target: RegsTarget | null = useMemo(() => (part !== null ? { kind: "part", item, partIx: part } : found.value ?? null),
    [part, item, found.value]);
  const inputs = useRegulations(store.value ?? null, target, day);
  const [who, setWho] = useAnglerProfile();
  const d = regsDay(day);
  const regs = useRegsPage(inputs.value ?? null, who, d.today);
  const regsError = store.error ?? found.error ?? inputs.error;
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

  if (sheet.state === "loading") {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card,
                     alignItems: "center", justifyContent: "center" }}>
        <FishSpinner palette={palette} size={110} label="Loading this water" />
      </View>
    );
  }
  if (sheet.state === "failed" || !sheet.value) {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card, padding: 20, gap: 10 }}>
        {onBack && <Back palette={palette} onPress={onBack} />}
        <Text style={{ ...TYPE.body, color: palette.closed }}>
          {sheet.state === "failed" ? String(sheet.error) : "No such water in this bundle."}
        </Text>
      </View>
    );
  }

  const s = sheet.value;

  // THE REGULATIONS PAGE (T23): its own header fixed above ONE scroll (RegsScreen); the states below keep the old frame
  if (loader && regs && !regsError) {
    const label = (t: string) => regs.header.tabs.find((x) => x.tab === t)?.label ?? "";
    return (
      <>
      <RegsScreen page={regs} palette={palette} images={fishImages} onDate={() => setDayOpen(true)} onBack={onBack}
                  // the water's own Map view, through its view strip — the same move as the strip's "Map" (one water screen)
                  onMap={views?.views.includes("map") ? () => views.onView("map") : undefined}
                  actions={{
                    onSheet: () => {},                                   // RegsScreen opens the sheet itself (T37 SheetHost)
                    onDay: (x) => onDay?.(mapDayOf(x, d.calendarYear)),
                    onBook: (pdf) => { void Linking.openURL(bookUrl(pdf, Platform.OS)); },   // D6 a: native opens the bare PDF
                  }}
                  // Gear / Methods / Licence (T34/T35); a part with no such section says it is not in the app yet
                  gear={regs.gear ? (a) => <GearTab vm={regs.gear!} palette={palette} actions={a} />
                    : <TabPending label={label("gear")} palette={palette} />}
                  methods={regs.methods ? (a) => <MethodsTab vm={regs.methods!} palette={palette} actions={a} />
                    : <TabPending label={label("methods")} palette={palette} />}
                  licence={regs.licence ? (a) => <LicenceTab vm={regs.licence!} who={regs.who} palette={palette} actions={a} onWho={setWho} />
                    : <TabPending label={label("licence")} palette={palette} />} />
      {daySheet}
      </>
    );
  }

  const page = (
    <>
      {/* THE SAME INSETS AS THE CONDITIONS VIEW, so the header does not move when the view changes. */}
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, gap: SPACE.s2 }}>
        {/* THE WATER'S HEADER, the same row in the same place on every view of it — see FaceBar. */}
        <FaceBar palette={palette} face="regulations" onBack={onBack} backLabel="Back" onFace={() => {}} />
        <Text style={{ ...TYPE.title, color: palette.ink }}>{s.name}</Text>
      </View>
      {/* No "Regulations" heading: the strip's selected view already says it. */}
      <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s4, paddingBottom: 32 }}>
        {!loader ? <RegulationsPlaceholder palette={palette} />
          : regsError ? <Text style={{ ...TYPE.body, color: palette.closed }}>{WORDS.REGS_UNAVAILABLE(regsError.message)}</Text>
          : <FishSpinner palette={palette} size={64} label={WORDS.REGS_LOADING} />}
      </View>
    </>
  );
  // the day sheet in the SAME place as beside the page above, so a reload's loading frame keeps it (it does not remount)
  return (
    <>
    <ScrollView style={{ flex: 1, backgroundColor: palette.card }} contentContainerStyle={{ flexGrow: 1 }}>
      {swipe ? <ViewSwipe swipe={swipe} testId="view-swipe-body">{page}</ViewSwipe> : page}
    </ScrollView>
    {daySheet}
    </>
  );
}

/** The plain back link, for the states where there is no sheet to put a FaceBar over. */
function Back({ palette, onPress }: { palette: Palette; onPress: () => void }) {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel="Back">
      <Text style={{ ...TYPE.micro, color: palette.accent }}>‹  Back</Text>
    </Pressable>
  );
}

