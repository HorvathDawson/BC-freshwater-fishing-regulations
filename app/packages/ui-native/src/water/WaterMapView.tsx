/**
 * THE WATER'S MAP VIEW (P15), drawn to the prototype's water map (`design/regs-v11.html` `mapHtml` / `markPop`): the real
 * map with the part you are on lit (`selected`) and the rest of the water lit as `water`, the camera on the part's box,
 * the boundary marks, ⤢ Whole river / lake, and under the map the bar — where the part sits, its name, ▼ Downstream /
 * ▲ Upstream (the ends disabled: "Lowest part" / "Highest part"), "Flows into <water> ›" at the mouth, and "Show
 * regulations for this part".
 *
 * Every value is `useWaterMap`'s (@app/ui): this draws the view model and hands the reader's taps back as actions. The
 * geometry is the tiles' only; the part's section ids and box come from the release.
 *
 * What leaves the water map is the caller's (the Shell's route): the exit opens the receiving water, "Show regulations"
 * returns to the Regulations view on the part, a tap on another water opens it, ▲ / ▼ off the end into the water this
 * one goes on from or as opens that water's map (`onCross`), and every step to another part is the frame's part
 * (`onPart`) — so the Regulations and Conditions views agree with the map.
 */
import { LoadingState } from "../LoadingState";
import { useMemo, useRef, type ComponentProps } from "react";
import { ScrollView, View, type ViewInstance } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { PART_FIT_MAX_ZOOM, WATER_MAP_WORDS, DAY_LABELS, bcNow } from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { inSeasonSections, useAsync, useBackLayer, useInSeasonCovers, useKeptWhilePending, useRegsStore, useReleaseLines, useSalmonKey, useTidalWaterMap, useWaterMap,
         type WaterMapPartVM, type WaterMapVM, type WaterMapAction } from "@app/ui";
import { MapScreen } from "../MapScreen";
import { useSplitReport, useWaterMapSplit, WaterInset, WaterSplit } from "../WaterFrame";
import { Icon } from "../icons";
import { useTestbedFlag } from "../testbedFlag";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { useCoachTarget } from "../CoachMarks";
import { useGuideControl } from "../guideControls";
import { useBarFloor } from "./useBarFloor";
import { PANE_HOLD_MS, usePaneMap, usePaneReady } from "../shell/paneReveal";

/** The map's own props: everything the water map does not decide. */
export type WaterMapMapProps = Omit<ComponentProps<typeof MapScreen>,
  "palette" | "highlight" | "inSeason" | "water" | "waterLine" | "selection" | "focus" | "near" | "marks" | "onPressMark" | "fit" | "onPressFeature">;

const NO_NEAR: readonly SectionId[] = [];

/** (T875) ▼ / ▲'s height: two lines of their words (the most they draw) and the padding and border, so a one-line pair
 *  ("Downstream" / "Upstream") is as tall as a pair that wraps ("Upstream into Chilliwack River"). 53 px at 1×. */
const STEP_HEIGHT = 2 * TYPE.bodyStrong.lineHeight + 2 * 4 + 2 * 1.5;

/** The place kept for the "Flows into" line: the 44 px target the line has when it is drawn. */
const EXIT_SLOT = 44;

export function WaterMapView({ loader, item, part, tapSid, palette, map, onOpenWater, onShowRegs, onOtherSection,
                               onCross, onPart, whole = false, onWhole, day = null }: {
  loader: ReleaseLoader | null;
  item: ItemId;
  /** The frame's part (null: the part the Regulations view opens for `tapSid`). */
  part: number | null;
  tapSid: SectionId | null;
  palette: Palette;
  map: WaterMapMapProps;
  /** "Flows into …": the receiving water, at the part the mouth meets. */
  onOpenWater: (item: ItemId, part: number | null) => void;
  /** "Show regulations for this part": the Regulations view on it (another item for a split lake's closure area). */
  onShowRegs: (item: ItemId, partIx: number) => void;
  /** A tap on a section that is not this water's, and (T369) where on it: the opened frame's "you are here". */
  onOtherSection: (sid: SectionId, at: { lat: number; lon: number } | null) => void;
  /** ▲ / ▼ into the water this one goes on from or as (`water_continue`), at its part there. */
  onCross: (item: ItemId, part: number) => void;
  /** A step to another part: the frame's part from now on (a split lake's other area: that water). */
  onPart?: (item: ItemId, partIx: number) => void;
  /** (T203, T369 D7) The frame's ⤢: framed on the whole water (a search result's, or ⤢ pressed). */
  whole?: boolean;
  /** (T369 D7) ⤢ pressed: the frame's `whole` toggled (the route's `whole`), so it outlives this view. */
  onWhole?: () => void;
  /** (T520) THE day (the Shell's map day; null = today): the in-season changes outlined are those in force then */
  day?: Date | null;
}) {
  const split = useWaterMapSplit();
  const report = useSplitReport();
  const store = useRegsStore(loader);
  // (T228, T517) parts of one run join only where salmon (the live DFO feed over the release) reads them alike
  const salmonKey = useSalmonKey(loader, store.value ?? null, item);
  const wm = useWaterMap(store.value ?? null, item, part, tapSid, onPart, whole, salmonKey, onWhole, loader);
  const vm = wm.value?.vm ?? null;
  const act = wm.value?.act ?? (() => {});
  // (T518) the provincial in-season changes on this water today, outlined on exactly their sections (the banner's own
  // coverage, `useInSeasonCovers`: one load path)
  const lines = useReleaseLines(loader);
  // the answer names the water it is for: `useAsync` keeps the last key's answer while the next loads
  const ordQ = useAsync(async () => ({ item, ord: await store.value!.itemOrd(item) }), `inseason-ord:${item}`,
                        !!store.value);
  const ord = ordQ.state === "ready" && ordQ.value.item === item ? ordQ.value.ord : null;
  // (T520) on THE day the Regulations view reads (the Shell's map day): the banner and the outline agree
  const covers = useInSeasonCovers(store.value ?? null, lines, ord === null ? null : { item, ord, pieces: null }, day);
  const inSeason = useMemo(() => (vm && covers.length ? inSeasonSections(covers, [...vm.selected, ...vm.water]) : NO_NEAR),
    [covers, vm?.selected, vm?.water]);
  // (T459) the water this one flows into stays in colour (muted) while every other water is grey; the test bed's
  // `receiving` switch draws it grey too, for the owner's pick between the two (shots in app/tools/testbed/out/t459/)
  const receivingKept = useTestbedFlag("receiving", true);
  const near = receivingKept ? wm.value?.receiving : undefined;
  // (T466) the walkthrough's closed example is reached with ▲ and opened with "Show regulations": its "Show me" presses
  // them as the reader's tap does (the same handlers), and it rings them
  const step = (dir: "up" | "down") => {
    const st = vm?.[dir];
    if (!st || st.disabled) return;
    if (st.cross) onCross(st.cross.item as ItemId, st.cross.part); else act({ type: dir });
  };
  useGuideControl(vm ? "map:up" : null, vm ? !vm.up.disabled : false, () => step("up"));
  useGuideControl(vm ? "map:regs" : null, true, () => { if (vm) onShowRegs(vm.regs.item as ItemId, vm.regs.partIx); });
  const regsRing = useCoachTarget("map:regs");
  // (T688) a wholly tidal water lists no part, so the map has no view model: it frames the water itself and says what is
  // true (a tidal water), instead of keeping the last water's camera under "not available"
  const td = useTidalWaterMap(store.value ?? null, item, !!store.value && wm.state === "ready" && vm === null);
  const tidal = td.value;
  const status = store.error || wm.error || td.error ? WATER_MAP_WORDS.unavailable
    : wm.state === "loading" || store.state === "loading" || td.state === "loading" ? WATER_MAP_WORDS.loading
    : vm === null && tidal === null ? WATER_MAP_WORDS.unavailable : null;
  // (T508) DRAWN WHOLE: the bar with the part's view model (or the words that say it is unavailable), and the map
  // with that part lit and framed (`usePaneMap`, armed only once the view model hands the map its part: a first draw of
  // nothing never counts) — what a pane held over the browse map waits for (`shell/paneReveal.ts`)
  usePaneReady(status !== WATER_MAP_WORDS.loading);
  const onDrawn = usePaneMap(vm !== null || tidal !== null || status === WATER_MAP_WORDS.unavailable);
  /*
   * (T731, owner 2026-11-14: "you click upstream or downstream it is like menus briefly close at bottom and cause a flash
   * of a resize then rerender") A STEP CHANGES THE BAR IN PLACE. A step into another water (▲ / ▼ `onCross`, a split
   * lake's other area) moves the frame to a water whose group is not read yet, and for that read the bar drew its
   * loading state: the steps, the name and "Show regulations" went, the bar shrank to a spinner, the map above it grew
   * into the room (a resize, and a refit on a phone), then all of it came back. Now what was last drawn — the bar and
   * everything the map is handed — stays while the next water is read, inert (no press reaches the last water's
   * handlers) and busy to a screen reader, then the new part is drawn into the same bar and the map glides to it. Not on
   * a first open (nothing drawn yet: the waiting state, held over the browse map as before), and never past
   * `PANE_HOLD_MS`: a slow read then says it is loading.
   */
  const shown = useKeptWhilePending({ vm, tidal, status, act, near, inSeason }, status === WATER_MAP_WORDS.loading, PANE_HOLD_MS);
  const kept = shown.kept;
  const d = shown.value;
  const dv = d.vm, dt = d.tidal;
  // (T731) a step into another water keeps the bar's height (an open parts list, a water with no exit: T875 holds the rest)
  const floor = useBarFloor(dv ? dv.regs.item : dt ? dt.regs.item : null);

  const mapPane = (
      <View style={split ? { flex: 1, minWidth: 0, minHeight: 0 } : { flex: 1 }}>
        <MapScreen palette={palette} {...map} onDrawn={onDrawn}
                   highlight={dv?.selected ?? []}
                   inSeason={d.inSeason}
                   water={dv?.water ?? []}
                   // (T175) the whole water's line at every zoom, and it opens no further out than its first zoom
                   waterLine={dv ? { sections: [...dv.selected, ...dv.water] } : undefined}
                   selection="dim"
                   // (T459) step mode: every other water grey, the receiving water kept
                   focus near={d.near ?? NO_NEAR}
                   marks={dv?.marks.map((m) => ({ lon: m.lon, lat: m.lat, label: m.label, a11y: m.a11y, on: m.on })) ?? []}
                   onPressMark={(i) => { const m = dv?.marks[i]; if (m && !kept) d.act({ type: "mark", ix: m.ix }); }}
                   // (T443) a part in its country, framed the same however it was reached (the first open, a step)
                   fit={dv ? { key: dv.fitKey, bbox: dv.bounds, refine: false, maxZoom: PART_FIT_MAX_ZOOM }
                     : dt ? { key: dt.fitKey, bbox: dt.bounds, refine: false, maxZoom: PART_FIT_MAX_ZOOM } : null}
                   onPressFeature={(_layer, id, lat, lon) => {
                     // (T731) the last water's map while the next is read: its sections are not this frame's to step on
                     if (kept) return;
                     const sid = Number(id);
                     if (vm && (vm.selected.includes(sid) || vm.water.includes(sid))) act({ type: "section", sid });
                     else onOtherSection(sid as SectionId, lat !== undefined && lon !== undefined ? { lat, lon } : null);
                   }} />
        {dv && (
          <Pressable onPress={() => { if (!kept) d.act({ type: "whole" }); }} accessibilityRole="button"
                     accessibilityLabel={dv.whole.aria} testID="water-map-whole" aria-disabled={kept}
                     style={{ position: "absolute", right: 10, bottom: 10, minHeight: 44, paddingHorizontal: 12,
                              justifyContent: "center", backgroundColor: palette.card, borderWidth: 1.5,
                              borderColor: palette.ink, ...palette.lift }}>
            <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{dv.whole.label}</Text>
          </Pressable>
        )}
        {/* (T731) a kept bar is the part stepped FROM: the mark the step closed is not drawn again */}
        {!kept && vm?.pop && <MarkPop vm={vm} palette={palette} act={act} />}
      </View>
  );
  /*
   * (T875, owner 2026-11-18: "The ui shifts here when moving upstream in desktop too") THE ▼ / ▲ STEPS STAY PUT, part to
   * part and water to water: they are measured from the edge the bar is anchored to, and only fixed-height rows lie
   * between them and it. The desktop's text column hangs from the top of the split, so the steps head it; everything
   * whose height a part decides (the parts row, the where line, the name, an open list) is below them. The phone's bar
   * stands on the foot of the screen (the map takes what is left above it), so there the steps keep their place above
   * "Show regulations" and the exit's place (both held at every part, T214), the bar's own content stands on its foot
   * (`justifyContent: "flex-end"`: room the T731 floor adds goes above it, not under it), and a water of one part holds
   * the parts row's place with a quiet line (`PartsOne`), so the bar and the map above it keep their size too.
   */
  const steps = (v: WaterMapVM) => (
    <View style={{ flexDirection: "row", gap: SPACE.s2, ...(split ? { marginBottom: 12 } : { marginTop: 10 }) }}
          testID="water-map-steps">
      {([["down", v.down], ["up", v.up]] as const).map(([dir, st]) => (
        <Step key={dir} dir={dir} palette={palette} label={st.label} aria={st.aria} disabled={st.disabled}
              onPress={() => step(dir)} />
      ))}
    </View>
  );
  const bar = (
      <>
      {/* THE BAR spans the canvas's width (the map is full-bleed under the water frame's header, T44); its words are on
          the frame's column, at the gutter every view's content starts at */}
      <View style={split ? { paddingTop: 14, paddingBottom: 14, backgroundColor: palette.card, flexShrink: 1, minHeight: 0 }
                         : { paddingTop: 10, paddingBottom: 14, borderTopWidth: 1.5, borderTopColor: palette.ink,
                             backgroundColor: palette.card, minHeight: floor.minHeight, justifyContent: "flex-end" }}
            testID="water-map-bar"
            onLayout={split ? undefined : floor.onLayout}
            // (T731) kept through a step's read: drawn as it was, taking no press, busy to a screen reader
            pointerEvents={kept ? "none" : "auto"} aria-busy={kept}>
        <BarInset split={split}>
        {dv ? (
          <>
            {/* (T875) THE STEPS STAY PUT (`steps` above): on a desktop they head the text column, on a phone they sit
                above "Show regulations" and the exit's place */}
            {split && steps(dv)}
            {!split && <Parts vm={dv} palette={palette} act={d.act} split={false} />}
            {/* (T582) which day the map's rules are for: a tab switch never changes the day silently */}
            <Text style={{ ...TYPE.metaBold, color: palette.ink }} testID="water-map-day">{DAY_LABELS.map(day, bcNow())}</Text>
            <Text style={{ ...TYPE.section, color: palette.faint }} testID="water-map-where">{dv.where}</Text>
            {/* (T214 item 4) two lines are kept for the name at every part: a name that wraps in one part and not in the
                next must not move the steps when the reader steps between them (a longer name is cut; its full words are the label) */}
            <Text style={{ ...TYPE.name, fontSize: 19, lineHeight: 23, minHeight: 46, color: palette.ink, marginTop: 2 }}
                  testID="water-map-name" numberOfLines={2} accessibilityLabel={dv.current.name}>{dv.current.name}</Text>
            {!split && steps(dv)}
            <Pressable ref={regsRing} onPress={() => onShowRegs(dv.regs.item as ItemId, dv.regs.partIx)} accessibilityRole="button"
                       accessibilityLabel={dv.regs.label} testID="water-map-regs"
                       style={{ minHeight: 52, marginTop: 8, alignItems: "center", justifyContent: "center",
                                backgroundColor: palette.accent }}>
              <Text style={{ ...TYPE.bodyStrong, fontFamily: TYPE.metaBold.fontFamily, color: palette.onAccent }}>
                {dv.regs.label}
              </Text>
            </Pressable>
            {split && <Parts vm={dv} palette={palette} act={d.act} split />}
            {/* (T203, T214 item 4) LAST, in a place kept for it. The bar sits at the foot of the screen, so a line that
                appears (only at the mouth part) grows it UPWARD and lifts every control above it, whatever its order
                (T203's "last" was not enough: the steps and "Show regulations" still jumped). On a water whose mouth
                flows into another the place is held at every part, empty or not, so nothing moves under a finger. */}
            {dv.exitHeld && (
              <View style={{ minHeight: EXIT_SLOT, marginTop: 8 }} testID="water-map-exit-slot">
                {/* (T345) the mouth meets tidal water: the line is said, with no way into it */}
                {dv.exit?.tidal && (
                  <View style={{ minHeight: EXIT_SLOT, justifyContent: "center" }} testID="water-map-exit">
                    <Text numberOfLines={1} style={{ ...TYPE.body, color: palette.faint }}>{dv.exit.label}</Text>
                  </View>
                )}
                {dv.exit && !dv.exit.tidal && (
                  <Pressable onPress={() => onOpenWater(dv.exit!.item as ItemId, dv.exit!.part)} accessibilityRole="link"
                             accessibilityLabel={dv.exit.label} testID="water-map-exit"
                             style={{ minHeight: EXIT_SLOT, justifyContent: "center" }}>
                    <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, color: palette.accent }}>{dv.exit.label}</Text>
                  </Pressable>
                )}
              </View>
            )}
          </>
        ) : dt ? (
          <>
            <Text style={{ ...TYPE.metaBold, color: palette.ink }} testID="water-map-tidal">{dt.title}</Text>
            <Text style={{ ...TYPE.name, fontSize: 19, lineHeight: 23, color: palette.ink, marginTop: 2 }}
                  testID="water-map-name" accessibilityLabel={dt.name}>{dt.name}</Text>
            <Text style={{ ...TYPE.body, color: palette.sub, marginTop: 6 }} testID="water-map-tidal-body">{dt.body}</Text>
            <Pressable onPress={() => onShowRegs(dt.regs.item as ItemId, dt.regs.partIx)} accessibilityRole="button"
                       accessibilityLabel={dt.regs.label} testID="water-map-regs"
                       style={{ minHeight: 52, marginTop: 8, alignItems: "center", justifyContent: "center",
                                backgroundColor: palette.accent }}>
              <Text style={{ ...TYPE.bodyStrong, fontFamily: TYPE.metaBold.fontFamily, color: palette.onAccent }}>
                {dt.regs.label}
              </Text>
            </Pressable>
          </>
        ) : (
          <View style={{ gap: 12, alignItems: "flex-start" }}>
            {/* (T257) still loading: the one spinner, beside the words that say what is coming */}
            {d.status === WATER_MAP_WORDS.loading && <LoadingState palette={palette} size={64} label={d.status} />}
            <Text style={{ ...TYPE.body, color: palette.sub }} testID="water-map-status">{d.status}</Text>
          </View>
        )}
        </BarInset>
      </View>
      </>
  );
  if (!split) {
    return (
      <View style={{ flex: 1, backgroundColor: palette.card }} testID="water-map">
        {mapPane}
        {bar}
      </View>
    );
  }
  // (T752) the split (the frame's `WaterSplit`): the text column does not scroll as a whole: the steps stay pinned and an
  // open parts list takes what room is left and scrolls inside it
  return (
    <View style={{ flex: 1, backgroundColor: palette.card }} testID="water-map">
      <WaterSplit palette={palette} map={mapPane}
                  text={<>{bar}<View style={{ paddingBottom: SPACE.s3 }}>{report}</View></>} />
    </View>
  );
}

/** The bar's words: on the frame's column and gutter under the canvas; in the split the text column already is the gutter's
 *  (T752), and the box may shrink so an open parts list scrolls instead of pushing the steps away. */
function BarInset({ split, children }: { split: boolean; children: React.ReactNode }) {
  return split ? <View style={{ flexShrink: 1, minHeight: 0 }}>{children}</View> : <WaterInset column>{children}</WaterInset>;
}

/** The parts row: the list's toggle on a water of many parts, the quiet one-part line on a water of one (T875). Under the
 *  map on a phone; under "Show regulations" in the split (the steps head the text column there). */
function Parts({ vm, palette, act, split }: { vm: WaterMapVM; palette: Palette; act: (a: WaterMapAction) => void; split: boolean }) {
  const place = split ? { marginTop: 8 } : { marginTop: -4, marginBottom: 6 };
  return vm.list ? <PartsList list={vm.list} palette={palette} act={act} tall={split} place={place} />
    : vm.onePart ? <PartsOne label={vm.onePart} palette={palette} place={place} /> : null;
}

/** (T875) A water of one part: the toggle's 44 px place, held by its words in a quiet line (nothing to open, no chevron). */
function PartsOne({ label, palette, place }: { label: string; palette: Palette; place: { marginTop: number; marginBottom?: number } }) {
  return (
    <View style={{ ...place, minHeight: 44, flexDirection: "row", alignItems: "center", gap: SPACE.s2 }} testID="water-map-parts-one">
      <Icon name="parts" colour={palette.faint} size={18} />
      <Text style={{ ...TYPE.body, flex: 1, color: palette.sub }}>{label}</Text>
    </View>
  );
}

/**
 * (T150) THE PARTS OF THIS WATER, under the map: one 44 px toggle (the list glyph the header's Parts button had, its
 * words "Show all 15 parts"), and open, every part by name — a radio group, the part shown checked (✓). The header's
 * Parts sheet moved here (owner 2026-10-23); tapping a part on the map still works. Normal weight throughout (T151: no
 * bold competing with the map's colours); the list scrolls inside a bounded box so the bar stays on screen.
 */
function PartsList({ list, palette, act, tall, place }: {
  list: NonNullable<WaterMapVM["list"]>; palette: Palette; act: (a: WaterMapAction) => void; tall: boolean;
  place: { marginTop: number; marginBottom?: number };
}) {
  // back closes the open list before it leaves the water (app/HANDOFF-ui.md "Back, everywhere")
  useBackLayer(list.open, () => act({ type: "list" }));
  // a pick closes the list, so the row that had focus is gone: focus goes back to the toggle (still drawn), not to the
  // top of the page — a keyboard or screen-reader reader stays where they were (T150 review)
  const toggle = useRef<ViewInstance>(null);
  const pick = (ix: number) => { act({ type: "part", ix }); (toggle.current as { focus?: () => void } | null)?.focus?.(); };
  return (
    <View style={{ ...place, flexShrink: 1, minHeight: 0 }} testID="water-map-parts">
      <Pressable ref={toggle} onPress={() => act({ type: "list" })} accessibilityRole="button" accessibilityLabel={list.toggle}
                 aria-expanded={list.open} testID="water-map-parts-toggle"
                 style={{ minHeight: 44, flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
        <Icon name="parts" colour={palette.accent} size={18} />
        <Text style={{ ...TYPE.body, flex: 1, color: palette.accent }}>{list.toggle}</Text>
        <Icon name={list.open ? "collapse" : "expand"} colour={palette.accent} size={18} />
      </Pressable>
      {list.open && (
        <ScrollView style={{ maxHeight: tall ? 380 : 232, flexShrink: 1, minHeight: tall ? 132 : 0, marginBottom: 10, borderTopWidth: 1, borderTopColor: palette.line,
                             borderBottomWidth: 1.5, borderBottomColor: palette.ink }}
                    accessibilityRole="radiogroup" accessibilityLabel={list.title} testID="water-map-parts-list">
          {list.rows.map((r) => (
            <Pressable key={r.ix} onPress={() => pick(r.ix)} accessibilityRole="radio"
                       aria-checked={r.here} accessibilityLabel={r.aria} testID={`water-map-part-${r.ix}`}
                       style={{ minHeight: 44, flexDirection: "row", alignItems: "center", gap: SPACE.s2,
                                paddingVertical: 4, borderBottomWidth: 1, borderBottomColor: palette.line }}>
              <Text style={{ ...TYPE.body, width: 16, color: palette.accent }}>{r.here ? WATER_MAP_WORDS.partsHere : ""}</Text>
              <View style={{ flex: 1 }}>
                <Text style={{ ...TYPE.body, color: palette.ink }}>{r.name}</Text>
                {r.note ? <Text style={{ ...TYPE.small, color: palette.sub }}>{r.note}</Text> : null}
              </View>
            </Pressable>
          ))}
        </ScrollView>
      )}
    </View>
  );
}

/** ▼ / ▲: a `STEP_HEIGHT` (53 px) button; at an end it is disabled, dashed, and says so ("Lowest part" / "Highest part"). Into another
 *  water its words say where (owner 2026-10-21 late: "▲ Upstream into Chilliwack River"): half the bar at 390 px, so
 *  the words wrap, centred, to two lines at most and a longer name is cut with an ellipsis (the full words stay the
 *  button's accessible name). */
function Step({ dir, palette, label, aria, disabled, onPress }: {
  dir: "up" | "down"; palette: Palette; label: string; aria: string; disabled: boolean; onPress: () => void;
}) {
  // (T186, Q162) the way's chevron is the icon set's — up `collapse`, down `expand` — beside the words, never a typed
  // ▲ / ▼; an end ("Lowest part") goes nowhere and draws none
  const ring = useCoachTarget(dir === "up" ? "map:up" : undefined);   // (T466) the walkthrough's "Walk upstream"
  return (
    <Pressable ref={ring} onPress={disabled ? undefined : onPress} disabled={disabled} accessibilityRole="button"
               accessibilityLabel={aria} aria-disabled={disabled}
               style={{ flex: 1, minWidth: 0, minHeight: STEP_HEIGHT, alignItems: "center", justifyContent: "center",
                        flexDirection: "row", gap: 4,
                        paddingHorizontal: SPACE.s2, paddingVertical: 4, borderWidth: 1.5,
                        borderColor: palette.ink, borderStyle: disabled ? "dashed" : "solid" }}>
      {!disabled && <Icon name={dir === "up" ? "collapse" : "expand"} colour={palette.ink} size={18} />}
      <Text numberOfLines={2} ellipsizeMode="tail" testID={`water-map-step-${dir}`}
            style={{ ...TYPE.bodyStrong, fontFamily: disabled ? TYPE.bodyStrong.fontFamily : TYPE.metaBold.fontFamily,
                     flexShrink: 1, textAlign: "center", color: disabled ? palette.sub : palette.ink }}>{label}</Text>
    </Pressable>
  );
}

/** A part named in a mark's pop: its name, its water beside it where the water has several parts. */
function PartLine({ p, palette }: { p: WaterMapPartVM; palette: Palette }) {
  return (
    <>
      <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{p.name}</Text>
      {p.note ? <Text style={{ ...TYPE.small, color: palette.sub }}>{p.note}</Text> : null}
    </>
  );
}

const card = (palette: Palette) => ({
  position: "absolute" as const, left: 10, right: 10, bottom: 10, paddingTop: 6, paddingBottom: 8, paddingLeft: 12,
  paddingRight: 6, backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, ...palette.lift,
});

function Head({ title, close, palette, onClose }: {
  title: string; close: string; palette: Palette; onClose: () => void;
}) {
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
      <Text style={{ ...TYPE.bodyStrong, fontFamily: TYPE.name.fontFamily, flex: 1, color: palette.ink }}>{title}</Text>
      <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel={close}
                 style={{ minWidth: 44, minHeight: 44, alignItems: "center", justifyContent: "center" }}>
        <Icon name="close" colour={palette.ink} size={20} />
      </Pressable>
    </View>
  );
}

/** A boundary mark, open: its words, the parts above it, the part below it (the prototype's `markPop`). */
function MarkPop({ vm, palette, act }: { vm: WaterMapVM; palette: Palette; act: (a: WaterMapAction) => void }) {
  const pop = vm.pop!;
  // back closes the open mark before it leaves the water (app/HANDOFF-ui.md "Back, everywhere")
  useBackLayer(true, () => act({ type: "mark", ix: null }));
  const side = (lab: string, parts: readonly WaterMapPartVM[]) => (
    <View style={{ paddingTop: 6, paddingBottom: 2, borderTopWidth: 1, borderTopColor: palette.line }}>
      <Text style={{ ...TYPE.section, color: palette.faint }}>{lab}</Text>
      {parts.map((p) => <PartLine key={p.ix} p={p} palette={palette} />)}
    </View>
  );
  return (
    <View style={card(palette)} role="dialog" aria-label={pop.title} testID="water-map-mark">
      <Head title={pop.title} close={pop.close} palette={palette} onClose={() => act({ type: "mark", ix: null })} />
      {side(pop.upLabel, pop.up)}
      {side(pop.downLabel, [pop.down])}
    </View>
  );
}
