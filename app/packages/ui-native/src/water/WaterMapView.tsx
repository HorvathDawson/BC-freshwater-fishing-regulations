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
import { useRef, type ComponentProps } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { WATER_MAP_WORDS } from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { useBackLayer, useRegsStore, useWaterMap, type WaterMapPartVM, type WaterMapVM, type WaterMapAction } from "@app/ui";
import { MapScreen } from "../MapScreen";
import { WaterInset } from "../WaterFrame";
import { Icon } from "../icons";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

/** The map's own props: everything the water map does not decide. */
export type WaterMapMapProps = Omit<ComponentProps<typeof MapScreen>,
  "palette" | "highlight" | "water" | "waterLine" | "selection" | "marks" | "onPressMark" | "fit" | "onPressFeature">;

/** The place kept for the "Flows into" line: the 44 px target the line has when it is drawn. */
const EXIT_SLOT = 44;

export function WaterMapView({ loader, item, part, tapSid, palette, map, onOpenWater, onShowRegs, onOtherSection,
                               onCross, onPart, whole = false }: {
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
  /** A tap on a section that is not this water's. */
  onOtherSection: (sid: SectionId) => void;
  /** ▲ / ▼ into the water this one goes on from or as (`water_continue`), at its part there. */
  onCross: (item: ItemId, part: number) => void;
  /** A step to another part: the frame's part from now on (a split lake's other area: that water). */
  onPart?: (item: ItemId, partIx: number) => void;
  /** (T203) Open framed on the whole water (from a search result). */
  whole?: boolean;
}) {
  const store = useRegsStore(loader);
  const wm = useWaterMap(store.value ?? null, item, part, tapSid, onPart, whole);
  const vm = wm.value?.vm ?? null;
  const act = wm.value?.act ?? (() => {});
  const status = store.error || wm.error ? WATER_MAP_WORDS.unavailable
    : wm.state === "loading" || store.state === "loading" ? WATER_MAP_WORDS.loading
    : vm === null ? WATER_MAP_WORDS.unavailable : null;

  return (
    <View style={{ flex: 1, backgroundColor: palette.card }} testID="water-map">
      <View style={{ flex: 1 }}>
        <MapScreen palette={palette} {...map}
                   highlight={vm?.selected ?? []}
                   water={vm?.water ?? []}
                   // (T175) the whole water's line at every zoom, and it opens no further out than its first zoom
                   waterLine={vm ? { sections: [...vm.selected, ...vm.water] } : undefined}
                   selection="dim"
                   marks={vm?.marks.map((m) => ({ lon: m.lon, lat: m.lat, label: m.label, a11y: m.a11y, on: m.on })) ?? []}
                   onPressMark={(i) => { const m = vm?.marks[i]; if (m) act({ type: "mark", ix: m.ix }); }}
                   fit={vm ? { key: vm.fitKey, bbox: vm.bounds, refine: false } : null}
                   onPressFeature={(_layer, id) => {
                     const sid = Number(id);
                     if (vm && (vm.selected.includes(sid) || vm.water.includes(sid))) act({ type: "section", sid });
                     else onOtherSection(sid as SectionId);
                   }} />
        {vm && (
          <Pressable onPress={() => act({ type: "whole" })} accessibilityRole="button"
                     accessibilityLabel={vm.whole.aria} testID="water-map-whole"
                     style={{ position: "absolute", right: 10, bottom: 10, minHeight: 44, paddingHorizontal: 12,
                              justifyContent: "center", backgroundColor: palette.card, borderWidth: 1.5,
                              borderColor: palette.ink, ...palette.lift }}>
            <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{vm.whole.label}</Text>
          </Pressable>
        )}
        {vm?.pop && <MarkPop vm={vm} palette={palette} act={act} />}
        {vm?.chooser && <Chooser vm={vm} palette={palette} act={act} />}
      </View>
      {/* THE BAR spans the canvas's width (the map is full-bleed under the water frame's header, T44); its words are on
          the frame's column, at the gutter every view's content starts at */}
      <View style={{ paddingTop: 10, paddingBottom: 14, borderTopWidth: 1.5, borderTopColor: palette.ink,
                     backgroundColor: palette.card }}>
        <WaterInset column>
        {vm ? (
          <>
            {vm.list && <PartsList list={vm.list} palette={palette} act={act} />}
            <Text style={{ ...TYPE.section, color: palette.faint }} testID="water-map-where">{vm.where}</Text>
            {/* (T214 item 4) two lines are kept for the name at every part: a name that wraps in one part and not in the
                next must not move the steps when the reader steps between them (a longer name is cut; its full words are the label) */}
            <Text style={{ ...TYPE.name, fontSize: 19, lineHeight: 23, minHeight: 46, color: palette.ink, marginTop: 2 }}
                  testID="water-map-name" numberOfLines={2} accessibilityLabel={vm.current.name}>{vm.current.name}</Text>
            <View style={{ flexDirection: "row", gap: SPACE.s2, marginTop: 10 }}>
              {([["down", vm.down], ["up", vm.up]] as const).map(([dir, st]) => (
                <Step key={dir} dir={dir} palette={palette} label={st.label} aria={st.aria} disabled={st.disabled}
                      onPress={() => (st.cross ? onCross(st.cross.item as ItemId, st.cross.part) : act({ type: dir }))} />
              ))}
            </View>
            <Pressable onPress={() => onShowRegs(vm.regs.item as ItemId, vm.regs.partIx)} accessibilityRole="button"
                       accessibilityLabel={vm.regs.label} testID="water-map-regs"
                       style={{ minHeight: 52, marginTop: 8, alignItems: "center", justifyContent: "center",
                                backgroundColor: palette.accent }}>
              <Text style={{ ...TYPE.bodyStrong, fontFamily: TYPE.metaBold.fontFamily, color: palette.onAccent }}>
                {vm.regs.label}
              </Text>
            </Pressable>
            {/* (T203, T214 item 4) LAST, in a place kept for it. The bar sits at the foot of the screen, so a line that
                appears (only at the mouth part) grows it UPWARD and lifts every control above it, whatever its order
                (T203's "last" was not enough: the steps and "Show regulations" still jumped). On a water whose mouth
                flows into another the place is held at every part, empty or not, so nothing moves under a finger. */}
            {vm.exitHeld && (
              <View style={{ minHeight: EXIT_SLOT, marginTop: 8 }} testID="water-map-exit-slot">
                {vm.exit && (
                  <Pressable onPress={() => onOpenWater(vm.exit!.item as ItemId, vm.exit!.part)} accessibilityRole="link"
                             accessibilityLabel={vm.exit.label} testID="water-map-exit"
                             style={{ minHeight: EXIT_SLOT, justifyContent: "center" }}>
                    <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, color: palette.accent }}>{vm.exit.label}</Text>
                  </Pressable>
                )}
              </View>
            )}
          </>
        ) : (
          <Text style={{ ...TYPE.body, color: palette.sub }} testID="water-map-status">{status}</Text>
        )}
        </WaterInset>
      </View>
    </View>
  );
}

/**
 * (T150) THE PARTS OF THIS WATER, under the map: one 44 px toggle (the list glyph the header's Parts button had, its
 * words "Show all 15 parts"), and open, every part by name — a radio group, the part shown checked (✓). The header's
 * Parts sheet moved here (owner 2026-10-23); tapping a part on the map still works. Normal weight throughout (T151: no
 * bold competing with the map's colours); the list scrolls inside a bounded box so the bar stays on screen.
 */
function PartsList({ list, palette, act }: {
  list: NonNullable<WaterMapVM["list"]>; palette: Palette; act: (a: WaterMapAction) => void;
}) {
  // back closes the open list before it leaves the water (app/HANDOFF-ui.md "Back, everywhere")
  useBackLayer(list.open, () => act({ type: "list" }));
  // a pick closes the list, so the row that had focus is gone: focus goes back to the toggle (still drawn), not to the
  // top of the page — a keyboard or screen-reader reader stays where they were (T150 review)
  const toggle = useRef<View>(null);
  const pick = (ix: number) => { act({ type: "part", ix }); (toggle.current as { focus?: () => void } | null)?.focus?.(); };
  return (
    <View style={{ marginTop: -4, marginBottom: 6 }} testID="water-map-parts">
      <Pressable ref={toggle} onPress={() => act({ type: "list" })} accessibilityRole="button" accessibilityLabel={list.toggle}
                 aria-expanded={list.open} testID="water-map-parts-toggle"
                 style={{ minHeight: 44, flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
        <Icon name="parts" colour={palette.accent} size={18} />
        <Text style={{ ...TYPE.body, flex: 1, color: palette.accent }}>{list.toggle}</Text>
        <Icon name={list.open ? "collapse" : "expand"} colour={palette.accent} size={18} />
      </Pressable>
      {list.open && (
        <ScrollView style={{ maxHeight: 232, marginBottom: 10, borderTopWidth: 1, borderTopColor: palette.line,
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

/** ▼ / ▲: a 52 px button; at an end it is disabled, dashed, and says so ("Lowest part" / "Highest part"). Into another
 *  water its words say where (owner 2026-10-21 late: "▲ Upstream into Chilliwack River"): half the bar at 390 px, so
 *  the words wrap, centred, to two lines at most and a longer name is cut with an ellipsis (the full words stay the
 *  button's accessible name). */
function Step({ dir, palette, label, aria, disabled, onPress }: {
  dir: "up" | "down"; palette: Palette; label: string; aria: string; disabled: boolean; onPress: () => void;
}) {
  // (T186, Q162) the way's chevron is the icon set's — up `collapse`, down `expand` — beside the words, never a typed
  // ▲ / ▼; an end ("Lowest part") goes nowhere and draws none
  return (
    <Pressable onPress={disabled ? undefined : onPress} disabled={disabled} accessibilityRole="button"
               accessibilityLabel={aria} aria-disabled={disabled}
               style={{ flex: 1, minWidth: 0, minHeight: 52, alignItems: "center", justifyContent: "center",
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

/** A part named in a pop or the chooser: its name, its water beside it where the water has several parts. */
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

/** ▲ or ▼ at a fork: which way, the main stem first. */
function Chooser({ vm, palette, act }: { vm: WaterMapVM; palette: Palette; act: (a: WaterMapAction) => void }) {
  const c = vm.chooser!;
  // back closes the chooser (no way taken) before it leaves the water
  useBackLayer(true, () => act({ type: "closeChooser" }));
  return (
    <View style={card(palette)} role="dialog" aria-label={c.title} testID="water-map-chooser">
      <Head title={c.title} close={c.close} palette={palette} onClose={() => act({ type: "closeChooser" })} />
      {c.options.map((p) => (
        <Pressable key={p.ix} onPress={() => act({ type: "choose", to: p.ix })} accessibilityRole="button"
                   accessibilityLabel={p.name}
                   style={{ minHeight: 44, paddingVertical: 6, justifyContent: "center", borderTopWidth: 1,
                            borderTopColor: palette.line }}>
          <PartLine p={p} palette={palette} />
        </Pressable>
      ))}
    </View>
  );
}
