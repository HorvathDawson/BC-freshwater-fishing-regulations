/**
 * (T48) SEARCH'S RESULTS SHEET, over the SAME map: names, aliases, and the towns you can ask "what is near here" about.
 *
 * Today's search screen became this sheet (approved transition, handoff/PLAN/app-data/search-topbar option 1): the top
 * bar's field takes focus in place, the map behind stays MOUNTED and dims, and this sheet rises over it. There is no
 * preview map any more: the MAIN map frames what the reader looks at (`useSearchSession`'s focus, drawn by the Shell),
 * so a second MapLibre is never made.
 *
 *   FULL while choosing (nothing picked): its top 8 px under the bar, the map dimmed under it (`palette.scrim`).
 *   HALF while looking (`searchSheet`): a water row tapped frames it on the map; a PLACE tapped is shown at once — the
 *        map flies there, its pin drops, the waters near it light and "Water near <place>" lists them (owner
 *        2026-10-21: no eye for places). The dim lifts and the keyboard drops, so map and list are both in view.
 *
 * LOOK, THEN GO. A tap on a water row only frames it; the SELECTED water row carries the one way out — its open action
 * (›), which opens the water screen through `openWater` (the one opener). Back from that water returns here as it was.
 *
 * ON A WIDE SCREEN it is a panel beside the map and closes where the eye is (owner 2026-10-22 late): its own ✕ at its
 * top-right, Escape wherever the focus is, a click on the map beside it (the Shell). A phone's sheet has no close of
 * its own: the field's ‹ is it, as before.
 *
 * Reduced motion: a cross-fade, no slide (`useReducedMotion`). Transforms and opacity only, on the native driver.
 *
 * Every decision — ranking, which water the map shows, the box, the sheet's height — is made in `@app/core` and
 * `@app/ui` (`rankWaters`, `resultGroups`, `searchStep`, `useSearchSession`); this file only draws it.
 *
 * Three things the design insists on and this preserves:
 *  - the alias line. Someone searching "Vedder" must see that it matched Chilliwack River, or the result looks like the
 *    wrong river.
 *  - the WHERE line. Two rows reading "Elk River" are one choice shown twice; "near Fernie" and "near Gold River" are
 *    two choices. Composed in core (`wherePhrase`).
 *  - a town is a question about the ground, not a water: its row says so, in its colour (`palette.place`).
 */
import { useEscapeCloses } from "./escapeCloses";
import { useEffect, useRef, useState } from "react";
import { Animated, FlatList, Pressable, Text, View, useWindowDimensions } from "react-native";
import { SEARCH_BAR, WATER_STATUS, nearCount, nearRowSub, spokenName, waterRowSub, watersNear,
         type StatusIndex, type WaterStatus } from "@app/core";
import type { ItemId, NameHit, NearHit, PlaceHit } from "@app/data";
import { isSelected, resultGroups, rowStatus, useBackLayer,
         type SearchAction, type SearchPick, type SearchSession, type SearchTarget } from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { Icon, IconText } from "./icons";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { TOP_BAR, searchPanelWidth } from "./TopBar";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { count } from "./format";

type Row =
  | { t: "head"; key: string; label: string; right?: string; back?: boolean; place?: boolean;
      /** Drawn on the places band — the heading of the places group. */
      band?: boolean }
  | { t: "place"; key: string; place: PlaceHit }
  | { t: "water"; key: string; hit: NameHit; dup: boolean }
  | { t: "near"; key: string; near: NearHit }
  | { t: "note"; key: string; text: string };

/** How far down the sheet sits at HALF: this share of its full height goes below the screen's edge. */
export const HALF_DROP = 0.45;
/** The transition's length, ms (F2–F3 of the approved frames: the dim and the rise together). */
export const SHEET_MS = 240;

/** The rows the sheet draws, from what the reader picked and what search answered. Pure: the render test reads it. */
export function sheetRows(pick: SearchPick, s: SearchSession): Row[] {
  const rows: Row[] = [];
  const place = pick.town;
  if (place) {
    const near = s.town.state === "ready" && s.town.value ? s.town.value.near : [];
    rows.push({ t: "head", key: "back", label: "All results", back: true,
                right: s.town.state === "ready" ? nearCount(near.length) : "" });
    rows.push({ t: "head", key: "near-h", label: `Water near ${place.name}`, place: true });
    if (s.town.state === "failed")
      rows.push({ t: "note", key: "fail", text: "Could not read what is near this town." });
    else if (s.town.state === "ready" && !near.length)
      rows.push({ t: "note", key: "none", text: watersNear(0, place.name) });
    for (const n of near) rows.push({ t: "near", key: `n:${n.item}`, near: n });
  } else if (s.typed) {
    for (const g of resultGroups(s.results)) {
      if (g.kind === "places") {
        rows.push({ t: "head", key: "ph", label: g.title, right: g.note, place: true, band: true });
        for (const p of g.places) rows.push({ t: "place", key: `p:${p.place}`, place: p });
      } else {
        rows.push({ t: "head", key: "wh", label: g.title });
        for (const w of g.waters) rows.push({ t: "water", key: `w:${w.hit.item}`, hit: w.hit, dup: w.dup });
      }
    }
    if (s.none) rows.push({ t: "note", key: "none", text: SEARCH_BAR.none });
    if (s.failed) rows.push({ t: "note", key: "fail", text: SEARCH_BAR.failed });
  } else {
    rows.push({ t: "note", key: "start", text: SEARCH_BAR.start });
  }
  return rows;
}

export function SearchSheet({ open, top, palette, session, pick, onAct, onOpenWater, onClose, statusIndex, total }: {
  /** Search is open (route `search`). Closed: the sheet slides (or fades) away and its rows go. */
  open: boolean;
  /**
   * Close search (route `search` off; the Shell restores the map). A WIDE screen's panel calls it from its ✕ at the
   * top-right and from Escape anywhere on the page (owner 2026-10-22 late); a phone's sheet never does (its ‹ is the
   * field's).
   */
  onClose: () => void;
  /** The bar's bottom edge: the sheet's top sits 8 px under it. */
  top: number;
  palette: Palette;
  session: SearchSession;
  pick: SearchPick;
  /** One step of `searchStep` (a row tapped, a town's ← All results). */
  onAct: (a: SearchAction) => void;
  /** The selected water row's open action: the water screen, through `openWater`. */
  onOpenWater: (item: ItemId) => void;
  /** The day's status of each water (the index the map colours by); null: no row carries a status. */
  statusIndex: StatusIndex | null;
  /** How many named waters there are (the source's count), for the sheet's head; absent until read. */
  total?: number;
}) {
  const still = useReducedMotion();
  // a wide screen: the sheet is a panel under the bar, the map clear beside it
  const panel = searchPanelWidth(useWindowDimensions().width);
  const half = session.size === "half";
  // 0 = in place; 1 = gone (slid below, or faded out). `drop` = how far down the half sheet sits, in px.
  const gone = useRef(new Animated.Value(open ? 0 : 1)).current;
  const lowered = useRef(new Animated.Value(0)).current;
  const [height, setHeight] = useState(0);
  const [shown, setShown] = useState(open);
  if (open && !shown) setShown(true);
  useEffect(() => {
    const run = Animated.timing(gone, { toValue: open ? 0 : 1, duration: SHEET_MS, useNativeDriver: NATIVE_DRIVER });
    run.start(({ finished }) => { if (finished && !open) setShown(false); });
    return () => run.stop();
  }, [open, gone]);
  useEffect(() => {
    const to = half ? 1 : 0;
    if (still) { lowered.setValue(to); return; }
    const run = Animated.timing(lowered, { toValue: to, duration: SHEET_MS, useNativeDriver: NATIVE_DRIVER });
    run.start();
    return () => run.stop();
  }, [half, still, lowered]);
  // back, everywhere (app/HANDOFF-ui.md): a town's list is a step inside search — back is its "← All results"
  useBackLayer(open && pick.town !== null, () => onAct({ t: "back" }));
  /*
   * ESCAPE CLOSES SEARCH ON A WIDE SCREEN, wherever the focus is (owner 2026-10-22 late): after a pick the field has
   * given up the focus, so the field's own Escape would no longer hear it. Typed or not, a town's list or not, it is
   * the whole search that goes (escapeCloses.web.ts; a dialog over the page keeps its own Escape).
   */
  useEscapeCloses(open && panel !== null, onClose);

  if (!shown) return null;
  const rows = open ? sheetRows(pick, session) : [];
  const drop = Math.round(height * HALF_DROP);
  const today = new Date();
  const act = (a: SearchAction) => onAct(a);
  return (
    <>
      {/* THE DIM, over the map and under the sheet: while choosing only; it lifts at half so the map is clear */}
      <Animated.View testID="search-dim" pointerEvents="none"
                     style={{ position: "absolute", top: 0, left: 0, right: 0, bottom: 0,
                              backgroundColor: palette.scrim,
                              opacity: Animated.multiply(gone.interpolate({ inputRange: [0, 1], outputRange: [1, 0] }),
                                                         lowered.interpolate({ inputRange: [0, 1], outputRange: [1, 0] })) }} />
      <Animated.View testID={`search-sheet-${session.size}`}
                     onLayout={(e) => setHeight(e.nativeEvent.layout.height)}
                     style={{ position: "absolute", top: top + 8, bottom: 0,
                              ...(panel === null ? { left: 0, right: 0 }
                                : { left: TOP_BAR.side, width: panel, borderLeftWidth: 1.5, borderRightWidth: 1.5,
                                    borderLeftColor: palette.ink, borderRightColor: palette.ink }),
                              backgroundColor: palette.card, borderTopWidth: 1.5, borderTopColor: palette.ink,
                              opacity: still ? gone.interpolate({ inputRange: [0, 1], outputRange: [1, 0] }) : 1,
                              transform: [{ translateY: Animated.add(
                                still ? 0 : gone.interpolate({ inputRange: [0, 1], outputRange: [0, height || 900] }),
                                lowered.interpolate({ inputRange: [0, 1], outputRange: [0, drop] })) }] }}>
        {/* the grabber: says this is a sheet over the map, not a page */}
        <View style={{ alignItems: "center", paddingTop: 7, paddingBottom: 3 }}>
          <View style={{ width: 38, height: 4, backgroundColor: palette.line2 }} />
        </View>
        {panel !== null ? (
          /*
           * A WIDE SCREEN'S PANEL CLOSES FROM ITS TOP-RIGHT (owner 2026-10-22 late): the ✕ sits by the results, where the
           * eye is, not in the field (whose ✕ only clears) nor across the screen. The count moves left of it.
           */
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "flex-end", gap: 6,
                         paddingLeft: 18, paddingRight: 6 }}>
            {!pick.town && total !== undefined && (
              <Text style={{ ...TYPE.section, color: palette.faint }}>{count(total, "named waters")}</Text>
            )}
            <Pressable testID="search-panel-close" onPress={onClose} accessibilityRole="button"
                       accessibilityLabel={SEARCH_BAR.close} hitSlop={4}
                       style={({ pressed }) => ({ width: 44, height: 44, alignItems: "center", justifyContent: "center",
                                                  borderRadius: palette.r.box,
                                                  backgroundColor: pressed ? palette.wash : "transparent" })}>
              <Icon name="close" colour={palette.ink} size={22} />
            </Pressable>
          </View>
        ) : !pick.town && total !== undefined && (
          <View style={{ flexDirection: "row", justifyContent: "flex-end", paddingHorizontal: 18, paddingBottom: 2 }}>
            <Text style={{ ...TYPE.section, color: palette.faint }}>{count(total, "named waters")}</Text>
          </View>
        )}
        {session.busy ? (
          <View style={{ alignItems: "center", paddingVertical: 28 }}>
            <FishSpinner palette={palette} size={86} label="Searching" />
          </View>
        ) : (
          <FlatList
            data={rows}
            keyExtractor={(r) => r.key}
            keyboardShouldPersistTaps="handled"
            // at half the sheet's lower part is below the screen's edge: the list ends where the screen does
            contentContainerStyle={{ paddingBottom: half ? drop : 0 }}
            extraData={pick}
            renderItem={({ item: r }) => {
              if (r.t === "head")
                return (
                  <View style={{ flexDirection: "row", alignItems: "baseline", gap: 10,
                                 paddingHorizontal: 18, paddingTop: 12, paddingBottom: 8,
                                 borderTopWidth: r.key === "near-h" || r.key === "back" ? 0 : 1,
                                 borderTopColor: palette.line,
                                 backgroundColor: r.band ? palette.placeBand : undefined }}>
                    {r.back ? (
                      <Pressable onPress={() => act({ t: "back" })} accessibilityRole="button"
                                 accessibilityLabel="Back to all results">
                        <IconText name="back" text={r.label} size={14} gap={2}
                                  style={{ ...TYPE.section, color: palette.accent }} />
                      </Pressable>
                    ) : (
                      <Text accessibilityRole="header"
                            style={{ ...TYPE.section, color: r.place ? palette.place : palette.faint }}>
                        {r.label}
                      </Text>
                    )}
                    {r.right ? (
                      <Text style={{ ...TYPE.section, marginLeft: "auto",
                                     // `faint` is under 4.5:1 on the band; `place` is not.
                                     color: r.band ? palette.place : palette.faint }}>
                        {r.right}
                      </Text>
                    ) : null}
                  </View>
                );
              if (r.t === "note")
                return (
                  <Text style={{ ...TYPE.small, color: palette.sub, paddingHorizontal: 18, paddingTop: 10 }}>
                    {r.text}
                  </Text>
                );
              if (r.t === "place") {
                const t: SearchTarget = { kind: "place", place: r.place };
                return (
                  <Hit palette={palette} tone={palette.place} bar band name={r.place.name}
                       sub={`${cap(r.place.kind)} · every named water around it`}
                       label={`Show ${r.place.name} on the map`}
                       onPress={() => act({ t: "select", target: t })} />
                );
              }
              if (r.t === "near") {
                const t: SearchTarget = { kind: "water", item: r.near.item, name: r.near.name };
                return (
                  <Hit palette={palette} tone={palette.accent} name={r.near.name}
                       status={rowStatus(r.near.item, statusIndex, today)}
                       sub={nearRowSub(r.near.kind, r.near.km)}
                       selected={isSelected(pick, t)}
                       label={`Show ${r.near.name} on the map`}
                       goLabel={SEARCH_BAR.open(r.near.name)}
                       onPress={() => act({ t: "select", target: t })}
                       onGo={() => onOpenWater(r.near.item)} />
                );
              }
              const h = r.hit;
              const named = spokenName(h.name, h.near);
              const t: SearchTarget = { kind: "water", item: h.item, name: h.name };
              return (
                <Hit palette={palette} tone={palette.accent} name={h.name}
                     status={rowStatus(h.item, statusIndex, today)}
                     alias={h.matchedAs !== null ? `also ${h.matchedAs}` : null}
                     sub={waterRowSub(h.kind, h.near, r.dup)}
                     strongSub={r.dup}
                     selected={isSelected(pick, t)}
                     label={`Show ${named} on the map`}
                     goLabel={SEARCH_BAR.open(named)}
                     onPress={() => act({ t: "select", target: t })}
                     onGo={() => onOpenWater(h.item)} />
              );
            }}
          />
        )}
      </Animated.View>
    </>
  );
}

const cap = (s: string) => (s ? s[0]!.toUpperCase() + s.slice(1) : s);

/**
 * One result. SELECT, THEN GO (`searchStep`): a tap (or Enter) on the row selects it — the main map frames and lights
 * it — and never navigates. Only the SELECTED water row carries the open action: a › at the row's end, labelled
 * "Open <name>", after the row in the tab order (T48: the eye is gone — a place is shown by its tap, and a water's way
 * out opens it). Two separate controls side by side — never one inside the other, which on the web is a button inside
 * a button and reaches a screen reader as one control with two names. The row announces its state with `aria-pressed`
 * (AGENTS 34).
 *
 * `tone` says what kind of thing the row is: the accent for a water, `palette.place` for a town, which also gets a bar
 * down its edge so the two groups cannot be mistaken for each other at a glance.
 */
function Hit({ palette, tone, bar, band, name, alias, sub, strongSub, selected, label, onPress,
               goLabel, onGo, status = null }: {
  palette: Palette; tone: string; bar?: boolean;
  /**
   * On the places band. The row takes the band's warm ground, and its secondary line is drawn in the place colour,
   * because the greys do not hold 4.5:1 on the tint.
   */
  band?: boolean; name: string; alias?: string | null;
  sub: string; strongSub?: boolean; selected?: boolean; label: string;
  onPress: () => void;
  /** The selected row's open action (a water's). Absent: the row has none (a place). */
  goLabel?: string; onGo?: () => void;
  /**
   * The water's status (`rowStatus`), a small dot in the colour the map paints it (`palette.waterStatus`), named for a
   * screen reader. Null — not asked: the status index has not loaded, or was refused for another atlas — draws nothing.
   */
  status?: WaterStatus | null;
}) {
  const [focus, setFocus] = useState<"row" | "go" | null>(null);
  // The ring is for the KEYBOARD. A tap focuses the control too, and a ring left round every row the reader clicked
  // reads as a second, louder selection — so a focus that follows a press is not drawn (what the web calls
  // :focus-visible).
  const pressing = useRef(false);
  const onFocusOf = (which: "row" | "go") => () => {
    if (!pressing.current) setFocus(which);
  };
  const blur = () => { pressing.current = false; setFocus(null); };
  const press = () => { pressing.current = true; setFocus(null); };
  const ring = { outlineWidth: 2, outlineStyle: "solid" as const, outlineColor: tone, outlineOffset: -2 };
  return (
    <View style={{ flexDirection: "row", alignItems: "stretch",
                   borderTopWidth: 1, borderTopColor: palette.line,
                   backgroundColor: band ? palette.placeBand : selected ? palette.wash : palette.card }}>
      {/* The edge bar: a town's colour always, a water's only while it is selected. */}
      <View style={{ width: 4, backgroundColor: bar ? tone : selected ? tone : "transparent" }} />
      <Pressable onPress={onPress} accessibilityRole="button"
                 accessibilityLabel={status ? `${label}. ${WATER_STATUS[status].label}` : label}
                 aria-pressed={!!selected}
                 onPressIn={press}
                 onFocus={onFocusOf("row")} onBlur={blur}
                 style={({ pressed }) => ({
                   flex: 1, minWidth: 0,
                   paddingLeft: 14, paddingRight: 8, paddingTop: 13, paddingBottom: 14, gap: 4,
                   backgroundColor: pressed ? palette.tint : "transparent",
                   ...(focus === "row" ? ring : null),
                 })}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
          <Text style={{ ...TYPE.name, fontSize: 20, lineHeight: 23, color: palette.ink, flexShrink: 1 }}>
            {name}
          </Text>
          {status ? (
            <View style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: palette.waterStatus[status] }} />
          ) : null}
        </View>
        {/* The alias line. Without it a matched alias looks like the wrong water. */}
        {alias ? (
          <Text style={{ ...TYPE.small, color: band ? palette.place : palette.faint, fontStyle: "italic" }}>
            {alias}
          </Text>
        ) : null}
        <Text style={{ ...(strongSub ? TYPE.bodyStrong : TYPE.small), fontSize: 13,
                       color: band ? palette.place : palette.sub }}>{sub}</Text>
      </Pressable>
      {/*
        THE OPEN ACTION — the selected water row's only. 44 × 44, the smallest target a thumb reliably hits, outlined in
        the row's tone. Focusable on the web in its own right, with a ring, so a keyboard selects with one stop and opens
        with the next.
      */}
      {selected && onGo && goLabel ? (
        <View style={{ justifyContent: "center", paddingRight: 14, paddingLeft: 4 }}>
          <Pressable onPress={onGo} accessibilityRole="button" accessibilityLabel={goLabel}
                     onPressIn={press}
                     onFocus={onFocusOf("go")} onBlur={blur}
                     hitSlop={6}
                     style={({ pressed }) => ({
                       width: 44, height: 44, alignItems: "center", justifyContent: "center",
                       borderWidth: 1.5, borderColor: tone, borderRadius: palette.r.box,
                       backgroundColor: pressed ? tone : palette.card,
                       ...(focus === "go" ? { ...ring, outlineColor: palette.ink, outlineOffset: 2 } : null),
                     })}>
            {({ pressed }) => <Icon name="next" colour={pressed ? palette.card : tone} size={24} />}
          </Pressable>
        </View>
      ) : null}
    </View>
  );
}
