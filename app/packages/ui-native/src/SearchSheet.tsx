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
import { SEARCH_BAR, type StatusIndex } from "@app/core";
import type { ItemId } from "@app/data";
import { useBackLayer, type SearchAction, type SearchPick, type SearchSession } from "@app/ui";
import { FishSpinner } from "./FishSpinner";
import { Icon } from "./icons";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { TOP_BAR, searchPanelWidth } from "./TopBar";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { count } from "./format";
import { sheetRows } from "./search/rows";
import { SheetRow } from "./search/SheetRow";

/** How far down the sheet sits at HALF: this share of its full height goes below the screen's edge. */
export const HALF_DROP = 0.45;
/** The transition's length, ms (F2–F3 of the approved frames: the dim and the rise together). */
export const SHEET_MS = 240;

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
              <Text style={{ ...TYPE.section, color: palette.faint }}>{count(total, SEARCH_BAR.namedWaters)}</Text>
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
            <Text style={{ ...TYPE.section, color: palette.faint }}>{count(total, SEARCH_BAR.namedWaters)}</Text>
          </View>
        )}
        {session.busy ? (
          <View style={{ alignItems: "center", paddingVertical: 28 }}>
            <FishSpinner palette={palette} size={86} label={SEARCH_BAR.searching} />
          </View>
        ) : (
          <FlatList
            data={rows}
            keyExtractor={(r) => r.key}
            keyboardShouldPersistTaps="handled"
            // at half the sheet's lower part is below the screen's edge: the list ends where the screen does
            contentContainerStyle={{ paddingBottom: half ? drop : 0 }}
            extraData={pick}
            renderItem={({ item: r }) => (
              <SheetRow r={r} palette={palette} pick={pick} act={act} onOpenWater={onOpenWater}
                        statusIndex={statusIndex} today={today} />
            )}
          />
        )}
      </Animated.View>
    </>
  );
}
