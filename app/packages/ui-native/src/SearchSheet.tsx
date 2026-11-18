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
 * top-right, Escape wherever the focus is, a click on the map beside it (the Shell). A phone's sheet closes by the
 * field's ‹, the system back, and (T730, owner 2026-11-14: "you can't swipe down to close it") A SWIPE DOWN, as every
 * bottom sheet in the app does (`Sheet`, `useSheetDrag`): from the grabber at any time, or from the list when it is at
 * its top. The sheet follows the finger; past a third of its height or on a downward flick search closes, otherwise it
 * springs back. A drag that starts with the list scrolled down scrolls the list; upward from the top scrolls as usual.
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
import { useEffect, useMemo, useRef, useState } from "react";
import { Animated, View, useWindowDimensions } from "react-native";
import { FlatList, GestureDetector, usePanGesture, type PanGestureConfig } from "react-native-gesture-handler";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { SEARCH_BAR, type StatusIndex, bcNow } from "@app/core";
import type { ItemId } from "@app/data";
import { useBackLayer, useSheetDrag, type SearchAction, type SearchPick, type SearchSession } from "@app/ui";
import { LoadingState } from "./LoadingState";
import { Icon } from "./icons";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { TOP_BAR, searchPanelWidth } from "./TopBar";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { count } from "./format";
import { sheetRows } from "./search/rows";
import { SheetRow } from "./search/SheetRow";
import { hitProps } from "./regs/hit";
import { CoachScrollContext, useCoachScroller } from "./coachScroll";

/** How far down the sheet sits at HALF: this share of its full height goes below the screen's edge. */
export const HALF_DROP = 0.45;
/** The transition's length, ms (F2–F3 of the approved frames: the dim and the rise together). */
export const SHEET_MS = 240;

export function SearchSheet({ open, top, palette, session, pick, onAct, onOpenWater, onClose, statusIndex, day = null, total, recents = [], onRecent, onClearRecents }: {
  /** Search is open (route `search`). Closed: the sheet slides (or fades) away and its rows go. */
  open: boolean;
  /**
   * Close search (route `search` off; the map stays where search left it, T730). A WIDE screen's panel calls it from its
   * ✕ at the top-right and from Escape anywhere on the page (owner 2026-10-22 late); a phone's sheet from a swipe down
   * (T730; its ‹ is the field's).
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
  /** The day the map is on (the picked day), null = today: the rows' status dots answer for THE SAME day (T620 G1). */
  day?: Date | null;
  /** How many named waters there are (the source's count), for the sheet's head; absent until read. */
  total?: number;
  /** (T247) recent searches (device-only) shown before two letters are typed, and what a tap or Clear does */
  recents?: readonly string[];
  onRecent?: (q: string) => void;
  onClearRecents?: () => void;
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
  /*
   * (T730) THE SWIPE DOWN (a phone's sheet only; the wide panel has no bottom edge to pull it to). `pull` is how far the
   * finger has the sheet below where it stands; reset whenever search opens.
   */
  const drag = useSheetDrag(onClose);
  const pull = useRef(new Animated.Value(0)).current;
  const tall = useRef(0);
  tall.current = height;
  const listTop = useRef(0);
  useEffect(() => { if (open) { pull.setValue(0); listTop.current = 0; } }, [open, pull]);
  // (T842) the walkthrough's highlight on a row (the Vedder River) rides the list's scroll on the UI thread, and the walk
  // may scroll the row into view (owner on 0.7.12: the box trailed a scroll; the row was under the keyboard and the card)
  const coach = useCoachScroller();
  const phone = panel === null;
  const handlePull = usePanGesture(useMemo((): PanGestureConfig => ({
    runOnJS: true, disableReanimated: true, testID: "search-sheet-drag", enabled: phone,
    activeOffsetY: 6, failOffsetX: [-24, 24],
    onUpdate: (e) => pull.setValue(drag.offset(e.translationY)),
    // `canceled`: the system took the drag after it went active — not a release, so it springs back
    onDeactivate: (e) => { if (e.canceled || !drag.release(e.translationY, e.velocityY, tall.current)) springBack(pull); },
  }), [drag, pull, phone]));
  const listPull = usePanGesture(useMemo((): PanGestureConfig => {
    let armed = false;
    return {
      runOnJS: true, disableReanimated: true, testID: "search-list-drag", enabled: phone,
      activeOffsetY: 6, failOffsetX: [-24, 24],
      // only a drag that starts with the list at its top moves the sheet
      onBegin: () => { armed = listTop.current <= 0; },
      onUpdate: (e) => { if (armed) pull.setValue(drag.offset(e.translationY)); },
      onDeactivate: (e) => {
        if (!armed) return;
        armed = false;
        if (e.canceled || !drag.release(e.translationY, e.velocityY, tall.current)) springBack(pull);
      },
    };
  }, [drag, pull, phone]));
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
  // (T730, owner 2026-11-16) back from a picked place closes search, as after a water (it used to step to the results
  // with the text kept, and a second back closed): one back returns to the map. The on-screen "← All results" row still
  // steps to the results (`act({ t: "back" })`, SheetRow)
  useBackLayer(open && pick.town !== null, onClose);
  /*
   * ESCAPE CLOSES SEARCH ON A WIDE SCREEN, wherever the focus is (owner 2026-10-22 late): after a pick the field has
   * given up the focus, so the field's own Escape would no longer hear it. Typed or not, a town's list or not, it is
   * the whole search that goes (escapeCloses.web.ts; a dialog over the page keeps its own Escape).
   */
  useEscapeCloses(open && panel !== null, onClose);

  if (!shown) return null;
  const rows = open ? sheetRows(pick, session, recents) : [];
  const drop = Math.round(height * HALF_DROP);
  const today = day ?? bcNow();
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
                              transform: [{ translateY: Animated.add(Animated.add(
                                still ? 0 : gone.interpolate({ inputRange: [0, 1], outputRange: [0, height || 900] }),
                                lowered.interpolate({ inputRange: [0, 1], outputRange: [0, drop] })), pull) }] }}>
        {/* the grabber: says this is a sheet over the map, not a page — and (T730) on a phone it is what a swipe down pulls */}
        <GestureDetector gesture={handlePull} touchAction="none">
          <View testID="search-sheet-handle" style={{ alignItems: "center", paddingTop: 7, paddingBottom: 3 }}>
            <View style={{ width: 38, height: 4, backgroundColor: palette.line2 }} />
          </View>
        </GestureDetector>
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
            <Pressable {...hitProps({ w: 44, h: 44 })} testID="search-panel-close" onPress={onClose} accessibilityRole="button"
                       accessibilityLabel={SEARCH_BAR.close}
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
          <LoadingState palette={palette} size={86} label={SEARCH_BAR.searching}
                        style={{ alignItems: "center", paddingVertical: 28 }} />
        ) : (
          <GestureDetector gesture={listPull}>
            {/* (T440's lesson) a real view between the pan's detector and the list's own: drawn as its direct child, Android
                left the list out of the accessibility tree */}
            <View testID="search-list-frame" collapsable={false} style={{ flex: 1 }}>
              <CoachScrollContext.Provider value={coach.scroller}>
              <FlatList
                ref={coach.ref}
                testID="search-list"
                data={rows}
                keyExtractor={(r) => r.key}
                keyboardShouldPersistTaps="handled"
                // the list's own scroll runs beside the pull (neither cancels the other); `bounces` off: at the top a
                // downward pull moves the sheet, not an iOS overscroll of the list
                simultaneousWith={listPull} bounces={false} scrollEventThrottle={16}
                onScroll={(e) => { listTop.current = e.nativeEvent.contentOffset.y; coach.track(listTop.current); }}
                // at half the sheet's lower part is below the screen's edge: the list ends where the screen does
                contentContainerStyle={{ paddingBottom: half ? drop : 0 }}
                extraData={pick}
                renderItem={({ item: r }) => (
                  <SheetRow r={r} palette={palette} pick={pick} act={act} onOpenWater={onOpenWater}
                            statusIndex={statusIndex} today={today} onRecent={onRecent}
                            onClearRecents={onClearRecents} />
                )}
              />
              </CoachScrollContext.Provider>
            </View>
          </GestureDetector>
        )}
      </Animated.View>
    </>
  );
}

/** A drag let go short of closing: the sheet springs back to where it stands. */
const springBack = (v: Animated.Value) => Animated.spring(v, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
