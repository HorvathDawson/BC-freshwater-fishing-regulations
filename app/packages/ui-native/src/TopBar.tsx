/**
 * (T48) THE TOP BAR over the browse maps: the search field ALONE, always open (owner 2026-10-21: "the search bar can be
 * always open"), with the SYNC ICON at its right end (owner 2026-10-22: "Flows checked 27h ago shouldnt be on the search
 * bar like this. but we do want the sync icon up there with a way to indicate if it needs to be refreshed"). The text
 * line that sat over the field is gone; its facts — every source's date and freshness — are More's Data block.
 *
 * THE SYNC ICON (`syncVM`, core/more.ts): Lucide's refresh glyph in a 40 px slot, with a small dot when the data NEEDS A
 * REFRESH (the feed past its cadence or not reached, or a newer data release) and a slow turn while CHECKING (still,
 * under reduced motion). Fresh: the glyph alone. Its tap refreshes what can be refreshed (`useSync().refresh`) and opens
 * nothing; its accessible label states the state. It gives way to the clear ✕ while search is open.
 *
 * Tapping the field focuses it IN PLACE — the same frame before and after (no jump): the magnifier's 40 px slot becomes
 * the ‹ that closes search, the edge turns accent, and the Shell raises the results sheet over the SAME map behind
 * (`SearchSheet`; no second map). Closing — ‹, Escape, the system back — reverses it: search closes first (route
 * `search`), the keyboard drops, and the Shell puts the camera back exactly.
 *
 * ON A WIDE SCREEN (owner 2026-10-22 late: "confusing to close search on desktop") the bar closes nothing: the magnifier
 * stays, and search closes by the results panel's ✕, Escape anywhere, or a click on the map beside it (`SearchSheet`,
 * the Shell). The ✕ inside the field, on every screen, only clears what was typed ("Clear search text").
 *
 * The placeholder is ONE line per opening of the app (`placeholderFor`), never cycled while on screen; the field is
 * always called "Search waters and towns" aloud, whatever the line says.
 */
import { forwardRef, useEffect, useRef } from "react";
import { Animated, Easing, Pressable, Text, TextInput, View, useWindowDimensions } from "react-native";
import { SEARCH_BAR, type SyncVM } from "@app/core";
import { Icon } from "./icons";
import { NATIVE_DRIVER, useReducedMotion } from "./motion";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The bar's measures: inset from the screen's edges, the field, the slab under it, the sync slot and its dot. */
export const TOP_BAR = { top: 10, side: 14, field: 44, slab: 3, wide: 720, panel: 440, slot: 40, dot: 8 } as const;

/** (T48) On a wide screen (tablet, desktop) the bar and its sheet are a left panel, not a full-width strip. */
export function searchPanelWidth(screen: number): number | null {
  return screen >= TOP_BAR.wide ? TOP_BAR.panel : null;
}

/** Where the bar's bottom edge is (the slab included): what the map's chips, rail and controls start under. */
export function topBarBottom(): number {
  return TOP_BAR.top + TOP_BAR.field + 3 + TOP_BAR.slab;
}

export interface TopBarProps {
  palette: Palette;
  /** This opening's line (`placeholderFor`). */
  placeholder: string;
  /** The sync icon's state (`syncVM`), or null: nothing to sync (no feed and no release to check). */
  sync: SyncVM | null;
  /** The sync icon's tap: refresh what can be refreshed. */
  onSync?: () => void;
  /** Search is open (route `search`): the ‹ in the magnifier's place, the accent edge. */
  open: boolean;
  query: string;
  onQuery: (q: string) => void;
  /** The field took focus: open search (once). */
  onOpen: () => void;
  /** ‹ or Escape: close search (the Shell restores the map). */
  onClose: () => void;
}

export const TopBar = forwardRef<TextInput, TopBarProps>(function TopBar(
  { palette, placeholder, sync, onSync, open, query, onQuery, onOpen, onClose }, ref) {
  const panel = searchPanelWidth(useWindowDimensions().width);
  return (
    <View testID="top-bar" pointerEvents="box-none"
          style={{ position: "absolute", top: TOP_BAR.top, left: TOP_BAR.side,
                   ...(panel === null ? { right: TOP_BAR.side } : { width: panel }) }}>
      <View testID="search-field"
            style={{ backgroundColor: palette.card, borderWidth: 1.5, borderRadius: palette.r.box,
                     borderColor: open ? palette.accent : palette.ink, ...palette.lift }}>
        <View style={{ height: TOP_BAR.field, flexDirection: "row", alignItems: "center" }}>
          {/* THE SAME 40 PX SLOT before and after focus: the magnifier, or (phone) the ‹ that closes search — no jump.
              A wide screen keeps the magnifier: its close is the results panel's ✕, by the results (`SearchSheet`). */}
          {open && panel === null ? (
            <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.close} hitSlop={4}
                       style={{ width: TOP_BAR.slot, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
              <Icon name="back" colour={palette.ink} size={22} />
            </Pressable>
          ) : (
            <View importantForAccessibility="no-hide-descendants" aria-hidden
                  style={{ width: TOP_BAR.slot, alignItems: "center", justifyContent: "center" }}>
              <Icon name="search" colour={palette.sub} size={20} />
            </View>
          )}
          <TextInput ref={ref}
                     value={query}
                     onChangeText={onQuery}
                     onFocus={() => { if (!open) onOpen(); }}
                     // a phone's Escape (a keyboard on the web) is the field's; a wide screen hears it window-wide
                     // (`SearchSheet`), so it is not heard twice here
                     onKeyPress={(e) => { if (panel === null && e.nativeEvent.key === "Escape") onClose(); }}
                     placeholder={placeholder}
                     placeholderTextColor={palette.faint}
                     accessibilityLabel={SEARCH_BAR.label}
                     autoCorrect={false} autoCapitalize="none" returnKeyType="search"
                     // the field's own edge (accent while open) is its focus mark: no second browser ring inside it
                     style={{ ...TYPE.body, fontSize: 16, lineHeight: 21, color: palette.ink, flex: 1, minWidth: 0,
                              outlineWidth: 0, outlineStyle: "solid",
                              paddingVertical: 0, paddingRight: query.length > 0 || (!open && sync) ? 0 : 12,
                              height: TOP_BAR.field }} />
          {query.length > 0 ? (
            <Pressable onPress={() => onQuery("")} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.clear}
                       style={{ width: TOP_BAR.slot, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
              <Icon name="close" colour={palette.sub} size={18} />
            </Pressable>
          ) : !open && sync ? (
            <SyncButton palette={palette} sync={sync} onPress={onSync} />
          ) : null}
        </View>
      </View>
    </View>
  );
});

/**
 * The sync icon: a hairline divides it from the field (it is a control, not part of what is typed); the dot sits on
 * the glyph's upper right, ringed in the card so it reads on the stroke. Needs-a-refresh is the one state that asks
 * for a look, so it alone is dotted — in `attention`, never a water-status colour (a red dot beside closed water reads
 * as "closed"); checking dims the glyph to `faint` while it turns.
 */
function SyncButton({ palette, sync, onPress }: { palette: Palette; sync: SyncVM; onPress?: () => void }) {
  const reduced = useReducedMotion();
  const turn = useRef(new Animated.Value(0)).current;
  const spinning = sync.kind === "checking" && !reduced;
  useEffect(() => {
    if (!spinning) { turn.stopAnimation(); turn.setValue(0); return; }
    const loop = Animated.loop(Animated.timing(turn, { toValue: 1, duration: 900, easing: Easing.linear,
                                                       useNativeDriver: NATIVE_DRIVER }));
    loop.start();
    return () => loop.stop();
  }, [spinning, turn]);
  const rotate = turn.interpolate({ inputRange: [0, 1], outputRange: ["0deg", "360deg"] });
  const D = TOP_BAR.dot;
  return (
    <Pressable testID="sync-button" onPress={onPress} disabled={sync.kind === "checking"}
               accessibilityRole="button" accessibilityLabel={sync.label} accessibilityHint={sync.hint}
               aria-busy={sync.kind === "checking"} hitSlop={2}
               style={({ pressed }) => ({ width: TOP_BAR.slot + 4, alignSelf: "stretch", alignItems: "center",
                                          justifyContent: "center", borderLeftWidth: 1, borderLeftColor: palette.line,
                                          backgroundColor: pressed ? palette.wash : "transparent" })}>
      <View importantForAccessibility="no-hide-descendants" aria-hidden>
        <Animated.View style={{ transform: [{ rotate }] }}>
          <Icon name="sync" colour={sync.kind === "checking" ? palette.faint : palette.sub} size={18} />
        </Animated.View>
        {sync.kind === "stale" && (
          <View testID="sync-dot"
                style={{ position: "absolute", top: -3, right: -4, width: D + 3, height: D + 3, borderRadius: (D + 3) / 2,
                         backgroundColor: palette.card, alignItems: "center", justifyContent: "center" }}>
            <View style={{ width: D, height: D, borderRadius: D / 2, backgroundColor: palette.attention }} />
          </View>
        )}
      </View>
    </Pressable>
  );
}
