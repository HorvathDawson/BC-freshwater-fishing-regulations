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
 * FOCUSED BUT CLOSED (T69, wide screens on the web): closing search by Escape or the panel's ✕ hands the focus back
 * to this field (the Shell), never to the page body. The field then holds the focus with search closed — its accent
 * edge says so — and typing, Enter or ↓ there opens search again, as does focusing it afresh. Enter never blurs the
 * field on a wide screen (it would drop the focus on the page body); a phone's Enter still drops the keyboard.
 *
 * (T858) OPEN, ON THE WEB, THE FIELD DRIVES THE RESULTS: ↓ / ↑ move a highlight through them and back to the field, and
 * Enter presses the highlighted one (`onListKey`, `listKeys.web.ts`); the focus stays here, and the field is a combobox
 * naming the highlight (`fieldA11y`). A phone's field is as before.
 *
 * The placeholder is ONE line per opening of the app (`placeholderFor`), never cycled while on screen; the field is
 * always called "Search waters and towns" aloud, whatever the line says.
 */
import { useCoachTarget } from "./CoachMarks";
import { forwardRef, useState, type ForwardRefExoticComponent, type RefAttributes } from "react";
import { TextInput, View, useWindowDimensions, type TextInputInstance } from "react-native";
import { Pressable } from "./Pressable";
import { SEARCH_BAR, type SyncVM } from "@app/core";
import { Icon } from "./icons";
import { BarControls, BAR_HAS_CONTROLS } from "./BarControls";
import { TOP_BAR } from "./barParts";

export { TOP_BAR, BellGlyph, SyncButton, SyncGlyph } from "./barParts";
import { CLOSE_KEEPS_FOCUS, closedFieldClick } from "./searchFocus";
import { TYPE, CAPPED_SCALE } from "./type";
import { type Palette } from "./theme";
import { hitProps } from "./regs/hit";
import type { FieldA11y, KeyMods } from "./search/keys";

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
  /** (T230) the bell: the count of what is pending (0: the bell without a badge — owner 2026-10-29, always drawn) and its
   *  tap; null: no bell (a screen alone, a test) */
  notices?: { count: number; label: string; onPress: () => void } | null;
  /** Search is open (route `search`): the ‹ in the magnifier's place, the accent edge. */
  open: boolean;
  query: string;
  onQuery: (q: string) => void;
  /** The field took focus, or was typed in / sent Enter or ↓ while focused but closed: open search. */
  onOpen: () => void;
  /** ‹ or Escape: close search (the Shell restores the map). */
  onClose: () => void;
  /** (T858) a key pressed in the field while search is open: true when the results' keyboard took it (web: ↓ ↑ Enter,
   *  `listKeys.web.ts`); absent or false, the field's own keys as before */
  onListKey?: (key: string, mods?: KeyMods) => boolean;
  /** (T858) the field's combobox attributes (web), naming the highlighted result; none on a phone */
  fieldA11y?: FieldA11y;
}

export const TopBar: ForwardRefExoticComponent<TopBarProps & RefAttributes<TextInputInstance>> = forwardRef<TextInputInstance, TopBarProps>(function TopBar(
  { palette, placeholder, sync, onSync, notices = null, open, query, onQuery, onOpen, onClose, onListKey, fieldA11y }, ref) {
  const panel = searchPanelWidth(useWindowDimensions().width);
  // the field holds the focus (open, or focused but closed): its edge turns accent either way — its one focus mark
  const [focused, setFocused] = useState(false);
  const coach = useCoachTarget("search");   // (T293) the walkthrough's first step points at it
  return (
    // drawn BEFORE the results sheet, so Tab reaches the bar before the panel under it (T69: the order the eye reads);
    // raised over the sheet and its dim so it still paints on top
    <View testID="top-bar" pointerEvents="box-none"
          style={{ position: "absolute", top: TOP_BAR.top, left: TOP_BAR.side, zIndex: 2,
                   ...(panel === null ? { right: TOP_BAR.side } : { width: panel }) }}>
      <View testID="search-field" ref={coach} collapsable={false}
            style={{ backgroundColor: palette.card, borderWidth: 1.5, borderRadius: palette.r.box,
                     borderColor: open || focused ? palette.accent : palette.ink, ...palette.lift }}>
        <View style={{ height: TOP_BAR.field, flexDirection: "row", alignItems: "center" }}>
          {/* THE SAME 40 PX SLOT before and after focus: the magnifier, or (phone) the ‹ that closes search — no jump.
              A wide screen keeps the magnifier: its close is the results panel's ✕, by the results (`SearchSheet`). */}
          {open && panel === null ? (
            <Pressable {...hitProps({ w: TOP_BAR.slot, h: TOP_BAR.field })} onPress={onClose} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.close}
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
                     // typing into a focused-but-closed field opens search with what was typed (T69)
                     onChangeText={(q) => { if (!open) onOpen(); onQuery(q); }}
                     onFocus={() => { setFocused(true); if (!open) onOpen(); }}
                     onBlur={() => setFocused(false)}
                     // Escape typed IN the field is the field's on every screen: react-native-web's input stops its
                     // keys at the field, so the wide panel's window-wide Escape (`SearchSheet`) never hears it.
                     // Focused but closed (T69), Enter or ↓ opens it again: the keys a list box opens on
                     // (T858) open, ↓ / ↑ move the results' highlight and Enter presses it (web): the key is the list's,
                     // so the caret does not move and Enter submits nothing
                     onKeyPress={(e) => {
                       const key = e.nativeEvent.key;
                       if (open && onListKey?.(key, e.nativeEvent as KeyMods)) e.preventDefault();
                       else if (open && key === "Escape") onClose();
                       else if (!open && (key === "Enter" || key === "ArrowDown")) onOpen();
                     }}
                     // a wide screen's Enter keeps the focus in the field on the web (a blur would drop it on the page
                     // body); the phone/tablet app's Enter drops it, and its keyboard, on every screen (searchFocus)
                     blurOnSubmit={!(CLOSE_KEEPS_FOCUS && panel !== null)}
                     // a click on the focused-but-closed field opens search again (web; the click is no fresh focus)
                     {...closedFieldClick(() => { if (!open) onOpen(); })}
                     placeholder={placeholder}
                     placeholderTextColor={palette.faint}
                     accessibilityLabel={SEARCH_BAR.label}
                     {...fieldA11y}
                     // (T243) the field is a fixed 44 px row: its words follow the OS text size to 2× (a 42 px line)
                     maxFontSizeMultiplier={CAPPED_SCALE}
                     autoCorrect={false} autoCapitalize="none" returnKeyType="search"
                     // the field's own edge (accent while open) is its focus mark: no second browser ring inside it
                     style={{ ...TYPE.body, fontSize: 16, lineHeight: 21, color: palette.ink, flex: 1, minWidth: 0,
                              outlineWidth: 0, outlineStyle: "solid",
                              paddingVertical: 0, paddingRight: query.length > 0 || (!open && BAR_HAS_CONTROLS && (sync || notices)) ? 0 : 12,
                              height: TOP_BAR.field }} />
          {query.length > 0 ? (
            <Pressable {...hitProps({ w: TOP_BAR.slot, h: TOP_BAR.field })} onPress={() => onQuery("")} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.clear}
                       style={{ width: TOP_BAR.slot, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
              <Icon name="close" colour={palette.sub} size={18} />
            </Pressable>
          ) : null}
          {/* (T909) the bell and the refresh button: the phone's (`BarControls.native.tsx`); the web's bar has neither */}
          {query.length === 0 && !open && <BarControls palette={palette} notices={notices} sync={sync} onSync={onSync} />}
        </View>
      </View>
    </View>
  );
});

