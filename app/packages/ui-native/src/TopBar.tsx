/**
 * (T48) THE TOP BAR over the browse maps: the search field ALONE, always open (owner 2026-10-21: "the search bar can be
 * always open"), with the SYNC STATUS line at the top of it (owner 2026-10-22: "we will still want sync info in top of
 * search bar") — today the regulations' last day and when the flows were last checked (`syncLine`, the Data sources
 * page's own facts); the slot "Update / sync" fills later.
 *
 * Tapping the field focuses it IN PLACE — the same frame before and after (no jump): the magnifier's 40 px slot becomes
 * the ‹ that closes search, the edge turns accent, and the Shell raises the results sheet over the SAME map behind
 * (`SearchSheet`; no second map). Closing — ‹, Escape, the system back — reverses it: search closes first (route
 * `search`), the keyboard drops, and the Shell puts the camera back exactly.
 *
 * The placeholder is ONE line per opening of the app (`placeholderFor`), never cycled while on screen; the field is
 * always called "Search waters and towns" aloud, whatever the line says.
 */
import { forwardRef } from "react";
import { Pressable, Text, TextInput, View, useWindowDimensions } from "react-native";
import { SEARCH_BAR } from "@app/core";
import { Icon } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The bar's measures: inset from the screen's edges, the sync line, the field, the slab under it. */
export const TOP_BAR = { top: 10, side: 14, sync: 20, field: 44, slab: 3, wide: 720, panel: 440 } as const;

/** (T48) On a wide screen (tablet, desktop) the bar and its sheet are a left panel, not a full-width strip. */
export function searchPanelWidth(screen: number): number | null {
  return screen >= TOP_BAR.wide ? TOP_BAR.panel : null;
}

/** Where the bar's bottom edge is (the slab included): what the map's chips, rail and controls start under. */
export function topBarBottom(sync: boolean): number {
  return TOP_BAR.top + (sync ? TOP_BAR.sync : 0) + TOP_BAR.field + 3 + TOP_BAR.slab;
}

export interface TopBarProps {
  palette: Palette;
  /** This opening's line (`placeholderFor`). */
  placeholder: string;
  /** The sync status line, or null: nothing to say (no line drawn). */
  sync: string | null;
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
  { palette, placeholder, sync, open, query, onQuery, onOpen, onClose }, ref) {
  const panel = searchPanelWidth(useWindowDimensions().width);
  return (
    <View testID="top-bar" pointerEvents="box-none"
          style={{ position: "absolute", top: TOP_BAR.top, left: TOP_BAR.side,
                   ...(panel === null ? { right: TOP_BAR.side } : { width: panel }) }}>
      <View testID="search-field"
            style={{ backgroundColor: palette.card, borderWidth: 1.5, borderRadius: palette.r.box,
                     borderColor: open ? palette.accent : palette.ink, ...palette.lift }}>
        {sync !== null && (
          <View style={{ height: TOP_BAR.sync, justifyContent: "center", paddingHorizontal: 12,
                         borderBottomWidth: 1, borderBottomColor: palette.line }}>
            <Text testID="sync-status" numberOfLines={1} accessibilityRole="text"
                  style={{ ...TYPE.micro, fontSize: 10.5, lineHeight: 13, color: palette.sub }}>
              {sync}
            </Text>
          </View>
        )}
        <View style={{ height: TOP_BAR.field, flexDirection: "row", alignItems: "center" }}>
          {/* THE SAME 40 PX SLOT before and after focus: the magnifier, or the ‹ that closes search — no jump */}
          {open ? (
            <Pressable onPress={onClose} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.close} hitSlop={4}
                       style={{ width: 40, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
              <Text style={{ ...TYPE.bodyStrong, fontSize: 22, lineHeight: 24, color: palette.ink }}>‹</Text>
            </Pressable>
          ) : (
            <View importantForAccessibility="no-hide-descendants" aria-hidden
                  style={{ width: 40, alignItems: "center", justifyContent: "center" }}>
              <Icon name="search" colour={palette.sub} size={18} />
            </View>
          )}
          <TextInput ref={ref}
                     value={query}
                     onChangeText={onQuery}
                     onFocus={() => { if (!open) onOpen(); }}
                     onKeyPress={(e) => { if (e.nativeEvent.key === "Escape") onClose(); }}
                     placeholder={placeholder}
                     placeholderTextColor={palette.faint}
                     accessibilityLabel={SEARCH_BAR.label}
                     autoCorrect={false} autoCapitalize="none" returnKeyType="search"
                     // the field's own edge (accent while open) is its focus mark: no second browser ring inside it
                     style={{ ...TYPE.body, fontSize: 16, lineHeight: 21, color: palette.ink, flex: 1, minWidth: 0,
                              outlineWidth: 0, outlineStyle: "solid",
                              paddingVertical: 0, paddingRight: query.length > 0 ? 0 : 12, height: TOP_BAR.field }} />
          {query.length > 0 && (
            <Pressable onPress={() => onQuery("")} accessibilityRole="button" accessibilityLabel={SEARCH_BAR.clear}
                       style={{ width: 40, alignSelf: "stretch", alignItems: "center", justifyContent: "center" }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.faint }}>✕</Text>
            </Pressable>
          )}
        </View>
      </View>
    </View>
  );
});
