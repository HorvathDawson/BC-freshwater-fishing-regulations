/**
 * A SOURCE'S FRESHNESS, drawn (owner 2026-10-22): the More tab's Data block (T167: the one list of sources and their
 * freshness) and the Regulations page's lapsed note draw it from one rule (`sourceFreshness`, core/menu.ts).
 *
 *   fresh    a filled dot in `live` (the app's live-data teal)
 *   stale    a filled dot in `attention` — the one state that asks the reader to look; never a water-status hue, since
 *            the badge's twin (the sync dot) sits over a map drawn in those
 *   unknown  an empty ring in `faint`: nothing to say, said quietly
 *   live     an empty ring in `live`: fetched when needed, so it has no age
 *
 * Filled against ring is the shape difference, so the states hold apart without their hue (the CVD palette, greyscale).
 */
import { Text, View } from "react-native";
import type { SourceFreshness, FreshnessVM } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";

export const FRESH_DOT = 8;

export function freshColour(p: Palette, k: SourceFreshness): string {
  return k === "fresh" || k === "live" ? p.live : k === "stale" ? p.attention : p.faint;
}

/** The dot alone (a line's lead): filled for an age (fresh, stale), a ring for none (unknown, live). */
export function FreshDot({ palette, kind, size = FRESH_DOT }: { palette: Palette; kind: SourceFreshness; size?: number }) {
  const c = freshColour(palette, kind);
  const filled = kind === "fresh" || kind === "stale";
  return (
    <View testID={`fresh-${kind}`} importantForAccessibility="no-hide-descendants" aria-hidden
          style={{ width: size, height: size, borderRadius: size / 2, borderWidth: filled ? 0 : 1.5, borderColor: c,
                   backgroundColor: filled ? c : "transparent" }} />
  );
}

/** The dot and its word ("Likely out of date"): the badge where there is room to say it. */
export function FreshBadge({ palette, badge, testID }: { palette: Palette; badge: FreshnessVM; testID?: string }) {
  const stale = badge.kind === "stale";
  return (
    <View testID={testID} style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
      <FreshDot palette={palette} kind={badge.kind} />
      <Text style={{ ...(stale ? TYPE.metaBold : TYPE.metaSemi), color: stale ? palette.ink : palette.sub }}>
        {badge.label}
      </Text>
    </View>
  );
}
