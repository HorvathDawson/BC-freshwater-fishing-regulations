/**
 * (T909) THE TOP BAR'S PARTS that the phone's bar controls (`BarControls.native.tsx`) and the Help page share with it: the
 * bar's measures, the bell's and the refresh button's pictures and the refresh button. They live apart from `TopBar.tsx`
 * so the platform file does not import the bar that imports it. `TopBar.tsx` re-exports them.
 */
import { LoadingState } from "./LoadingState";
import { useCoachTarget } from "./CoachMarks";
import { View } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import type { SyncVM } from "@app/core";
import { Icon } from "./icons";
import { SyncRing } from "./SyncRing";
import { TYPE } from "./type";
import { round, type Palette } from "./theme";
import { hitProps } from "./regs/hit";

/** The bar's measures: inset from the screen's edges, the field, the slab under it, the sync slot and its dot. */
export const TOP_BAR = { top: 10, side: 14, field: 44, slab: 3, wide: 720, panel: 440, slot: 40, dot: 8,
                         /** (T231) the error badge, and the progress ring's box round the glyph */
                         badge: 15, ring: 26 } as const;

/** The bell and its count, as the top bar draws them; exported so the Help page (T248) shows the same picture. */
export function BellGlyph({ palette, count }: { palette: Palette; count: number }) {
  return (
    <View importantForAccessibility="no-hide-descendants" aria-hidden>
      <Icon name="bell" colour={palette.sub} size={18} />
      {count > 0 && <View testID="inseason-count"
            style={{ position: "absolute", top: -7, right: -9, minWidth: 16, height: 16, paddingHorizontal: 3,
                     borderRadius: round(16), backgroundColor: palette.attention, borderWidth: 1.5, borderColor: palette.card,
                     alignItems: "center", justifyContent: "center" }}>
        <Text style={{ ...TYPE.metaBold, fontSize: 10, lineHeight: 12, color: palette.card }}>{count > 99 ? "99+" : count}</Text>
      </View>}
    </View>
  );
}

/**
 * The sync icon: a hairline divides it from the field (it is a control, not part of what is typed); the dot sits on
 * the glyph's upper right, ringed in the card so it reads on the stroke. Needs-a-refresh is the one state that asks
 * for a look, so it alone is dotted — in `attention`, never a water-status colour (a red dot beside closed water reads
 * as "closed"); checking dims the glyph to `faint` while it turns.
 *
 * (T231) DOWNLOADING: a ring of progress round the glyph (`SyncRing`, accent on a `line` track) and the dot. DONE: the
 * glyph gives way to a check in the accent for a moment after a check the reader asked for. ERROR (owner 2026-10-28:
 * "ERROR badge (red)"): a badge in `closed` — the app's error red, as the failed-boot screen and a danger button use
 * it — with a "!" in `onAccent`, where the dot sits; the tap then opens what failed (the Shell's `SyncErrorSheet`).
 */
export function SyncButton({ palette, sync, onPress, bare = false }: {
  palette: Palette; sync: SyncVM; onPress?: () => void;
  /** (T300) the More tab's: the same button and states, a plain square with no field edge, and not the tour's target */
  bare?: boolean;
}) {
  const target = useCoachTarget("sync");   // (T257) the first-run tour points at it
  return (
    <Pressable {...hitProps({ w: TOP_BAR.slot + 4, h: TOP_BAR.field })} ref={bare ? undefined : target} testID={bare ? "more-sync-button" : "sync-button"} onPress={onPress} disabled={sync.kind === "checking"}
               accessibilityRole="button" accessibilityLabel={sync.label} accessibilityHint={sync.hint}
               aria-busy={sync.kind === "checking" || sync.kind === "downloading"}
               style={({ pressed }) => ({ width: TOP_BAR.slot + 4, alignSelf: "stretch", alignItems: "center",
                                          justifyContent: "center", borderLeftWidth: bare ? 0 : 1, borderLeftColor: palette.line,
                                          ...(bare ? { height: TOP_BAR.field, borderRadius: palette.r.chip } : {}),
                                          backgroundColor: pressed ? palette.wash : "transparent" })}>
      <SyncGlyph palette={palette} sync={sync} />
    </Pressable>
  );
}

/**
 * THE SYNC ICON'S PICTURE, in each state. Exported so the Help page (T248) draws the very glyph the button wears — a
 * state added here is drawn there, and a help picture cannot drift from the icon.
 */
export function SyncGlyph({ palette, sync }: { palette: Palette; sync: SyncVM }) {
  const D = TOP_BAR.dot, B = TOP_BAR.badge, R = TOP_BAR.ring;
  const dotted = sync.kind === "stale" || sync.kind === "downloading";
  return (
    <View importantForAccessibility="no-hide-descendants" aria-hidden
          style={{ width: R, height: R, alignItems: "center", justifyContent: "center" }}>
      {sync.kind === "downloading" && (
        <View style={{ position: "absolute", top: 0, left: 0 }}>
          <SyncRing progress={sync.progress ?? 0} size={R} track={palette.line} colour={palette.accent} />
        </View>
      )}
      {sync.kind === "checking" && (
        // CHECKING (T257): the app's one spinner, the fish, circling the glyph — through the one delayed show, so a quick
        // check never flashes it (still, under reduced motion)
        <View testID="sync-checking" style={{ position: "absolute", top: 0, left: 0 }}>
          <LoadingState palette={palette} size={R} colour={palette.accent} />
        </View>
      )}
      {sync.kind === "done" ? (
        <View testID="sync-done"><Icon name="check" colour={palette.accent} size={18} /></View>
      ) : (
        <Icon name="sync" colour={sync.kind === "checking" ? palette.accent : palette.sub}
              size={sync.kind === "downloading" || sync.kind === "checking" ? 14 : 18} />
      )}
      {dotted && (
        <View testID="sync-dot"
              style={{ position: "absolute", top: -1, right: -2, width: D + 3, height: D + 3, borderRadius: round(D + 3),
                       backgroundColor: palette.card, alignItems: "center", justifyContent: "center" }}>
          <View style={{ width: D, height: D, borderRadius: round(D), backgroundColor: palette.attention }} />
        </View>
      )}
      {sync.kind === "error" && (
        <View testID="sync-error"
              style={{ position: "absolute", top: -4, right: -6, width: B, height: B, borderRadius: round(B),
                       backgroundColor: palette.closed, borderWidth: 1.5, borderColor: palette.card,
                       alignItems: "center", justifyContent: "center" }}>
          <Text style={{ ...TYPE.metaBold, fontSize: 10, lineHeight: 12, color: palette.onAccent }}>!</Text>
        </View>
      )}
    </View>
  );
}
