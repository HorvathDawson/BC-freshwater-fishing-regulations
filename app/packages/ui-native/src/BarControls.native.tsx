/** (T909) The phone's bar: the bell and the refresh button at the field's right end. See `BarControls.d.ts`. */
import type { ReactNode } from "react";
import { Pressable } from "./Pressable";
import { useCoachTarget } from "./CoachMarks";
import { hitProps } from "./regs/hit";
import type { Palette } from "./theme";
import type { SyncVM } from "@app/core";
import { BellGlyph, SyncButton, TOP_BAR } from "./barParts";

export const BAR_HAS_CONTROLS = true;

/**
 * (T230) THE BELL: the provincial in-season changes not placed on the map yet, the updates that failed and the data
 * update's state — a bell with their count, in `attention` (never a water-status colour); its tap opens the notice.
 * (owner 2026-10-29) ALWAYS DRAWN beside the sync button: with nothing pending it has no badge and opens "No notifications".
 */
function NoticeButton({ palette, count, label, onPress }: { palette: Palette; count: number; label: string; onPress: () => void }) {
  const target = useCoachTarget("bell");   // (T257) the first-run tour points at it
  return (
    <Pressable {...hitProps({ w: TOP_BAR.slot + 4, h: TOP_BAR.field })} ref={target} testID="inseason-bell" onPress={onPress} accessibilityRole="button" accessibilityLabel={label}
               style={({ pressed }) => ({ width: TOP_BAR.slot + 4, alignSelf: "stretch", alignItems: "center",
                                          justifyContent: "center", borderLeftWidth: 1, borderLeftColor: palette.line,
                                          backgroundColor: pressed ? palette.wash : "transparent" })}>
      <BellGlyph palette={palette} count={count} />
    </Pressable>
  );
}

export function BarControls({ palette, notices, sync, onSync }: {
  palette: Palette;
  notices: { count: number; label: string; onPress: () => void } | null;
  sync: SyncVM | null;
  onSync?: () => void;
}): ReactNode {
  return (
    <>
      {notices && <NoticeButton palette={palette} count={notices.count} label={notices.label} onPress={notices.onPress} />}
      {sync && <SyncButton palette={palette} sync={sync} onPress={onSync} />}
    </>
  );
}
