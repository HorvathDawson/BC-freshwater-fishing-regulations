/**
 * (T260) THE CHIP — one pill-shaped choice, extracted unchanged from the three the screens had typed out. Three looks of
 * one control, named by the job:
 *
 *   choice  a radio in a group on a sheet or page (Sheet `Choice`, "Who is fishing?"): wash ground, ink frame; accent when on.
 *   day     a quick pick beside a field (SpotCapture's Today / Yesterday …): clear ground, hairline frame; accent when on.
 *   chart   a chart's own switch (ChartControls: flow / level, 72 hours / year): card ground on the map's slab shadow, small type.
 *
 * `tools/one-button.test.ts` refuses a chip typed out again (an accent-filled pill Pressable with the same padding).
 */
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { TYPE } from "./type";
import { hitProps } from "./regs/hit";
import type { Palette } from "./theme";

export type ChipLook = "choice" | "day" | "chart";

export function Chip({ palette, look, label, on, onPress, role = "radio", testID, a11yLabel, hit }: {
  palette: Palette; look: ChipLook; label: string; on: boolean; onPress: () => void;
  /** "radio" in a group (aria-checked / aria-selected as the look had it), "button" for a quick pick */
  role?: "radio" | "button";
  testID?: string; a11yLabel?: string;
  /** the press's known box (hitProps); the default is the look's own */
  hit?: { w: number; h: number };
}) {
  const box = hit ?? (look === "chart" ? { w: 80, h: 28 } : { w: 60, h: 36 });
  const state = look === "choice" ? { "aria-checked": on } : look === "chart" ? { "aria-selected": on }
    : role === "radio" ? { "aria-checked": on } : {};
  const frame = look === "chart"
    ? { paddingVertical: 7, paddingHorizontal: 13, borderRadius: palette.r.pill, borderWidth: 1,
        borderColor: on ? palette.accent : palette.line2, backgroundColor: on ? palette.tint : palette.card, ...palette.lift }
    : look === "choice"
    ? { paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill, backgroundColor: on ? palette.accent : palette.wash,
        borderWidth: 1, borderColor: on ? palette.accent : palette.ink }
    : { paddingVertical: 9, paddingHorizontal: 14, borderRadius: palette.r.pill, borderWidth: 1,
        borderColor: on ? palette.accent : palette.line2, backgroundColor: on ? palette.accent : "transparent" };
  const words = look === "chart"
    ? { ...TYPE.micro, fontSize: 11.5, fontWeight: on ? "700" : "600", color: on ? palette.accent : palette.ink } as const
    : { ...TYPE.micro, fontSize: 12.5, color: on ? palette.onAccent : look === "choice" ? palette.sub : palette.ink } as const;
  return (
    <Pressable {...hitProps(box)} testID={testID} onPress={onPress} accessibilityRole={role} accessibilityLabel={a11yLabel ?? label}
               {...state} style={frame}>
      <Text style={words}>{label}</Text>
    </Pressable>
  );
}
