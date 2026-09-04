/**
 * The one button.
 *
 * There were three: `SpotsScreen.Button`, `SpotCapture.Action` and `SpotScreen.Button`.
 * They were written to look the same and had drifted — 12px radius against 13, 13/18
 * padding against 14/20, and a ghost button whose label was `palette.ink` on one screen and
 * `palette.accent` on the others. Nothing was wrong enough to report and every screen was
 * slightly different from its neighbour, which is the exact shape of the drift rule 23
 * exists to stop, one layer below where that rule usually gets applied.
 *
 * Three variants, because three is what the screens actually needed:
 *
 *   solid   the one thing this screen is for   (Save, Add a spot, Use this point)
 *   ghost   the way out                        (Cancel, Discard)
 *   danger  destructive and deliberate         (Delete)
 *
 * `onPress` omitted means DISABLED, not inert: the button dims and reports its state to a
 * screen reader, rather than silently ignoring a tap.
 */
import { Pressable, Text, type ViewStyle } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export type ButtonKind = "solid" | "ghost" | "danger";

export function Button({ palette, label, onPress, kind = "solid", grow = true, style }: {
  palette: Palette;
  label: string;
  /** Omitted -> disabled. */
  onPress?: () => void;
  kind?: ButtonKind;
  /** Fill the row it sits in. Off for a button beside a wide one, e.g. Delete. */
  grow?: boolean;
  style?: ViewStyle;
}) {
  const off = onPress === undefined;
  const ghost = kind === "ghost";
  const bg = ghost ? "transparent" : kind === "danger" ? palette.closed : palette.accent;
  return (
    <Pressable onPress={onPress} disabled={off} accessibilityRole="button"
               accessibilityLabel={label} aria-disabled={off}
               style={{ flexGrow: grow ? 1 : 0, alignItems: "center", borderRadius: 13,
                        paddingVertical: 14, paddingHorizontal: 20, opacity: off ? 0.4 : 1,
                        backgroundColor: bg,
                        borderWidth: ghost ? 1 : 0, borderColor: palette.line2, ...style }}>
      <Text style={{ ...TYPE.bodyStrong, fontSize: 15, fontWeight: "700",
                     color: ghost ? palette.accent : palette.onAccent }}>{label}</Text>
    </Pressable>
  );
}
