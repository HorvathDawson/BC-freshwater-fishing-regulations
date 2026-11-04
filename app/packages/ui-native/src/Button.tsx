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
 * The kinds are what the screens actually draw (T257: the small and link kinds were extracted, unchanged, from the copies
 * that had been typed out at their call sites; `tools/one-button.test.ts` refuses a new copy):
 *
 *   solid   the one thing this screen is for   (Save, Add a spot, Use this point)
 *   ghost   the way out                        (Cancel, Discard)
 *   danger  destructive and deliberate         (Delete)
 *   small   a small action beside a line       (Refresh, Ask for the flow): card, a `line2` hairline, accent micro words
 *   link    words alone, in the accent          (Try again): `size` "body" (a page's) or "meta" (a banner's)
 *
 * `onPress` omitted means DISABLED, not inert: the button dims and reports its state to a
 * screen reader, rather than silently ignoring a tap. `busy` (T257) is disabled AND waiting: the label stays (it is still
 * what the button does) and the FishSpinner shows beside it, through the one delayed show (`LoadingState`).
 */
import { View, type ViewStyle } from "react-native";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { Icon, type IconName } from "./icons";
import { LoadingState } from "./LoadingState";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export type ButtonKind = "solid" | "ghost" | "danger" | "small" | "link";

export function Button({ palette, label, onPress, kind = "solid", grow, style, busy = false, icon, size = "body", testID, a11yLabel }: {
  palette: Palette;
  label: string;
  /** Omitted -> disabled. */
  onPress?: () => void;
  kind?: ButtonKind;
  /** Fill the row it sits in (solid / ghost / danger: on by default). Off for a button beside a wide one, e.g. Delete. */
  grow?: boolean;
  style?: ViewStyle;
  /** (T257) the action is running: disabled, and the spinner beside the label */
  busy?: boolean;
  /** (T257) an icon before the label */
  icon?: IconName;
  /** (T257, link only) the words' size: a page's body, or a banner's meta */
  size?: "body" | "meta";
  testID?: string;
  /** (T257) said aloud when it says more than the words shown ("Refresh this spot" for "Refresh") */
  a11yLabel?: string;
}) {
  const off = onPress === undefined || busy;
  if (kind === "small" || kind === "link") {
    const small = kind === "small";
    const words = small ? { ...TYPE.micro, fontSize: 12.5 } : size === "meta" ? TYPE.metaBold : TYPE.bodyStrong;
    return (
      <Pressable testID={testID} onPress={onPress} disabled={off} accessibilityRole="button" accessibilityLabel={a11yLabel ?? label}
                 aria-disabled={off} aria-busy={busy || undefined}
                 // (T243) small: 8 + 8 padding round a 14 px line; link: its 44 px row
                 {...hitProps(small ? { w: 60, h: 30 } : { w: 60, h: 44 })}
                 style={small
                   ? { borderRadius: palette.r.pill, paddingVertical: 8, paddingHorizontal: 14, backgroundColor: palette.card,
                       borderWidth: 1, borderColor: palette.line2, opacity: off ? 0.5 : 1, ...(grow ? { flexGrow: 1 } : {}), ...style }
                   : { minHeight: 44, justifyContent: "center", opacity: onPress === undefined ? 0.4 : 1, ...style }}>
        <Inner palette={palette} label={label} icon={icon} busy={busy} colour={palette.accent} words={words} spin={small ? 14 : 18} />
      </Pressable>
    );
  }
  const ghost = kind === "ghost";
  const bg = ghost ? "transparent" : kind === "danger" ? palette.closed : palette.accent;
  const ink = ghost ? palette.accent : palette.onAccent;
  return (
    <Pressable testID={testID} onPress={onPress} disabled={off} accessibilityRole="button"
               accessibilityLabel={a11yLabel ?? label} aria-disabled={off} aria-busy={busy || undefined}
               // (T243) 14 + 14 padding round a 21 px line
               {...hitProps({ w: 44, h: 49 })}
               style={{ flexGrow: grow ?? true ? 1 : 0, alignItems: "center", borderRadius: palette.r.box,
                        paddingVertical: 14, paddingHorizontal: 20, opacity: off && !busy ? 0.4 : 1,
                        backgroundColor: bg,
                        // A ghost button IS its border — in `line2` it was a suggestion.
                        borderWidth: ghost ? 1.5 : 0, borderColor: palette.ink,
                        ...(ghost ? {} : palette.lift), ...style }}>
      <Inner palette={palette} label={label} icon={icon} busy={busy} colour={ink}
             words={{ ...TYPE.bodyStrong, fontSize: 15, fontWeight: "700" }} spin={20} />
    </Pressable>
  );
}

function Inner({ palette, label, icon, busy, colour, words, spin }: {
  palette: Palette; label: string; icon?: IconName; busy: boolean; colour: string; words: object; spin: number;
}) {
  const text = <Text style={{ ...words, color: colour }}>{label}</Text>;
  if (!icon && !busy) return text;
  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
      {busy ? <LoadingState palette={palette} size={spin} colour={colour} /> : icon ? <Icon name={icon} colour={colour} size={16} /> : null}
      {text}
    </View>
  );
}
