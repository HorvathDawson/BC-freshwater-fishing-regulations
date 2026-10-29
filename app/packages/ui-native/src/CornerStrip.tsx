/**
 * THE MAP CORNER'S ONE CONTROL SHAPE (refinement pass; T213): a joined segmented strip — a single card with hairline
 * dividers — for one choice of day. Conditions draws its forecast horizons with it, the Map tab its day (Today ·
 * Tomorrow · a calendar segment), in the same top-left corner, so the two maps choose a day the same way. The chosen
 * segment is FILLED, not merely tinted: it repaints the whole map, so which one is on has to read from the corner of an
 * eye. Each segment is a 44 × 44 press (`hitProps`).
 */
import { Pressable, Text, View } from "react-native";
import { hitProps } from "./regs/hit";
import { Icon, type IconName } from "./icons";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** a segment: at least 46 wide and 33 tall by its style (8 + 17 + 8), grown to 44 × 44 to the touch */
const SEGMENT_HIT = hitProps({ w: 46, h: 33 });

export interface CornerSegment {
  readonly key: string;
  readonly chip: string;
  /** said aloud */
  readonly label: string;
  readonly on: boolean;
  /** (T130) a second, greyed line: a day this segment does not answer */
  readonly note?: string;
  readonly icon?: IconName;
  /** a MODE, not a day: set off from the days by a heavier divider (Conditions' Change) */
  readonly mode?: boolean;
  /** false: shown, refused (`aria-disabled`) */
  readonly enabled?: boolean;
  readonly onPress: () => void;
  /** (radio strips) the choice is loading: `aria-busy` */
  readonly busy?: boolean;
}

export function CornerStrip({ palette, segments, testID, label, radio = false }: {
  palette: Palette; segments: readonly CornerSegment[]; testID: string; label?: string;
  /** one choice of several (Standard / Salmon): a radiogroup of radios, `aria-checked` not `aria-pressed` */
  radio?: boolean;
}) {
  return (
    <View testID={testID} accessibilityLabel={label} accessibilityRole={radio ? "radiogroup" : undefined}
          style={{ flexDirection: "row", backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line2,
                   borderRadius: palette.r.pill, overflow: "hidden", ...palette.lift }}>
      {segments.map((h, i) => {
        const enabled = h.enabled !== false;
        const ink = h.on ? palette.onAccent : h.note || !enabled ? palette.sub : palette.ink;
        return (
          <Pressable key={h.key} testID={`${testID}-${h.key}`} onPress={h.onPress} disabled={!enabled}
                     accessibilityRole={radio ? "radio" : "button"} accessibilityLabel={h.label} aria-disabled={!enabled}
                     aria-busy={h.busy} {...(radio ? { "aria-checked": h.on } : { "aria-pressed": h.on })} {...SEGMENT_HIT}>
            <View style={{ minWidth: 46, paddingVertical: h.note ? 4 : 8, paddingHorizontal: 10, flexDirection: "row",
                           gap: 5, alignItems: "center", justifyContent: "center",
                           backgroundColor: h.on ? palette.accent : palette.card, opacity: enabled ? 1 : 0.45,
                           borderLeftWidth: i ? (h.mode ? 2 : 1) : 0, borderLeftColor: h.mode ? palette.ink : palette.line2 }}>
              {h.icon && <Icon name={h.icon} colour={ink} size={h.chip ? 15 : 17} />}
              <View style={{ alignItems: "center" }}>
                {h.chip ? (
                  <Text style={{ ...TYPE.micro, fontSize: 13, lineHeight: h.note ? 14 : 17, fontWeight: "600", color: ink }}>
                    {h.chip}
                  </Text>
                ) : null}
                {h.note && (
                  <Text style={{ ...TYPE.micro, fontSize: 10, lineHeight: 11, letterSpacing: 0, color: palette.sub }}>
                    {h.note}
                  </Text>
                )}
              </View>
            </View>
          </Pressable>
        );
      })}
    </View>
  );
}
