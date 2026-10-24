/** T170: the trip day's own flow from the day's record, and the action that asks for it (`useDayFlow`). */
import { Pressable, Text } from "react-native";
import type { DayFlowView } from "@app/ui";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Block } from "./parts";

export function DayFlowBlock({ df, onAsk, palette }: {
  df: DayFlowView; onAsk: (() => void) | null | undefined; palette: Palette;
}) {
  return (
    <Block palette={palette} title={df.heading}>
      {df.line ? (
        <Text testID="day-flow-line" style={{ ...TYPE.body, color: palette.ink }}>{df.line}</Text>
      ) : null}
      {df.status ? (
        <Text testID="day-flow-status" aria-live="polite"
              style={{ ...TYPE.small, color: palette.sub }}>{df.status}</Text>
      ) : null}
      {df.action && onAsk ? (
        <Pressable onPress={onAsk} disabled={df.action.busy}
                   accessibilityRole="button" accessibilityLabel={df.action.label}
                   aria-disabled={df.action.busy}
                   style={{ alignSelf: "flex-start", borderRadius: palette.r.pill, paddingVertical: 8,
                            paddingHorizontal: 14, backgroundColor: palette.card, borderWidth: 1,
                            borderColor: palette.line2, opacity: df.action.busy ? 0.5 : 1 }}>
          <Text style={{ ...TYPE.micro, fontSize: 12.5, color: palette.accent }}>{df.action.label}</Text>
        </Pressable>
      ) : null}
    </Block>
  );
}
