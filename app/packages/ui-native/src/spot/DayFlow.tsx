/** T170: the trip day's own flow from the day's record, and the action that asks for it (`useDayFlow`). */
import { Button } from "../Button";
import { ErrorState } from "../ErrorState";
import { Text } from "../Text";
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
      {df.error ? <ErrorState palette={palette} message={df.error} testID="day-flow-error" /> : null}
      {df.action && onAsk ? (
        <Button palette={palette} kind="small" label={df.action.label} onPress={onAsk} busy={df.action.busy}
                style={{ alignSelf: "flex-start" }} />
      ) : null}
    </Block>
  );
}
