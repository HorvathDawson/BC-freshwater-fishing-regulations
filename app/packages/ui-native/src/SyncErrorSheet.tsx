/**
 * (T231) WHAT COULD NOT BE UPDATED: the sheet the red-badged sync icon opens — each failed source (gauges, DFO salmon,
 * in-season changes, the data release), what went wrong in the reader's words, when it last succeeded, and Retry (a
 * check now, as the icon's own tap). The words are `@app/core` `syncErrorVM`'s, whole; this draws them. A dialog on a
 * wide screen, a bottom sheet on a phone. The app keeps the data it has: said under the title.
 */
import { syncErrorVM, type SyncErrorVM, type UpdateSourceInput } from "@app/core";
import { Text, View } from "react-native";
import { Button } from "./Button";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function SyncErrorSheet({ open, failed, palette, checking, onRetry, onClose }: {
  open: boolean; failed: readonly UpdateSourceInput[]; palette: Palette;
  /** a check is under way: Retry says so and waits */
  checking: boolean;
  onRetry: () => void; onClose: () => void;
}) {
  const vm = syncErrorVM(failed, Date.now());
  return (
    <Sheet open={open} onClose={onClose} title={vm.title} sub={vm.sub} palette={palette} dialog>
      <View testID="sync-error-sheet" style={{ paddingHorizontal: 20, paddingTop: 12 }}>
        <SyncFailures vm={vm} palette={palette} checking={checking} onRetry={onRetry} />
      </View>
    </Sheet>
  );
}

/** The failed sources and Retry: this sheet's body, and the bell's sync section (`InSeasonNotice`). */
export function SyncFailures({ vm, palette, checking, onRetry }: {
  vm: SyncErrorVM; palette: Palette; checking: boolean; onRetry: () => void;
}) {
  return (
    <View testID="sync-failures" style={{ gap: 12 }}>
      {vm.rows.map((r) => (
        <View key={r.k} testID={`sync-error-${r.k}`} accessible accessibilityLabel={r.label}
              style={{ borderLeftWidth: 4, borderLeftColor: palette.closed, paddingLeft: 10, gap: 2 }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.name}</Text>
          <Text style={{ ...TYPE.small, color: palette.ink }}>{r.problem}</Text>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{r.last}</Text>
        </View>
      ))}
      <View style={{ flexDirection: "row", paddingTop: 4 }}>
        <Button palette={palette} label={checking ? vm.retrying : vm.retry} onPress={checking ? undefined : onRetry} />
      </View>
    </View>
  );
}
