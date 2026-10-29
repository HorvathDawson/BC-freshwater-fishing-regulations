/**
 * (T248, owner 2026-10-28, proposal P14) THE FIRST-RUN WALKTHROUGH: three or four short cards — the map's modes, the date
 * bar, tapping a water, offline and the refresh button / bell — shown once per device, skippable, and re-openable from
 * More → Help. It SEQUENCES behind the first-run notice (T236): `blocked` holds it while that notice is up, so the two
 * never stack. Finishing or skipping stores `WALKTHROUGH_VERSION` (`@app/data` `openWalkthroughStore`: localStorage on the
 * web, a file on a phone); the words are core's (`WALKTHROUGH_WORDS`). The pictures are the app's own pieces (the corner
 * strip, the legend swatches, the sync glyph and the bell), never a screenshot, so a card cannot show a control that is gone.
 */
import { useCallback, useEffect, useState } from "react";
import { Text, View } from "react-native";
import { DAY_PICKER, RUN_WORDS, SALMON_WORDS, syncVM, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, WATER_STATUS, WATER_STATUSES,
         walkthroughSeen, type WalkthroughArt } from "@app/core";
import { Button } from "./Button";
import { LegendCount } from "./Chrome";
import { CornerStrip } from "./CornerStrip";
import { Sheet } from "./Sheet";
import { BellGlyph, SyncGlyph } from "./TopBar";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** Where the seen version is kept (`@app/data` `openWalkthroughStore`). */
export interface WalkthroughStore { read(): string | null; write(text: string): void }

export interface Walkthrough { readonly open: boolean; finish(): void; reopen(): void }

/**
 * Open until seen or skipped (stored), but never while `blocked` (the first-run notice is up); `reopen` shows it again on
 * request. No store (a test, a screen alone): never opens by itself.
 */
export function useWalkthrough(store: WalkthroughStore | null, blocked: boolean): Walkthrough {
  const [pending, setPending] = useState(() => {
    if (!store) return false;
    try { return !walkthroughSeen(store.read()); } catch { return true; }
  });
  const [manual, setManual] = useState(false);
  const finish = useCallback(() => {
    try { store?.write(WALKTHROUGH_VERSION); } catch { /* a refused write: shown again next start, no harm */ }
    setPending(false); setManual(false);
  }, [store]);
  const reopen = useCallback(() => setManual(true), []);
  return { open: (pending && !blocked) || manual, finish, reopen };
}

const NOP = () => {};

function Art({ art, palette }: { art: WalkthroughArt; palette: Palette }) {
  switch (art) {
    case "modes":
      return (
        <CornerStrip palette={palette} testID="walk-modes" radio label={WALKTHROUGH_WORDS.cards[0]!.title}
                     segments={[
                       { key: "standard", chip: SALMON_WORDS.toggle.standard, label: SALMON_WORDS.toggle.standard, on: true, onPress: NOP },
                       { key: "salmon", chip: SALMON_WORDS.toggle.salmon, label: SALMON_WORDS.toggle.salmon, on: false, onPress: NOP },
                       { key: "runs", chip: RUN_WORDS.mode, label: RUN_WORDS.mode, on: false, onPress: NOP },
                     ]} />
      );
    case "days":
      return (
        <CornerStrip palette={palette} testID="walk-days" label={DAY_PICKER.strip}
                     segments={[
                       { key: "today", chip: DAY_PICKER.today, label: DAY_PICKER.today, on: true, onPress: NOP },
                       { key: "tomorrow", chip: DAY_PICKER.tomorrow, label: DAY_PICKER.tomorrow, on: false, onPress: NOP },
                       { key: "pick", chip: "", icon: "calendar", label: DAY_PICKER.pick, on: false, onPress: NOP },
                     ]} />
      );
    case "tap":
      return (
        <View testID="walk-tap" style={{ gap: 6 }}>
          {WATER_STATUSES.filter((s) => s !== "no_access").map((s) => (
            <LegendCount key={s} palette={palette} colour={palette.waterStatus[s]} line label={WATER_STATUS[s].label} />
          ))}
        </View>
      );
    case "offline": {
      const fresh = syncVM({ feed: false, feedFetchedAt: null, checkedAt: null, checking: false, newRelease: null, now: 0 });
      return (
        <View testID="walk-offline" style={{ flexDirection: "row", gap: 28, alignItems: "center", paddingHorizontal: 22,
                                             paddingVertical: 12, borderWidth: 1, borderColor: palette.line2, backgroundColor: palette.card }}>
          <SyncGlyph palette={palette} sync={fresh} />
          <BellGlyph palette={palette} count={2} />
        </View>
      );
    }
  }
}

export function WalkthroughSheet({ state, palette }: { state: Walkthrough; palette: Palette }) {
  const w = WALKTHROUGH_WORDS;
  const [i, setI] = useState(0);
  useEffect(() => { if (state.open) setI(0); }, [state.open]);
  const card = w.cards[i]!;
  const last = i === w.cards.length - 1;
  return (
    <Sheet open={state.open} onClose={state.finish} title={w.label} sub={w.step(i + 1, w.cards.length)} palette={palette} dialog>
      <View testID="walkthrough" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 14 }}>
        <View testID={`walk-card-${card.key}`} style={{ gap: 12 }}>
          <View style={{ minHeight: 64, alignItems: "flex-start", justifyContent: "center" }}>
            <Art art={card.key} palette={palette} />
          </View>
          <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, fontSize: 18, color: palette.ink }}>{card.title}</Text>
          <Text style={{ ...TYPE.body, color: palette.ink }}>{card.body}</Text>
        </View>
        <View accessibilityLabel={w.step(i + 1, w.cards.length)} style={{ flexDirection: "row", gap: 6 }}>
          {w.cards.map((c, k) => (
            <View key={c.key} testID={k === i ? "walk-dot-on" : "walk-dot"}
                  style={{ width: 8, height: 8, borderRadius: 4, backgroundColor: k === i ? palette.accent : palette.line2 }} />
          ))}
        </View>
        <View style={{ flexDirection: "row", gap: 10 }}>
          {!last && <Button palette={palette} kind="ghost" grow={false} label={w.skip} onPress={state.finish} />}
          {i > 0 && <Button palette={palette} kind="ghost" grow={false} label={w.back} onPress={() => setI(i - 1)} />}
          <Button palette={palette} label={last ? w.done : w.next} onPress={last ? state.finish : () => setI(i + 1)} />
        </View>
      </View>
    </Sheet>
  );
}
