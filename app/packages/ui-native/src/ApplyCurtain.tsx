/**
 * (T746, owner 2026-11-14: "when changes apply to app we want to make it so that it has a loading icon with text like
 * splash so it doesnt looking like it is frozen") THE CURTAIN OVER THE APP WHILE A DOWNLOADED DATA RELEASE IS PUT IN USE.
 * The one state for every in-session apply — the safe moment on a still map, "Apply now", the return from the
 * background — on the phone and the web alike (a start that applies one says the same line on its boot screen).
 *
 *   - It takes every tap from the moment the apply starts: nothing underneath is pressed while the set is swapped.
 *   - It is DRAWN only once the wait has lasted `SPINNER_TIMING.showAfterMs` (300 ms) and then stays at least `holdMs`
 *     (400 ms): an apply quicker than that never flashes; a longer one never looks frozen (the fish turns on the native
 *     driver / in CSS, so a JS-thread commit does not stop it).
 *   - Drawn, it is the boot screen's look: the card colour, the loading fish at the centre, one line beneath.
 *   - A failed apply says so here, at once, until the reader closes it (the failure itself is reported where it was
 *     caught, `@app/ui` `useReleaseBoot`); the app keeps the data it had.
 */
import { View } from "react-native";
import { APPLY_WORDS } from "@app/core";
import { useBackLayer } from "@app/ui";
import { Text } from "./Text";
import { ErrorState } from "./ErrorState";
import { LoadingState, useDelayedShow } from "./LoadingState";
import { TYPE } from "./type";
import type { Palette } from "./theme";

/** The fish's size: the boot screen's (`BOOT_FISH`, apps/mobile App.tsx), so the two read as the same screen. */
const FISH = 110;
const NOTHING = () => {};

export function ApplyCurtain({ palette, applying, failed, onClose }: {
  palette: Palette;
  /** an apply is in flight */
  applying: boolean;
  /** the apply that just failed (null: none, or closed) */
  failed: Error | null;
  onClose: () => void;
}) {
  const wait = useDelayedShow(applying);
  // a back layer while it is up: the system back closes a failed apply's message, and during the apply it is taken
  // here (nothing underneath moves while the set is swapped)
  useBackLayer(wait.pending || !!failed, failed ? onClose : NOTHING);
  if (!wait.pending && !failed) return null;
  const drawn = wait.shown || !!failed;
  return (
    <View testID="apply-curtain" aria-busy={applying} accessibilityViewIsModal
          // every tap stops here, drawn or not yet drawn
          onStartShouldSetResponder={() => true}
          style={{ position: "absolute", top: 0, left: 0, right: 0, bottom: 0, zIndex: 1000,
                   // (Android) above every elevated sheet, so the taps land here
                   elevation: 1000, backgroundColor: drawn ? palette.card : "transparent" }}>
      {drawn && (
        <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 12, paddingHorizontal: 24 }}>
          {failed ? (
            <ErrorState palette={palette} size="body" align="center" testID="apply-failed"
                        message={APPLY_WORDS.failed} onRetry={onClose} retryLabel={APPLY_WORDS.ok}
                        retryTestID="apply-failed-ok" />
          ) : (
            <>
              <LoadingState palette={palette} size={FISH} label={APPLY_WORDS.line} shown />
              <Text testID="apply-line" style={{ ...TYPE.body, fontSize: 15, color: palette.ink, textAlign: "center" }}>
                {APPLY_WORDS.line}
              </Text>
            </>
          )}
        </View>
      )}
    </View>
  );
}
