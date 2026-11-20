/**
 * (T911, owner 2026-11-19: "on the join the test buttons it should have a popup with either a straight to email button or
 * a copy email option") THE JOIN-THE-TEST CARD: a Join the test press opens a small card over the page (the T859 card:
 * the palette's scrim, ✕, Escape and a press outside close it, focus goes in and back where the web Modal puts it).
 * One line on how to join, "Email me" (the mailto of `joinTestAsk`) and "Copy email" (the address, then "Copied"). A
 * clipboard the browser refuses is said so and the address is shown to select: never a silent failure (W46).
 * The words are `WEB_SPOTS_WORDS.join.popup`; this is drawn only where the web's notice is (the phone has no such notice).
 */
import { useState } from "react";
import { Modal, View } from "react-native";
import { environmentError, reportFailure, SHEET_WORDS, WEB_SPOTS_WORDS as W, type JoinAsk, type JoinStore } from "@app/core";
import { useBackLayer } from "@app/ui";
import { Text } from "./Text";
import { Button } from "./Button";
import { Pressable } from "./Pressable";
import { Icon } from "./icons";
import { linkTo } from "./openLink";
import { hitProps } from "./regs/hit";
import { TYPE } from "./type";
import { GUTTER, SPACE, type Palette } from "./theme";

const SCRIM_HIT = hitProps({ w: 320, h: 200 });

/**
 * The browser's clipboard; false when it is absent or refuses (not a secure page, no permission) — the card then SAYS so
 * and shows the address, and the failure is REPORTED: a missing clipboard or a refusal is a defect, except a permission
 * the reader refused (`NotAllowedError`), which is the environment's (core failure.ts: shown, never reported).
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  const clip = (globalThis as { navigator?: { clipboard?: { writeText(t: string): Promise<void> } } }).navigator?.clipboard;
  if (!clip) { reportFailure(new Error("join card: this browser has no clipboard (navigator.clipboard)")); return false; }
  try { await clip.writeText(text); return true; } catch (e) {
    // (a DOMException: not every engine's is an `Error` instance, so it is told by its name)
    const refusedByReader = !!e && typeof e === "object" && (e as { name?: unknown }).name === "NotAllowedError";
    reportFailure(refusedByReader ? environmentError(e as Error) : e);
    return false;
  }
}

export function JoinTestCard({ store, ask, palette, onClose, copy = copyToClipboard }: {
  store: JoinStore; ask: JoinAsk; palette: Palette; onClose: () => void;
  /** (tests) the copy; default the browser's clipboard */
  copy?: (text: string) => Promise<boolean>;
}) {
  const requestClose = useBackLayer(true, onClose);
  const [said, setSaid] = useState<"copied" | "refused" | null>(null);
  const P = W.join.popup;
  const address = ask.address;
  return (
    <Modal transparent animationType="fade" onRequestClose={requestClose} aria-label={ask.button}>
      <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: GUTTER, zIndex: 0 }}>
        <View testID="join-test-card" accessibilityViewIsModal
              style={{ width: "100%", maxWidth: 420, backgroundColor: palette.card, borderWidth: 1.5, borderColor: palette.ink, zIndex: 1 }}>
          <View style={{ flexDirection: "row", alignItems: "flex-start", gap: SPACE.s3, paddingHorizontal: GUTTER,
                         paddingTop: SPACE.s3, paddingBottom: SPACE.s3, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
            <Text accessibilityRole="header" style={{ ...TYPE.name, color: palette.ink, flex: 1, paddingTop: 8 }}>{ask.button}</Text>
            <Pressable {...hitProps({ w: 40, h: 40 })} testID="join-test-close" onPress={requestClose} accessibilityRole="button"
                       accessibilityLabel={P.close}
                       style={{ width: 40, height: 40, alignItems: "center", justifyContent: "center", marginRight: -SPACE.s2 }}>
              <Icon name="close" colour={palette.ink} size={22} />
            </Pressable>
          </View>
          <View style={{ padding: GUTTER, gap: SPACE.s3 }}>
            <Text style={{ ...TYPE.body, color: palette.ink }}>{P[store]}</Text>
            <Button palette={palette} label={P.email} testID="join-test-email" onPress={ask.mailto !== null ? linkTo(ask.mailto) : undefined} />
            {address !== null && (
              <Button palette={palette} kind="ghost" label={P.copy} testID="join-test-copy"
                      onPress={() => { void copy(address).then((ok) => setSaid(ok ? "copied" : "refused")); }} />
            )}
            <View accessibilityLiveRegion="polite" aria-live="polite">
              {said === "copied" && <Text testID="join-test-said" style={{ ...TYPE.small, color: palette.sub, textAlign: "center" }}>{P.copied}</Text>}
              {said === "refused" && (
                <View testID="join-test-refused" style={{ gap: SPACE.s1 }}>
                  <Text style={{ ...TYPE.small, color: palette.closed }}>{P.refused}</Text>
                  <Text selectable testID="join-test-address" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{address}</Text>
                </View>
              )}
            </View>
          </View>
        </View>
        <Pressable {...SCRIM_HIT} testID="join-test-scrim" onPress={requestClose} tabIndex={-1} accessibilityRole="button"
                   accessibilityLabel={SHEET_WORDS.close}
                   style={{ position: "absolute", top: 0, bottom: 0, left: 0, right: 0, zIndex: 0, backgroundColor: palette.scrim }} />
      </View>
    </Modal>
  );
}
