/**
 * (T888, owner 2026-11-18: "When a user opens the app and a new app version is available it should give a popup with the
 * swimming fish and the new update list. That they need to dismiss (then it goes to Bell) for both apple and android .")
 * THE POPUP ON OPENING: the swimming coho on top, "Version 0.8.2 is out", that version's "What's new" bullets, Update and
 * Not now, in the app's own sheet (a centred dialog on a wide screen, as the first-run notice). Everything it says is
 * core's (`updatePopupVM`, from the bell's own view model); when it comes up and when it stops is `@app/ui`
 * `useAppUpdate`'s. Closing it any way (Not now, Done, the scrim, back) is Not now: the bell keeps the update.
 */
import { View } from "react-native";
import type { UpdatePopupVM } from "@app/core";
import { Text } from "./Text";
import { Button } from "./Button";
import { CohoLoader } from "./CohoLoader";
import { COHO_GROUND_PAD } from "./coho";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

/** The coho's width here: smaller than the start screen's (192) so the bullets and both buttons fit a phone's sheet. */
const POPUP_COHO = 160;

export function AppUpdatePopup({ popup, palette, onUpdate, onDismiss }: {
  /** null: closed */
  popup: UpdatePopupVM | null;
  palette: Palette;
  onUpdate: () => void;
  onDismiss: () => void;
}) {
  const p = popup;
  return (
    <Sheet open={p !== null} onClose={onDismiss} title={p?.title ?? ""} sub={null} palette={palette} scroll dialog>
      {p && (
        <View testID="app-update-popup" style={{ paddingTop: SPACE.s4, paddingBottom: SPACE.s2, gap: SPACE.s4 }}>
          {/* the swimming coho: a picture here, not a wait, so it is hidden from a screen reader */}
          <View importantForAccessibility="no-hide-descendants" aria-hidden
                style={{ alignSelf: "center", marginVertical: COHO_GROUND_PAD }}>
            <CohoLoader palette={palette} width={POPUP_COHO} />
          </View>
          {!!p.sub && <Text style={{ ...TYPE.body, color: palette.sub }}>{p.sub}</Text>}
          {p.whatsNew && (
            <View testID="app-update-popup-whatsnew" style={{ gap: 6, borderLeftWidth: 2, borderLeftColor: palette.line2,
                                                              paddingLeft: 10 }}>
              <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{p.whatsNew.title}</Text>
              {p.whatsNew.notes
                ? p.whatsNew.notes.map((n, i) => (
                  <Text key={i} testID="app-update-popup-note" style={{ ...TYPE.body, color: palette.ink }}>{`• ${n}`}</Text>
                ))
                : <Text testID="app-update-popup-notes-missing" style={{ ...TYPE.body, color: palette.sub }}>{p.whatsNew.missing}</Text>}
            </View>
          )}
          <View style={{ gap: SPACE.s2, marginTop: SPACE.s1 }}>
            {p.button && (
              <Button palette={palette} label={p.button.label} onPress={onUpdate} testID="app-update-popup-update" />
            )}
            {!!p.said && (
              <Text testID="app-update-popup-said" accessibilityLiveRegion="polite"
                    style={{ ...TYPE.small, color: p.failed ? palette.closed : palette.sub, textAlign: "center" }}>{p.said}</Text>
            )}
            <Button palette={palette} kind="ghost" label={p.notNow} onPress={onDismiss} testID="app-update-popup-later" />
            <Text style={{ ...TYPE.small, color: palette.faint, textAlign: "center" }}>{p.later}</Text>
          </View>
        </View>
      )}
    </Sheet>
  );
}
