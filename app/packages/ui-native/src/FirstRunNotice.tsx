/**
 * (T236, owner 2026-10-28) THE FIRST-RUN NOTICE: the app is not the official regulations and not legal advice — said
 * once per device, before anything else opens by itself (the Shell holds the in-season notice back while it is up).
 * "I understand" stores the notice's version (core `FIRST_RUN_VERSION`); closing it any other way shows it again on the
 * next start. Words are core's (`LEGAL_WORDS.firstRun`); About carries the same disclaimer for good.
 */
import { useCallback, useState } from "react";
import { View } from "react-native";
import { Text } from "./Text";
import { FIRST_RUN_VERSION, firstRunAccepted, LEGAL_WORDS } from "@app/core";
import { Button } from "./Button";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

/** Where the accepted version is kept (`@app/data` `openFirstRunStore`: localStorage on the web, a file on a phone). */
export interface FirstRunStore { read(): string | null; write(text: string): void }

export interface FirstRun { readonly open: boolean; accept(): void; close(): void }

/** Open until accepted (stored) or closed (this run only). No store (a test, a screen alone): never opens. */
export function useFirstRun(store: FirstRunStore | null): FirstRun {
  const [open, setOpen] = useState(() => {
    if (!store) return false;
    try { return !firstRunAccepted(store.read()); } catch { return true; }
  });
  const accept = useCallback(() => {
    try { store?.write(FIRST_RUN_VERSION); } catch { /* a refused write: shown again next start, no harm */ }
    setOpen(false);
  }, [store]);
  const close = useCallback(() => setOpen(false), []);
  return { open, accept, close };
}

/**
 * (T257, owner 2026-10-29) A CALM SHEET, in the app's own sheet: the title, one quiet line, three short numbered points
 * (what the app is, who decides, what changes), "I understand", and the way to the page that names every source.
 */
export function FirstRunNotice({ state, palette, onSources }: { state: FirstRun; palette: Palette; onSources?: () => void }) {
  const w = LEGAL_WORDS.firstRun;
  return (
    <Sheet open={state.open} onClose={state.close} title={w.title} sub={null} palette={palette} scroll dialog>
      <View testID="first-run-notice" style={{ paddingTop: SPACE.s4, paddingBottom: SPACE.s2, gap: SPACE.s4 }}>
        <Text style={{ ...TYPE.body, color: palette.sub }}>{w.lead}</Text>
        {w.points.map((p, i) => (
          <View key={p.title} testID={`first-run-point-${i + 1}`} style={{ flexDirection: "row", gap: SPACE.s3 + 2 }}>
            <View importantForAccessibility="no-hide-descendants" aria-hidden
                  style={{ width: 28, height: 28, borderWidth: 1.5, borderColor: palette.ink, alignItems: "center",
                           justifyContent: "center" }}>
              <Text style={{ ...TYPE.figure, color: palette.ink }}>{i + 1}</Text>
            </View>
            <View style={{ flex: 1, gap: 2 }}>
              <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{p.title}</Text>
              <Text style={{ ...TYPE.body, color: palette.sub }}>{p.body}</Text>
            </View>
          </View>
        ))}
        <View style={{ gap: SPACE.s1, marginTop: SPACE.s2 }}>
          <Button palette={palette} label={w.accept} onPress={state.accept} testID="first-run-accept" />
          {onSources && (
            <Button palette={palette} kind="link" label={w.sources} onPress={onSources} testID="first-run-sources"
                    style={{ alignSelf: "center" }} />
          )}
        </View>
      </View>
    </Sheet>
  );
}
