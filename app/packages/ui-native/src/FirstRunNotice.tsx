/**
 * (T236, owner 2026-10-28) THE FIRST-RUN NOTICE: the app is not the official regulations and not legal advice — said
 * once per device, before anything else opens by itself (the Shell holds the in-season notice back while it is up).
 * "I understand" stores the notice's version (core `FIRST_RUN_VERSION`); closing it any other way shows it again on the
 * next start. Words are core's (`LEGAL_WORDS.firstRun`); About carries the same disclaimer for good.
 */
import { useCallback, useState } from "react";
import { Text, View } from "react-native";
import { FIRST_RUN_VERSION, firstRunAccepted, LEGAL_WORDS } from "@app/core";
import { Button } from "./Button";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";

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

export function FirstRunNotice({ state, palette }: { state: FirstRun; palette: Palette }) {
  const w = LEGAL_WORDS.firstRun;
  return (
    <Sheet open={state.open} onClose={state.close} title={w.title} sub={null} palette={palette} scroll dialog>
      <View testID="first-run-notice" style={{ paddingHorizontal: 20, paddingTop: 8, paddingBottom: 12, gap: 12 }}>
        {/* the authority sentence boxed in ink, as About boxes it, so it cannot be read as a footnote */}
        <View style={{ borderWidth: 1.5, borderColor: palette.ink, padding: 12, gap: 8, backgroundColor: palette.tint }}>
          {w.lines.map((l, i) => (
            <Text key={i} style={{ ...(i === 1 ? TYPE.bodyStrong : TYPE.body), color: palette.ink }}>{l}</Text>
          ))}
        </View>
        <Button palette={palette} label={w.accept} onPress={state.accept} />
      </View>
    </Sheet>
  );
}
