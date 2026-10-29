/**
 * (T248, owner 2026-10-28, proposal P14; T257 owner 2026-10-29) THE FIRST-RUN WALKTHROUGH: a coach-mark tour over the Map
 * tab — the date bar, Standard / Salmon / Run timing, a water to tap, the Conditions tab, the refresh button and the bell —
 * each a spotlight on the real control with a short callout anchored to it (`CoachMarks`). Shown once per device,
 * skippable, and re-openable from More → Help. It SEQUENCES behind the first-run notice (T236): `blocked` holds it while
 * that notice is up (and while the Map tab is not in front), so the two never stack. Finishing or skipping stores
 * `WALKTHROUGH_VERSION` (`@app/data` `openWalkthroughStore`: localStorage on the web, a file on a phone); the words are
 * core's (`WALKTHROUGH_WORDS`). It points at the controls themselves, so it cannot show one that is gone.
 */
import { useCallback, useState } from "react";
import { WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, walkthroughSeen } from "@app/core";
import { CoachTour, type CoachRegistry, type CoachStep } from "./CoachMarks";
import type { Palette } from "./theme";

/** Where the seen version is kept (`@app/data` `openWalkthroughStore`). */
export interface WalkthroughStore { read(): string | null; write(text: string): void }

export interface Walkthrough { readonly open: boolean; finish(): void; reopen(): void }

/**
 * Open until seen or skipped (stored), but never while `blocked` (the first-run notice is up, or the Map tab is not in
 * front: the tour points at its controls); `reopen` shows it again on request, once nothing blocks it. No store (a test, a screen alone): never opens by itself.
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
  return { open: !blocked && (pending || manual), finish, reopen };
}

const STEPS: readonly CoachStep[] = WALKTHROUGH_WORDS.cards.map((c) => ({
  key: c.key, targets: c.targets, title: c.title, body: c.body, ...(c.key === "map" ? { shape: "spot" as const } : {}),
}));

export function WalkthroughSheet({ state, palette, registry }: { state: Walkthrough; palette: Palette; registry: CoachRegistry }) {
  return <CoachTour open={state.open} steps={STEPS} palette={palette} registry={registry} onFinish={state.finish}
                    words={WALKTHROUGH_WORDS} />;
}
