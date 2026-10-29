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
import { Platform } from "react-native";
import { TAB_TOUR_WORDS, TAB_TOURS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, walkthroughCards, walkthroughSeen, tabTourSeen, withSeen,
         type TabTourKey } from "@app/core";
import { CoachTour, type CoachRegistry, type CoachStep } from "./CoachMarks";
import type { Palette } from "./theme";

/** Where the seen version is kept (`@app/data` `openWalkthroughStore`). */
export interface WalkthroughStore { read(): string | null; write(text: string): void }

export interface Walkthrough { readonly open: boolean; /** (T276) not yet seen: the tab tutorials wait for it */ readonly pending: boolean; finish(): void; reopen(): void }

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
    try { store?.write(withSeen(store.read(), WALKTHROUGH_VERSION)); } catch { /* a refused write: shown again next start, no harm */ }
    setPending(false); setManual(false);
  }, [store]);
  const reopen = useCallback(() => setManual(true), []);
  return { open: !blocked && (pending || manual), pending, finish, reopen };
}

/** (owner 2026-10-29) one tour, two step lists: the web's says what its refresh button does there (no offline copy) */
export const stepsFor = (platform: "web" | "app"): readonly CoachStep[] => walkthroughCards(platform).map((c) => ({
  key: c.key, targets: c.targets, title: c.title, body: c.body, ...(c.key === "map" ? { shape: "spot" as const } : {}),
}));
const STEPS = stepsFor(Platform.OS === "web" ? "web" : "app");

export function WalkthroughSheet({ state, palette, registry }: { state: Walkthrough; palette: Palette; registry: CoachRegistry }) {
  return <CoachTour open={state.open} steps={STEPS} palette={palette} registry={registry} onFinish={state.finish}
                    words={WALKTHROUGH_WORDS} />;
}

// ── (T276) THE TAB TUTORIALS ────────────────────────────────────────────────────────────────────────────────────────

export interface TabTour { readonly open: boolean; finish(): void; reopen(): void }

/**
 * A tab's tutorial: opens by itself the first time its tab is in front (and nothing else is holding the screen: the
 * first-run notice, the welcome tour), is remembered in the SAME store as the welcome tour (one line per tour), and
 * `reopen` shows it again from More → Help. No store (a test): never opens by itself.
 */
export function useTabTour(store: WalkthroughStore | null, tour: TabTourKey, active: boolean, blocked: boolean): TabTour {
  const [pending, setPending] = useState(() => {
    if (!store) return false;
    try { return !tabTourSeen(store.read(), tour); } catch { return true; }
  });
  const [manual, setManual] = useState(false);
  const finish = useCallback(() => {
    try { store?.write(withSeen(store.read(), TAB_TOURS[tour])); } catch { /* shown again next start, no harm */ }
    setPending(false); setManual(false);
  }, [store, tour]);
  const reopen = useCallback(() => setManual(true), []);
  return { open: active && !blocked && (pending || manual), finish, reopen };
}

const tabSteps = (k: TabTourKey): readonly CoachStep[] => TAB_TOUR_WORDS[k].map((c) => ({
  key: c.key, targets: c.targets, title: c.title, body: c.body, ...(c.targets[0] === "map" ? { shape: "spot" as const } : {}),
}));
const TAB_STEPS = { runs: tabSteps("runs"), conditions: tabSteps("conditions") };

export function TabTourSheet({ tour, which, palette, registry }: { tour: TabTour; which: TabTourKey; palette: Palette; registry: CoachRegistry }) {
  return <CoachTour open={tour.open} steps={TAB_STEPS[which]} palette={palette} registry={registry} onFinish={tour.finish}
                    words={{ ...WALKTHROUGH_WORDS, label: TAB_TOUR_WORDS.label[which] }} />;
}
