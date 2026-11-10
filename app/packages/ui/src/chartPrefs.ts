/**
 * (T584, T580 F6) THE READER'S CHART CHOICES, kept for the session beside Show: the span (72 h / Year) and the model pick.
 *
 * They were `useState` inside the Conditions panel, so they survived a part step on the same water (the panel stays
 * mounted) and reset on a new water, a deep link or a search (it remounts): the same part led with the year span
 * after one way in and 72 h after another. They are the reader's, not the gauge's or the water's, so they live here
 * and every way in reads the same. The GAUGE pick is the other way round: it belongs to a section (`useConditionsView`
 * drops it when the section changes). One store, no platform file; not persisted across a cold start (neither platform
 * restores the chart view; the README's kept-state table).
 */
import { useSyncExternalStore } from "react";
import type { ChartSpan } from "@app/core";
import { NO_MODEL_PICK, type ModelPick } from "./panel/change";

export interface ChartPrefs { readonly span: ChartSpan; readonly pick: ModelPick }

const START: ChartPrefs = { span: "72h", pick: NO_MODEL_PICK };
let prefs: ChartPrefs = START;
const listeners = new Set<() => void>();
const set = (next: ChartPrefs) => { prefs = next; listeners.forEach((l) => l()); };

/** Test seam: the store is module state. */
export function resetChartPrefs(): void { set(START); }

export function useChartPrefs(): { span: ChartSpan; setSpan: (s: ChartSpan) => void;
                                   pick: ModelPick; setPick: (f: (p: ModelPick) => ModelPick) => void } {
  const now = useSyncExternalStore((l) => { listeners.add(l); return () => { listeners.delete(l); }; },
                                   () => prefs, () => START);
  return { span: now.span, setSpan: (span) => set({ ...prefs, span }), pick: now.pick,
           setPick: (f) => set({ ...prefs, pick: f(prefs.pick) }) };
}
