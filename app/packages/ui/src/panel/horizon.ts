/**
 * THE HORIZON: how far ahead the Conditions map is painted and the panel answers — the chips every control draws, what
 * a chip's tap does, and the one state the Shell holds.
 */
import { useCallback, useMemo, useState } from "react";
import { HORIZONS, type Horizon, type Parameter } from "@app/data";
import { CHANGE_NEEDS_FLOW, HORIZON_LABELS, horizonChipLabel } from "@app/core";
import type { FlowParam } from "../mapModes";

export interface HorizonChipVM {
  day: Horizon; chip: string; label: string; on: boolean;
  /** Drawn greyed with this line under the chip (core's words): the day is not answered for this quantity. */
  note: string | null;
  /** The quantity a tap moves to first (`pickHorizon`); null: the chip answers as it is. */
  needs: Parameter | null;
}

/** Does this day need a quantity other than the one shown? Change is flow only (T52, owner: "never mix flow and level"). */
const needsFor = (d: Horizon, quantity?: FlowParam): Parameter | null =>
  d === "change" && quantity !== undefined && quantity !== "discharge" ? "discharge" : null;

/**
 * THE HORIZON CHIPS, for every control that offers them — the map's corner (Conditions tab)
 * and the panel's header. One function over core's HORIZONS and words, so the two controls
 * cannot list different days or name one differently (P16's worry about a second copy).
 *
 * THE CHANGE CHIP IS ALWAYS THERE (T130). It is flow only — the publisher writes no change in level — and under Level
 * or Temperature it was dropped, which with the Show choice remembered read as the chip having vanished. Now it stays,
 * greyed with core's "Flow only", and a tap moves to Flow (`pickHorizon`). The day lit is the one answered
 * (`flowHorizon`): Change chosen under Level answers as Now, so Now is lit.
 */
export function horizonChips(on: Horizon, quantity?: FlowParam): HorizonChipVM[] {
  const lit = quantity === undefined ? on : flowHorizon(on, quantity);
  return HORIZONS.map((d) => {
    const needs = needsFor(d, quantity);
    return needs
      ? { day: d, chip: HORIZON_LABELS[d].chip, label: CHANGE_NEEDS_FLOW.accessible, on: false,
          note: CHANGE_NEEDS_FLOW.note, needs }
      : { day: d, chip: HORIZON_LABELS[d].chip, label: horizonChipLabel(d, d === lit), on: d === lit,
          note: null, needs: null };
  });
}

/**
 * WHAT A CHIP'S TAP DOES: the day, and the quantity to move to first when the day needs one (Change under Level or
 * Temperature → Flow). Every control that draws the chips picks through this, so none can set Change and leave the map
 * on Level answering Now.
 */
export function pickHorizon(d: Horizon, quantity: FlowParam): { horizon: Horizon; show: Parameter | null } {
  return { horizon: d, show: needsFor(d, quantity) };
}

/**
 * The one pick for the Shell's horizon controls: `pickHorizon`, then the quantity (when it moves) and the day.
 */
export function useHorizonPick(pick: (d: Horizon) => void, quantity: FlowParam,
                               setQuantity: (q: FlowParam) => void): (d: Horizon) => void {
  return useCallback((d: Horizon) => {
    const p = pickHorizon(d, quantity);
    if (p.show) setQuantity(p.show);
    pick(p.horizon);
  }, [pick, quantity, setQuantity]);
}

/**
 * THE HORIZON THE MAP AND PANEL ACT ON for this quantity: the chosen one, except Change outside Flow, which answers as
 * Now. The choice itself is kept, so switching back to Flow returns to Change — no effect, no reset.
 */
export function flowHorizon(h: Horizon, quantity: FlowParam): Horizon {
  return h === "change" && quantity !== "discharge" ? 0 : h;
}

/** The one horizon state the Shell holds and both controls set — see `useHorizon`. */
export interface HorizonState {
  horizon: Horizon;
  chips: readonly HorizonChipVM[];
  pick: (d: Horizon) => void;
}

/**
 * ONE HORIZON, TWO CONTROLS (owner 2026-10-20). The map's corner control and the panel's
 * header both draw `chips` and call `pick`, so choosing +10 on the map is what the panel
 * opens on, and stepping back to Now in the panel is what the map then paints. 0 is where
 * it opens: a forecast is what you ask for, never what you are shown without asking.
 */
export function useHorizon(initial: Horizon = 0): HorizonState {
  const [horizon, pick] = useState<Horizon>(initial);
  const chips = useMemo(() => horizonChips(horizon), [horizon]);
  return { horizon, chips, pick };
}
