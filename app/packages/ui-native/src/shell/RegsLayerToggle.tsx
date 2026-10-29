/**
 * (T211) STANDARD / SALMON — the Map tab's regulations switch (owner 2026-10-27). T213 moved it under the Layers button;
 * (owner 2026-10-28) it now sits BESIDE the day strip in the top-left corner, the same strip (`CornerStrip`: same height,
 * radius, tokens, filled active segment), a radiogroup of two radios — Standard, and Salmon with the fish. A press moves
 * only the map's modes: the salmon chains are already on every tile (`useSalmonColour`), so it is instant. Bait ban and
 * fly only are no choice here: they are marks on Standard (`@app/map` `gearPaint`). (T182) On the web the first press of
 * Salmon reads the salmon file: the choice is busy until the map can paint it (T257: the fish turns in its icon's place).
 */
import { SALMON_WORDS } from "@app/core";
import { CornerStrip } from "../CornerStrip";
import type { Palette } from "../theme";
import type { MapColouring } from "./useMapColouring";

export function RegsLayerToggle({ col, palette }: { col: Pick<MapColouring, "regsLayer" | "setRegsLayer"> &
                                                   Partial<Pick<MapColouring, "salmonLoading" | "salmonOffered">>;
                                                 palette: Palette }) {
  const W = SALMON_WORDS.toggle;
  // (T255) Run timing is no choice here any more: it has its own tab, Runs
  const keys = (["standard", "salmon"] as const).filter((k) => k === "standard" || col.salmonOffered !== false);
  return (
    <CornerStrip palette={palette} testID="regs-paint" label={W.label} radio coach="modes"
                 segments={keys.map((k) => {
                   const busy = k === "salmon" && col.salmonLoading === true;
                   const word = W[k];
                   return { key: k, chip: word, label: word, on: col.regsLayer === k, busy,
                            icon: k === "salmon" ? "fish" as const : "map" as const,
                            onPress: () => col.setRegsLayer(k) };
                 })} />
  );
}
