/**
 * The hooks that fetch a panel and hand it to the arithmetic, and the view models the conditions surfaces draw.
 *
 * THE ARITHMETIC ITSELF IS IN `@app/data` — see `panel.ts` there. It has three callers and
 * one of them is not a screen (`captureSpot` freezes an answer into a saved spot), so it
 * cannot live behind a hook.
 *
 * One module per subject: `feed` (the live index as these hooks read it), `standings` (the cached panel reads behind
 * the map and the sheet), `horizon` (the chips and the one horizon state), `donor` (the donor panel and gauge switch),
 * `conditions` (the conditions panel, finished) and `change` (the expected-change block, T52).
 */

/** Re-exported so a screen has one import for the whole subject. */
export { answerFrom, HORIZONS } from "@app/data";
export type { DonorRow, Horizon, PanelAnswer, Quantity } from "@app/data";

export * from "./feed";
export * from "./standings";
export * from "./horizon";
export * from "./donor";
export * from "./conditions";
export * from "./change";
