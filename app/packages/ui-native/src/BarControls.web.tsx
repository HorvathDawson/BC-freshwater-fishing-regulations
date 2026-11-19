/**
 * (T909) The browser's bar: no bell, no refresh button — except in a store-shots build (T319, `storeShots.ts`), which
 * draws the phone's screens for the store's pictures and so the phone's bar. See `BarControls.d.ts`.
 */
import { PhoneBarControls } from "./barParts";
import { phoneScreensOnWeb } from "./storeShots";
import type { BarControlsProps } from "./BarControls";

/** the website's bar draws neither; a store-shots build draws the phone's */
export const barHasControls = (): boolean => phoneScreensOnWeb();
export function BarControls(props: BarControlsProps) {
  return phoneScreensOnWeb() ? <PhoneBarControls {...props} /> : null;
}
