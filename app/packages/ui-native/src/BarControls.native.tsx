/** (T909) The phone's bar: the bell and the refresh button at the field's right end. See `BarControls.d.ts`. */
import { PhoneBarControls } from "./barParts";
import type { BarControlsProps } from "./BarControls";

/** the phone's bar always draws them */
export const barHasControls = (): boolean => true;
export function BarControls(props: BarControlsProps) {
  return <PhoneBarControls {...props} />;
}
