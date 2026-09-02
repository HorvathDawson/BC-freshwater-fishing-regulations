/**
 * The shared declaration for `./FishSpinner`.
 *
 * `FishSpinner.native.tsx` drives two `Animated.Value`s on the platform driver;
 * `FishSpinner.web.tsx` is two CSS keyframes and no JavaScript. Bundlers pick by platform
 * extension; TypeScript does not, so both are checked against this one signature.
 */
import type { Palette } from "./theme";

export declare function FishSpinner(props: {
  palette: Palette;
  size?: number;
  label?: string;
  /** Defaults to `palette.live` — the water colour, not the selection colour. */
  colour?: string;
}): JSX.Element;
