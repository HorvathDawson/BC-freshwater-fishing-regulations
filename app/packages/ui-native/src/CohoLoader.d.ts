/**
 * The shared declaration for `./CohoLoader` (T814): the owner's linocut coho, the loader of the start, download and
 * setup screen and the applying curtain. `CohoLoader.native.tsx` steps its sprite sheet on the platform driver;
 * `CohoLoader.web.tsx` is two CSS `steps()` keyframes and no JavaScript.
 */
import type { Palette } from "./theme";

export declare function CohoLoader(props: {
  palette: Palette;
  /** Its width in dp; the height follows the frame (`cohoHeight`). Defaults to `COHO_WIDTH`. */
  width?: number;
  /** said aloud */
  label?: string;
}): JSX.Element;
