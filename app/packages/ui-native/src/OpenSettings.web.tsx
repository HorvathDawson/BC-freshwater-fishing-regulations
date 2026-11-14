/** (T723 G6) The browser's: no Settings page to open. See `OpenSettings.d.ts`. */
import type { ReactNode } from "react";
import type { Palette } from "./theme";

export function OpenSettings(_: { palette: Palette; line: string | null | undefined; inset?: number }): ReactNode {
  return null;
}
