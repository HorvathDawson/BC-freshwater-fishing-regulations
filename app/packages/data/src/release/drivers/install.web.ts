/**
 * THE WEB INSTALLS NOTHING (P11): it reads lazily into Cache Storage (`source.web.ts`). See `install.d.ts`.
 */
import type { Device } from "./install";

export type { Device } from "./install";

export function device(_fetchImpl?: typeof fetch): Device | null {
  return null;
}
