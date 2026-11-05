/** (T234) The web raises no notifications (phone-only v1, owner 2026-11-05): nothing here, nothing in its bundle. */
import type { NotifyData } from "@app/data";
import type { NotifyControls } from "@app/ui";

export const notifications: NotifyControls | null = null;
export function onTap(_on: (d: NotifyData) => void): () => void { return () => {}; }
