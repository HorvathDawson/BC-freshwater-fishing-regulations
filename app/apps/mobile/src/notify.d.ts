/**
 * The shared declaration for `./notify` (T234): the phone's local notifications — the settings, the permission, the
 * background check's schedule (`notify.native.ts`). The web has none (`notify.web.ts`: no package in the web bundle, no
 * first-paint weight; More lists no Notifications and no gauge offers "Alert me"). Bundlers pick by platform extension,
 * TypeScript does not.
 */
import type { NotifyData } from "@app/data";
import type { NotifyControls } from "@app/ui";

export declare const notifications: NotifyControls | null;
/** Every tap on one of the app's notifications (the one that started the app first); returns the undo. */
export declare function onTap(on: (d: NotifyData) => void): () => void;
