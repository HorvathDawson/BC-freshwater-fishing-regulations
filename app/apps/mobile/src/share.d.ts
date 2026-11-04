/**
 * The shared declaration for `./share` (T385): the platform's way to send a water's link. `share.native.ts` is the
 * phone's share sheet (React Native `Share`); `share.web.ts` is the browser's (`navigator.share`), else the clipboard.
 * Bundlers pick by platform extension, TypeScript does not.
 */
import type { SharePort } from "@app/ui";

export declare const share: SharePort | null;
