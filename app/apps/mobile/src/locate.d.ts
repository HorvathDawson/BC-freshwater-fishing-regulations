/**
 * The shared declaration for `./locate` (owner 2026-10-21, the Spots menu's "At my location"): where the device is,
 * and whether the app may ask. `locate.web.ts` is the browser's geolocation (offered when granted, or when the tap
 * itself will ask); `locate.native.ts` is expo-location (T74, deps.md). Bundlers pick by platform extension, TypeScript does not.
 */
import type { Locate } from "@app/ui";

export declare const locate: Locate | null;
