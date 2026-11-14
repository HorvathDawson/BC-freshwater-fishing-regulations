/**
 * (T723 G6) THE "OPEN SETTINGS" ACTION under a line that says a permission was refused (camera, location). `.native.tsx`
 * opens this app's page in the phone's Settings; `.web.tsx` draws nothing (a browser has no such page: the reader changes
 * the site's permission in the address bar). Drawn only when `line` says a refusal (core's `needsSettings`).
 */
import type { ReactNode } from "react";
import type { Palette } from "./theme";

export declare function OpenSettings(props: { palette: Palette; line: string | null | undefined }): ReactNode;
