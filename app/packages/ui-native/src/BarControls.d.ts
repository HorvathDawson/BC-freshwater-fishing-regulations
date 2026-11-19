/**
 * (T909) THE BELL AND THE REFRESH BUTTON AT THE SEARCH BAR'S RIGHT END. `.native.tsx` is the phone's: the bell (T230) and the
 * refresh button (T231), as before. `.web.tsx` draws neither (owner 2026-11-19: "Remove both on web"): a browser has no
 * notifications to open and reloads itself; What's new stays in More, About. `BAR_HAS_CONTROLS` is whether the bar draws
 * them, for the field's right padding. The walkthrough has no step on either (T842), so none is passed on the web.
 */
import type { ReactNode } from "react";
import type { Palette } from "./theme";
import type { SyncVM } from "@app/core";

export declare const BAR_HAS_CONTROLS: boolean;
export declare function BarControls(props: {
  palette: Palette;
  notices: { count: number; label: string; onPress: () => void } | null;
  sync: SyncVM | null;
  onSync?: () => void;
}): ReactNode;
