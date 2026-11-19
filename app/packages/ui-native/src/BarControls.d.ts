/**
 * (T909) THE BELL AND THE REFRESH BUTTON AT THE SEARCH BAR'S RIGHT END. `.native.tsx` is the phone's: the bell (T230) and the
 * refresh button (T231), as before. `.web.tsx` draws neither (owner 2026-11-19: "Remove both on web"): a browser has no
 * notifications to open and reloads itself; What's new stays in More, About. A store-shots web build (T319) draws the
 * phone's screens for the Play images, so there the web draws the phone's pair too (`PhoneBarControls`, barParts.tsx).
 * `barHasControls()` is whether the bar draws them, for the field's right padding (a call: the store-shots flag is set
 * at start). The walkthrough has no step on either (T842), so none is passed on the web.
 */
import type { ReactNode } from "react";
import type { Palette } from "./theme";
import type { SyncVM } from "@app/core";

export type BarControlsProps = {
  palette: Palette;
  notices: { count: number; label: string; onPress: () => void } | null;
  sync: SyncVM | null;
  onSync?: () => void;
};
export declare const barHasControls: () => boolean;
export declare function BarControls(props: BarControlsProps): ReactNode;
