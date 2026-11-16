/**
 * (T723 G10) A "Done" BAR OVER THE iOS NUMBER PADS. iOS's decimal and number pads have no return key, so once one is up
 * nothing on it closes it. A field names the bar by `inputAccessoryViewID={id}`; the bar rides on top of the keyboard and
 * its Done drops the keyboard. `.native.tsx` is the phone's (React Native's `InputAccessoryView`, which draws on iOS
 * only: Android's pads have their own Done key, and there it renders nothing); `.web.tsx` renders nothing.
 */
import type { ReactNode } from "react";
import type { Palette } from "./theme";

export declare function DoneBar(props: { id: string; palette: Palette }): ReactNode;
