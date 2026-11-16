/**
 * (T723 F7) THE LEFT-EDGE BACK SWIPE INSIDE A MODAL. A React Native `Modal` is a window of its own, so the Shell's
 * `EdgeBack` does not reach it, and `onRequestClose` only fires on Android. Wrapped around a modal's content, an iOS
 * swipe in from the left edge calls `onBack`: pass the modal's `useBackLayer` back (what Android's back calls through
 * `onRequestClose`), so it is one step back, and it never reaches the page under. `.native.tsx` is the phone's (the
 * gesture, only where `EdgeBack` has one: iOS; on Android the children are drawn untouched); `.web.tsx` draws the
 * children as they are (the browser owns that edge).
 */
import type { ReactNode } from "react";

export declare function ModalBack(props: { onBack: () => void; children: ReactNode }): ReactNode;
