/**
 * (T723, owner 2026-11-16: "we should fix anything that doesnt seem desirable": a spot's Save row is LIFTED above the
 * keyboard) A BAR THAT RIDES ON THE iOS KEYBOARD. A field names it by `inputAccessoryViewID={id}`; while that field has
 * the keyboard up, the bar's children sit on top of it, follow it on an interactive dismiss, and go away with it. iOS
 * counts the bar as part of the keyboard, so a scroll view's `automaticallyAdjustKeyboardInsets` keeps the focused field
 * above the bar too (a row drawn over the page could not: the scroll would put the field's end under it).
 * `.native.tsx` is the phone's (React Native's `InputAccessoryView`, iOS only: Android resizes its page for the keyboard,
 * so its own row is lifted already, T819, and there it renders nothing); `.web.tsx` renders nothing (no soft keyboard).
 */
import type { ReactNode } from "react";
import type { Palette } from "./theme";

export declare function KeyboardBar(props: { id: string; palette: Palette; children: ReactNode }): ReactNode;
