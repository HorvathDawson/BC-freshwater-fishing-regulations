/**
 * (T723 item 7) THE ONE PLACE A SYMBOL IS ASKED TO DRAW AS TEXT. iOS draws a character that has both a text and an emoji
 * form ("↗" U+2197 in "Rule book p.7 ↗", "↩ Release", "⚠") as a coloured EMOJI tile unless U+FE0E (variation selector 15,
 * "text form") follows it. Android's and the browser's fonts already draw the text form, so only the iPhone is asked; the
 * words themselves stay as written in `@app/core` (copying, search and tests see the plain character).
 * Every ui-native `Text` goes through `guardChildren` (Text.tsx), so no call site thinks about it.
 */
import { Children, type ReactNode } from "react";
import { Platform } from "react-native";

/** The characters this app prints that iOS may draw as emoji: arrows, ▶ ◀, ⚠, ✔, ℹ, ✖. (© ® ™ are left as written.) */
const EMOJI_CAPABLE = /([↔-↙↩↪▶◀⚠✔✖ℹ⬅-⬇➡])(?!︎)/g;

/** `s` with each emoji-capable symbol followed by U+FE0E (text form); already-marked ones are left alone. */
export function textForm(s: string): string {
  return s.replace(EMOJI_CAPABLE, "$1︎");
}

/** True on the iPhone: the only platform that is asked (a one-value choice, `Platform.select`). */
const asksTextForm = (): boolean => Platform.select({ ios: true, default: false });

/** A Text's children with every plain string put in text form; unchanged (same object) where `ios` is false. */
export function guardChildren(children: ReactNode, ios: boolean = asksTextForm()): ReactNode {
  if (!ios) return children;
  if (typeof children === "string") return textForm(children);
  if (children == null || typeof children !== "object") return children;
  return Children.map(children, (c) => (typeof c === "string" ? textForm(c) : c));
}
