/**
 * T40 — every press on the Regulations view goes through this: a Pressable told its known style `box` (the size its own
 * style guarantees — `minHeight: 44`, a 20 px glyph, a tab's label run), which it grows to 44 × 44 with `hitSlop` and
 * records as `data-hitslop` for the a11y render test (`hit.ts`). Roles, names and state (aria-expanded / -selected /
 * -checked) are the caller's, as on any Pressable. No logic.
 */
import { forwardRef, type ForwardRefExoticComponent, type RefAttributes } from "react";
import { type PressableProps, type ViewInstance } from "react-native";
import { Pressable } from "../Pressable";
import { hitProps, type HitBox, type Insets } from "./hit";

export type RegsPressProps = Omit<PressableProps, "hitSlop"> & {
  readonly box: HitBox;
  /** (T1004) an explicit slop instead of the one that grows the box to 44 × 44: only for a press whose neighbours leave no room (`SHORT_PRESS`, a11yAudit) */
  readonly slop?: Insets;
};

export const RegsPress: ForwardRefExoticComponent<RegsPressProps & RefAttributes<ViewInstance>> = forwardRef<ViewInstance, RegsPressProps>(function RegsPress({ box, slop, ...rest }, ref) {
  return <Pressable ref={ref} {...rest} {...hitProps(box, slop)} />;
});
