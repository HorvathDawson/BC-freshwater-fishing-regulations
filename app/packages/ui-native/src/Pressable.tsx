/**
 * (T342) THE APP'S PRESSABLE: React Native's, with its words never selectable — a tap target's long-press keeps its own
 * meaning inside a pane whose reading text is (`Text.tsx`). Every ui-native component takes `Pressable` from here.
 */
import { forwardRef, type ForwardRefExoticComponent, type RefAttributes } from "react";
import { Pressable as RNPressable, type PressableProps, type ViewInstance } from "react-native";
import { Selectable } from "./Text";

export const Pressable: ForwardRefExoticComponent<PressableProps & RefAttributes<ViewInstance>> = forwardRef<ViewInstance, PressableProps>(function Pressable({ children, ...p }, ref) {
  return (
    <RNPressable ref={ref} {...p}>
      {typeof children === "function"
        ? (s) => <Selectable on={false}>{children(s)}</Selectable>
        : <Selectable on={false}>{children}</Selectable>}
    </RNPressable>
  );
});
