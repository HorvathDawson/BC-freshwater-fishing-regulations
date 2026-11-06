/**
 * (T342, owner 2026-10-31: "a user could use basic phone controls to copy the text that is on screen … on the panes,
 * not the map") THE APP'S TEXT: React Native's, selectable with the platform's own long-press → Copy wherever a pane
 * says so. A pane's root opens `<Selectable>` (WaterFrame, Sheet, MenuPage, SpotScreen); a tap target closes it
 * (`Pressable.tsx`), so a long-press on a button, tab or chip keeps its own meaning; the map and its chrome never open
 * it. An explicit `selectable` on one Text still wins. Every ui-native component takes `Text` from here
 * (`tools/selectable-text.test.ts` refuses react-native's `Text` elsewhere).
 */
import { createContext, forwardRef, useContext, type ForwardRefExoticComponent, type ReactNode, type RefAttributes } from "react";
import { Text as RNText, type TextProps, type TextInstance } from "react-native";

/** undefined outside any pane: no opinion, the platform's own default (and unchanged markup) */
const SelectableContext = createContext<boolean | undefined>(undefined);

/** Text under this is selectable (`on`, default true) or not (`on={false}`: a tap target inside a pane). */
export function Selectable({ on = true, children }: { on?: boolean; children?: ReactNode }) {
  return <SelectableContext.Provider value={on}>{children}</SelectableContext.Provider>;
}

export const Text: ForwardRefExoticComponent<TextProps & RefAttributes<TextInstance>> = forwardRef<TextInstance, TextProps>(function Text(p, ref) {
  const on = useContext(SelectableContext);
  return <RNText ref={ref} {...p} selectable={p.selectable ?? on} />;
});
