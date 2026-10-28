/** (T50) The browser's: no system bars over a page, so the minimum. See `WindowInset.d.ts`. */
import type { ReactNode } from "react";
import { View, type StyleProp, type ViewStyle } from "react-native";

export function BottomInset({ min }: { min: number }): ReactNode {
  return <View style={{ height: min }} />;
}

export function WindowInsets({ style, children }: { style?: StyleProp<ViewStyle>; children?: ReactNode }): ReactNode {
  return <View style={style}>{children}</View>;
}
