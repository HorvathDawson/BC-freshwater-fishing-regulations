/** (T50) The phone's: see `WindowInset.d.ts`. */
import type { ReactNode } from "react";
import { View, type StyleProp, type ViewStyle } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

export function BottomInset({ min }: { min: number }): ReactNode {
  return <SafeAreaView edges={{ bottom: "maximum" }} style={{ paddingBottom: min }}><View /></SafeAreaView>;
}

export function WindowInsets({ style, children }: { style?: StyleProp<ViewStyle>; children?: ReactNode }): ReactNode {
  return <SafeAreaView style={style}>{children}</SafeAreaView>;
}
