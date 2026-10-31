/** (T50) The phone's: see `WindowInset.d.ts`. */
import type { ReactNode } from "react";
import { Platform, View, type StyleProp, type ViewStyle } from "react-native";
import { SafeAreaView, useSafeAreaInsets } from "react-native-safe-area-context";

export function BottomInset({ min }: { min: number }): ReactNode {
  return <SafeAreaView edges={{ bottom: "maximum" }} style={{ paddingBottom: min }}><View /></SafeAreaView>;
}

export function WindowInsets({ style, children }: { style?: StyleProp<ViewStyle>; children?: ReactNode }): ReactNode {
  return Platform.OS === "ios" ? <IosInsets style={style}>{children}</IosInsets> : <SafeAreaView style={style}>{children}</SafeAreaView>;
}

/**
 * (T313) iOS: a menu page is a full-screen Modal that slides in, and the native SafeAreaView measured itself mid-slide
 * (off screen: no overlap with the status bar) and kept a top inset of 0 — "‹ Back" sat under the clock. The app root's
 * insets (the same full-screen window) are padded instead; React context reaches through the Modal.
 */
function IosInsets({ style, children }: { style?: StyleProp<ViewStyle>; children?: ReactNode }): ReactNode {
  const i = useSafeAreaInsets();
  return <View style={[style, { paddingTop: i.top, paddingBottom: i.bottom, paddingLeft: i.left, paddingRight: i.right }]}>{children}</View>;
}
