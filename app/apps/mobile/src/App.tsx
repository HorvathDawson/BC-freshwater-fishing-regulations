/**
 * The app shell. Views only — no regulation logic ever lands here (layers.json).
 *
 * ONE tree for all three targets: expo start --web renders this through
 * react-native-web, expo run:android / run:ios render the same file natively. The
 * screen itself lives in @app/ui-native, so mobile web and the native app cannot
 * show different things.
 */
import { SafeAreaView, StatusBar, StyleSheet, Platform } from "react-native";
import { PlaceholderScreen } from "@app/ui-native";

export default function App() {
  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" />
      <PlaceholderScreen target={`expo · ${Platform.OS}`} />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#0f1417" },
});
