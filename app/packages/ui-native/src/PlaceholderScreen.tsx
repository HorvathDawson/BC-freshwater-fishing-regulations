/**
 * The wiring probe. Not product UI — it exists so that a broken workspace link fails
 * VISIBLY on screen instead of silently resolving to a stale copy.
 *
 * It deliberately reaches through the two layers that must be shared:
 *   @app/core  (freshness)      — pure domain logic, no platform
 *   @app/ui    (formFactorFor)  — the headless hook layer
 * and renders in React Native primitives only, so the SAME file paints the native app
 * and mobile web through react-native-web.
 */
import { StyleSheet, Text, View, useWindowDimensions } from "react-native";
import { freshness } from "@app/core";
import { formFactorFor } from "@app/ui";
import { RADIUS } from "./theme";

const MINUTE = 60_000;

export function PlaceholderScreen({ target }: { target: string }) {
  const { width } = useWindowDimensions();
  const now = Date.now();
  // Two fixed ages so the union's branches are both observable at a glance.
  const live = freshness(now - 10_000, now, MINUTE);
  const stale = freshness(now - 5 * MINUTE, now, MINUTE);
  const unknown = freshness(null, now, MINUTE);

  return (
    <View style={styles.root}>
      <Text style={styles.title}>canifishthis</Text>
      <Text style={styles.sub}>{target}</Text>

      <View style={styles.card}>
        <Text style={styles.label}>@app/ui</Text>
        <Text style={styles.value}>
          formFactorFor({Math.round(width)}) = {formFactorFor(width)}
        </Text>
      </View>

      <View style={styles.card}>
        <Text style={styles.label}>@app/core</Text>
        <Text style={styles.value}>freshness(now-10s) = {live.state}</Text>
        <Text style={styles.value}>freshness(now-5m) = {stale.state}</Text>
        <Text style={styles.value}>freshness(null) = {unknown.state}</Text>
      </View>

      <Text style={styles.note}>
        Placeholder. Real screens wait on the content schema (13-build-plan steps 6-7);
        the map waits on tiles that do not exist yet.
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { flex: 1, backgroundColor: "#0f1417", paddingHorizontal: 24, paddingTop: 72, gap: 16 },
  title: { color: "#e8f1f2", fontSize: 30, fontWeight: "700" },
  sub: { color: "#6f8b95", fontSize: 14, marginTop: -12 },
  card: { backgroundColor: "#182026", borderRadius: RADIUS.box, padding: 16, gap: 4 },
  label: { color: "#4fb3d9", fontSize: 12, fontWeight: "700", letterSpacing: 1 },
  value: { color: "#cfe0e6", fontSize: 15, fontVariant: ["tabular-nums"] },
  note: { color: "#5d757e", fontSize: 12, lineHeight: 18, marginTop: 8 },
});
