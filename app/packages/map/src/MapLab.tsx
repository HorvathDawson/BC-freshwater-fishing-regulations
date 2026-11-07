/**
 * (T420) THE MAP LAB'S CARD: the switches of `lab.ts`, opened by a long press on the credits chip in a lab build only.
 * A square card at the top of the map (the map UI rule: a card, square corners, our type); every row names what it
 * changes and what today's map does, and the foot says when the map differs from today's.
 */
import { Pressable, Switch, Text, View } from "react-native";
import { GEOMETRY } from "./chrome";
import { LAB_DEFAULT, setLab, useLab, type Lab } from "./lab";
import type { MapChrome } from "./map-props";

const TEXT = "Archivo_400Regular", SEMI = "Archivo_600SemiBold";

const ROWS: readonly { key: keyof Lab; label: string; hint: string; on: Lab[keyof Lab]; off: Lab[keyof Lab] }[] = [
  { key: "pixelCap", label: "Pixel cap 2", hint: "fewer pixels filled (today: on)", on: true, off: false },
  { key: "prefetch", label: "Prefetch delta 2", hint: "closer placeholder tiles (today: 4)", on: 2, off: 4 },
  { key: "stateGapMs", label: "Colour batches ≥ 250 ms apart", hint: "fewer colour updates mid-glide (today: per tile)", on: 250, off: 0 },
  { key: "hideInvisible", label: "Skip invisible layers", hint: "layers drawn at opacity 0 left out (same look)", on: true, off: false },
  { key: "streamsOneBucket", label: "Streams in one bucket", hint: "stream tiles laid out once, not 4×", on: true, off: false },
  { key: "roads", label: "Basemap roads", hint: "test only: off hides roads", on: true, off: false },
  { key: "landuse", label: "Basemap greens", hint: "test only: off hides landuse", on: true, off: false },
  { key: "labels", label: "Labels", hint: "test only: off hides every label", on: true, off: false },
];

export function MapLab({ chrome, onClose }: { chrome: MapChrome; onClose: () => void }) {
  const lab = useLab();
  const changed = ROWS.filter((r) => lab[r.key] !== LAB_DEFAULT[r.key]).map((r) => r.label);
  return (
    <View testID="map-lab" accessibilityLabel="Map lab"
          style={{ position: "absolute", top: GEOMETRY.right, left: GEOMETRY.right, right: GEOMETRY.right,
                   backgroundColor: chrome.card, borderWidth: 1, borderColor: chrome.ink, padding: 10, gap: 8,
                   boxShadow: `2px 2px 0 ${chrome.shadow}` }}>
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
        <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 13, lineHeight: 16 }}>Map lab (test build)</Text>
        <Pressable testID="map-lab-close" accessibilityRole="button" accessibilityLabel="Close the map lab"
                   onPress={onClose} hitSlop={10}
                   style={{ paddingHorizontal: 6, paddingVertical: 2, borderWidth: 1, borderColor: chrome.ink }}>
          <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 12, lineHeight: 15 }}>Close</Text>
        </Pressable>
      </View>
      {ROWS.map((r) => (
        <View key={r.key} style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
          <View style={{ flexShrink: 1 }}>
            <Text style={{ color: chrome.ink, fontFamily: SEMI, fontSize: 12, lineHeight: 15 }}>{r.label}</Text>
            <Text style={{ color: chrome.sub, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>{r.hint}</Text>
          </View>
          <Switch testID={`map-lab-${r.key}`} accessibilityLabel={r.label} value={lab[r.key] === r.on}
                  onValueChange={(v) => setLab({ [r.key]: v ? r.on : r.off } as Partial<Lab>)} />
        </View>
      ))}
      <Text testID="map-lab-state" style={{ color: chrome.sub, fontFamily: TEXT, fontSize: 11, lineHeight: 14 }}>
        {changed.length ? `Differs from today: ${changed.join(", ")}. Resets when the app restarts.` : "Today's map."}
      </Text>
    </View>
  );
}
