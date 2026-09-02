/**
 * The native map. NOT BUILT.
 *
 * `@maplibre/maplibre-react-native` is a native module: it needs a development build
 * (`expo prebuild` + `run:ios`/`run:android`), not Expo Go, so it cannot be added and
 * verified from here. Rather than ship a component that renders nothing on a phone and
 * looks like a loading failure, this says so.
 *
 * The web implementation lives in `Map.web.tsx` and Metro picks it by platform extension.
 * When this is written it must consume the SAME `runtimeStyle()` output and the same
 * adapter contract — that is what stops the two platforms drawing different maps.
 */
import { Text, View } from "react-native";
import type { MapProps } from "./map-props";

export function Map({ style }: MapProps) {
  return (
    <View style={[{ flex: 1, alignItems: "center", justifyContent: "center", padding: 28 },
                  style]}>
      <Text style={{ textAlign: "center", opacity: 0.6 }}>
        The native map needs @maplibre/maplibre-react-native and a development build.
        Mobile web renders the real map today.
      </Text>
    </View>
  );
}
