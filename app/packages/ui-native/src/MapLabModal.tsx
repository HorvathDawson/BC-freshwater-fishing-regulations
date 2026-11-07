/**
 * (T420) THE MAP LAB, OPEN: `@app/map`'s card in a Modal of its own (over every chrome: drawn inside the map, the search
 * bar and button stack swallowed its taps), closed by its Close, a tap beside it, or the one back (`useBackLayer`).
 * Only a lab build opens it (`LAB_ON`: staging and test-bed builds with EXPO_PUBLIC_MAP_LAB, never production).
 */
import { Modal } from "react-native";
import { GEOMETRY, MAP_LAB_CLOSE, MapLab, mapChrome } from "@app/map";
import { useBackLayer } from "@app/ui";
import { Pressable } from "./Pressable";

export function MapLabModal({ open, theme, onClose }: { open: boolean; theme: string; onClose: () => void }) {
  const requestClose = useBackLayer(open, onClose);
  if (!open) return null;
  return (
    <Modal transparent animationType="none" onRequestClose={requestClose}>
      <Pressable testID="map-lab-backdrop" accessibilityLabel={MAP_LAB_CLOSE} onPress={requestClose}
                 style={{ flex: 1, justifyContent: "center", padding: GEOMETRY.right }}>
        <MapLab chrome={mapChrome(theme)} onClose={requestClose} />
      </Pressable>
    </Modal>
  );
}
