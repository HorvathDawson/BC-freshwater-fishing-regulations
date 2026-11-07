/**
 * (T476, from T420's MapLabModal) THE DEVELOPER CARD, OPEN: `DeveloperCard` in a Modal of its own (over every chrome:
 * drawn inside the map, the search bar and button stack swallowed its taps), closed by its Close, a tap beside it, or the
 * one back (`useBackLayer`). Only a development or staging build opens it (`LAB_ON`, @app/map; never production).
 */
import { Modal } from "react-native";
import { GEOMETRY, mapChrome } from "@app/map";
import { useBackLayer } from "@app/ui";
import { Pressable } from "./Pressable";
import { DeveloperCard, DEVELOPER_CLOSE } from "./DeveloperCard";
import type { MenuInfo } from "./menu/MenuHost";
import type { Palette } from "./theme";

export function DeveloperModal({ open, theme, palette, info, release, onClose }: {
  open: boolean; theme: string; palette: Palette; info: MenuInfo; release: string | null; onClose: () => void;
}) {
  const requestClose = useBackLayer(open, onClose);
  if (!open) return null;
  return (
    <Modal transparent animationType="none" onRequestClose={requestClose}>
      <Pressable testID="developer-backdrop" accessibilityLabel={DEVELOPER_CLOSE} onPress={requestClose}
                 style={{ flex: 1, justifyContent: "center", padding: GEOMETRY.right }}>
        <DeveloperCard palette={palette} chrome={mapChrome(theme)} info={info} release={release} onClose={requestClose} />
      </Pressable>
    </Modal>
  );
}
