/** (T310) Pick the spot's parking on the map: tap the point, then "Use this point". */
import { Text, View } from "react-native";
import { SPOT_WORDS as W, SPOT_CAPTURE_WORDS as CW } from "@app/core";
import type { Camera, TileEndpoints } from "@app/map";
import { useBackLayer } from "@app/ui";
import { MapScreen } from "../MapScreen";
import { Button } from "../Button";
import { TYPE } from "../type";
import type { Palette } from "../theme";

export function ParkingPick({ palette, tiles, theme, camera, point, onPoint, onUse, onCancel }: {
  palette: Palette; tiles: TileEndpoints; theme: string; camera: Camera; point: { lat: number; lon: number } | null;
  onPoint: (p: { lat: number; lon: number }) => void; onUse: () => void; onCancel: () => void;
}) {
  useBackLayer(true, onCancel);
  return (
    <View style={{ flex: 1 }}>
      <MapScreen at={tiles} palette={palette} theme={theme} camera={camera} view="plain"
                 onMapPoint={(lat, lon) => onPoint({ lat, lon })} marker={point} />
      <View style={{ backgroundColor: palette.card, borderTopWidth: 1, borderTopColor: palette.line, padding: 16, gap: 12 }}>
        <View style={{ gap: 3 }}>
          <Text style={{ ...TYPE.name, fontSize: 17, color: palette.ink }}>{W.pickParking}</Text>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{W.pickParkingDetail}</Text>
          {point && <Text style={{ ...TYPE.bodyStrong, fontSize: 13.5, color: palette.accent, marginTop: 3 }}>
            {`${point.lat.toFixed(5)}, ${point.lon.toFixed(5)}`}</Text>}
        </View>
        <View style={{ flexDirection: "row", gap: 10 }}>
          <Button palette={palette} label={CW.cancel} onPress={onCancel} kind="ghost" />
          <Button palette={palette} label={W.parkingUse} onPress={point ? onUse : undefined} />
        </View>
      </View>
    </View>
  );
}
