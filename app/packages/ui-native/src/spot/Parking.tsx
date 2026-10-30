/** (T310) THE SPOT'S PARKING: where it is (coordinates), Directions (view), and set / pick / remove (edit). */
import { Text, View } from "react-native";
import { SPOT_WORDS as W } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Button } from "../Button";
import { Block } from "./parts";

export function ParkingBlock({ palette, parking, editable, notice, onHere, onPick, onRemove, onDirections }: {
  palette: Palette; parking: { lat: number; lon: number } | null; editable: boolean; notice: string | null;
  onHere?: () => void; onPick?: () => void; onRemove?: () => void; onDirections?: () => void;
}) {
  return (
    <Block palette={palette} title={W.parking}>
      <View testID="spot-parking" style={{ gap: 10 }}>
        <Text selectable style={{ ...TYPE.body, color: parking ? palette.ink : palette.faint }}>
          {parking ? `${parking.lat.toFixed(5)}, ${parking.lon.toFixed(5)}` : W.noParking}
        </Text>
        {notice && <Text style={{ ...TYPE.small, color: palette.sub }}>{notice}</Text>}
        <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10 }}>
          {!editable && parking && onDirections && (
            <Button palette={palette} kind="small" label={W.directions} a11yLabel={W.directionsLabel} onPress={onDirections} />
          )}
          {editable && onHere && <Button palette={palette} kind="small" label={W.parkingHere} onPress={onHere} />}
          {editable && onPick && <Button palette={palette} kind="small" label={W.parkingPick} onPress={onPick} />}
          {editable && parking && onRemove && <Button palette={palette} kind="ghost" label={W.parkingRemove} onPress={onRemove} />}
        </View>
      </View>
    </Block>
  );
}
