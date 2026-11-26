/**
 * (T310) THE SPOT'S PARKING: "Parking saved · Directions" when viewed (T819 item 14: words, not raw coordinates, which
 * stay small under it), and set / pick / remove while editing — one button style in the row.
 */
import { View } from "react-native";
import { Text } from "../Text";
import { latLon, SPOT_WORDS as W } from "@app/core";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { Button } from "../Button";
import { OpenSettings } from "../OpenSettings";
import { Block } from "./parts";

export function ParkingBlock({ palette, parking, editable, notice, onHere, onPick, onRemove, onDirections }: {
  palette: Palette; parking: { lat: number; lon: number } | null; editable: boolean; notice: string | null;
  onHere?: () => void; onPick?: () => void; onRemove?: () => void; onDirections?: () => void;
}) {
  return (
    <Block palette={palette} title={W.parking}>
      <View testID="spot-parking" style={{ gap: 6 }}>
        <View style={{ flexDirection: "row", alignItems: "center", flexWrap: "wrap", columnGap: 6 }}>
          <Text style={{ ...TYPE.body, color: parking ? palette.ink : palette.faint }}>
            {parking ? W.parkingSaved : W.noParking}
          </Text>
          {!editable && parking && onDirections && (
            <>
              <Text style={{ ...TYPE.body, color: palette.faint }}>·</Text>
              <Button palette={palette} kind="link" label={W.directions} a11yLabel={W.directionsLabel} onPress={onDirections} />
            </>
          )}
        </View>
        {parking && <Text selectable testID="spot-parking-place" style={{ ...TYPE.caption, color: palette.faint }}>{latLon(parking)}</Text>}
        {notice && <Text style={{ ...TYPE.small, color: palette.sub }}>{notice}</Text>}
        {/* (T1163 G6) a refused location: the way out is Settings (the phone's draws the button, the browser's nothing) */}
        <OpenSettings palette={palette} line={notice} inset={0} />
        {editable && (
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10 }}>
            {onHere && <Button palette={palette} kind="ghost" grow={false} label={W.parkingHere} onPress={onHere} />}
            {onPick && <Button palette={palette} kind="ghost" grow={false} label={W.parkingPick} onPress={onPick} />}
            {parking && onRemove && <Button palette={palette} kind="ghost" grow={false} label={W.parkingRemove} onPress={onRemove} />}
          </View>
        )}
      </View>
    </Block>
  );
}
