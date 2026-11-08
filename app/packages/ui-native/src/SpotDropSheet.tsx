/**
 * A LONG PRESS ON THE MAP (owner 2026-10-21), or the first tap in pick mode ("Pick on the map", owner 2026-10-22), drops
 * a provisional pin and raises this: where it is, what comes next, and the choice. Continue goes on into the one spot
 * capture (pick the water near the pin, when you were there,
 * photos, the lead-up); Cancel — and back, Done, the scrim, a drag down — removes the pin and saves nothing.
 */
import { SPOT_DROP, type LatLon } from "@app/core";
import { View } from "react-native";
import { Text } from "./Text";
import { Button } from "./Button";
import { Sheet } from "./Sheet";
import { TYPE } from "./type";
import type { Palette } from "./theme";
import { CoachSpot } from "./CoachMarks";

export function SpotDropSheet({ at, palette, onCancel, onContinue, overlay }: {
  /** The pin; null: the sheet is down. */
  at: LatLon | null; palette: Palette; onCancel: () => void; onContinue: () => void;
  /** (T466) the walkthrough's card while its step is about this sheet (the sheet is its own window) */
  overlay?: React.ReactNode;
}) {
  return (
    <Sheet open={at !== null} onClose={onCancel} title={SPOT_DROP.title} palette={palette} overlay={overlay}>
      <View style={{ paddingHorizontal: 20, paddingTop: 16, gap: 6 }}>
        <Text testID="spot-drop-where" style={{ ...TYPE.bodyStrong, fontSize: 15, color: palette.accent }}>
          {at ? SPOT_DROP.where(at.lat, at.lon) : ""}
        </Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{SPOT_DROP.detail}</Text>
      </View>
      <View style={{ flexDirection: "row", gap: 10, paddingHorizontal: 20, paddingTop: 18 }}>
        <Button palette={palette} label={SPOT_DROP.cancel} onPress={onCancel} kind="ghost" />
        <CoachSpot k="spot:drop" style={{ flex: 1 }}><Button palette={palette} label={SPOT_DROP.go} onPress={onContinue} /></CoachSpot>
      </View>
    </Sheet>
  );
}
