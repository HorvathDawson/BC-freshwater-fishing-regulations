/**
 * A weekday row's status box (prototype weekSheet `.wkstat`): the day read ("Today · Sat") and whose row it is, the
 * weekday status ("Weekends: keep 5 · Weekdays: release"), the data's weekday sentence. No logic.
 */
import { Text, View } from "react-native";
import { TYPE } from "../../type";
import type { Palette } from "../../theme";

export function WeekBox({ day, scope, status, says, palette }: { day: string; scope: string | null; status: string; says: string; palette: Palette }) {
  return (
    <View style={{ marginTop: 8, padding: 12, backgroundColor: palette.tint }}>
      <Text style={[TYPE.metaHeavy, { color: palette.accent }]}>
        {day}{scope ? <Text style={[TYPE.metaSemi, { color: palette.sub }]}>{` ${scope}`}</Text> : null}
      </Text>
      <Text style={[TYPE.regsBodyBold, { color: palette.ink, marginTop: 4 }]}>{status}</Text>
      <Text style={[TYPE.regsBody, { color: palette.ink, marginTop: 2 }]}>{says}</Text>
    </View>
  );
}
