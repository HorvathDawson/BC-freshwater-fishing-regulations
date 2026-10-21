/**
 * T34 — the sentence a Gear / Methods tab says instead of its items (prototype `p.foot`): "No fishing here until Jul 1.",
 * or on tidal water the Keep banner's words, one paragraph each. No logic.
 */
import { Text } from "react-native";
import { TYPE } from "../type";
import type { Palette } from "../theme";

export function ClosedNote({ text, palette }: { text: string; palette: Palette }) {
  return <Text testID="regs-closed" style={[TYPE.meta, { color: palette.sub, paddingTop: 10, paddingHorizontal: 16, paddingBottom: 20 }]}>{text}</Text>;
}
