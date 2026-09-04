/**
 * The small pills on a search row: the outcome, the live reading, whether it is stocked.
 *
 * `StatusPill` is the ANSWER and looks like one. These are secondary — same vocabulary,
 * quieter treatment — so a list can carry several without any of them shouting.
 */
import { Text, View } from "react-native";
import { percentileLabel } from "@app/core";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Chip({ palette, label, colour, tone = "wash" }: {
  palette: Palette; label: string; colour?: string; tone?: "wash" | "tint";
}) {
  return (
    <View style={{ borderRadius: palette.r.pill, paddingVertical: 5, paddingHorizontal: 10,
                   backgroundColor: tone === "tint" ? palette.tint : palette.wash }}>
      <Text style={{ ...TYPE.micro, color: colour ?? palette.sub }}>{label}</Text>
    </View>
  );
}

/**
 * "p3rd", "p0.4th". A percentile below one still has to read as a number, because
 * rounding it to "p0th" would say the river has never been lower, which is a different
 * and much stronger claim.
 */
export const ordinal = percentileLabel;
