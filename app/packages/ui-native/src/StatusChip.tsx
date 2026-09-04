/**
 * The small pills on a search row: the outcome, the live reading, whether it is stocked.
 *
 * `StatusPill` is the ANSWER and looks like one. These are secondary — same vocabulary,
 * quieter treatment — so a list can carry several without any of them shouting.
 */
import { Text, View } from "react-native";
import { TYPE } from "./type";
import type { Palette } from "./theme";

export function Chip({ palette, label, colour, tone = "wash" }: {
  palette: Palette; label: string; colour?: string; tone?: "wash" | "tint";
}) {
  return (
    <View style={{ borderRadius: 999, paddingVertical: 5, paddingHorizontal: 10,
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
export function ordinal(p: number): string {
  const pct = p * 100;
  const n = pct < 1 ? Number(pct.toFixed(1)) : Math.round(pct);
  const t = Math.round(n) % 100;
  const suffix = t >= 11 && t <= 13 ? "th"
    : ["th", "st", "nd", "rd"][Math.round(n) % 10] ?? "th";
  return `p${n}${suffix}`;
}
