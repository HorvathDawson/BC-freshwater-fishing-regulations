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

/*
 * THERE IS NO `ordinal` HERE ANY MORE. This file re-exported `percentileLabel` under that
 * name, so `ordinal` meant "p3rd" imported from @app/ui-native and "3rd" imported from
 * @app/core — two functions, one name, both correct in isolation. Nothing consumed the
 * alias; it existed only to be picked by autocomplete one day. Import `percentileLabel`
 * from core, which is what this file does.
 */
