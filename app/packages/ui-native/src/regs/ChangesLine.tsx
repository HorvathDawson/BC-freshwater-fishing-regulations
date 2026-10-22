/**
 * T34 — a dated change under a gear line or the bait item (prototype `gnote` → `.chg`): the chip ("Next" / "Now", caps on
 * the tint — round 20h keeps the changes chip tinted, tokens.md §1.1), then "<b>May 1–31:</b> fly fishing only" and, while
 * in force, "· in force today". No link, no ⓘ, no Rule book page: the change's rule is in the item's source line.
 */
import { Text, View } from "react-native";
import { GEAR, WORDS, type ChangeVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

export function ChangesLine({ change, palette }: { change: ChangeVM; palette: Palette }) {
  const strong = { fontFamily: TYPE.metaBold.fontFamily, color: palette.ink };
  return (
    <View testID="regs-change" style={{ flexDirection: "row", alignItems: "baseline", columnGap: 6, marginTop: SPACE.s1 }}>
      <Text testID="chip:change" style={[TYPE.chip, { color: palette.ink, backgroundColor: palette.tint, paddingHorizontal: 5 }]}>{change.chip}</Text>
      <Text style={[TYPE.metaSemi, { color: palette.sub, flexShrink: 1, fontVariant: ["tabular-nums"] }]}>
        <Text style={strong}>{change.when}{WORDS.COLON}</Text> {change.says}
        {change.inForce && <> <Text style={strong}>{GEAR.IN_FORCE}</Text></>}
      </Text>
    </View>
  );
}
