/** The spot screen's small parts: a block with its heading, a header link, and when a value was observed. */
import type { ReactNode } from "react";
import { Pressable, Text, View } from "react-native";
import { observedLine } from "@app/data/spots";
import { TYPE } from "../type";
import type { Palette } from "../theme";
import { IconText, type IconName } from "../icons";
import { hitProps } from "../regs/hit";

/** When a value was observed, and whether it was filled in after the fact. */
export function Meta({ palette, at, backfilled, station }: {
  palette: Palette; at: string | null; backfilled?: boolean; station?: string | null;
}) {
  return (
    <Text testID="spot-observed" style={{ ...TYPE.caption, color: palette.faint }}>
      {observedLine(at, backfilled, station)}
    </Text>
  );
}

export function Block({ palette, title, children }: {
  palette: Palette; title: string; children: ReactNode;
}) {
  return (
    <View style={{ gap: 8 }}>
      <Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.6,
                     color: palette.faint }}>{title.toUpperCase()}</Text>
      {children}
    </View>
  );
}

/**
 * (T284) One of the spot page's three headed blocks — That day, What you added, The next few days. A heading in the
 * screen-title face over a rule, then its parts a section-gap apart; `Block` stays the small-caps sub-heading inside it.
 */
export function Section({ palette, title, testID, children }: {
  palette: Palette; title: string; testID: string; children: ReactNode;
}) {
  return (
    <View testID={testID} style={{ gap: 18 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink, paddingBottom: 6,
                                                borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>{title}</Text>
      {children}
    </View>
  );
}

export function Link({ palette, label, text, onPress, tone, icon }: {
  palette: Palette; label: string; text: string; onPress: () => void; tone?: string; icon?: IconName;
}) {
  const style = { ...TYPE.micro, fontSize: 13, color: tone ?? palette.accent };
  return (
    <Pressable {...hitProps({ w: 60, h: 44 })} onPress={onPress} accessibilityRole="button" accessibilityLabel={label} style={{ minHeight: 44, justifyContent: "center" }}>
      {icon ? <IconText name={icon} text={text} style={style} /> : <Text style={style}>{text}</Text>}
    </Pressable>
  );
}
