/**
 * (T235) THE LICENCES, at the foot of the Sources and licences page: each dataset, who publishes it, what the app shows
 * from it, its licence (linked) and the words the licence asks for. Draws core's `licencesVM` and decides nothing.
 */
import { Linking, Pressable, Text, View } from "react-native";
import { LICENCE_WORDS, WORDS, type LicencesVM } from "@app/core";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

const open = (url: string) => { void Linking.openURL(url); };

export function LicencesList({ vm, palette }: { vm: LicencesVM; palette: Palette }) {
  return (
    <View testID="sources-licences" style={{ gap: SPACE.s2 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{vm.title}</Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>{vm.intro}</Text>
      <View style={{ borderBottomWidth: 1, borderBottomColor: palette.line }}>
        {vm.rows.map((r) => (
          <View key={r.k} testID={`licence-${r.k}`}
                style={{ paddingVertical: SPACE.s3 - 2, gap: 2, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.name}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{r.from}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{`${LICENCE_WORDS.shows}: ${r.shows}`}</Text>
            {r.licenceUrl ? (
              <Pressable onPress={() => open(r.licenceUrl!)} accessibilityRole="link"
                         accessibilityLabel={LICENCE_WORDS.licenceFor(r.name, r.licenceLabel)}>
                <Text style={{ ...TYPE.small, color: palette.accent }}>
                  {`${LICENCE_WORDS.licence}: ${r.licenceLabel} ${WORDS.LINK_OUT}`}
                </Text>
              </Pressable>
            ) : (
              <Text style={{ ...TYPE.small, color: palette.sub }}>{`${LICENCE_WORDS.licence}: ${r.licenceLabel}`}</Text>
            )}
            {r.wording !== null && (
              <Text selectable testID={`licence-words-${r.k}`} style={{ ...TYPE.small, fontStyle: "italic", color: palette.sub }}>
                {r.wording}
              </Text>
            )}
            {r.url !== null && (
              <Pressable onPress={() => open(r.url!)} accessibilityRole="link" accessibilityLabel={`${LICENCE_WORDS.open}: ${r.name}`}>
                <Text style={{ ...TYPE.small, color: palette.accent }}>{`${LICENCE_WORDS.open} ${WORDS.LINK_OUT}`}</Text>
              </Pressable>
            )}
          </View>
        ))}
      </View>
      <Text testID="sources-odbl" style={{ ...TYPE.small, color: palette.sub }}>{vm.odbl}</Text>
    </View>
  );
}
