/**
 * The menu's reading pages — About, Data sources & dates, Contact. Each draws one VM from core/menu.ts and decides
 * nothing; a link only says where it goes.
 */
import { Linking, Pressable, Text, View } from "react-native";
import { MENU_WORDS, type AboutVM, type ContactVM, type SourceDateRowVM } from "@app/core";
import { Button } from "../Button";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

const open = (url: string) => { void Linking.openURL(url); };

export function AboutPage({ vm, palette }: { vm: AboutVM; palette: Palette }) {
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.what}</Text>
      {/* THE AUTHORITY DISCLAIMER: boxed in ink so it cannot be read as a footnote */}
      <View testID="about-not-official" style={{ borderWidth: 1.5, borderColor: palette.ink, padding: SPACE.s3,
                                                 backgroundColor: palette.tint }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.notOfficial}</Text>
      </View>
      <Pressable onPress={() => open(vm.official.url)} accessibilityRole="link" accessibilityLabel={vm.official.label}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{`${vm.official.label} ↗`}</Text>
      </Pressable>
      <View style={{ gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.section, color: palette.sub }}>{vm.versionsTitle}</Text>
        {vm.versions.map((v) => (
          <View key={v.label} style={{ flexDirection: "row", gap: SPACE.s3, paddingVertical: 6,
                                       borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.small, color: palette.sub, width: 104 }}>{v.label}</Text>
            <Text selectable testID={`about-version-${v.label}`}
                  style={{ ...TYPE.figure, fontSize: 12.5, color: palette.ink, flex: 1 }}>{v.value}</Text>
          </View>
        ))}
      </View>
    </>
  );
}

export function SourcesPage({ vm, palette }: {
  vm: { intro: string; rows: readonly SourceDateRowVM[]; footnote: string }; palette: Palette;
}) {
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.sub }}>{vm.intro}</Text>
      <View>
        {vm.rows.map((r) => (
          <View key={r.k} testID={`source-${r.k}`}
                style={{ paddingVertical: SPACE.s3, gap: 2, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.name}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{r.what}</Text>
            <Text style={{ ...TYPE.metaBold, fontSize: 12.5, color: palette.ink }}>{r.when}</Text>
          </View>
        ))}
      </View>
      <Text style={{ ...TYPE.small, color: palette.ink }}>{vm.footnote}</Text>
    </>
  );
}

export function ContactPage({ vm, palette, onReport }: { vm: ContactVM; palette: Palette; onReport: () => void }) {
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.intro}</Text>
      {vm.kind === "none"
        ? <Text testID="contact-placeholder" style={{ ...TYPE.body, color: palette.sub }}>{vm.text}</Text>
        : (
          <Pressable onPress={() => open(vm.url)} accessibilityRole="link" accessibilityLabel={`${vm.label}: ${vm.value}`}
                     style={{ gap: 2 }}>
            <Text style={{ ...TYPE.section, color: palette.sub }}>{vm.label}</Text>
            <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{vm.value}</Text>
          </Pressable>
        )}
      <Button palette={palette} kind="ghost" label={MENU_WORDS.items.report.label} onPress={onReport} />
    </>
  );
}
