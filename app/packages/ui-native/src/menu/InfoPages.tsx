/**
 * The menu's reading pages — About, Data sources & dates. Each draws one VM from core/menu.ts and decides nothing; a link
 * only says where it goes. (Owner 2026-10-22) Display and Contact are drawn on the More tab itself (`MoreScreen`).
 */
import { Linking, Pressable, Text, View } from "react-native";
import type { AboutVM, SourceDateRowVM, SourcesPicturesVM } from "@app/core";
import { Credits } from "../Credits";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { FreshBadge } from "./Fresh";

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
  vm: { intro: string; rows: readonly SourceDateRowVM[]; footnote: string; pictures: SourcesPicturesVM; icons: SourcesPicturesVM };
  palette: Palette;
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
            {/* the date, then how fresh that makes it — the same badge as the More tab's Data block */}
            <View style={{ flexDirection: "row", alignItems: "center", flexWrap: "wrap", columnGap: SPACE.s3, rowGap: 2,
                           marginTop: 2 }}>
              <Text style={{ ...TYPE.metaBold, fontSize: 12.5, color: palette.ink }}>{r.when}</Text>
              <FreshBadge palette={palette} badge={r.badge} testID={`source-badge-${r.k}`} />
            </View>
          </View>
        ))}
      </View>
      <Text style={{ ...TYPE.small, color: palette.ink }}>{vm.footnote}</Text>
      {/* (T43) every fish picture the app can show, credited once (the regs page's foot lists its own) */}
      <Credits palette={palette} ink={palette.sub} lines={vm.pictures.lines} label={vm.pictures.label} testID="sources-fish-credits" />
      {/* (N1) the icon set's ISC notice: the licence asks every copy to carry it */}
      <Credits palette={palette} ink={palette.sub} lines={vm.icons.lines} label={vm.icons.label} testID="sources-icon-credits" />
    </>
  );
}
