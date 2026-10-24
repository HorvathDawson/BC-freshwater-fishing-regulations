/**
 * The menu's reading pages — About, Sources and credits. Each draws one VM from core/menu.ts and decides nothing; a link
 * only says where it goes. (Owner 2026-10-22) Display and Contact are drawn on the More tab itself (`MoreScreen`).
 */
import { Linking, Pressable, Text, View } from "react-native";
import type { AboutVM, SourcesVM } from "@app/core";
import { Credits } from "../Credits";
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

/**
 * (T167) SOURCES AND CREDITS: references only — each source's name and who publishes it (its licence where it has one),
 * and the exact edition or tile build in use. No dates and no freshness: those are the More tab's list, drawn once.
 */
export function SourcesPage({ vm, palette }: { vm: SourcesVM; palette: Palette }) {
  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.sub }}>{vm.intro}</Text>
      <View style={{ borderBottomWidth: 1, borderBottomColor: palette.line }}>
        {vm.rows.map((r) => (
          <View key={r.k} testID={`source-${r.k}`}
                style={{ paddingVertical: SPACE.s3 - 2, gap: 1, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.name}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{r.from}</Text>
            {/* the edition in words; the tile build in figures (a hash is read character by character) */}
            {r.detail !== null && (
              <Text selectable testID={`source-detail-${r.k}`}
                    style={r.k === "tiles" ? { ...TYPE.figure, fontSize: 12, color: palette.sub }
                                            : { ...TYPE.small, fontStyle: "italic", color: palette.sub }}>{r.detail}</Text>
            )}
          </View>
        ))}
      </View>
      {/* (T43) every fish picture the app can show, credited once (the regs page's foot lists its own) */}
      <Credits palette={palette} ink={palette.sub} lines={vm.pictures.lines} label={vm.pictures.label} testID="sources-fish-credits" />
      {/* (N1) the icon set's ISC notice: the licence asks every copy to carry it */}
      <Credits palette={palette} ink={palette.sub} lines={vm.icons.lines} label={vm.icons.label} testID="sources-icon-credits" />
    </>
  );
}
