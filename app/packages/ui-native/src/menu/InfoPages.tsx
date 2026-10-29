/**
 * The menu's reading pages — About, Sources and credits. Each draws one VM from core/menu/ and decides nothing; a link
 * only says where it goes. (Owner 2026-10-22) Display and Contact are drawn on the More tab itself (`MoreScreen`).
 */
import { Linking, Pressable, Text, View } from "react-native";
import { LICENCE_WORDS, licencesVM, MENU_WORDS, UPDATE_LOG_WORDS, WORDS, type AboutEnvVM, type AboutVM, type AppEnv, type SourcesVM } from "@app/core";
import { Credits } from "../Credits";
import { LicencesList } from "./Licences";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

const open = (url: string) => { void Linking.openURL(url); };

export function AboutPage({ vm, palette, onEnv, onSources, onUpdatesLog, onReport }: { vm: AboutVM; palette: Palette;
                            onEnv?: (env: AppEnv) => void;
                            /** (T237) open Report a problem */ onReport?: () => void;
                            /** (T235) open Sources and licences */ onSources?: () => void;
                            /** (T251) open the Updates log (development and staging builds) */ onUpdatesLog?: () => void }) {
  return (
    <>
      {vm.env && <EnvBadge vm={vm.env} palette={palette} />}
      <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.what}</Text>
      {/* THE AUTHORITY DISCLAIMER: boxed in ink so it cannot be read as a footnote */}
      <View testID="about-not-official" style={{ borderWidth: 1.5, borderColor: palette.ink, padding: SPACE.s3,
                                                 backgroundColor: palette.tint }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.notOfficial}</Text>
      </View>
      <Pressable onPress={() => open(vm.official.url)} accessibilityRole="link" accessibilityLabel={vm.official.label}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{`${vm.official.label} ${WORDS.LINK_OUT}`}</Text>
      </Pressable>
      {onSources && (
        <Pressable testID="about-sources" onPress={onSources} accessibilityRole="link" accessibilityLabel={LICENCE_WORDS.fromAbout}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{LICENCE_WORDS.fromAboutLink}</Text>
        </Pressable>
      )}
      {onReport && (
        <Pressable testID="about-report" onPress={onReport} accessibilityRole="link" accessibilityLabel={MENU_WORDS.report.fromAbout}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{MENU_WORDS.report.fromAbout}</Text>
        </Pressable>
      )}
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
      {vm.env && <EnvBlock vm={vm.env} palette={palette} onEnv={onEnv} />}
      {onUpdatesLog && (
        <Pressable testID="about-updates-log" onPress={onUpdatesLog} accessibilityRole="link"
                   accessibilityLabel={UPDATE_LOG_WORDS.aboutLink}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{UPDATE_LOG_WORDS.aboutLinkShown}</Text>
        </Pressable>
      )}
    </>
  );
}

/** (T232) The data source's badge, small, at the head of About — a development or staging build only. */
function EnvBadge({ vm, palette }: { vm: AboutEnvVM; palette: Palette }) {
  return (
    <View style={{ flexDirection: "row" }}>
      <Text testID="about-env-badge" accessibilityLabel={`${vm.label}: ${vm.value}`}
            style={{ ...TYPE.small, fontSize: 11, letterSpacing: 0.6, color: palette.sub, borderWidth: 1,
                     borderColor: palette.line, paddingHorizontal: 6, paddingVertical: 1 }}>{vm.badge}</Text>
    </View>
  );
}

/** (T232) The data source: where this build reads, and in a development build the switch (applied on the next start). */
function EnvBlock({ vm, palette, onEnv }: { vm: AboutEnvVM; palette: Palette; onEnv?: (env: AppEnv) => void }) {
  return (
    <View testID="about-env" style={{ gap: SPACE.s2 }}>
      <View style={{ flexDirection: "row", gap: SPACE.s3, paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
        <Text style={{ ...TYPE.small, color: palette.sub, width: 104 }}>{vm.label}</Text>
        <Text selectable testID="about-env-value"
              style={{ ...TYPE.figure, fontSize: 12.5, color: palette.ink, flex: 1 }}>{vm.value}</Text>
      </View>
      {vm.choose && onEnv && (
        <View accessibilityRole="radiogroup" accessibilityLabel={vm.choose.title} style={{ gap: SPACE.s2 }}>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{vm.choose.title}</Text>
          <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
            {vm.choose.choices.map((c) => (
              <Pressable key={c.k} testID={`about-env-${c.k}`} onPress={() => onEnv(c.k)} accessibilityRole="radio"
                         aria-checked={c.on} accessibilityLabel={c.label}
                         style={{ flex: 1, alignItems: "center", paddingVertical: SPACE.s2, borderWidth: c.on ? 1.5 : 1,
                                  borderColor: c.on ? palette.ink : palette.line,
                                  backgroundColor: c.on ? palette.tint : "transparent" }}>
                <Text style={{ ...TYPE.small, color: c.on ? palette.ink : palette.sub }}>{c.label}</Text>
              </Pressable>
            ))}
          </View>
        </View>
      )}
      {vm.note && <Text testID="about-env-note" style={{ ...TYPE.small, color: palette.sub }}>{vm.note}</Text>}
    </View>
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
      {/* (T235) every dataset's licence and the words it asks for */}
      <LicencesList vm={licencesVM()} palette={palette} />
    </>
  );
}
