/**
 * The menu's reading pages — About, Sources and credits. Each draws one VM from core/menu/ and decides nothing; a link
 * only says where it goes. (Owner 2026-10-22) Display and Contact are drawn on the More tab itself (`MoreScreen`).
 */
import { View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { CHANGELOG_WORDS, changelogVM, LICENCE_WORDS, licencesVM, MENU_WORDS, PRIVACY_URL, WORDS, type AboutEnvVM, type AboutVM, type SourcesVM } from "@app/core";
import { Credits } from "../Credits";
import { LicencesList } from "./Licences";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";
import { hitProps } from "../regs/hit";
import { openOfficial } from "./openOfficial";
import { openLink } from "../openLink";

const open = (url: string) => { void openLink(url); };
/** (T285) the official Synopsis: its link on the web, our copy on a phone (openOfficial.web / .native) */
const officialAt = (url: string) => () => openOfficial(url);

/** (T243) a link on its own line is a 44 px row (the touch-target floor), its words centred in it */
const LINK_ROW = { minHeight: 44, justifyContent: "center" } as const;

export function AboutPage({ vm, palette, onSources, onReport, onChangelog }: { vm: AboutVM; palette: Palette;
                            /** (T234 v1.1) open What's new (every version) */ onChangelog?: () => void;
                            /** (T237) open Report a problem */ onReport?: () => void;
                            /** (T235) open Sources and licences */ onSources?: () => void }) {
  return (
    <>
      {vm.env && <EnvBadge vm={vm.env} palette={palette} />}
      <Text style={{ ...TYPE.body, color: palette.ink }}>{vm.what}</Text>
      {/* THE AUTHORITY DISCLAIMER: boxed in ink so it cannot be read as a footnote */}
      <View testID="about-not-official" style={{ borderWidth: 1.5, borderColor: palette.ink, padding: SPACE.s3,
                                                 backgroundColor: palette.tint }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{vm.notOfficial}</Text>
      </View>
      <Pressable {...hitProps({ w: 120, h: 44 })} onPress={officialAt(vm.official.url)} accessibilityRole="link" accessibilityLabel={vm.official.label} style={LINK_ROW}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{`${vm.official.label} ${WORDS.LINK_OUT}`}</Text>
      </Pressable>
      {onSources && (
        <Pressable {...hitProps({ w: 120, h: 44 })} testID="about-sources" onPress={onSources} accessibilityRole="link" accessibilityLabel={LICENCE_WORDS.fromAbout} style={LINK_ROW}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{LICENCE_WORDS.fromAboutLink}</Text>
        </Pressable>
      )}
      <Pressable {...hitProps({ w: 120, h: 44 })} testID="about-privacy" onPress={() => open(PRIVACY_URL)} accessibilityRole="link"
                 accessibilityLabel={MENU_WORDS.about.privacy} style={LINK_ROW}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{`${MENU_WORDS.about.privacy} ${WORDS.LINK_OUT}`}</Text>
      </Pressable>
      {onReport && (
        <Pressable {...hitProps({ w: 120, h: 44 })} testID="about-report" onPress={onReport} accessibilityRole="link" accessibilityLabel={MENU_WORDS.report.fromAbout} style={LINK_ROW}>
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
      {/* (T234 v1.1) every version's changes, tucked under the versions (never a More row) */}
      {onChangelog && (
        <Pressable {...hitProps({ w: 120, h: 44 })} testID="about-changelog" onPress={onChangelog} accessibilityRole="link"
                   accessibilityLabel={CHANGELOG_WORDS.open} accessibilityHint={CHANGELOG_WORDS.openHint} style={LINK_ROW}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{CHANGELOG_WORDS.open}</Text>
        </Pressable>
      )}
    </>
  );
}

/** (T232) The data source's badge, small, at the head of About — a development or staging build only (T476: the rest is the Developer card). */
function EnvBadge({ vm, palette }: { vm: AboutEnvVM; palette: Palette }) {
  return (
    <View style={{ flexDirection: "row" }}>
      <Text testID="about-env-badge" accessibilityLabel={`${vm.label}: ${vm.value}`}
            style={{ ...TYPE.small, fontSize: 11, letterSpacing: 0.6, color: palette.sub, borderWidth: 1,
                     borderColor: palette.line, paddingHorizontal: 6, paddingVertical: 1 }}>{vm.badge}</Text>
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
      <Pressable {...hitProps({ w: 120, h: 44 })} testID="sources-privacy" onPress={() => open(PRIVACY_URL)} accessibilityRole="link"
                 accessibilityLabel={MENU_WORDS.about.privacy} style={LINK_ROW}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{`${MENU_WORDS.about.privacy} ${WORDS.LINK_OUT}`}</Text>
      </Pressable>
    </>
  );
}

/**
 * (T234 v1.1) WHAT'S NEW — every version, newest first (core `changelogVM` over THE ONE changelog, `@app/core`
 * changelog.ts): a card per version, its date (or "Coming next"), the installed one marked, its bullets; a version with
 * none says so.
 */
export function ChangelogPage({ palette, installed }: { palette: Palette; installed: string | null }) {
  const rows = changelogVM(installed);
  return (
    <View testID="changelog" style={{ gap: SPACE.s3 }}>
      {rows.map((r) => (
        <View key={r.version} testID={`changelog-${r.version}`}
              style={{ gap: SPACE.s1, padding: SPACE.s3, borderWidth: r.installed ? 1.5 : 1,
                       borderColor: r.installed ? palette.ink : palette.line2, backgroundColor: palette.card }}>
          <View style={{ flexDirection: "row", alignItems: "baseline", gap: SPACE.s2, flexWrap: "wrap" }}>
            <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.title}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{r.when}</Text>
            {r.installed && (
              <Text testID="changelog-installed" style={{ ...TYPE.metaBold, color: palette.accent }}>{CHANGELOG_WORDS.installed}</Text>
            )}
          </View>
          {r.missing
            ? <Text style={{ ...TYPE.small, color: palette.sub }}>{r.missing}</Text>
            : r.notes.map((n, i) => <Text key={i} style={{ ...TYPE.small, color: palette.ink }}>{`• ${n}`}</Text>)}
        </View>
      ))}
    </View>
  );
}
