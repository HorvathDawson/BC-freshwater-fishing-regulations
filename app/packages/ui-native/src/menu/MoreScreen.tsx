/**
 * THE MORE TAB (T48; owner 2026-10-22 refinement pass; T167 2026-10-23: "details … simplified … references to a
 * separate thing … this stuff formatted into the main tab without adding much bulk … desktop feels a little lacking").
 * (T703, owner 2026-11-13) A short list: every control moved into the Settings screen, and the update lines above the
 * table that repeated it are gone. Top to bottom:
 *
 *   back <tab>  back to the tab More was opened from (the route's `prev`; the system back does the same)
 *   DATA        ALWAYS SHOWN, compact, under ONE summary badge: the refresh button (its state in words only when it asks
 *               something), what is NOT in the table below (a newer release, a release this app cannot use, a source that
 *               failed), the in-season changes' warning (not read by this app) with the Province's link, then ONE ROW PER
 *               SOURCE — a freshness dot, its name, its date or age, and the state in words only where it asks the reader
 *               to look ("4h ago · Likely out of date"). The table is the one place a source's age is said.
 *   rows        Settings (opens `SettingsPage`: Data, Privacy, Display), then Glossary, Sources and credits, About,
 *               Report a problem: each opens a page over the tab (`MenuHost`)
 *   CONTACT     inline at the bottom, never a nested page
 *
 * On a wide screen (tablet, desktop) the data stands in its own column beside the rest (`MORE_WIDE`), each block a boxed
 * card on the wash; a phone draws one edge-to-edge column. Draws `menuVM()` (core/more.ts) and decides nothing. MOVED,
 * not copied: there is one list of these pages in the app (tools/one-menu.test.ts).
 */
import { createContext, useContext, useState } from "react";
import { ScrollView, View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { useLayoutClass, useNotifications } from "@app/ui";
import { menuVM, syncRowWords, WORDS, type DataInput, type MenuPageKey, type MoreVM, type SyncVM } from "@app/core";
import { SyncButton, TOP_BAR } from "../TopBar";
import { TYPE } from "../type";
import { GUTTER, MORE_WIDE, SPACE, type Palette, type ThemeName } from "../theme";
import { FreshDot, FreshBadge } from "./Fresh";
import { Icon } from "../icons";
import { hitProps } from "../regs/hit";
import { linkTo } from "../openLink";
import { SettingsPage, type MobileDataControl } from "./SettingsPage";

export type { MobileDataControl };

export interface MoreScreenProps {
  palette: Palette;
  /** what the Data block reads: the release's dates, the tiles' pair, the feed's age, a newer release */
  data: DataInput;
  /** the name of the tab the ‹ returns to, and the one back */
  back: string;
  onBack: () => void;
  /** the app's ONE contact value (config), or null */
  contact: string | null;
  /** the palette in use and how to change it (App's theme state) */
  theme: ThemeName;
  onTheme: (t: ThemeName) => void;
  /** open a page over the tab */
  onPick: (page: MenuPageKey) => void;
  /** (T300) the SYNC BUTTON of the map's top bar — its state (`syncVM`) and its tap — beside the sources; absent: none */
  sync?: SyncVM | null;
  onSync?: () => void;
  /**
   * (T231, T298) the mobile-data setting — Always / Ask / Wi-Fi only — and how to change it: a phone only (the web
   * downloads no large set); absent: no row
   */
  metered?: MobileDataControl | null;
}

/** A wide screen: the blocks are boxed cards in columns, not edge-to-edge bands. */
const Wide = createContext(false);

export function MoreScreen({ palette, data, back, onBack, contact, theme, onTheme, onPick, metered = null, sync = null, onSync }: MoreScreenProps) {
  // (T234) a phone that can notify lists More → Notifications
  const vm = menuVM({ data, back, contact, notifications: useNotifications() !== null });
  const wide = useLayoutClass() !== "phone";
  // (T703) the Settings screen: opened here (it holds no state the Shell needs); a back layer while open (MenuPage)
  const [settings, setSettings] = useState(false);
  const dataBlock = <DataBlock vm={vm.data} palette={palette} sync={sync} onSync={onSync} />;
  const rest = (
    <>
      <Pages vm={vm} palette={palette} onPick={onPick} onSettings={() => setSettings(true)} />
      <ContactBlock vm={vm.contact} palette={palette} />
    </>
  );
  return (
    <Wide.Provider value={wide}>
      <View testID="more-screen" style={{ flex: 1, backgroundColor: palette.wash }}>
        <ScrollView contentContainerStyle={{ paddingBottom: SPACE.s4 * 2 }}>
          <Head vm={vm} palette={palette} onBack={onBack} />
          {wide
            ? (
              <View testID="more-columns"
                    style={{ width: "100%", maxWidth: MORE_WIDE.maxWidth + 2 * GUTTER, alignSelf: "center",
                             paddingHorizontal: GUTTER, flexDirection: "row", alignItems: "flex-start", gap: MORE_WIDE.gap }}>
                <View testID="more-column-data" style={{ flex: MORE_WIDE.data, minWidth: 0 }}>{dataBlock}</View>
                <View testID="more-column-rest" style={{ flex: 1, minWidth: 0 }}>{rest}</View>
              </View>
            )
            : <>{dataBlock}{rest}</>}
        </ScrollView>
        <SettingsPage open={settings} onBack={() => setSettings(false)} palette={palette} theme={theme} onTheme={onTheme}
                      metered={metered} />
      </View>
    </Wide.Provider>
  );
}

/** "‹ Map" over the tab's title — the menu pages' head shape (MenuPage); on a wide screen its words sit on the columns' edge. */
function Head({ vm, palette, onBack }: { vm: MoreVM; palette: Palette; onBack: () => void }) {
  const wide = useContext(Wide);
  return (
    <View style={{ backgroundColor: palette.card, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      <View style={{ width: "100%", maxWidth: wide ? MORE_WIDE.maxWidth + 2 * GUTTER : undefined, alignSelf: "center",
                     paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, gap: SPACE.s1 }}>
        <Pressable {...hitProps({ w: 60, h: 40 })} testID="more-back" onPress={onBack} accessibilityRole="button" accessibilityLabel={vm.back.label} style={{ alignSelf: "flex-start", minHeight: 40, flexDirection: "row", alignItems: "center", gap: 2 }}>
          <Icon name="back" colour={palette.accent} size={16} />
          <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }}>{vm.back.to}</Text>
        </Pressable>
        <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{vm.title}</Text>
      </View>
    </View>
  );
}

/** A section's kicker over its card. */
function Kicker({ palette, children, right }: { palette: Palette; children: string; right?: React.ReactNode }) {
  const wide = useContext(Wide);
  return (
    <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: SPACE.s2,
                   paddingHorizontal: wide ? 0 : GUTTER, paddingTop: SPACE.s4 + SPACE.s1, paddingBottom: SPACE.s2 }}>
      {/* one height for every kicker, badge or none, so the two columns' kickers sit on one line */}
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub, lineHeight: 18 }}>{children}</Text>
      {right && <View style={{ height: 18, justifyContent: "center" }}>{right}</View>}
    </View>
  );
}

/** A card of rows: ruled top and bottom in `line`, edge to edge on a phone; boxed on a wide screen. */
function Group({ palette, children, testID }: { palette: Palette; children: React.ReactNode; testID?: string }) {
  const wide = useContext(Wide);
  return (
    <View testID={testID} style={{ backgroundColor: palette.card, borderTopWidth: 1, borderBottomWidth: 1,
                                   borderLeftWidth: wide ? 1 : 0, borderRightWidth: wide ? 1 : 0,
                                   borderColor: palette.line }}>
      {children}
    </View>
  );
}

function Chevron({ palette }: { palette: Palette }) {
  return (
    <Icon name="next" colour={palette.faint} size={18} />
  );
}

/**
 * THE DATA BLOCK: the summary badge on the kicker's line; a newer release, then the in-season warning (the one source
 * the app does not read — a ruled note with the Province's link); then one row per source: dot, name, and on the right
 * its date or age, with the state in words after it when stale (drawn in ink so the eye finds it). Each row says its
 * state aloud in full ("Live gauge readings: 4h ago. Likely out of date.").
 */
function DataBlock({ vm, palette, sync, onSync }: { vm: MoreVM["data"]; palette: Palette;
                                              sync: SyncVM | null; onSync?: () => void }) {
  // a wide row is long from name to date: a hairline between rows carries the eye across (a phone's rows are short)
  const ruled = useContext(Wide);
  return (
    <>
      <Kicker palette={palette}
              right={<FreshBadge palette={palette} testID="data-summary"
                                 badge={{ kind: vm.summary.kind, label: vm.summary.label }} />}>
        {vm.title}
      </Kicker>
      <Group palette={palette} testID="more-data">
        {/* (T300) THE MAP'S SYNC BUTTON, the same component in the same states; a tap checks now (or opens what failed).
            (T703) beside it its NAME while all is well — the table below says every age — and its state in words only when
            it asks something (checking, downloading, a failure, a newer release) */}
        {sync && (
          <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, paddingLeft: GUTTER, paddingTop: SPACE.s2 }}>
            <SyncButton palette={palette} sync={sync} onPress={onSync} bare />
            <Text testID="more-sync-label" numberOfLines={2} style={{ ...TYPE.small, color: palette.sub, flex: 1 }}>{syncRowWords(sync)}</Text>
          </View>
        )}
        {/* (T703) only what the table does not say: a release this app cannot use, a download (when there is no sync button
            to say it), and a source that failed */}
        {vm.updates && <UpdateLines vm={vm.updates} palette={palette} showRelease={!sync} indent={sync ? TOP_BAR.slot + 4 + SPACE.s2 : 0} />}
        {/* a newer release: the sync button's words say it, and where there is none, this does */}
        {vm.newRelease !== null && !sync && (
          <View testID="data-new-release"
                style={{ marginHorizontal: GUTTER, marginTop: SPACE.s3, paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3,
                         borderLeftWidth: 3, borderLeftColor: palette.attention, backgroundColor: palette.tint }}>
            <Text style={{ ...TYPE.small, color: palette.ink }}>{vm.newRelease}</Text>
          </View>
        )}
        <View testID="data-inseason"
              style={{ marginHorizontal: GUTTER, marginTop: SPACE.s3, paddingVertical: SPACE.s2 + 2, paddingHorizontal: SPACE.s3,
                       gap: 2, borderLeftWidth: 3, borderLeftColor: palette.attention, backgroundColor: palette.tint }}>
          <Text style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink }}>{vm.inseason.title}</Text>
          <Text style={{ ...TYPE.small, color: palette.ink }}>{vm.inseason.text}</Text>
          <Pressable {...hitProps({ w: 120, h: 28 })} testID="data-inseason-link" onPress={linkTo(vm.inseason.link.url)}
                     accessibilityRole="link" accessibilityLabel={vm.inseason.link.label}
                     style={{ alignSelf: "flex-start", minHeight: 28, justifyContent: "center" }}>
            <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }}>{`${vm.inseason.link.label} ${WORDS.LINK_OUT}`}</Text>
          </Pressable>
        </View>
        <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s2, paddingBottom: SPACE.s2 }}>
          {vm.rows.map((r, i) => {
            const stale = r.badge.kind === "stale";
            return (
              <View key={r.k} testID={`data-row-${r.k}`} accessible role="group" accessibilityLabel={r.label}
                    style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, minHeight: ruled ? 34 : 32,
                             borderTopWidth: ruled && i ? 1 : 0, borderTopColor: palette.line }}>
                <FreshDot palette={palette} kind={r.badge.kind} />
                <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink, flexShrink: 0 }}>
                  {r.name}
                </Text>
                <Text numberOfLines={1}
                      style={{ ...(stale ? TYPE.metaBold : TYPE.meta), color: stale ? palette.ink : palette.sub,
                               flex: 1, textAlign: "right" }}>
                  {r.shown}
                </Text>
              </View>
            );
          })}
        </View>
        {/* (T253) the web's background prefetch, quietly: which regions an unnamed-stream tap reads from the device */}
        {vm.offline && (
          <Text testID="data-offline" style={{ ...TYPE.small, color: palette.faint, paddingHorizontal: GUTTER,
                                               paddingBottom: SPACE.s3, ...(vm.updates ? {} : { paddingTop: SPACE.s2 }) }}>
            {vm.offline}
          </Text>
        )}
      </Group>
    </>
  );
}

/**
 * (T231, T428, T703) WHAT THE TABLE DOES NOT SAY about the update service, as plain small lines (nothing there looks
 * tappable; the one tap is the sync button): a release this app cannot use (`notice`), a download under way, waiting or
 * ready (`release`, only without the sync button, whose words say it) and each source that FAILED, in words. Nothing
 * when none holds: "Checked 3 min ago" and each source's check time repeated the table's ages. A failure is never hidden.
 */
function UpdateLines({ vm, palette, indent, showRelease }: { vm: NonNullable<MoreVM["data"]["updates"]>; palette: Palette; indent: number;
                                                           showRelease: boolean }) {
  const lines = [
    ...(vm.notice ? [{ k: "notice", text: vm.notice, problem: true }] : []),
    ...(showRelease && vm.release ? [{ k: "release", text: `${vm.title}: ${vm.release}`, problem: false }] : []),
    ...vm.sources.filter((s) => s.problem).map((s) => ({ k: s.k, text: s.shown, problem: true })),
  ];
  if (lines.length === 0) return null;
  return (
    <View testID="data-updates" accessible role="group" accessibilityLabel={lines.map((l) => l.text).join(" ")}
          style={{ paddingLeft: GUTTER + indent, paddingRight: GUTTER, paddingTop: SPACE.s1, paddingBottom: SPACE.s2, gap: 2 }}>
      {lines.map((l) => (
        <Text key={l.k} testID={`data-updates-${l.k}`} style={{ ...TYPE.small, color: l.problem ? palette.ink : palette.sub }}>{l.text}</Text>
      ))}
    </View>
  );
}

/** The pages that open over the tab, as rows: a label and its one-line hint. */
function Pages({ vm, palette, onPick, onSettings }: { vm: MoreVM; palette: Palette; onPick: (k: MenuPageKey) => void;
                 onSettings: () => void }) {
  // (T703) Settings first: not a `MenuPageKey` (More opens it itself), drawn as the same row
  const rows = [{ k: "settings" as const, label: vm.settings.label, hint: vm.settings.hint, open: onSettings },
                ...vm.rows.map((r) => ({ ...r, open: () => onPick(r.k) }))];
  return (
    <>
      <View style={{ height: SPACE.s4 + SPACE.s2 }} />
      <Group palette={palette}>
        {rows.map((it, i) => (
          <Pressable {...hitProps({ w: 300, h: 56 })} key={it.k} testID={`menu-item-${it.k}`} onPress={it.open}
                     accessibilityRole="button" accessibilityLabel={it.label} accessibilityHint={it.hint}
                     style={({ pressed }) => ({ flexDirection: "row", alignItems: "center", gap: SPACE.s3,
                                                paddingVertical: SPACE.s3, paddingHorizontal: GUTTER, minHeight: 56,
                                                borderTopWidth: i ? 1 : 0, borderTopColor: palette.line,
                                                backgroundColor: pressed ? palette.tint : palette.card })}>
            <View style={{ flex: 1, gap: 1 }}>
              <Text style={{ ...TYPE.bodyStrong, fontSize: 15.5, color: palette.ink }}>{it.label}</Text>
              <Text style={{ ...TYPE.small, color: palette.sub }}>{it.hint}</Text>
            </View>
            <Chevron palette={palette} />
          </Pressable>
        ))}
      </Group>
    </>
  );
}

/** CONTACT, inline at the bottom (owner: "contact could just be at the bottom not in a nested tab"). */
function ContactBlock({ vm, palette }: { vm: MoreVM["contact"]; palette: Palette }) {
  const c = vm.vm;
  return (
    <>
      <Kicker palette={palette}>{vm.title}</Kicker>
      <Group palette={palette} testID="more-contact">
        <View style={{ paddingHorizontal: GUTTER, paddingVertical: SPACE.s3, gap: SPACE.s2 }}>
          <Text style={{ ...TYPE.body, color: palette.ink }}>{c.intro}</Text>
          {c.kind === "none"
            ? <Text testID="contact-placeholder" style={{ ...TYPE.small, color: palette.sub }}>{c.text}</Text>
            : (
              <Pressable {...hitProps({ w: 120, h: 32 })} onPress={linkTo(c.url)} accessibilityRole="link"
                         accessibilityLabel={`${c.label}: ${c.value}`}
                         style={{ flexDirection: "row", alignItems: "baseline", gap: SPACE.s2, minHeight: 32 }}>
                <Text style={{ ...TYPE.section, color: palette.sub }}>{c.label}</Text>
                <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{c.value}</Text>
              </Pressable>
            )}
        </View>
      </Group>
    </>
  );
}
