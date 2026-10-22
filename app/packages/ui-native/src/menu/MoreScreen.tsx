/**
 * THE MORE TAB (T48; owner 2026-10-22 refinement pass: "inside the more tab we should format it nicer"). Top to bottom:
 *
 *   ‹ <tab>     back to the tab More was opened from (the route's `prev`; the system back does the same)
 *   DATA        ALWAYS SHOWN, minimal: one line per source — a freshness dot, its name, its date in few words — under
 *               ONE summary badge in words; the whole block opens the detailed references (Data sources & dates)
 *   DISPLAY     the app's colours as three preview tiles (a radiogroup): each tile is a miniature of that palette — its
 *               ground, a card and the three water status lines — so the choice is seen, not read
 *   rows        Glossary, About, Report a problem: each opens a page over the tab (`MenuHost`, a back layer)
 *   CONTACT     inline at the bottom, never a nested page
 *
 * Draws `menuVM()` (core/more.ts) and decides nothing. MOVED, not copied: there is one list of these pages in the app
 * (tools/one-menu.test.ts).
 */
import { Linking, Pressable, ScrollView, Text, View } from "react-native";
import { useLayoutClass } from "@app/ui";
import { menuVM, WATER_STATUSES, type DataInput, type MenuPageKey, type MoreVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, SHEET_DIALOG, SPACE, THEMES, type Palette, type ThemeName } from "../theme";
import { FreshDot, FreshBadge } from "./Fresh";

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
  /** open a page over the tab: a row, or the Data block ("sources") */
  onPick: (page: MenuPageKey) => void;
}

export function MoreScreen({ palette, data, back, onBack, contact, theme, onTheme, onPick }: MoreScreenProps) {
  const vm = menuVM({ data, back, contact });
  const wide = useLayoutClass() !== "phone";
  return (
    <View testID="more-screen" style={{ flex: 1, backgroundColor: palette.wash }}>
      <ScrollView contentContainerStyle={{ paddingBottom: SPACE.s4 * 2 }}>
        {/* one column, a dialog's width on a wide screen — ruled at its sides there, so its cards do not float */}
        <View style={{ width: "100%", maxWidth: SHEET_DIALOG.maxWidth, alignSelf: "center",
                       ...(wide ? { borderLeftWidth: 1, borderRightWidth: 1, borderColor: palette.line } : {}) }}>
          <Head vm={vm} palette={palette} onBack={onBack} />
          <DataBlock vm={vm.data} palette={palette} onOpen={() => onPick("sources")} />
          <DisplayTiles vm={vm.display} palette={palette} theme={theme} onTheme={onTheme} />
          <View style={{ height: SPACE.s4 + SPACE.s2 }} />
          <Group palette={palette}>
            {vm.rows.map((it, i) => (
              <Pressable key={it.k} testID={`menu-item-${it.k}`} onPress={() => onPick(it.k)}
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
          <ContactBlock vm={vm.contact} palette={palette} />
        </View>
      </ScrollView>
    </View>
  );
}

/** "‹ Map" over the tab's title — the menu pages' head shape (MenuPage), so a page and its tab read as one place. */
function Head({ vm, palette, onBack }: { vm: MoreVM; palette: Palette; onBack: () => void }) {
  return (
    <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, gap: SPACE.s1,
                   backgroundColor: palette.card, borderBottomWidth: 1.5, borderBottomColor: palette.ink }}>
      <Pressable testID="more-back" onPress={onBack} accessibilityRole="button" accessibilityLabel={vm.back.label}
                 hitSlop={8} style={{ alignSelf: "flex-start", minHeight: 32, justifyContent: "center" }}>
        <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }}>{`‹  ${vm.back.to}`}</Text>
      </Pressable>
      <Text accessibilityRole="header" style={{ ...TYPE.screen, color: palette.ink }}>{vm.title}</Text>
    </View>
  );
}

/** A section's kicker over its card. */
function Kicker({ palette, children, right }: { palette: Palette; children: string; right?: React.ReactNode }) {
  return (
    <View style={{ flexDirection: "row", alignItems: "flex-end", justifyContent: "space-between", gap: SPACE.s2,
                   paddingHorizontal: GUTTER, paddingTop: SPACE.s4 + SPACE.s1, paddingBottom: SPACE.s2 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{children}</Text>
      {right}
    </View>
  );
}

/** A card of rows: ruled top and bottom in `line`, on the card ground, edge to edge on a phone. */
function Group({ palette, children, testID }: { palette: Palette; children: React.ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={{ backgroundColor: palette.card, borderTopWidth: 1, borderBottomWidth: 1,
                                   borderColor: palette.line }}>
      {children}
    </View>
  );
}

function Chevron({ palette }: { palette: Palette }) {
  return (
    <Text importantForAccessibility="no" aria-hidden
          style={{ ...TYPE.bodyStrong, fontSize: 20, lineHeight: 22, color: palette.faint }}>›</Text>
  );
}

/**
 * THE DATA BLOCK: the summary badge on the kicker's line; one line per source (dot, name, the date in few words); a
 * stale line's date drawn in ink so the eye finds it. ONE press target, the detailed references; each line says its
 * state aloud in full ("River flows: Checked 27h ago. Likely out of date").
 */
function DataBlock({ vm, palette, onOpen }: { vm: MoreVM["data"]; palette: Palette; onOpen: () => void }) {
  return (
    <>
      <Kicker palette={palette}
              right={<FreshBadge palette={palette} testID="data-summary"
                                 badge={{ kind: vm.summary.kind, label: vm.summary.label }} />}>
        {vm.title}
      </Kicker>
      <Group palette={palette}>
        <Pressable testID="more-data" onPress={onOpen} accessibilityRole="button"
                   accessibilityLabel={`${vm.detailsLabel}. ${vm.summary.label}`} accessibilityHint={vm.detailsHint}
                   style={({ pressed }) => ({ backgroundColor: pressed ? palette.tint : palette.card })}>
          {vm.newRelease !== null && (
            <View testID="data-new-release"
                  style={{ marginHorizontal: GUTTER, marginTop: SPACE.s3, paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3,
                           borderLeftWidth: 3, borderLeftColor: palette.attention, backgroundColor: palette.tint }}>
              <Text style={{ ...TYPE.small, color: palette.ink }}>{vm.newRelease}</Text>
            </View>
          )}
          <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s2 }}>
            {vm.lines.map((l) => {
              const stale = l.badge.kind === "stale";
              return (
                <View key={l.k} testID={`data-line-${l.k}`} accessible
                      accessibilityLabel={`${l.name}: ${l.when}. ${l.badge.label}`}
                      style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, minHeight: 34 }}>
                  <FreshDot palette={palette} kind={l.badge.kind} />
                  <Text numberOfLines={1} style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink, flexShrink: 0 }}>
                    {l.name}
                  </Text>
                  <Text numberOfLines={1}
                        style={{ ...(stale ? TYPE.metaBold : TYPE.meta), color: stale ? palette.ink : palette.sub,
                                 flex: 1, textAlign: "right" }}>
                    {l.when}
                  </Text>
                </View>
              );
            })}
          </View>
          <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "flex-end", gap: SPACE.s1,
                         paddingHorizontal: GUTTER, paddingTop: SPACE.s1, paddingBottom: SPACE.s3,
                         marginTop: SPACE.s1, borderTopWidth: 1, borderTopColor: palette.line }}>
            <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent, paddingTop: SPACE.s2 }}>
              {`${vm.details}  ›`}
            </Text>
          </View>
        </Pressable>
      </Group>
    </>
  );
}

/**
 * DISPLAY: the three palettes as preview tiles — each drawn in ITS OWN palette's tokens (its ground, its card, its ink
 * and its three water status colours), so the reader sees what they pick. A radiogroup: each tile a radio named by
 * its word; the one in use ringed in the CURRENT palette's accent with a filled mark.
 */
function DisplayTiles({ vm, palette, theme, onTheme }: {
  vm: MoreVM["display"]; palette: Palette; theme: ThemeName; onTheme: (t: ThemeName) => void;
}) {
  return (
    <>
      <Kicker palette={palette}>{vm.title}</Kicker>
      <Group palette={palette} testID="display-palette">
        <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s3, paddingBottom: SPACE.s3, gap: SPACE.s3 }}>
          <View accessibilityRole="radiogroup" accessibilityLabel={vm.palette}
                style={{ flexDirection: "row", gap: SPACE.s2 }}>
            {vm.options.map((o) => {
              const on = o.k === theme;
              const p = THEMES[o.k];
              return (
                <Pressable key={o.k} testID={`theme-${o.k}`} onPress={() => onTheme(o.k)} accessibilityRole="radio"
                           aria-checked={on} accessibilityLabel={o.t}
                           style={{ flex: 1, minWidth: 0, borderWidth: on ? 2 : 1, borderColor: on ? palette.accent : palette.line,
                                    padding: on ? 0 : 1, backgroundColor: palette.card }}>
                  {/* the miniature: that palette's ground, a card on it, a title stroke and the three status lines */}
                  <View style={{ height: 58, backgroundColor: p.wash, padding: 8, justifyContent: "flex-end" }}>
                    <View style={{ backgroundColor: p.card, borderWidth: 1, borderColor: p.line, padding: 6, gap: 4 }}>
                      <View style={{ width: "55%", height: 4, backgroundColor: p.ink }} />
                      {WATER_STATUSES.map((s, i) => (
                        <View key={s} style={{ width: `${88 - i * 18}%`, height: 3, backgroundColor: p.waterStatus[s] }} />
                      ))}
                    </View>
                  </View>
                  <View style={{ flexDirection: "row", alignItems: "center", gap: 6, paddingHorizontal: 8, paddingVertical: 7,
                                 borderTopWidth: 1, borderTopColor: palette.line }}>
                    <View style={{ width: 14, height: 14, borderRadius: 7, borderWidth: 1.5,
                                   borderColor: on ? palette.accent : palette.faint, alignItems: "center",
                                   justifyContent: "center" }}>
                      {on && <View style={{ width: 6, height: 6, borderRadius: 3, backgroundColor: palette.accent }} />}
                    </View>
                    <Text numberOfLines={1} style={{ ...TYPE.metaBold, flexShrink: 1, color: on ? palette.ink : palette.sub }}>
                      {o.t}
                    </Text>
                  </View>
                </Pressable>
              );
            })}
          </View>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{vm.note}</Text>
        </View>
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
              <Pressable onPress={() => { void Linking.openURL(c.url); }} accessibilityRole="link"
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
