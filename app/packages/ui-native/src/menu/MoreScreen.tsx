/**
 * THE MORE TAB (T48; owner 2026-10-22 refinement pass; T167 2026-10-23: "details … simplified … references to a
 * separate thing … this stuff formatted into the main tab without adding much bulk … desktop feels a little lacking").
 * Top to bottom:
 *
 *   back <tab>  back to the tab More was opened from (the route's `prev`; the system back does the same)
 *   DATA        ALWAYS SHOWN, compact, under ONE summary badge: the in-season changes' warning (not read by this app)
 *               with the Province's link, then one row per source — a freshness dot, its name, its date or age, and
 *               the state in words only where it asks the reader to look ("4h ago · Likely out of date")
 *   DISPLAY     the app's colours as three preview tiles (a radiogroup): each tile is a miniature of that palette — its
 *               ground, a card and the three water status lines — so the choice is seen, not read
 *   rows        Glossary, Sources and credits, About, Report a problem: each opens a page over the tab (`MenuHost`)
 *   CONTACT     inline at the bottom, never a nested page
 *
 * On a wide screen (tablet, desktop) the data stands in its own column beside the rest (`MORE_WIDE`), each block a boxed
 * card on the wash; a phone draws one edge-to-edge column. Draws `menuVM()` (core/more.ts) and decides nothing. MOVED,
 * not copied: there is one list of these pages in the app (tools/one-menu.test.ts).
 */
import { createContext, useContext } from "react";
import { Linking, Pressable, ScrollView, Text, View } from "react-native";
import { useLayoutClass } from "@app/ui";
import { menuVM, WATER_STATUS_LINE, WATER_STATUSES, WORDS, type DataInput, type MenuPageKey, type MoreVM } from "@app/core";
import { TYPE } from "../type";
import { GUTTER, MORE_WIDE, SPACE, THEMES, type Palette, type ThemeName } from "../theme";
import { FreshDot, FreshBadge } from "./Fresh";
import { DottedSwatch } from "../Chrome";
import { Icon } from "../icons";

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
}

/** A wide screen: the blocks are boxed cards in columns, not edge-to-edge bands. */
const Wide = createContext(false);

export function MoreScreen({ palette, data, back, onBack, contact, theme, onTheme, onPick }: MoreScreenProps) {
  const vm = menuVM({ data, back, contact });
  const wide = useLayoutClass() !== "phone";
  const dataBlock = <DataBlock vm={vm.data} palette={palette} />;
  const rest = (
    <>
      <DisplayTiles vm={vm.display} palette={palette} theme={theme} onTheme={onTheme} />
      <Pages vm={vm} palette={palette} onPick={onPick} />
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
        <Pressable testID="more-back" onPress={onBack} accessibilityRole="button" accessibilityLabel={vm.back.label}
                   hitSlop={8} style={{ alignSelf: "flex-start", minHeight: 32, flexDirection: "row", alignItems: "center", gap: 2 }}>
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
function DataBlock({ vm, palette }: { vm: MoreVM["data"]; palette: Palette }) {
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
        {vm.newRelease !== null && (
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
          <Pressable testID="data-inseason-link" onPress={() => { void Linking.openURL(vm.inseason.link.url); }}
                     accessibilityRole="link" accessibilityLabel={vm.inseason.link.label} hitSlop={6}
                     style={{ alignSelf: "flex-start", minHeight: 28, justifyContent: "center" }}>
            <Text style={{ ...TYPE.metaBold, fontSize: 13, color: palette.accent }}>{`${vm.inseason.link.label} ${WORDS.LINK_OUT}`}</Text>
          </Pressable>
        </View>
        <View style={{ paddingHorizontal: GUTTER, paddingTop: SPACE.s2, paddingBottom: SPACE.s2 }}>
          {vm.rows.map((r, i) => {
            const stale = r.badge.kind === "stale";
            return (
              <View key={r.k} testID={`data-row-${r.k}`} accessible accessibilityLabel={r.label}
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
      </Group>
    </>
  );
}

/** The pages that open over the tab, as rows: a label and its one-line hint. */
function Pages({ vm, palette, onPick }: { vm: MoreVM; palette: Palette; onPick: (k: MenuPageKey) => void }) {
  return (
    <>
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
                        WATER_STATUS_LINE[s] === "dotted"
                          ? <DottedSwatch key={s} colour={p.waterStatus[s]} width={36} />
                          : <View key={s} style={{ width: `${88 - i * 18}%`, height: 3, backgroundColor: p.waterStatus[s] }} />
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
