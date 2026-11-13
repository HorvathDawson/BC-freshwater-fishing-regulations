/**
 * (T703, owner 2026-11-13: "all the toggles for settings like crash reports or download source etc should be in a
 * settings menu in here") THE SETTINGS SCREEN, opened from the first row of the More tab. Every control the app has, MOVED
 * from the More tab unchanged (same words, same stored keys, same testIDs), under three headings, each a square card:
 *
 *   DATA     Download over mobile data (Always / Ask / Wi-Fi only: a phone), and the low-bandwidth mode (Automatic / On /
 *            Off, with what it is doing now: the website) — a heading with nothing to show is not drawn
 *   PRIVACY  Send crash reports (on by default; off sends nothing)
 *   DISPLAY  the app's colours as three preview tiles, each drawn in its own palette (a radiogroup)
 *
 * One component tree for the web and the phone. A menu page (`MenuPage`): a back layer, so the ‹, Android's back and the
 * browser's back all return to More. Decides nothing: the choices and their words are core's.
 */
import { Platform, View } from "react-native";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { useCrashSetting, useNet, useNetBps } from "@app/ui";
import { CRASH_WORDS, dataModeLine, MENU_WORDS, MOBILE_DATA_WORDS, NET_WORDS, WATER_STATUS_LINE, WATER_STATUSES, type MobileDataSetting } from "@app/core";
import { TYPE } from "../type";
import { SPACE, THEMES, type Palette, type ThemeName, round } from "../theme";
import { DottedSwatch } from "../Chrome";
import { hitProps } from "../regs/hit";
import { MenuPage } from "./MenuPage";

/** (T298) The mobile-data setting as Settings shows it: the choice, and how to change it. */
export interface MobileDataControl { value: MobileDataSetting; set: (s: MobileDataSetting) => void }

export interface SettingsPageProps {
  open: boolean;
  onBack: () => void;
  palette: Palette;
  /** the palette in use and how to change it (App's theme state) */
  theme: ThemeName;
  onTheme: (t: ThemeName) => void;
  /** the mobile-data setting: a phone only (the web downloads no large set); absent: no row */
  metered?: MobileDataControl | null;
}

/** A heading over its square card. */
function Section({ title, palette, testID, children }: { title: string; palette: Palette; testID: string; children: React.ReactNode }) {
  return (
    <View style={{ gap: SPACE.s2 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{title}</Text>
      <View testID={testID} style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.line }}>{children}</View>
    </View>
  );
}

export function SettingsPage({ open, onBack, palette, theme, onTheme, metered = null }: SettingsPageProps) {
  const W = MENU_WORDS.settings;
  const crash = useCrashSetting();
  const { net } = useNet();
  // (T298, owner 2026-10-30) the low-bandwidth mode is a website setting: on a phone every regulation is on the device, and
  // large downloads are the "Download over mobile data" setting's
  const low = !!net && Platform.OS === "web";
  return (
    <MenuPage open={open} title={W.label} onBack={onBack} palette={palette} testID="menu-page-settings">
      {open ? (
        <View style={{ gap: SPACE.s4 }}>
          {(metered || low) && (
            <Section title={W.sections.data} palette={palette} testID="settings-data">
              {metered && <MeteredSetting palette={palette} metered={metered} first />}
              {low && <LowDataRow palette={palette} first={!metered} />}
            </Section>
          )}
          {crash && (
            <Section title={W.sections.privacy} palette={palette} testID="settings-privacy">
              <CrashRow palette={palette} crash={crash} />
            </Section>
          )}
          <Section title={W.sections.display} palette={palette} testID="display-palette">
            <DisplayTiles palette={palette} theme={theme} onTheme={onTheme} />
          </Section>
        </View>
      ) : null}
    </MenuPage>
  );
}

const ROW = (palette: Palette, first: boolean) => ({
  paddingHorizontal: SPACE.s3, paddingVertical: SPACE.s3, gap: SPACE.s1,
  borderTopWidth: first ? 0 : 1, borderTopColor: palette.line,
}) as const;

/** (T298, owner 2026-10-29) the mobile-data setting: Always / Ask / Wi-Fi only (the first download's answer lands here). */
function MeteredSetting({ palette, metered, first }: { palette: Palette; metered: MobileDataControl; first: boolean }) {
  const C = MOBILE_DATA_WORDS;
  return (
    <View testID="data-updates-metered" style={ROW(palette, first)}>
      <Text style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink }}>{C.label}</Text>
      <View accessibilityRole="radiogroup" accessibilityLabel={C.label}
            style={{ flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" }}>
        {(["always", "ask", "wifi"] as const).map((k) => {
          const on = metered.value === k;
          return (
            <Pressable {...hitProps({ w: 64, h: 40 })} key={k} testID={`data-mobile-${k}`} onPress={() => metered.set(k)}
                       accessibilityRole="radio" aria-checked={on} accessibilityLabel={`${C.label}: ${C.choices[k]}`}
                       accessibilityHint={C.hint}
                       style={({ pressed }) => ({ minHeight: 40, minWidth: 64, paddingHorizontal: SPACE.s3,
                                                  justifyContent: "center", alignItems: "center", borderWidth: 1.5,
                                                  borderColor: on ? palette.accent : palette.line,
                                                  backgroundColor: on || pressed ? palette.tint : palette.card })}>
              <Text style={{ ...(on ? TYPE.metaBold : TYPE.meta), color: on ? palette.accent : palette.ink }}>{C.choices[k]}</Text>
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

/**
 * (T254) THE LOW-BANDWIDTH SETTING: Automatic (the app decides from the link it measures), On, Off — a three-way
 * choice, and under it what the mode is doing now and why ("On now — slow connection (0.4 Mbit/s)"). Kept on the device.
 */
function LowDataRow({ palette, first }: { palette: Palette; first: boolean }) {
  const { mode, setting, setSetting, net } = useNet();
  const bps = useNetBps(net);
  const W = NET_WORDS.setting;
  return (
    <View testID="data-low" style={ROW(palette, first)}>
      <Text style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink }}>{W.label}</Text>
      <Text style={{ ...TYPE.small, color: palette.sub }}>{W.hint}</Text>
      <View accessibilityRole="radiogroup" accessibilityLabel={W.label}
            style={{ flexDirection: "row", gap: SPACE.s2, marginTop: SPACE.s1, flexWrap: "wrap" }}>
        {(["auto", "on", "off"] as const).map((k) => {
          const on = setting === k;
          return (
            <Pressable {...hitProps({ w: 64, h: 40 })} key={k} testID={`data-low-${k}`} onPress={() => setSetting(k)} accessibilityRole="radio"
                       aria-checked={on} accessibilityLabel={`${W.label}: ${W.choices[k]}`}
                       style={({ pressed }) => ({ minHeight: 40, minWidth: 64, paddingHorizontal: SPACE.s3, justifyContent: "center",
                                                  alignItems: "center", borderWidth: 1.5,
                                                  borderColor: on ? palette.accent : palette.line,
                                                  backgroundColor: on ? palette.tint : pressed ? palette.tint : palette.card })}>
              <Text style={{ ...(on ? TYPE.metaBold : TYPE.meta), color: on ? palette.accent : palette.ink }}>{W.choices[k]}</Text>
            </Pressable>
          );
        })}
      </View>
      <Text testID="data-low-now" style={{ ...TYPE.small, color: mode.low ? palette.ink : palette.faint }}>
        {dataModeLine(mode, bps)}
      </Text>
    </View>
  );
}

/**
 * (T242) THE CRASH-REPORT SWITCH: on by default; off sends nothing (and forgets an error kept for the next start). Not
 * drawn where the app has no reporter (a screen mounted alone in a test, a build with no report address).
 */
function CrashRow({ palette, crash }: { palette: Palette; crash: NonNullable<ReturnType<typeof useCrashSetting>> }) {
  const W = CRASH_WORDS;
  return (
    <View testID="data-crash" style={ROW(palette, true)}>
      <Pressable {...hitProps({ w: 200, h: 40 })} testID="data-crash-switch" onPress={() => crash.set(!crash.on)}
                 accessibilityRole="switch" aria-checked={crash.on} accessibilityLabel={W.label} accessibilityHint={W.hint}
                 style={{ flexDirection: "row", alignItems: "center", minHeight: 40 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 14, color: palette.ink, flex: 1 }}>{W.label}</Text>
        <Text style={{ ...TYPE.metaBold, color: crash.on ? palette.accent : palette.sub }}>{crash.on ? W.on : W.off}</Text>
      </Pressable>
      <Text style={{ ...TYPE.small, color: palette.sub }}>{W.hint}</Text>
    </View>
  );
}

/**
 * DISPLAY: the three palettes as preview tiles — each drawn in ITS OWN palette's tokens (its ground, its card, its ink
 * and its three water status colours), so the reader sees what they pick. A radiogroup: each tile a radio named by
 * its word; the one in use ringed in the CURRENT palette's accent with a filled mark.
 */
function DisplayTiles({ palette, theme, onTheme }: { palette: Palette; theme: ThemeName; onTheme: (t: ThemeName) => void }) {
  const d = MENU_WORDS.display;
  return (
    <View style={{ padding: SPACE.s3, gap: SPACE.s3 }}>
      <View accessibilityRole="radiogroup" accessibilityLabel={d.palette}
            style={{ flexDirection: "row", gap: SPACE.s2 }}>
        {d.options.map((o) => {
          const on = o.k === theme;
          const p = THEMES[o.k];
          return (
            <Pressable {...hitProps({ w: 90, h: 60 })} key={o.k} testID={`theme-${o.k}`} onPress={() => onTheme(o.k)} accessibilityRole="radio"
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
                <View style={{ width: 14, height: 14, borderRadius: round(14), borderWidth: 1.5,
                               borderColor: on ? palette.accent : palette.faint, alignItems: "center",
                               justifyContent: "center" }}>
                  {on && <View style={{ width: 6, height: 6, borderRadius: round(6), backgroundColor: palette.accent }} />}
                </View>
                <Text numberOfLines={1} style={{ ...TYPE.metaBold, flexShrink: 1, color: on ? palette.ink : palette.sub }}>
                  {o.t}
                </Text>
              </View>
            </Pressable>
          );
        })}
      </View>
      <Text style={{ ...TYPE.small, color: palette.sub }}>{d.cvd}</Text>
    </View>
  );
}
