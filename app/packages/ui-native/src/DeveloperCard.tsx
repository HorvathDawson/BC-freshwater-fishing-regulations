/**
 * (T476) THE DEVELOPER CARD: one card behind the long press on the map credits, in a development or staging build only
 * (`LAB_ON`, @app/map: any build not named production; no tap-code, no runtime unlock). Four sections, one at a time:
 *   Map lab (T420)  ·  Data source / channel (T232, T423)  ·  Updates log (T251)  ·  Build info.
 * A square card in our type (the map UI rule). It draws what App.tsx hands it (`MenuInfo`) and decides nothing; the
 * Modal and the one back are `DeveloperModal`'s.
 */
import { useState } from "react";
import { ScrollView, View } from "react-native";
import { MapLab, type mapChrome } from "@app/map";
import { DEV_WORDS, MENU_WORDS, reportFailure, type AboutEnvVM, type AppEnv, aboutEnvVM } from "@app/core";
import { Text } from "./Text";
import { Pressable } from "./Pressable";
import { NoneLine } from "./EmptyState";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";
import { hitProps } from "./regs/hit";
import type { MenuInfo } from "./menu/MenuHost";
import { ChannelPicker, UpdatesLogPage } from "./menu/UpdatesLogPage";

export type DeveloperSection = keyof typeof DEV_WORDS.sections;
export const DEVELOPER_SECTIONS = (Object.keys(DEV_WORDS.sections) as DeveloperSection[])
  .map((k) => ({ k, label: DEV_WORDS.sections[k] }));
export const DEVELOPER_CLOSE = DEV_WORDS.closeLabel;

const W = MENU_WORDS.about;

/** (T232) The data source: where this build reads, and in a development build the switch (applied on the next start). */
export function EnvBlock({ vm, palette, onEnv }: { vm: AboutEnvVM; palette: Palette; onEnv?: (env: AppEnv) => void }) {
  return (
    <View testID="dev-env" style={{ gap: SPACE.s2 }}>
      <View style={{ flexDirection: "row", gap: SPACE.s3, paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
        <Text style={{ ...TYPE.small, color: palette.sub, width: 104 }}>{vm.label}</Text>
        <Text selectable testID="dev-env-value"
              style={{ ...TYPE.figure, fontSize: 12.5, color: palette.ink, flex: 1 }}>{vm.value}</Text>
      </View>
      {vm.choose && onEnv && (
        <View accessibilityRole="radiogroup" accessibilityLabel={vm.choose.title} style={{ gap: SPACE.s2 }}>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{vm.choose.title}</Text>
          <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
            {vm.choose.choices.map((c) => (
              <Pressable {...hitProps({ w: 60, h: 34 })} key={c.k} testID={`dev-env-${c.k}`} onPress={() => onEnv(c.k)} accessibilityRole="radio"
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
      {vm.note && <Text testID="dev-env-note" style={{ ...TYPE.small, color: palette.sub }}>{vm.note}</Text>}
      {vm.failed && <Text testID="dev-env-failed" style={{ ...TYPE.small, color: palette.closed }}>{vm.failed}</Text>}
    </View>
  );
}


/** The data source's switch with its state: pending until the next start, a write that fails shown and reported (T423). */
function DataSource({ info, palette }: { info: MenuInfo; palette: Palette }) {
  const [pending, setPending] = useState<AppEnv | null>(info.env?.pending ?? null);
  const [failed, setFailed] = useState<AppEnv | null>(null);
  const vm = info.env ? aboutEnvVM({ ...info.env.about, pending, failed }) : null;
  const pick = info.env ? (k: AppEnv) => {
    try { info.env!.pick(k); setFailed(null); setPending(k === info.env!.about.env ? null : k); }
    catch (e) { reportFailure(e); setFailed(k); }
  } : undefined;
  return (
    <View style={{ gap: SPACE.s3 }}>
      {vm ? <EnvBlock vm={vm} palette={palette} onEnv={pick} /> : <NoneLine palette={palette}>{W.notStated}</NoneLine>}
      {info.updatesLog ? <ChannelPicker c={info.updatesLog} palette={palette} /> : null}
    </View>
  );
}

/** Build info: the app, the platform, the data release, the environment and origin, the channel. */
function BuildInfo({ info, release, palette }: { info: MenuInfo; release: string | null; palette: Palette }) {
  const h = info.updatesLog?.header();
  const env = info.env?.about;
  const rows: readonly (readonly [string, string, string])[] = [
    ["app", W.app, info.appBuild ?? W.notStated],
    ...(h ? [["platform", DEV_WORDS.build.platform, h.platform] as const] : []),
    ["release", DEV_WORDS.build.release, release ?? h?.release ?? W.devBundle],
    ["env", DEV_WORDS.build.env, env ? env.env : W.notStated],
    ["origin", DEV_WORDS.build.origin, env ? env.origin.replace(/^https?:\/\//, "") : W.notStated],
    ["channel", DEV_WORDS.build.channel, h ? h.channel : W.notStated],
  ];
  return (
    <View testID="dev-build" style={{ gap: 0 }}>
      {rows.map(([k, label, value]) => (
        <View key={k} style={{ flexDirection: "row", gap: SPACE.s3, paddingVertical: 6, borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text style={{ ...TYPE.small, color: palette.sub, width: 104 }}>{label}</Text>
          <Text selectable testID={`dev-build-${k}`} style={{ ...TYPE.figure, fontSize: 12.5, color: palette.ink, flex: 1 }}>{value}</Text>
        </View>
      ))}
    </View>
  );
}

export function DeveloperCard({ palette, chrome, info, release, onClose, initial = "lab" }: {
  palette: Palette; chrome: ReturnType<typeof mapChrome>; info: MenuInfo;
  /** the data release in use (null: the development bundle) */
  release: string | null;
  onClose: () => void; initial?: DeveloperSection;
}) {
  const [section, setSection] = useState<DeveloperSection>(initial);
  return (
    <Pressable testID="developer-card" accessibilityLabel={DEV_WORDS.name} onPress={() => {}}
               style={{ backgroundColor: palette.card, borderWidth: 1, borderColor: palette.ink, padding: SPACE.s3, gap: SPACE.s3,
                        maxHeight: "92%", boxShadow: `2px 2px 0 ${chrome.shadow}` }}>
      <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{DEV_WORDS.title}</Text>
        <Pressable {...hitProps({ w: 60, h: 44 })} testID="developer-close" accessibilityRole="button" accessibilityLabel={DEVELOPER_CLOSE}
                   onPress={onClose} style={{ paddingHorizontal: SPACE.s2, paddingVertical: 2, borderWidth: 1, borderColor: palette.ink }}>
          <Text style={{ ...TYPE.small, color: palette.ink }}>{DEV_WORDS.close}</Text>
        </Pressable>
      </View>
      <View accessibilityRole="tablist" style={{ flexDirection: "row", flexWrap: "wrap", gap: SPACE.s2 }}>
        {DEVELOPER_SECTIONS.map((s) => {
          const on = s.k === section;
          return (
            <Pressable key={s.k} testID={`developer-tab-${s.k}`} accessibilityRole="tab" aria-selected={on} accessibilityLabel={s.label}
                       onPress={() => setSection(s.k)}
                       style={{ paddingHorizontal: SPACE.s3, paddingVertical: SPACE.s2, borderWidth: on ? 1.5 : 1,
                                borderColor: on ? palette.ink : palette.line2, backgroundColor: on ? palette.tint : "transparent" }}>
              <Text style={{ ...TYPE.small, color: on ? palette.ink : palette.sub }}>{s.label}</Text>
            </Pressable>
          );
        })}
      </View>
      <ScrollView style={{ flexShrink: 1 }} contentContainerStyle={{ gap: SPACE.s3 }}>
        {section === "lab" && <MapLab chrome={chrome} />}
        {section === "source" && <DataSource info={info} palette={palette} />}
        {section === "updates" && (info.updatesLog
          ? <UpdatesLogPage c={info.updatesLog} palette={palette} />
          : <NoneLine palette={palette}>{DEV_WORDS.noUpdatesLog}</NoneLine>)}
        {section === "build" && <BuildInfo info={info} release={release} palette={palette} />}
      </ScrollView>
    </Pressable>
  );
}
