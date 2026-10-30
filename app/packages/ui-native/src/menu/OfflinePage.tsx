/**
 * (T238, owner 2026-10-29) MORE → OFFLINE MAPS: storage used; the fixed first row "Regulations: all of B.C." (always
 * available offline — no action, ever); the map (one province-wide file today) and the regions listed as parts of it
 * until the map is split by region (T85/T202); on a phone the release download (progress, Resume now) and the Wi-Fi-only
 * setting. Draws `offlineVM` (core offline.ts) through `useOfflineMaps` (@app/ui); decides nothing. Every press is named
 * and reaches 44 × 44.
 */
import { Pressable, Text, View } from "react-native";
import type { OfflineControls } from "@app/ui";
import type { OfflineRowVM } from "@app/core";
import { Icon } from "../icons";
import { hitProps } from "../regs/hit";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

function Bar({ value, palette }: { value: number; palette: Palette }) {
  const pct = Math.round(Math.max(0, Math.min(1, value)) * 100);
  return (
    <View accessibilityRole="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}
          style={{ height: 4, backgroundColor: palette.line, marginTop: SPACE.s1 }}>
      <View style={{ width: `${pct}%`, height: 4, backgroundColor: palette.accent }} />
    </View>
  );
}

function Row({ r, palette, first, strong }: { r: OfflineRowVM; palette: Palette; first: boolean; strong?: boolean }) {
  return (
    <View testID={`offline-row-${r.k}`} accessible accessibilityLabel={r.aloud}
          style={{ paddingVertical: SPACE.s2, borderTopWidth: first ? 0 : 1, borderTopColor: palette.line, minHeight: strong ? 52 : 44,
                   justifyContent: "center", gap: 2 }}>
      <View style={{ flexDirection: "row", alignItems: "baseline", gap: SPACE.s3 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: strong ? 15.5 : 14.5, color: palette.ink, flex: 1 }}>{r.name}</Text>
        {r.size && <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{r.size}</Text>}
      </View>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s1, flexWrap: "wrap" }}>
        {r.kept && <Icon name="check" colour={palette.open} size={13} />}
        <Text testID={`offline-state-${r.k}`} style={{ ...TYPE.small, color: r.kept ? palette.ink : palette.sub }}>{r.state}</Text>
      </View>
    </View>
  );
}

function Kicker({ palette, children }: { palette: Palette; children: string }) {
  return <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{children}</Text>;
}

export function OfflinePage({ c, palette }: { c: OfflineControls; palette: Palette }) {
  const v = c.vm;
  return (
    <View testID="offline-page" style={{ gap: SPACE.s4 }}>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{v.intro}</Text>
      {v.total && (
        <View testID="offline-total" accessible accessibilityLabel={v.total.aloud}
              style={{ flexDirection: "row", alignItems: "baseline", justifyContent: "space-between", paddingVertical: SPACE.s2,
                       borderTopWidth: 1.5, borderBottomWidth: 1, borderTopColor: palette.ink, borderBottomColor: palette.line }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{v.total.label}</Text>
          <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{v.total.value}</Text>
        </View>
      )}
      {/* the fixed first row: the regulations for all of B.C. — always kept, never a choice */}
      <View style={{ paddingHorizontal: SPACE.s3, borderLeftWidth: 3, borderLeftColor: palette.open, backgroundColor: palette.tint }}>
        <Row r={v.regs} palette={palette} first strong />
      </View>
      {v.update && (
        <View testID="offline-update" style={{ gap: SPACE.s1, paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3,
                                               borderLeftWidth: 3, borderLeftColor: palette.accent, backgroundColor: palette.tint }}>
          <Text style={{ ...TYPE.small, color: palette.ink }}>{v.update.line}</Text>
          {v.update.progress !== null && <Bar value={v.update.progress} palette={palette} />}
          {v.update.resume && (
            <Pressable testID="offline-update-resume" onPress={c.resume} accessibilityRole="button"
                       accessibilityLabel={v.update.resume} {...hitProps({ w: 120, h: 32 })}
                       style={{ alignSelf: "flex-start", minHeight: 32, justifyContent: "center" }}>
              <Text style={{ ...TYPE.metaBold, color: palette.accent }}>{v.update.resume}</Text>
            </Pressable>
          )}
        </View>
      )}
      {v.book && <Row r={v.book} palette={palette} first />}
      <Row r={v.map} palette={palette} first />
      <View style={{ gap: SPACE.s1 }}>
        <Kicker palette={palette}>{v.regionsHead}</Kicker>
        <View>
          {v.regions.map((r, i) => <Row key={r.k} r={r} palette={palette} first={i === 0} />)}
        </View>
      </View>
      {v.wifi && (
        <Pressable testID="offline-wifi" onPress={() => c.setWifiOnly(!v.wifi!.on)} accessibilityRole="switch"
                   aria-checked={v.wifi.on} accessibilityLabel={v.wifi.label} accessibilityHint={v.wifi.hint}
                   {...hitProps({ w: 300, h: 44 })}
                   style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, minHeight: 44,
                            borderTopWidth: 1, borderTopColor: palette.line, paddingTop: SPACE.s2 }}>
          <View style={{ flex: 1, gap: 2 }}>
            <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{v.wifi.label}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{v.wifi.hint}</Text>
          </View>
          <Text style={{ ...TYPE.metaBold, color: v.wifi.on ? palette.accent : palette.sub }}>{v.wifi.value}</Text>
        </Pressable>
      )}
      <Text style={{ ...TYPE.small, color: palette.sub }}>{v.note}</Text>
    </View>
  );
}
