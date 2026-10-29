/**
 * (T238) MORE → OFFLINE MAPS: storage used, the province-wide parts (the map, the shared data), then one row per fishing
 * region — its size, kept or not, and on the web Save / Remove / Resume with a progress bar while saving; on a phone the
 * release download (progress, Resume now) and the Wi-Fi-only setting. Draws `offlineVM` (core offline.ts) through
 * `useOfflineMaps` (@app/ui); decides nothing. Every press is named with what it acts on and reaches 44 × 44.
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

function Row({ r, palette, onAct, first }: { r: OfflineRowVM; palette: Palette; onAct?: (k: string, kind: string) => void;
                                             first: boolean }) {
  const kept = r.kept;
  const a = r.action;
  return (
    <View testID={`offline-row-${r.k}`}
          style={{ paddingVertical: SPACE.s2, borderTopWidth: first ? 0 : 1, borderTopColor: palette.line, minHeight: 52,
                   justifyContent: "center" }}>
      <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3 }}>
        <View accessible accessibilityLabel={r.aloud} style={{ flex: 1, gap: 2 }}>
          <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{r.name}</Text>
          <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s1, flexWrap: "wrap" }}>
            {kept && <Icon name="check" colour={palette.open} size={13} />}
            <Text testID={`offline-state-${r.k}`} style={{ ...TYPE.small, color: kept ? palette.ink : palette.sub }}>
              {`${r.size} · ${r.state}`}
            </Text>
          </View>
        </View>
        {a && onAct && (
          <Pressable testID={`offline-${a.kind}-${r.k}`} onPress={a.busy ? undefined : () => onAct(r.k, a.kind)}
                     disabled={a.busy} aria-disabled={a.busy} accessibilityRole="button" accessibilityLabel={a.aloud}
                     {...hitProps({ w: 72, h: 36 })}
                     style={({ pressed }) => ({ minWidth: 72, height: 36, paddingHorizontal: SPACE.s3, alignItems: "center",
                                                justifyContent: "center", borderWidth: 1.5,
                                                borderColor: a.kind === "remove" ? palette.line2 : palette.accent,
                                                backgroundColor: pressed ? palette.tint : palette.card,
                                                opacity: a.busy ? 0.45 : 1 })}>
            <Text style={{ ...TYPE.metaBold, color: a.kind === "remove" ? palette.ink : palette.accent }}>{a.label}</Text>
          </Pressable>
        )}
      </View>
      {r.progress !== null && <Bar value={r.progress} palette={palette} />}
    </View>
  );
}

function Kicker({ palette, children }: { palette: Palette; children: string }) {
  return <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{children}</Text>;
}

export function OfflinePage({ c, palette }: { c: OfflineControls; palette: Palette }) {
  const v = c.vm;
  const act = (k: string, kind: string) => (kind === "remove" ? c.remove(k) : c.save(k));
  return (
    <View testID="offline-page" style={{ gap: SPACE.s4 }}>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{v.intro}</Text>
      <View testID="offline-total" accessible accessibilityLabel={v.total.aloud}
            style={{ flexDirection: "row", alignItems: "baseline", justifyContent: "space-between", paddingVertical: SPACE.s2,
                     borderTopWidth: 1.5, borderBottomWidth: 1, borderTopColor: palette.ink, borderBottomColor: palette.line }}>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{v.total.label}</Text>
        <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{v.total.value}</Text>
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
      {v.failed && (
        <Text testID="offline-failed" accessibilityRole="alert"
              style={{ ...TYPE.small, color: palette.ink, paddingVertical: SPACE.s2, paddingHorizontal: SPACE.s3,
                       borderLeftWidth: 3, borderLeftColor: palette.attention, backgroundColor: palette.tint }}>
          {v.failed}
        </Text>
      )}
      <View>
        {v.parts.map((r, i) => <Row key={r.k} r={r} palette={palette} onAct={act} first={i === 0} />)}
      </View>
      <View style={{ gap: SPACE.s1 }}>
        <Kicker palette={palette}>{v.regionsHead}</Kicker>
        <View>
          {v.regions.map((r, i) => <Row key={r.k} r={r} palette={palette} onAct={act} first={i === 0} />)}
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
      {v.clear && (
        <Pressable testID="offline-clear" onPress={c.clear} accessibilityRole="button" accessibilityLabel={v.clear}
                   {...hitProps({ w: 200, h: 36 })}
                   style={({ pressed }) => ({ alignSelf: "flex-start", height: 36, paddingHorizontal: SPACE.s3,
                                              justifyContent: "center", borderWidth: 1.5, borderColor: palette.line2,
                                              backgroundColor: pressed ? palette.tint : palette.card })}>
          <Text style={{ ...TYPE.metaBold, color: palette.ink }}>{v.clear}</Text>
        </Pressable>
      )}
      <Text style={{ ...TYPE.small, color: palette.sub }}>{v.note}</Text>
    </View>
  );
}
