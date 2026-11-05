/**
 * (T234, owner 2026-11-05: "a section in More where we configure the notifications") MORE → NOTIFICATIONS: the global
 * switch, what the phone allows (and, refused, the way to its settings), "new regulations data", the gauge alerts with
 * Change and Delete (an alert is ADDED from a gauge's card: "Alert me"), and the last background checks (when the phone
 * actually ran them — how late an alert can be). Draws `useNotifications` (@app/ui) in core's words; decides nothing.
 */
import { useState } from "react";
import { View } from "react-native";
import { alertLine, NOTIFY_WORDS, runLine } from "@app/core";
import type { NotificationsVM } from "@app/ui";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { Button } from "../Button";
import { AlertForm } from "../AlertMe";
import { hitProps } from "../regs/hit";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

const W = NOTIFY_WORDS;

function Switch({ palette, testID, label, hint, on, onPress }: {
  palette: Palette; testID: string; label: string; hint: string; on: boolean; onPress: () => void;
}) {
  return (
    <Pressable testID={testID} onPress={onPress} accessibilityRole="switch" aria-checked={on} accessibilityLabel={label}
               accessibilityHint={hint} {...hitProps({ w: 300, h: 44 })}
               style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, minHeight: 44,
                        borderTopWidth: 1, borderTopColor: palette.line, paddingTop: SPACE.s2 }}>
      <View style={{ flex: 1, gap: 2 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{label}</Text>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{hint}</Text>
      </View>
      <Text style={{ ...TYPE.metaBold, color: on ? palette.accent : palette.sub }}>{on ? W.master.on : W.master.off}</Text>
    </Pressable>
  );
}

function Kicker({ palette, children }: { palette: Palette; children: string }) {
  return <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{children}</Text>;
}

export function NotificationsPage({ n, palette }: { n: NotificationsVM; palette: Palette }) {
  const s = n.state;
  const [editing, setEditing] = useState<string | null>(null);
  const p = n.permission;
  return (
    <View testID="notifications-page" style={{ gap: SPACE.s4 }}>
      <Text style={{ ...TYPE.body, color: palette.ink }}>{W.intro}</Text>
      <View style={{ gap: SPACE.s2 }}>
        <Switch palette={palette} testID="notify-master" label={W.master.label} hint={W.master.hint} on={s.on}
                onPress={() => { void n.setOn(!s.on); }} />
        <Text testID="notify-permission" accessibilityLiveRegion="polite"
              style={{ ...TYPE.small, color: p === "denied" && s.on ? palette.closed : palette.sub }}>
          {n.asking ? W.permission.asking : p ? W.permission[p] : ""}
        </Text>
        {p === "denied" && (
          <Button palette={palette} kind="link" size="meta" label={W.permission.open} testID="notify-open-settings"
                  onPress={n.openSettings} />
        )}
        {n.error && <Text testID="notify-error" style={{ ...TYPE.small, color: palette.closed }}>{n.error}</Text>}
        <Switch palette={palette} testID="notify-update-ready" label={W.updateReady.label} hint={W.updateReady.hint}
                on={s.on && s.updateReady} onPress={() => { void n.setUpdateReady(!(s.on && s.updateReady)); }} />
      </View>

      <View style={{ gap: SPACE.s1 }}>
        <Kicker palette={palette}>{W.alerts.title}</Kicker>
        {s.rules.length === 0 && <Text testID="notify-no-alerts" style={{ ...TYPE.small, color: palette.sub }}>{W.alerts.none}</Text>}
        {s.rules.map((r, i) => {
          const line = alertLine(r);
          return (
            <View key={r.id} testID={`notify-alert-${r.id}`}
                  style={{ paddingVertical: SPACE.s2, gap: SPACE.s1, borderTopWidth: i ? 1 : 0, borderTopColor: palette.line }}>
              <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{r.water?.name ?? r.stationName}</Text>
              <Text style={{ ...TYPE.small, color: palette.sub }}>{`${r.stationName} · ${line}`}</Text>
              {editing === r.id
                ? <AlertForm palette={palette} quantity={r.quantity} current={null} rule={r}
                             onCancel={() => setEditing(null)}
                             onSave={async (direction, value) => { await n.putRule({ ...r, direction, value }); setEditing(null); }} />
                : (
                  <View style={{ flexDirection: "row", gap: SPACE.s4 }}>
                    <Button palette={palette} kind="link" size="meta" label={W.alerts.edit} testID={`notify-edit-${r.id}`}
                            a11yLabel={`${W.alerts.edit}: ${line}`} onPress={() => setEditing(r.id)} />
                    <Button palette={palette} kind="link" size="meta" label={W.alerts.remove} testID={`notify-remove-${r.id}`}
                            a11yLabel={W.alerts.removeAloud(line)} onPress={() => n.removeRule(r.id)} />
                  </View>
                )}
            </View>
          );
        })}
      </View>

      <View style={{ gap: SPACE.s1 }}>
        <Kicker palette={palette}>{W.runs.title}</Kicker>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{W.runs.note}</Text>
        {s.runs.length === 0
          ? <Text testID="notify-no-runs" style={{ ...TYPE.small, color: palette.sub }}>{W.runs.none}</Text>
          : [...s.runs].reverse().map((r) => (
            <Text key={r.at} testID="notify-run" style={{ ...TYPE.small, color: palette.ink }}>{runLine(r)}</Text>
          ))}
      </View>
    </View>
  );
}
