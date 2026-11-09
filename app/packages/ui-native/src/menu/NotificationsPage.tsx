/**
 * (T234, owner 2026-11-05: "a section in More where we configure the notifications"; T527, owner 2026-11-09: "a clean
 * alerting system like you would have for stocks") MORE → NOTIFICATIONS, top to bottom, in cards:
 *
 *   the switch       every notification on or off, and what the phone allows (refused: the way to its settings)
 *   gauge alerts     A PLAIN LIST, one line per alert ("Vedder Crossing · Flow crosses above 50 m³/s · last fired …"),
 *                    each with its own On/Off and Delete; then "Add an alert": 1. pick a gauge (by name or number), 2–3.
 *                    the editor (condition, value or percentile), Save — the same editor as a gauge card's "Alert me"
 *   not kept         (T527) saved alerts this version could not keep, said with their words until dismissed
 *   other            "New regulations data" and "New app version" (v1.1; only where the build can learn of one)
 *   the log          the last background checks, in a fold closed by default (how late the phone ran them)
 *
 * Draws `useNotifications` (@app/ui) in core's words; decides nothing. Every control a 44 px target with its label.
 */
import { useState } from "react";
import { TextInput, View } from "react-native";
import { alertListLine, NOTIFY_WORDS, runLine, type AlertQuantity, type GaugeAlertRule } from "@app/core";
import type { StationId, WaterSource } from "@app/data";
import { useAlertGauges, useGaugeParameters, type AlertGauge, type NotificationsVM } from "@app/ui";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { Button } from "../Button";
import { AlertEditor, AlertStatus, newAlert } from "../AlertMe";
import { Fold } from "../regs/sheet/Fold";
import { hitProps } from "../regs/hit";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

const W = NOTIFY_WORDS;

/** A switch row: its label and hint, On / Off in words, the whole row the target. */
function SwitchRow({ palette, testID, label, hint, on, onPress, first = false, aloud }: {
  palette: Palette; testID: string; label: string; hint?: string; on: boolean; onPress: () => void; first?: boolean;
  /** what a screen reader says, when the label alone is not unique on the page */
  aloud?: string;
}) {
  return (
    <Pressable testID={testID} onPress={onPress} accessibilityRole="switch" aria-checked={on} accessibilityLabel={aloud ?? label}
               accessibilityHint={hint} {...hitProps({ w: 300, h: 44 })}
               style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, minHeight: 44,
                        borderTopWidth: first ? 0 : 1, borderTopColor: palette.line, paddingTop: first ? 0 : SPACE.s2 }}>
      <View style={{ flex: 1, gap: 2 }}>
        <Text style={{ ...TYPE.bodyStrong, fontSize: 14.5, color: palette.ink }}>{label}</Text>
        {hint ? <Text style={{ ...TYPE.small, color: palette.sub }}>{hint}</Text> : null}
      </View>
      <View style={{ minWidth: 44, alignItems: "center", paddingVertical: 4, paddingHorizontal: SPACE.s2, borderWidth: 1.5,
                     borderColor: on ? palette.accent : palette.line2, backgroundColor: on ? palette.tint : "transparent" }}>
        <Text style={{ ...TYPE.metaBold, color: on ? palette.accent : palette.sub }}>{on ? W.master.on : W.master.off}</Text>
      </View>
    </Pressable>
  );
}

function Card({ palette, children, testID }: { palette: Palette; children: React.ReactNode; testID?: string }) {
  return (
    <View testID={testID} style={{ gap: SPACE.s2, padding: SPACE.s3, borderWidth: 1, borderColor: palette.line2,
                                   borderRadius: palette.r.box, backgroundColor: palette.card }}>
      {children}
    </View>
  );
}

function Kicker({ palette, children }: { palette: Palette; children: string }) {
  return <Text accessibilityRole="header" style={{ ...TYPE.section, color: palette.sub }}>{children}</Text>;
}

/** Step 2–3 for a picked gauge: its quantities (the release's record), then the editor; a gauge with none says so. */
function PickedGauge({ palette, source, gauge, onSave, onCancel }: {
  palette: Palette; source: WaterSource; gauge: AlertGauge;
  onSave: (r: GaugeAlertRule) => Promise<void>; onCancel: () => void;
}) {
  const params = useGaugeParameters(source, gauge.station as StationId);
  if (params.state === "loading") return <Text style={{ ...TYPE.small, color: palette.sub }}>{W.editor.loading}</Text>;
  if (params.state === "failed")
    return <Text testID="notify-add-failed" style={{ ...TYPE.small, color: palette.closed }}>{params.error.message}</Text>;
  const qs = params.value as readonly AlertQuantity[];
  if (!qs.length)
    return (
      <View style={{ gap: SPACE.s2 }}>
        <Text testID="notify-add-no-quantity" style={{ ...TYPE.small, color: palette.sub }}>{W.add.noQuantity}</Text>
        <Button palette={palette} kind="ghost" label={W.add.cancel} onPress={onCancel} />
      </View>
    );
  return (
    <AlertEditor palette={palette} source={source}
                 gauge={{ station: gauge.station, stationName: gauge.name, quantity: qs[0]!, quantities: qs, current: null,
                          water: null }}
                 onCancel={onCancel} onSave={(d) => onSave(newAlert(gauge.station, d))} />
  );
}

/** "Add an alert": 1. a gauge by name or number; then the editor on it. */
function AddAlert({ palette, source, n, onDone, onCancel }: { palette: Palette; source: WaterSource; n: NotificationsVM;
                                                             onDone: () => void; onCancel: () => void }) {
  const [q, setQ] = useState("");
  const [picked, setPicked] = useState<AlertGauge | null>(null);
  const hits = useAlertGauges(source, q);
  const A = W.add;
  return (
    <Card palette={palette} testID="notify-add">
      <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{A.title}</Text>
      {picked
        ? <PickedGauge palette={palette} source={source} gauge={picked} onCancel={onCancel}
                       onSave={async (r) => { await n.putRule(r); onDone(); }} />
        : (
          <>
            <Text style={{ ...TYPE.metaBold, color: palette.sub }}>{A.step}</Text>
            <TextInput testID="notify-add-search" value={q} onChangeText={setQ} accessibilityLabel={A.searchAloud}
                       placeholder={A.search} placeholderTextColor={palette.faint} autoFocus
                       style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: palette.ink, minHeight: 44,
                                borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card }} />
            {hits.state === "loading" && <Text style={{ ...TYPE.small, color: palette.sub }}>{A.loading}</Text>}
            {hits.state === "failed" && (
              <Text testID="notify-add-failed" style={{ ...TYPE.small, color: palette.closed }}>{hits.error.message}</Text>
            )}
            {hits.state === "ready" && q.trim().length >= 2 && hits.value.length === 0 && (
              <Text testID="notify-add-none" style={{ ...TYPE.small, color: palette.sub }}>{A.none}</Text>
            )}
            {hits.state === "ready" && hits.value.map((h) => (
              <Pressable key={h.station} testID={`notify-add-hit-${h.station}`} onPress={() => setPicked(h)}
                         accessibilityRole="button" accessibilityLabel={`${h.name} (${h.station})`} {...hitProps({ w: 300, h: 44 })}
                         style={{ minHeight: 44, justifyContent: "center", borderTopWidth: 1, borderTopColor: palette.line }}>
                <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{h.name}</Text>
                <Text style={{ ...TYPE.small, color: palette.sub }}>{h.station}</Text>
              </Pressable>
            ))}
            <Button palette={palette} kind="ghost" label={A.cancel} testID="notify-add-cancel" onPress={onCancel} />
          </>
        )}
    </Card>
  );
}

/** One alert, one line: its gauge, its condition and when it last fired; its own On/Off and Delete. */
function AlertRow({ n, r, palette, first }: { n: NotificationsVM; r: GaugeAlertRule; palette: Palette; first: boolean }) {
  const line = alertListLine(r, n.state.ruleState[r.id]);
  return (
    <View testID={`notify-alert-${r.id}`}
          style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2, paddingTop: first ? 0 : SPACE.s2,
                   borderTopWidth: first ? 0 : 1, borderTopColor: palette.line }}>
      <View style={{ flex: 1 }}>
        <SwitchRow palette={palette} testID={`notify-alert-on-${r.id}`} first label={line} aloud={W.alerts.switchAloud(line)}
                   on={r.on} onPress={() => { void n.setRuleOn(r.id, !r.on); }} />
      </View>
      <Button palette={palette} kind="link" size="meta" label={W.alerts.remove} testID={`notify-remove-${r.id}`}
              a11yLabel={W.alerts.removeAloud(line)} onPress={() => n.removeRule(r.id)} />
    </View>
  );
}

export function NotificationsPage({ n, palette, source = null }: {
  n: NotificationsVM; palette: Palette;
  /** the gauges: "Add an alert" and its editor; null: adding is not offered here */
  source?: WaterSource | null;
}) {
  const s = n.state;
  const p = n.permission;
  const [adding, setAdding] = useState(false);
  const [saved, setSaved] = useState(false);
  return (
    <View testID="notifications-page" style={{ gap: SPACE.s4 }}>
      <Card palette={palette}>
        <SwitchRow palette={palette} testID="notify-master" first label={W.master.label} hint={W.master.hint} on={s.on}
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
        <Text style={{ ...TYPE.small, color: palette.sub }}>{W.intro}</Text>
      </Card>

      <View style={{ gap: SPACE.s2 }}>
        <Kicker palette={palette}>{W.sections.alerts}</Kicker>
        <Card palette={palette} testID="notify-alerts">
          {s.rules.length === 0
            ? <Text testID="notify-no-alerts" style={{ ...TYPE.small, color: palette.sub }}>{W.alerts.none}</Text>
            : s.rules.map((r, i) => <AlertRow key={r.id} n={n} r={r} palette={palette} first={i === 0} />)}
          <Text style={{ ...TYPE.small, color: palette.sub }}>{W.alerts.how}</Text>
        </Card>
        {s.refused.length > 0 && (
          <Card palette={palette} testID="notify-refused">
            <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{W.refused.title}</Text>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{W.refused.note}</Text>
            {s.refused.map((r, i) => (
              <Text key={i} testID="notify-refused-item" style={{ ...TYPE.small, color: palette.ink }}>{W.refused.item(r)}</Text>
            ))}
            <Button palette={palette} kind="link" size="meta" label={W.refused.ok} testID="notify-refused-ok"
                    onPress={n.dismissRefused} />
          </Card>
        )}
        {source && (adding
          ? <AddAlert palette={palette} source={source} n={n} onDone={() => { setAdding(false); setSaved(true); }}
                      onCancel={() => setAdding(false)} />
          : (
            <View style={{ flexDirection: "row" }}>
              <Button palette={palette} kind="small" icon="bell" label={W.alerts.add} a11yLabel={W.alerts.add}
                      testID="notify-add-open" onPress={() => { setSaved(false); setAdding(true); }} />
            </View>
          ))}
        {saved && !adding && <AlertStatus n={n} palette={palette} />}
      </View>

      <View style={{ gap: SPACE.s2 }}>
        <Kicker palette={palette}>{W.sections.other}</Kicker>
        <Card palette={palette}>
          <SwitchRow palette={palette} testID="notify-update-ready" first label={W.updateReady.label} hint={W.updateReady.hint}
                     on={s.on && s.updateReady} onPress={() => { void n.setUpdateReady(!(s.on && s.updateReady)); }} />
          {n.canAppVersion && (
            <SwitchRow palette={palette} testID="notify-app-version" label={W.appVersion.label} hint={W.appVersion.hint}
                       on={s.on && s.appVersion} onPress={() => { void n.setAppVersion(!(s.on && s.appVersion)); }} />
          )}
        </Card>
      </View>

      <Card palette={palette} testID="notify-runs">
        <Fold summary={W.runs.title} palette={palette} testID="notify-runs-fold">
          <View style={{ gap: SPACE.s1 }}>
            <Text style={{ ...TYPE.small, color: palette.sub }}>{W.runs.note}</Text>
            {s.runs.length === 0
              ? <Text testID="notify-no-runs" style={{ ...TYPE.small, color: palette.sub }}>{W.runs.none}</Text>
              : [...s.runs].reverse().map((r) => (
                <Text key={r.at} testID="notify-run" style={{ ...TYPE.small, color: palette.ink }}>{runLine(r)}</Text>
              ))}
          </View>
        </Fold>
      </Card>
    </View>
  );
}
