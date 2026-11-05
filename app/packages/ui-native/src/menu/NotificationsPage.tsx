/**
 * (T234, owner 2026-11-05: "a section in More where we configure the notifications … make the UI for this nice please,
 * and in a menu in More") MORE → NOTIFICATIONS, top to bottom, in cards:
 *
 *   the switch       every notification on or off, and what the phone allows (refused: the way to its settings)
 *   gauge alerts     "Add an alert" (find a water by name → its Conditions, where its gauge card's "Alert me" opens the
 *                    editor prefilled), then the alerts GROUPED BY WATER: a card per water, each alert its gauge, its
 *                    line ("Flow above 30 m³/s · at most once a day for this water · last sent …"), its own switch,
 *                    Edit (the editor in place) and Delete
 *   other            "New regulations data" and "New app version" (v1.1; only where the build can learn of one)
 *   the log          the last background checks, in a fold closed by default (how late the phone ran them)
 *
 * Draws `useNotifications` (@app/ui) in core's words; decides nothing. Every control a 44 px target with its label.
 */
import { useMemo, useState } from "react";
import { TextInput, View } from "react-native";
import { alertCardLine, alertLine, alertWaterKey, NOTIFY_WORDS, runLine, type GaugeAlertRule } from "@app/core";
import type { WaterSource } from "@app/data";
import { useAlertWaterSearch, type NotificationsVM } from "@app/ui";
import { Text } from "../Text";
import { Pressable } from "../Pressable";
import { Button } from "../Button";
import { AlertEditor } from "../AlertMe";
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

/** "Add an alert": a water by name; a pick opens its Conditions (the gauge card's "Alert me" sets the alert there). */
function AddAlert({ palette, source, onPick, onCancel }: { palette: Palette; source: WaterSource;
                                                           onPick: (item: string) => void; onCancel: () => void }) {
  const [q, setQ] = useState("");
  const hits = useAlertWaterSearch(source, q);
  const A = W.add;
  return (
    <Card palette={palette} testID="notify-add">
      <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{A.title}</Text>
      <TextInput testID="notify-add-search" value={q} onChangeText={setQ} accessibilityLabel={A.searchAloud}
                 placeholder={A.search} placeholderTextColor={palette.faint} autoFocus
                 style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: palette.ink, minHeight: 44,
                          borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card }} />
      <Text style={{ ...TYPE.small, color: palette.sub }}>{A.pick}</Text>
      {hits.state === "ready" && hits.value.length === 0 && (
        <Text testID="notify-add-none" style={{ ...TYPE.small, color: palette.sub }}>{A.none}</Text>
      )}
      {hits.state === "failed" && (
        <Text testID="notify-add-failed" style={{ ...TYPE.small, color: palette.closed }}>{hits.error.message}</Text>
      )}
      {hits.state === "ready" && hits.value.map((h) => (
        <Pressable key={h.item} testID={`notify-add-hit-${h.item}`} onPress={() => onPick(h.item)} accessibilityRole="button"
                   accessibilityLabel={h.name} {...hitProps({ w: 300, h: 44 })}
                   style={{ minHeight: 44, justifyContent: "center", borderTopWidth: 1, borderTopColor: palette.line }}>
          <Text style={{ ...TYPE.bodyStrong, color: palette.accent }}>{h.name}</Text>
        </Pressable>
      ))}
      <Button palette={palette} kind="ghost" label={A.cancel} testID="notify-add-cancel" onPress={onCancel} />
    </Card>
  );
}

/** One alert in its water's card: its gauge, its line, its switch, Edit and Delete (or the editor, in place). */
function AlertRow({ n, r, palette, source, first }: { n: NotificationsVM; r: GaugeAlertRule; palette: Palette;
                                                      source: WaterSource | null; first: boolean }) {
  const [editing, setEditing] = useState(false);
  // aloud, an alert is its water and gauge with its line: two waters' alerts on one page are never the same words
  const line = `${W.alerts.heading(r.water?.name ?? null, r.stationName)}, ${alertLine(r)}`;
  const card = alertCardLine(r, n.state.ruleState[r.id]);
  return (
    <View testID={`notify-alert-${r.id}`}
          style={{ gap: SPACE.s1, paddingTop: first ? 0 : SPACE.s2, borderTopWidth: first ? 0 : 1, borderTopColor: palette.line }}>
      <SwitchRow palette={palette} testID={`notify-alert-on-${r.id}`} first label={r.stationName} hint={card}
                 aloud={W.alerts.switchAloud(line)}
                 on={r.on} onPress={() => { void n.setRuleOn(r.id, !r.on); }} />
      {editing
        ? <AlertEditor palette={palette} source={source} rule={r}
                       gauge={{ station: r.station, stationName: r.stationName, quantity: r.quantity, current: null,
                                water: r.water }}
                       onCancel={() => setEditing(false)}
                       onSave={async (d) => { await n.putRule({ ...r, ...d }); setEditing(false); }} />
        : (
          <View style={{ flexDirection: "row", gap: SPACE.s4 }}>
            <Button palette={palette} kind="link" size="meta" label={W.alerts.edit} testID={`notify-edit-${r.id}`}
                    a11yLabel={W.alerts.editAloud(line)} onPress={() => setEditing(true)} />
            <Button palette={palette} kind="link" size="meta" label={W.alerts.remove} testID={`notify-remove-${r.id}`}
                    a11yLabel={W.alerts.removeAloud(line)} onPress={() => n.removeRule(r.id)} />
          </View>
        )}
    </View>
  );
}

export function NotificationsPage({ n, palette, source = null, onFindWater }: {
  n: NotificationsVM; palette: Palette;
  /** the waters: the editor's reading and normal range, and "Add an alert"'s search; null: neither is offered */
  source?: WaterSource | null;
  /** "Add an alert": open the picked water's Conditions */
  onFindWater?: (item: string) => void;
}) {
  const s = n.state;
  const p = n.permission;
  const [adding, setAdding] = useState(false);
  // the alerts grouped by water, in the order each water's first alert was added
  const groups = useMemo(() => {
    const by = new Map<string, { name: string; rules: GaugeAlertRule[] }>();
    for (const r of s.rules) {
      const k = alertWaterKey(r);
      const g = by.get(k) ?? { name: r.water?.name ?? r.stationName, rules: [] };
      g.rules.push(r);
      by.set(k, g);
    }
    return [...by.entries()];
  }, [s.rules]);
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
        {s.rules.length === 0 && !adding && (
          <Card palette={palette}><Text testID="notify-no-alerts" style={{ ...TYPE.small, color: palette.sub }}>{W.alerts.none}</Text></Card>
        )}
        {groups.map(([k, g]) => (
          <Card key={k} palette={palette} testID={`notify-water-${k}`}>
            <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{g.name}</Text>
            {g.rules.map((r, i) => <AlertRow key={r.id} n={n} r={r} palette={palette} source={source} first={i === 0} />)}
          </Card>
        ))}
        {source && onFindWater && (adding
          ? <AddAlert palette={palette} source={source} onPick={(item) => { setAdding(false); onFindWater(item); }}
                      onCancel={() => setAdding(false)} />
          : (
            <View style={{ flexDirection: "row" }}>
              <Button palette={palette} kind="small" icon="bell" label={W.alerts.add} a11yLabel={W.alerts.add}
                      testID="notify-add-open" onPress={() => setAdding(true)} />
            </View>
          ))}
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
