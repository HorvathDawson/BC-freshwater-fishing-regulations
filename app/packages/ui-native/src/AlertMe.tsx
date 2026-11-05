/**
 * (T234) "ALERT ME" ON A GAUGE'S CARD — the one way a gauge alert is added (owner 2026-11-05: managed in More →
 * Notifications). One small control: a tap opens the form in place — rises above / falls below, a value in the units
 * the chart shows (the gauge's own quantity), Save. Saving turns notifications on and, the first time, the phone asks
 * for its permission; a refusal is said here. Draws nothing where the app has no notifications (the web, a test).
 * Every word is core's (`NOTIFY_WORDS`); the state and actions are `@app/ui` `useNotifications`.
 */
import { useState } from "react";
import { TextInput, View } from "react-native";
import { alertLine, NOTIFY_WORDS, now, parseAlertValue, type AlertDirection, type AlertQuantity, type GaugeAlertRule } from "@app/core";
import { useNotifications, type NotificationsVM } from "@app/ui";
import { Text } from "./Text";
import { Button } from "./Button";
import { Chip } from "./Chip";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

const W = NOTIFY_WORDS.alertMe;

export interface AlertTarget {
  station: string;
  stationName: string;
  quantity: AlertQuantity;
  /** the reading now, prefilled and said */
  current: number | null;
  water: { item: string | null; name: string } | null;
}

export function AlertMe({ palette, target }: { palette: Palette; target: AlertTarget }) {
  const n = useNotifications();
  const [open, setOpen] = useState(false);
  const [saved, setSaved] = useState(false);
  if (!n) return null;
  const mine = n.state.rules.filter((r) => r.station === target.station);
  return (
    <View testID="alert-me" style={{ gap: SPACE.s2 }}>
      {!open && (
        <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s3, flexWrap: "wrap" }}>
          <Button palette={palette} kind="small" icon="bell" label={W.button} a11yLabel={`${W.button}: ${target.stationName}`}
                  testID="alert-me-open" onPress={() => { setSaved(false); setOpen(true); }} />
          {mine.length > 0 && (
            <Text testID="alert-me-count" style={{ ...TYPE.small, color: palette.sub }}>{W.on(mine.length)}</Text>
          )}
        </View>
      )}
      {open && (
        <AlertForm palette={palette} quantity={target.quantity} current={target.current}
                   onCancel={() => setOpen(false)}
                   onSave={async (direction, value) => {
                     await n.putRule({ id: `${target.station}-${now().toString(36)}`, station: target.station,
                                       stationName: target.stationName, quantity: target.quantity, direction, value,
                                       water: target.water, createdAt: now() });
                     setOpen(false);
                     setSaved(true);
                   }} />
      )}
      {saved && <AlertStatus n={n} palette={palette} />}
    </View>
  );
}

/** After a save: where to manage it, or why nothing can be shown (the phone refused, a write failed). */
function AlertStatus({ n, palette }: { n: NotificationsVM; palette: Palette }) {
  if (n.error) return <Text testID="alert-me-error" style={{ ...TYPE.small, color: palette.closed }}>{n.error}</Text>;
  if (n.permission === "denied")
    return (
      <View style={{ gap: SPACE.s1 }}>
        <Text testID="alert-me-denied" style={{ ...TYPE.small, color: palette.closed }}>{NOTIFY_WORDS.permission.denied}</Text>
        <Button palette={palette} kind="link" size="meta" label={NOTIFY_WORDS.permission.open} onPress={n.openSettings} />
      </View>
    );
  return <Text testID="alert-me-saved" style={{ ...TYPE.small, color: palette.sub }}>{W.saved}</Text>;
}

/** The form: rises above / falls below and the value. Also More → Notifications' "Change" (an existing rule). */
export function AlertForm({ palette, quantity, current, rule, onSave, onCancel }: {
  palette: Palette; quantity: AlertQuantity; current: number | null;
  /** the rule being changed; absent: a new one */
  rule?: Pick<GaugeAlertRule, "direction" | "value">;
  onSave: (d: AlertDirection, v: number) => void | Promise<void>; onCancel: () => void;
}) {
  const [direction, setDirection] = useState<AlertDirection>(rule?.direction ?? "above");
  const [text, setText] = useState(rule ? String(rule.value) : current !== null ? String(current) : "");
  const [bad, setBad] = useState(false);
  const [busy, setBusy] = useState(false);
  const save = async () => {
    const v = parseAlertValue(text);
    if (v === null) { setBad(true); return; }
    setBusy(true);
    try { await onSave(direction, v); } finally { setBusy(false); }
  };
  const v = parseAlertValue(text);
  return (
    <View testID="alert-form" style={{ gap: SPACE.s2 }}>
      <View accessibilityRole="radiogroup" style={{ flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" }}>
        {(["above", "below"] as const).map((d) => (
          <Chip key={d} palette={palette} look="choice" label={d === "above" ? W.above : W.below} on={direction === d}
                testID={`alert-form-${d}`} onPress={() => setDirection(d)} />
        ))}
      </View>
      <TextInput testID="alert-form-value" value={text} onChangeText={(t) => { setText(t); setBad(false); }}
                 accessibilityLabel={W.value(quantity)} placeholder={W.value(quantity)} placeholderTextColor={palette.faint}
                 keyboardType="decimal-pad" inputMode="decimal"
                 style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: bad ? palette.closed : palette.ink,
                          borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card }} />
      <Text testID="alert-form-line" style={{ ...TYPE.small, color: bad ? palette.closed : palette.sub }}>
        {bad ? W.bad : [v !== null ? `${alertLine({ quantity, direction, value: v })}.` : W.value(quantity),
                        current !== null ? W.now(quantity, current) : null].filter(Boolean).join(" ")}
      </Text>
      <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
        <Button palette={palette} label={W.save} testID="alert-form-save" busy={busy} onPress={() => { void save(); }} />
        <Button palette={palette} kind="ghost" label={W.cancel} testID="alert-form-cancel" onPress={onCancel} />
      </View>
    </View>
  );
}
