/**
 * (T234) "ALERT ME" ON A GAUGE'S CARD, AND THE ALERT EDITOR (v1.1, owner 2026-11-05: "on x gauge if it rises to y point
 * or z times then send notifications … make the UI for this nice").
 *
 *   AlertMe      the gauge card's control: a tap opens the editor in place, PREFILLED from the card (its gauge, the
 *                quantity on the chart, the reading now). Saving turns notifications on and, the first time, the phone
 *                asks for its permission; a refusal is said here.
 *   AlertEditor  quantity (flow / level, the ones the gauge has) · condition (rises above / falls below / rises by % in
 *                24 h / falls by % in 24 h) · the value, with the reading now and today's normal range beside it
 *                (`useAlertContext`: the same series the card draws) · how often at most (the default once a day for the
 *                water, or the alert's own gap) · ONE plain line of what will happen (`alertSummary`). Also More →
 *                Notifications' Edit.
 * Draws nothing where the app has no notifications (the web, a test). Every word is core's (`NOTIFY_WORDS`); the state
 * and actions are `@app/ui` `useNotifications`.
 */
import { useState } from "react";
import { TextInput, View } from "react-native";
import {
  ALERT_GAPS_H, ALERT_KINDS, alertContextLine, alertSummary, alertValueOk, CONDITIONS_WORDS, isChangeKind, NOTIFY_WORDS,
  now, parseAlertValue, type AlertKind, type AlertQuantity, type GaugeAlertRule,
} from "@app/core";
import type { WaterSource } from "@app/data";
import { useAlertContext, useNotifications, type NotificationsVM } from "@app/ui";
import { Text } from "./Text";
import { Button } from "./Button";
import { Chip } from "./Chip";
import { TYPE } from "./type";
import { SPACE, type Palette } from "./theme";

const W = NOTIFY_WORDS.alertMe;
const E = NOTIFY_WORDS.editor;

export interface AlertTarget {
  station: string;
  stationName: string;
  /** the quantity on the chart: the editor opens on it */
  quantity: AlertQuantity;
  /** every quantity the gauge has (the editor offers these); absent: the one above */
  quantities?: readonly AlertQuantity[];
  /** the reading now, prefilled */
  current: number | null;
  water: { item: string | null; name: string } | null;
}

export function AlertMe({ palette, target, source = null }: { palette: Palette; target: AlertTarget; source?: WaterSource | null }) {
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
        <AlertEditor palette={palette} source={source} gauge={target}
                     onCancel={() => setOpen(false)}
                     onSave={async (r) => {
                       await n.putRule({ ...r, id: `${target.station}-${now().toString(36)}`, createdAt: now(), on: true });
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

/** What the editor saves (the caller adds the id, the time and the switch). */
export type AlertDraft = Omit<GaugeAlertRule, "id" | "createdAt" | "on">;

function Field({ palette, label, children }: { palette: Palette; label: string; children: React.ReactNode }) {
  return (
    <View style={{ gap: SPACE.s1 }}>
      <Text accessibilityRole="header" style={{ ...TYPE.metaBold, color: palette.sub }}>{label}</Text>
      {children}
    </View>
  );
}

/** The reading now and today's normal range, read where a source is (a test without one: not drawn). */
function ContextLine({ palette, source, station, quantity }: { palette: Palette; source: WaterSource; station: string;
                                                                 quantity: AlertQuantity }) {
  const c = useAlertContext(source, station, quantity);
  return (
    <Text testID="alert-form-context" accessibilityLiveRegion="polite" style={{ ...TYPE.small, color: palette.sub }}>
      {c.loading ? E.loading : alertContextLine(quantity, c.current, c.normal)}
    </Text>
  );
}

export function AlertEditor({ palette, source, gauge, rule, onSave, onCancel }: {
  palette: Palette; source: WaterSource | null;
  gauge: Pick<AlertTarget, "station" | "stationName" | "quantity" | "quantities" | "current" | "water">;
  /** the alert being edited; absent: a new one, prefilled from `gauge` */
  rule?: GaugeAlertRule;
  onSave: (r: AlertDraft) => void | Promise<void>; onCancel: () => void;
}) {
  const quantities = gauge.quantities?.length ? gauge.quantities : [gauge.quantity];
  const [quantity, setQuantity] = useState<AlertQuantity>(rule?.quantity ?? gauge.quantity);
  const [kind, setKind] = useState<AlertKind>(rule?.kind ?? "above");
  const [text, setText] = useState(rule ? String(rule.value) : gauge.current !== null ? String(gauge.current) : "");
  const [gapH, setGapH] = useState<number | null>(rule?.gapH ?? null);
  const [bad, setBad] = useState(false);
  const [busy, setBusy] = useState(false);
  const v = parseAlertValue(text);
  const ok = alertValueOk(kind, v);
  const draft: AlertDraft = { station: gauge.station, stationName: gauge.stationName, quantity, kind, value: ok ? v : 0,
                              gapH, water: rule?.water ?? gauge.water };
  const pickKind = (k: AlertKind) => {
    // between a value and a % the number means something else: a fresh start (a % opens on 50)
    if (isChangeKind(k) !== isChangeKind(kind)) setText(isChangeKind(k) ? "50" : gauge.current !== null && quantity === gauge.quantity ? String(gauge.current) : "");
    setKind(k); setBad(false);
  };
  const save = async () => {
    if (!ok) { setBad(true); return; }
    setBusy(true);
    try { await onSave(draft); } finally { setBusy(false); }
  };
  return (
    <View testID="alert-form" style={{ gap: SPACE.s3, padding: SPACE.s3, borderWidth: 1, borderColor: palette.line2,
                                       borderRadius: palette.r.box, backgroundColor: palette.wash }}>
      <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{E.title(gauge.stationName)}</Text>
      {quantities.length > 1 && (
        <Field palette={palette} label={E.quantity}>
          <View accessibilityRole="radiogroup" aria-label={E.quantity} style={{ flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" }}>
            {quantities.map((q) => (
              <Chip key={q} palette={palette} look="choice" label={CONDITIONS_WORDS.quantity[q]} on={quantity === q}
                    testID={`alert-form-q-${q}`} onPress={() => { setQuantity(q); setBad(false); }} />
            ))}
          </View>
        </Field>
      )}
      <Field palette={palette} label={E.condition}>
        <View accessibilityRole="radiogroup" aria-label={E.condition} style={{ flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" }}>
          {ALERT_KINDS.map((k) => (
            <Chip key={k} palette={palette} look="choice" label={E.kinds[k]} on={kind === k}
                  testID={`alert-form-${k}`} onPress={() => pickKind(k)} />
          ))}
        </View>
      </Field>
      <Field palette={palette} label={E.value(kind, quantity)}>
        <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
          <TextInput testID="alert-form-value" value={text} onChangeText={(t) => { setText(t); setBad(false); }}
                     accessibilityLabel={E.value(kind, quantity)} placeholder={E.value(kind, quantity)}
                     placeholderTextColor={palette.faint} keyboardType="decimal-pad" inputMode="decimal"
                     style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: bad ? palette.closed : palette.ink,
                              borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card, minHeight: 44,
                              minWidth: 110 }} />
          <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{E.unit(kind, quantity)}</Text>
        </View>
        {source && <ContextLine palette={palette} source={source} station={gauge.station} quantity={quantity} />}
      </Field>
      <Field palette={palette} label={E.cadence}>
        <View accessibilityRole="radiogroup" aria-label={E.cadence} style={{ flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" }}>
          <Chip palette={palette} look="choice" label={E.cadenceDefault} on={gapH === null} testID="alert-form-gap-default"
                onPress={() => setGapH(null)} />
          {ALERT_GAPS_H.map((h) => (
            <Chip key={h} palette={palette} look="choice" label={E.cadenceGap(h)} on={gapH === h}
                  testID={`alert-form-gap-${h}`} onPress={() => setGapH(h)} />
          ))}
        </View>
      </Field>
      <Text testID="alert-form-line" accessibilityLiveRegion="polite"
            style={{ ...TYPE.small, color: bad ? palette.closed : palette.ink }}>
        {bad || (!ok && text.trim() !== "") ? (isChangeKind(kind) ? E.badPct : E.bad) : ok ? alertSummary(draft) : E.value(kind, quantity)}
      </Text>
      <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
        <Button palette={palette} label={E.save} testID="alert-form-save" busy={busy} onPress={() => { void save(); }} />
        <Button palette={palette} kind="ghost" label={E.cancel} testID="alert-form-cancel" onPress={onCancel} />
      </View>
    </View>
  );
}
