/**
 * (T234, T527) "ALERT ME" ON A GAUGE'S CARD, AND THE ALERT EDITOR — stock-style (owner 2026-11-09: "a clean alerting
 * system like you would have for stocks"): gauge → condition → value → done.
 *
 *   AlertMe      the gauge card's control: a tap opens the editor in place on THIS gauge (its quantity on the chart, the
 *                reading now). Saving turns notifications on and, the first time, the phone asks for its permission; a
 *                refusal is said here.
 *   AlertEditor  quantity (flow / level, the ones the gauge has) · "Crosses above" / "Crosses below" · the line: a value
 *                (prefilled with the reading now, the reading and today's normal range beside it) or a percentile for the
 *                day (10th / 25th / median / 75th / 90th or a custom one, from the gauge's own record: `useAlertRecord`,
 *                the chart's year envelope; a gauge with none shows the choice disabled with the reason) · ONE plain line
 *                of what will happen (`alertSummary`) · Save. Also step 2 of More → Notifications' "Add an alert".
 * Draws nothing where the app has no notifications (the web, a test). Every word is core's (`NOTIFY_WORDS`); the state
 * and actions are `@app/ui` `useNotifications`.
 */
import { useState } from "react";
import { TextInput, View } from "react-native";
import {
  ALERT_DIRECTIONS, alertContextLine, alertSummary, CONDITIONS_WORDS, NOTIFY_WORDS, now, parseAlertValue, percentileByDay,
  percentileOk, PERCENTILE_PRESETS, type AlertDirection, type AlertQuantity, type AlertThreshold, type GaugeAlertRule,
} from "@app/core";
import type { WaterSource } from "@app/data";
import { alertNowValue, useAlertContext, useAlertRecord, useNotifications, type AlertRecord, type NotificationsVM } from "@app/ui";
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

/** A new alert from the editor's draft: its id, time and switch. */
export const newAlert = (station: string, d: AlertDraft): GaugeAlertRule =>
  ({ ...d, id: `${station}-${now().toString(36)}`, createdAt: now(), on: true });

export function AlertMe({ palette, target, source }: { palette: Palette; target: AlertTarget; source: WaterSource }) {
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
                     onSave={async (d) => {
                       await n.putRule(newAlert(target.station, d));
                       setOpen(false);
                       setSaved(true);
                     }} />
      )}
      {saved && <AlertStatus n={n} palette={palette} />}
    </View>
  );
}

/** After a save: where to manage it, or why nothing can be shown (the phone refused, a write failed). */
export function AlertStatus({ n, palette }: { n: NotificationsVM; palette: Palette }) {
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

const row = { flexDirection: "row", gap: SPACE.s2, flexWrap: "wrap" } as const;

/** Why the percentile line cannot be picked now, or null when it can. */
function recordWhy(rec: AlertRecord, q: AlertQuantity): string | null {
  switch (rec.state) {
    case "ready": return null;
    case "loading": return E.recordLoading;
    case "none": return E.noRecord(q);
    case "failed": return E.recordFailed(rec.error.message);
  }
}

export function AlertEditor({ palette, source, gauge, onSave, onCancel }: {
  palette: Palette; source: WaterSource;
  gauge: Pick<AlertTarget, "station" | "stationName" | "quantity" | "quantities" | "current" | "water">;
  onSave: (r: AlertDraft) => void | Promise<void>; onCancel: () => void;
}) {
  const quantities = gauge.quantities?.length ? gauge.quantities : [gauge.quantity];
  const [quantity, setQuantity] = useState<AlertQuantity>(gauge.quantity);
  const [direction, setDirection] = useState<AlertDirection>("above");
  const [lineKind, setLineKind] = useState<AlertThreshold["kind"]>("value");
  const [text, setText] = useState(gauge.current !== null ? String(gauge.current) : "");
  const [pct, setPct] = useState<number | "custom">(75);
  const [pctText, setPctText] = useState("");
  const [bad, setBad] = useState(false);
  const [busy, setBusy] = useState(false);
  const ctx = useAlertContext(source, gauge.station, quantity);
  const rec = useAlertRecord(source, gauge.station, quantity);
  const why = recordWhy(rec, quantity);
  // percentile picked but this quantity has no record (another quantity picked): nothing to save, the reason said
  const percentile = lineKind === "percentile" && why === null;

  const v = parseAlertValue(text);
  const p = pct === "custom" ? parseAlertValue(pctText) : pct;
  const ok = percentile ? percentileOk(p) : lineKind === "value" && v !== null;
  const threshold: AlertThreshold | null = !ok ? null
    : percentile && rec.state === "ready" ? { kind: "percentile", pct: p!, byDay: percentileByDay(rec.yearBand, p!) }
    : { kind: "value", value: v! };
  const draft: AlertDraft | null = threshold
    ? { station: gauge.station, stationName: gauge.stationName, quantity, direction, threshold, water: gauge.water } : null;

  const save = async () => {
    if (!draft) { setBad(true); return; }
    setBusy(true);
    try { await onSave(draft); } finally { setBusy(false); }
  };
  const typed = percentile ? (pct === "custom" ? pctText.trim() !== "" : true) : text.trim() !== "";
  const message = lineKind === "percentile" && why !== null ? why
    : (bad || (!ok && typed)) ? (lineKind === "percentile" ? E.badPct : E.bad)
    : draft ? alertSummary(draft, ctx.current) : lineKind === "value" ? E.value(quantity) : E.customLabel;
  return (
    <View testID="alert-form" style={{ gap: SPACE.s3, padding: SPACE.s3, borderWidth: 1, borderColor: palette.line2,
                                       borderRadius: palette.r.box, backgroundColor: palette.wash }}>
      <Text accessibilityRole="header" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{E.title(gauge.stationName)}</Text>
      {quantities.length > 1 && (
        <Field palette={palette} label={E.quantity}>
          <View accessibilityRole="radiogroup" aria-label={E.quantity} style={row}>
            {quantities.map((q) => (
              <Chip key={q} palette={palette} look="choice" label={CONDITIONS_WORDS.quantity[q]} on={quantity === q}
                    testID={`alert-form-q-${q}`}
                    onPress={() => {
                      // the number typed was in the other quantity's units: start again from this one's reading
                      if (q !== quantity) setText(q === gauge.quantity && gauge.current !== null ? String(gauge.current) : "");
                      setQuantity(q); setBad(false);
                    }} />
            ))}
          </View>
        </Field>
      )}
      <Field palette={palette} label={E.condition}>
        <View accessibilityRole="radiogroup" aria-label={E.condition} style={row}>
          {ALERT_DIRECTIONS.map((d) => (
            <Chip key={d} palette={palette} look="choice" label={E.directions[d]} on={direction === d}
                  testID={`alert-form-${d}`} onPress={() => setDirection(d)} />
          ))}
        </View>
      </Field>
      <Field palette={palette} label={E.line}>
        <View accessibilityRole="radiogroup" aria-label={E.line} style={row}>
          <Chip palette={palette} look="choice" label={E.lineKinds.value} on={lineKind === "value"} testID="alert-form-line-value"
                onPress={() => { setLineKind("value"); setBad(false); }} />
          <Chip palette={palette} look="choice" label={E.lineKinds.percentile} on={lineKind === "percentile"}
                testID="alert-form-line-percentile" disabled={why !== null}
                onPress={() => { setLineKind("percentile"); setBad(false); }} />
        </View>
        {why !== null && lineKind === "value" && (
          <Text testID="alert-form-no-percentile" style={{ ...TYPE.small, color: palette.sub }}>{why}</Text>
        )}
      </Field>
      {lineKind === "value" && (
        <Field palette={palette} label={E.value(quantity)}>
          <View style={{ flexDirection: "row", alignItems: "center", gap: SPACE.s2 }}>
            <TextInput testID="alert-form-value" value={text} onChangeText={(t) => { setText(t); setBad(false); }}
                       accessibilityLabel={E.value(quantity)} placeholder={E.value(quantity)}
                       placeholderTextColor={palette.faint} keyboardType="decimal-pad" inputMode="decimal"
                       style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: bad ? palette.closed : palette.ink,
                                borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card, minHeight: 44,
                                minWidth: 110 }} />
            <Text style={{ ...TYPE.bodyStrong, color: palette.sub }}>{CONDITIONS_WORDS.unit[quantity]}</Text>
          </View>
          <Text testID="alert-form-context" accessibilityLiveRegion="polite" style={{ ...TYPE.small, color: palette.sub }}>
            {ctx.loading ? E.loading : alertContextLine(quantity, alertNowValue(quantity, gauge, ctx.current?.value ?? null), ctx.normal)}
          </Text>
        </Field>
      )}
      {percentile && (
        <Field palette={palette} label={E.percentile}>
          <View accessibilityRole="radiogroup" aria-label={E.percentile} style={row}>
            {PERCENTILE_PRESETS.map((x) => (
              <Chip key={x} palette={palette} look="choice" label={E.presets(x)} on={pct === x} testID={`alert-form-p${x}`}
                    onPress={() => { setPct(x); setBad(false); }} />
            ))}
            <Chip palette={palette} look="choice" label={E.custom} on={pct === "custom"} testID="alert-form-p-custom"
                  onPress={() => { setPct("custom"); setBad(false); }} />
          </View>
          {pct === "custom" && (
            <TextInput testID="alert-form-pct" value={pctText} onChangeText={(t) => { setPctText(t); setBad(false); }}
                       accessibilityLabel={E.customLabel} placeholder={E.customLabel} placeholderTextColor={palette.faint}
                       keyboardType="number-pad" inputMode="numeric"
                       style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5, borderColor: bad ? palette.closed : palette.ink,
                                borderRadius: palette.r.box, padding: SPACE.s2, backgroundColor: palette.card, minHeight: 44,
                                maxWidth: 220 }} />
          )}
        </Field>
      )}
      <Text testID="alert-form-line" accessibilityLiveRegion="polite"
            style={{ ...TYPE.small, color: bad ? palette.closed : palette.ink }}>
        {message}
      </Text>
      <View style={{ flexDirection: "row", gap: SPACE.s2 }}>
        <Button palette={palette} label={E.save} testID="alert-form-save" busy={busy} onPress={() => { void save(); }} />
        <Button palette={palette} kind="ghost" label={E.cancel} testID="alert-form-cancel" onPress={onCancel} />
      </View>
    </View>
  );
}
