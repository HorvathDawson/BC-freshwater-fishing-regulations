/**
 * REPORT A PROBLEM (owner 2026-10-21): what is wrong, in the reader's words, and the diagnostics they can SEE and switch
 * off before anything leaves the device. Nothing is sent in the background and nothing is sent without a press. The
 * exact message is shown before it goes ("This is what will be sent").
 *
 * (T237) "Send report" POSTs it (the reason, the words, an optional email and the rows left on — `reportPayload`) to the
 * report endpoint through the app's outbox: sent, or kept on the device while offline and sent with the update
 * service's retry. Email and the share sheet stay as the way to send it yourself.
 *
 * Draws `diagnosticRows` / `reportMessage` (core/menu/report.ts) and `useReportSend` (@app/ui); decides nothing.
 */
import { useMemo, useState } from "react";
import { Linking, Share, Switch, Text, TextInput, View } from "react-native";
import { contactVM, diagnosticRows, emailProblem, mailtoUrl, MENU_WORDS, REPORT_REASONS, reportMessage, type DiagKey,
         type Diagnostics, type ReportReason } from "@app/core";
import { useReportSend, type ReportSender } from "@app/ui";
import { Button } from "../Button";
import { Choice } from "../Sheet";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

export function ReportPage({ diagnostics, contact, palette, sender = null }: {
  diagnostics: Diagnostics; contact: string | null; palette: Palette;
  /** (T237) the app's report outbox; null: this build has no report address (email / share only) */
  sender?: ReportSender | null;
}) {
  const w = MENU_WORDS.report;
  const [reason, setReason] = useState<ReportReason>("rule");
  const [email, setEmail] = useState("");
  const tx = useReportSend(sender);
  const [what, setWhat] = useState("");
  const [off, setOff] = useState<Partial<Record<DiagKey, boolean>>>({});
  const [shareFailed, setShareFailed] = useState(false);
  const rows = useMemo(() => diagnosticRows(diagnostics), [diagnostics]);
  const on = useMemo(() => Object.fromEntries(rows.map((r) => [r.k, !off[r.k]])) as Partial<Record<DiagKey, boolean>>,
                     [rows, off]);
  const message = useMemo(() => reportMessage(what, diagnostics, on), [what, diagnostics, on]);
  const to = contactVM(contact);
  const ready = what.trim().length > 0;
  const emailBad = emailProblem(email);
  const sendNow = tx.send && ready && !emailBad && tx.state.kind !== "sending"
    ? () => tx.send!({ reason, what, email, diagnostics, on }) : undefined;
  const reasons = useMemo(() => REPORT_REASONS.map((k) => ({ k, t: w.reasons[k] })), [w]);

  const mail = to.kind === "email" && ready ? () => { void Linking.openURL(mailtoUrl(to.value, message)); } : undefined;
  const share = ready ? () => {
    setShareFailed(false);
    Share.share({ title: message.subject, message: `${message.subject}\n\n${message.body}` })
      .catch(() => setShareFailed(true));
  } : undefined;

  // (T237) sent, or kept for later: the confirmation in the form's place, and a way to report something else
  if (tx.state.kind === "sent" || tx.state.kind === "queued") {
    const said = tx.state.kind === "sent" ? w.sent : tx.state.offline ? w.queued : w.queuedRetry;
    return (
      <>
        <View testID="report-done" accessibilityRole="alert"
              style={{ borderWidth: 1.5, borderColor: palette.ink, padding: SPACE.s3, backgroundColor: palette.tint }}>
          <Text testID="report-done-text" style={{ ...TYPE.bodyStrong, color: palette.ink }}>{said}</Text>
        </View>
        <Text style={{ ...TYPE.small, color: palette.sub }}>{w.privacy}</Text>
        <Button palette={palette} kind="ghost" label={w.another}
                onPress={() => { setWhat(""); setEmail(""); setOff({}); tx.reset(); }} />
      </>
    );
  }

  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.sub }}>{w.intro}</Text>
      {tx.send && (
        <View style={{ gap: SPACE.s2 }} testID="report-reasons">
          <Text style={{ ...TYPE.section, color: palette.sub }}>{w.reason}</Text>
          <Choice palette={palette} options={reasons} value={reason} onChange={setReason} label={w.reason} flush />
        </View>
      )}
      <View style={{ gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.section, color: palette.sub }}>{w.what}</Text>
        <TextInput testID="report-what" value={what} onChangeText={setWhat} multiline placeholder={w.whatHint}
                   accessibilityLabel={w.what} placeholderTextColor={palette.faint} textAlignVertical="top"
                   style={{ ...TYPE.body, color: palette.ink, minHeight: 112, borderWidth: 1.5, borderColor: palette.ink,
                            borderRadius: palette.r.box, padding: SPACE.s3, backgroundColor: palette.card }} />
      </View>

      {tx.send && (
        <View style={{ gap: SPACE.s2 }}>
          <Text style={{ ...TYPE.section, color: palette.sub }}>{w.email}</Text>
          <TextInput testID="report-email" value={email} onChangeText={setEmail} placeholder="name@example.com"
                     accessibilityLabel={w.email} placeholderTextColor={palette.faint} autoCapitalize="none"
                     autoCorrect={false} keyboardType="email-address" inputMode="email" autoComplete="email"
                     style={{ ...TYPE.body, color: palette.ink, borderWidth: 1.5,
                              borderColor: emailBad ? palette.closed : palette.ink,
                              borderRadius: palette.r.box, padding: SPACE.s3, backgroundColor: palette.card }} />
          <Text testID={emailBad ? "report-email-bad" : undefined}
                style={{ ...TYPE.small, color: emailBad ? palette.closed : palette.sub }}>{emailBad ?? w.emailHint}</Text>
        </View>
      )}

      {rows.length > 0 && (
        <View style={{ gap: SPACE.s1 }}>
          <Text style={{ ...TYPE.section, color: palette.sub }}>{w.diagnostics}</Text>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{w.diagnosticsHint}</Text>
          {rows.map((r) => (
            <View key={r.k} testID={`report-diag-${r.k}`}
                  style={{ flexDirection: "row", gap: SPACE.s3, paddingVertical: SPACE.s2,
                           borderTopWidth: 1, borderTopColor: palette.line }}>
              <View style={{ flex: 1, gap: 2, opacity: on[r.k] ? 1 : 0.45 }}>
                <Text style={{ ...TYPE.bodyStrong, color: palette.ink }}>{r.label}</Text>
                {r.lines.map((l, i) => (
                  <Text key={i} selectable style={{ ...TYPE.figure, fontSize: 12, color: palette.sub }}>{l}</Text>
                ))}
              </View>
              <Switch value={!!on[r.k]} onValueChange={(v) => setOff((o) => ({ ...o, [r.k]: !v }))}
                      accessibilityLabel={r.label} aria-checked={!!on[r.k]}
                      trackColor={{ false: palette.line2, true: palette.accent }} thumbColor={palette.card} />
            </View>
          ))}
        </View>
      )}

      {!ready && <Text testID="report-empty" style={{ ...TYPE.small, color: palette.sub }}>{w.empty}</Text>}
      {tx.send && (
        <View style={{ gap: SPACE.s2 }}>
          <Button palette={palette} label={tx.state.kind === "sending" ? w.sending : w.sendNow} onPress={sendNow} />
          {tx.state.kind === "refused" && (
            <Text testID="report-refused" style={{ ...TYPE.small, color: palette.closed }}>{w.refused(tx.state.error)}</Text>
          )}
          <Text testID="report-privacy" style={{ ...TYPE.small, color: palette.sub }}>{w.privacy}</Text>
          <Text style={{ ...TYPE.small, color: palette.sub }}>{w.orEmail}</Text>
        </View>
      )}
      <View style={{ gap: SPACE.s2 }}>
        {to.kind === "email" && <Button palette={palette} kind={tx.send ? "ghost" : "solid"} label={w.send} onPress={mail} />}
        <Button palette={palette} kind={to.kind === "email" || tx.send ? "ghost" : "solid"} label={w.share} onPress={share} />
      </View>
      {shareFailed && <Text testID="report-noshare" style={{ ...TYPE.small, color: palette.closed }}>{w.noShare}</Text>}

      {/* EXACTLY WHAT WILL BE SENT — selectable, so it can be copied when neither way out works */}
      <View style={{ gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.section, color: palette.sub }}>{w.preview}</Text>
        <View style={{ borderWidth: 1, borderColor: palette.line2, padding: SPACE.s3,
                       backgroundColor: palette.wash }}>
          <Text testID="report-preview" selectable style={{ ...TYPE.figure, fontSize: 12, color: palette.ink }}>
            {`${message.subject}\n\n${message.body}`}
          </Text>
        </View>
      </View>
    </>
  );
}
