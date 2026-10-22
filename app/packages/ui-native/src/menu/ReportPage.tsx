/**
 * REPORT A PROBLEM (owner 2026-10-21): what is wrong, in the reader's words, and the diagnostics they can SEE and switch
 * off before anything leaves the device. Nothing is sent in the background and nothing is sent without a press: the
 * report goes out through the platform — an email to the contact address (`mailto:`) or the share sheet — and there
 * is no server behind either. The exact message is shown before it goes ("This is what will be sent").
 *
 * Draws `diagnosticRows` / `reportMessage` (core/menu.ts); decides nothing.
 */
import { useMemo, useState } from "react";
import { Linking, Share, Switch, Text, TextInput, View } from "react-native";
import { contactVM, diagnosticRows, mailtoUrl, MENU_WORDS, reportMessage, type DiagKey, type Diagnostics } from "@app/core";
import { Button } from "../Button";
import { TYPE } from "../type";
import { SPACE, type Palette } from "../theme";

export function ReportPage({ diagnostics, contact, palette }: {
  diagnostics: Diagnostics; contact: string | null; palette: Palette;
}) {
  const w = MENU_WORDS.report;
  const [what, setWhat] = useState("");
  const [off, setOff] = useState<Partial<Record<DiagKey, boolean>>>({});
  const [shareFailed, setShareFailed] = useState(false);
  const rows = useMemo(() => diagnosticRows(diagnostics), [diagnostics]);
  const on = useMemo(() => Object.fromEntries(rows.map((r) => [r.k, !off[r.k]])) as Partial<Record<DiagKey, boolean>>,
                     [rows, off]);
  const message = useMemo(() => reportMessage(what, diagnostics, on), [what, diagnostics, on]);
  const to = contactVM(contact);
  const ready = what.trim().length > 0;

  const email = to.kind === "email" && ready ? () => { void Linking.openURL(mailtoUrl(to.value, message)); } : undefined;
  const share = ready ? () => {
    setShareFailed(false);
    Share.share({ title: message.subject, message: `${message.subject}\n\n${message.body}` })
      .catch(() => setShareFailed(true));
  } : undefined;

  return (
    <>
      <Text style={{ ...TYPE.body, color: palette.sub }}>{w.intro}</Text>
      <View style={{ gap: SPACE.s2 }}>
        <Text style={{ ...TYPE.section, color: palette.sub }}>{w.what}</Text>
        <TextInput testID="report-what" value={what} onChangeText={setWhat} multiline placeholder={w.whatHint}
                   accessibilityLabel={w.what} placeholderTextColor={palette.faint} textAlignVertical="top"
                   style={{ ...TYPE.body, color: palette.ink, minHeight: 112, borderWidth: 1.5, borderColor: palette.ink,
                            borderRadius: palette.r.box, padding: SPACE.s3, backgroundColor: palette.card }} />
      </View>

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
      <View style={{ gap: SPACE.s2 }}>
        {to.kind === "email" && <Button palette={palette} label={w.send} onPress={email} />}
        <Button palette={palette} kind={to.kind === "email" ? "ghost" : "solid"} label={w.share} onPress={share} />
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
