/**
 * IssueReport (mobile) — faithful port of the web `IssueReport.tsx`. Presents a
 * modal feedback form with the same fields (category, description, contact), the
 * same client-side validation (a non-empty description), and posts the assembled
 * report to the same Worker route (`/api/feedback`) with the same JSON payload
 * (`{ title, body, hp }`). Only the rendering layer changed: RN primitives + a
 * Platform-appropriate environment snapshot in place of the browser one.
 */
import React, { useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Modal,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

import { FEEDBACK_URL } from '../config';
import { colors, radius, spacing, typography } from '../theme/tokens';

/** Snapshot of app state attached to a report so a maintainer can reproduce it. */
export interface IssueReportContext {
  waterbodyName?: string;
  waterbodyId?: string;
  waterbodyType?: string;
  lat?: number;
  lng?: number;
  zoom?: number;
  dataVersion?: string;
  pageUrl?: string;
  /** Extra diagnostics (map view internals, selected-feature metadata, …)
   *  rendered verbatim under "Technical details". Empty values are skipped. */
  details?: Record<string, string | number | boolean | null | undefined>;
}

export interface IssueReportProps {
  visible: boolean;
  context: IssueReportContext | null;
  onClose: () => void;
}

const CATEGORIES = [
  'Incorrect regulation',
  'Wrong location or boundary',
  'Missing waterbody',
  'Naming issue',
  'Bug or technical problem',
  'Other',
] as const;
type Category = (typeof CATEGORIES)[number];

/**
 * Capture universal runtime environment for the report. Mobile analogue of the
 * web `collectEnvironment` — swaps browser globals for `Platform`/`Intl`.
 */
function collectEnvironment(): Record<string, string | number> {
  const env: Record<string, string | number> = {};
  env['Platform'] = `${Platform.OS} ${String(Platform.Version)}`;
  try {
    env['Timezone'] = Intl.DateTimeFormat().resolvedOptions().timeZone;
    env['Language'] = Intl.DateTimeFormat().resolvedOptions().locale;
  } catch {
    // Intl unavailable — skip.
  }
  env['Local time'] = new Date().toString();
  return env;
}

/**
 * Build the Markdown report body — mirrors the web builder 1:1 so the payload a
 * maintainer receives is identical regardless of client.
 */
function buildReportBody(
  category: Category,
  description: string,
  contact: string,
  ctx: IssueReportContext,
  environment: Record<string, string | number>,
): string {
  const lines: string[] = [
    '### Description',
    description.trim() || '_(none provided)_',
    '',
    '### Category',
    category,
    '',
  ];

  const context: string[] = [];
  if (ctx.waterbodyName) {
    const meta = [ctx.waterbodyType, ctx.waterbodyId ? `id ${ctx.waterbodyId}` : null]
      .filter(Boolean)
      .join(', ');
    context.push(`- Waterbody: ${ctx.waterbodyName}${meta ? ` (${meta})` : ''}`);
  }
  if (ctx.lat != null && ctx.lng != null) {
    const zoom = ctx.zoom != null ? `, zoom ${ctx.zoom.toFixed(1)}` : '';
    context.push(`- Map location: ${ctx.lat.toFixed(5)}, ${ctx.lng.toFixed(5)}${zoom}`);
  }
  if (ctx.pageUrl) context.push(`- Page: ${ctx.pageUrl}`);
  if (ctx.dataVersion) context.push(`- Data version: ${ctx.dataVersion}`);
  if (contact.trim()) context.push(`- Contact: ${contact.trim()}`);

  if (context.length) {
    lines.push('### Context', ...context, '');
  }

  const diagnostics: Record<string, string | number | boolean> = {};
  for (const [k, v] of Object.entries(ctx.details ?? {})) {
    if (v !== null && v !== undefined && v !== '') diagnostics[k] = v;
  }
  for (const [k, v] of Object.entries(environment)) {
    if (v !== null && v !== undefined && v !== '') diagnostics[k] = v;
  }
  const detailKeys = Object.keys(diagnostics);
  if (detailKeys.length) {
    lines.push('### Technical details');
    for (const k of detailKeys) lines.push(`- ${k}: ${diagnostics[k]}`);
    lines.push('');
  }

  return lines.join('\n').trim();
}

export function IssueReport({ visible, context, onClose }: IssueReportProps): React.JSX.Element | null {
  const [category, setCategory] = useState<Category>('Incorrect regulation');
  const [description, setDescription] = useState('');
  const [contact, setContact] = useState('');
  const [ctx, setCtx] = useState<IssueReportContext>({});
  const [environment, setEnvironment] = useState<Record<string, string | number>>({});
  const [submitState, setSubmitState] = useState<'idle' | 'sending' | 'sent' | 'error'>('idle');
  // Honeypot: hidden field; only bots fill it. Kept out of the report body.
  const [hp, setHp] = useState('');

  // Fresh slate + context snapshot each time the modal opens.
  useEffect(() => {
    if (!visible) return;
    setCtx(context ?? {});
    setEnvironment(collectEnvironment());
    setCategory('Incorrect regulation');
    setDescription('');
    setContact('');
    setSubmitState('idle');
    setHp('');
  }, [visible, context]);

  const body = useMemo(
    () => buildReportBody(category, description, contact, ctx, environment),
    [category, description, contact, ctx, environment],
  );

  const title = useMemo(() => {
    const wb = ctx.waterbodyName ? `: ${ctx.waterbodyName}` : '';
    return `[Report] ${category}${wb}`;
  }, [category, ctx.waterbodyName]);

  if (!visible) return null;

  const canSubmit = description.trim().length > 0;

  // Send the report directly — no account needed. The worker stores it and
  // (if configured) emails it.
  const handleSend = async () => {
    if (!canSubmit || submitState === 'sending' || submitState === 'sent') return;
    setSubmitState('sending');
    try {
      const resp = await fetch(FEEDBACK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, body, hp }),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      setSubmitState('sent');
    } catch {
      setSubmitState('error');
    }
  };

  return (
    <Modal
      visible={visible}
      transparent
      animationType="fade"
      onRequestClose={onClose}
    >
      <Pressable style={styles.overlay} onPress={onClose}>
        <KeyboardAvoidingView
          behavior={Platform.OS === 'ios' ? 'padding' : undefined}
          style={styles.avoider}
        >
          <Pressable style={styles.modal} onPress={() => {}} accessibilityViewIsModal>
            <View style={styles.headerRow}>
              <Text style={styles.heading}>Feedback</Text>
              <Pressable
                onPress={onClose}
                accessibilityRole="button"
                accessibilityLabel="Close issue report"
                style={styles.closeBtn}
              >
                <Text style={styles.closeTxt}>✕</Text>
              </Pressable>
            </View>

            <ScrollView
              style={styles.content}
              contentContainerStyle={styles.contentInner}
              keyboardShouldPersistTaps="handled"
            >
              <Text style={styles.intro}>
                Spotted a wrong regulation, a misplaced boundary, or a bug? Let us know.{' '}
                <Text style={styles.introStrong}>Send report</Text> delivers it straight to the
                maintainers—no account needed.
              </Text>

              <View style={styles.field}>
                <Text style={styles.label}>Type of issue</Text>
                <View style={styles.chips}>
                  {CATEGORIES.map((c) => {
                    const selected = c === category;
                    return (
                      <Pressable
                        key={c}
                        onPress={() => setCategory(c)}
                        accessibilityRole="button"
                        accessibilityState={{ selected }}
                        style={[styles.chip, selected && styles.chipSelected]}
                      >
                        <Text style={[styles.chipTxt, selected && styles.chipTxtSelected]}>{c}</Text>
                      </Pressable>
                    );
                  })}
                </View>
              </View>

              <View style={styles.field}>
                <Text style={styles.label}>
                  What&apos;s wrong? <Text style={styles.req}>*</Text>
                </Text>
                <TextInput
                  value={description}
                  onChangeText={setDescription}
                  placeholder="Describe the problem…"
                  placeholderTextColor={colors.textSubtle}
                  multiline
                  numberOfLines={4}
                  style={[styles.input, styles.textarea]}
                />
              </View>

              <View style={styles.field}>
                <Text style={styles.label}>Contact (optional)</Text>
                <TextInput
                  value={contact}
                  onChangeText={setContact}
                  placeholder="Email or name, if you'd like a reply"
                  placeholderTextColor={colors.textSubtle}
                  autoCapitalize="none"
                  autoCorrect={false}
                  style={styles.input}
                />
              </View>

              <View style={styles.field}>
                <Text style={styles.previewLabel}>What gets sent</Text>
                <View style={styles.preview}>
                  <Text style={styles.previewTxt}>{body}</Text>
                </View>
              </View>

              {/* Honeypot: off-screen; bots that fill it are silently dropped. */}
              <TextInput
                value={hp}
                onChangeText={setHp}
                autoComplete="off"
                importantForAutofill="no"
                accessibilityElementsHidden
                importantForAccessibility="no-hide-descendants"
                style={styles.honeypot}
              />
            </ScrollView>

            {submitState === 'sent' ? (
              <Text style={[styles.status, styles.statusSuccess]} accessibilityRole="text">
                ✓ Thanks — your report was sent.
              </Text>
            ) : null}
            {submitState === 'error' ? (
              <Text style={[styles.status, styles.statusError]} accessibilityRole="alert">
                Couldn&apos;t send. Please try again.
              </Text>
            ) : null}

            <View style={styles.actions}>
              <Pressable
                onPress={handleSend}
                disabled={!canSubmit || submitState === 'sending' || submitState === 'sent'}
                accessibilityRole="button"
                accessibilityLabel="Send report"
                style={[
                  styles.primary,
                  (!canSubmit || submitState === 'sending' || submitState === 'sent') &&
                    styles.primaryDisabled,
                ]}
              >
                {submitState === 'sending' ? (
                  <View style={styles.primaryRow}>
                    <ActivityIndicator size="small" color={colors.text} />
                    <Text style={styles.primaryTxt}>Sending…</Text>
                  </View>
                ) : submitState === 'sent' ? (
                  <Text style={styles.primaryTxt}>✓ Sent</Text>
                ) : (
                  <Text style={styles.primaryTxt}>Send report</Text>
                )}
              </Pressable>
            </View>
          </Pressable>
        </KeyboardAvoidingView>
      </Pressable>
    </Modal>
  );
}

/** Small footer link that opens the feedback modal (mirror of web `IssueReportLink`). */
export function IssueReportLink({ onPress }: { onPress: () => void }): React.JSX.Element {
  return (
    <Pressable onPress={onPress} accessibilityRole="button" accessibilityLabel="Send feedback">
      <Text style={styles.linkTxt}>Feedback</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.6)',
    justifyContent: 'center',
    padding: spacing.lg,
  },
  avoider: {
    justifyContent: 'center',
  },
  modal: {
    backgroundColor: colors.panel,
    borderRadius: radius.lg,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    maxHeight: '90%',
    overflow: 'hidden',
  },
  headerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: spacing.lg,
    paddingTop: spacing.lg,
    paddingBottom: spacing.md,
  },
  heading: { color: colors.text, fontSize: typography.title, fontWeight: '700' },
  closeBtn: { padding: spacing.sm },
  closeTxt: { color: colors.textMuted, fontSize: typography.title },
  content: { paddingHorizontal: spacing.lg },
  contentInner: { paddingBottom: spacing.md, gap: spacing.lg },
  intro: { color: colors.textMuted, fontSize: typography.small, lineHeight: 20 },
  introStrong: { color: colors.text, fontWeight: '700' },
  field: { gap: spacing.sm },
  label: { color: colors.text, fontSize: typography.small, fontWeight: '600' },
  req: { color: colors.danger },
  chips: { flexDirection: 'row', flexWrap: 'wrap', gap: spacing.sm },
  chip: {
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.sm,
    borderRadius: radius.pill,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    backgroundColor: colors.panelElevated,
  },
  chipSelected: { backgroundColor: colors.accent, borderColor: colors.accent },
  chipTxt: { color: colors.textMuted, fontSize: typography.small },
  chipTxtSelected: { color: colors.text, fontWeight: '600' },
  input: {
    backgroundColor: colors.panelElevated,
    borderRadius: radius.md,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    color: colors.text,
    fontSize: typography.body,
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.md,
  },
  textarea: { minHeight: 96, textAlignVertical: 'top' },
  previewLabel: { color: colors.textMuted, fontSize: typography.small, fontWeight: '600' },
  preview: {
    backgroundColor: colors.bg,
    borderRadius: radius.md,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    padding: spacing.md,
  },
  previewTxt: {
    color: colors.textMuted,
    fontSize: typography.caption,
    fontFamily: Platform.OS === 'ios' ? 'Menlo' : 'monospace',
  },
  honeypot: { height: 0, width: 0, opacity: 0, padding: 0, margin: 0 },
  status: {
    paddingHorizontal: spacing.lg,
    paddingTop: spacing.sm,
    fontSize: typography.small,
  },
  statusSuccess: { color: colors.success },
  statusError: { color: colors.danger },
  actions: {
    flexDirection: 'row',
    justifyContent: 'flex-end',
    padding: spacing.lg,
    gap: spacing.md,
  },
  primary: {
    backgroundColor: colors.accent,
    borderRadius: radius.md,
    paddingHorizontal: spacing.lg,
    paddingVertical: spacing.md,
    minWidth: 130,
    alignItems: 'center',
  },
  primaryDisabled: { opacity: 0.5 },
  primaryRow: { flexDirection: 'row', alignItems: 'center', gap: spacing.sm },
  primaryTxt: { color: colors.text, fontSize: typography.body, fontWeight: '700' },
  linkTxt: { color: colors.accent, fontSize: typography.small, fontWeight: '600' },
});
