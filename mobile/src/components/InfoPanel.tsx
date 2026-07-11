/**
 * InfoPanel (mobile) — bottom sheet showing the tapped feature's name, aliases,
 * and its resolved regulations. This is a lean port of the web
 * `InfoPanel.tsx`; the full sectioned rule cards, PDF/source-image viewers, and
 * issue-report flow are ported incrementally from the web component.
 */
import React from 'react';
import { ScrollView, StyleSheet, Text, TouchableOpacity, View } from 'react-native';

import type { BathymetrySurvey, Regulation } from '../types/regulations';
import { buildAliasLines, getColorForType, type NameVariant } from '../utils/featureUtils';
import { colors, radius, spacing, typography } from '../theme/tokens';

export interface InfoPanelData {
  displayName: string;
  featureType: string;
  nameVariants: NameVariant[];
  regulations: Regulation[];
  bathymetry?: BathymetrySurvey[];
}

interface InfoPanelProps {
  data: InfoPanelData | null;
  onClose: () => void;
  /** Tablet layout docks the panel to the right instead of a bottom sheet. */
  isTablet?: boolean;
  /** Open a bathymetry depth-map PDF. */
  onOpenPdf?: (survey: BathymetrySurvey) => void;
  /** Open a synopsis source-page image (PNG filename + human label). */
  onOpenSource?: (png: string, label: string) => void;
  /** Open the issue-report form for this waterbody. */
  onReportIssue?: () => void;
}

export function InfoPanel({
  data,
  onClose,
  isTablet = false,
  onOpenPdf,
  onOpenSource,
  onReportIssue,
}: InfoPanelProps): React.JSX.Element | null {
  if (!data) return null;

  const accent = getColorForType(data.featureType as never);
  const aliasLines = buildAliasLines(
    data.nameVariants.filter((v) => v.name.toLowerCase() !== data.displayName.toLowerCase()),
  );

  return (
    <View style={isTablet ? styles.sheetTablet : styles.sheet}>
      <View style={[styles.header, { borderLeftColor: accent }]}>
        <View style={styles.headerText}>
          <Text style={styles.title} numberOfLines={2}>
            {data.displayName}
          </Text>
          {aliasLines.alsoKnownAs ? (
            <Text style={styles.subtle}>{aliasLines.alsoKnownAs}</Text>
          ) : null}
          {aliasLines.inContext ? (
            <Text style={styles.subtle}>{aliasLines.inContext}</Text>
          ) : null}
        </View>
        <TouchableOpacity
          onPress={onClose}
          accessibilityRole="button"
          accessibilityLabel="Close details"
          style={styles.closeBtn}
        >
          <Text style={styles.closeTxt}>✕</Text>
        </TouchableOpacity>
      </View>

      <ScrollView style={styles.body} contentContainerStyle={styles.bodyContent}>
        {data.regulations.length === 0 ? (
          <Text style={styles.muted}>No regulations found for this waterbody.</Text>
        ) : (
          data.regulations.map((reg) => (
            <View key={reg.regulation_id} style={styles.card}>
              <Text style={styles.ruleText}>{reg.rule_text}</Text>
              {reg.restriction_details ? (
                <Text style={styles.details}>{reg.restriction_details}</Text>
              ) : null}
              {reg.provenance ? (
                <Text style={styles.provenance}>via {reg.provenance}</Text>
              ) : null}
              {reg.source_image ? (
                <TouchableOpacity
                  onPress={() =>
                    onOpenSource?.(
                      reg.source_image as string,
                      reg.source_page ? `Synopsis page ${reg.source_page}` : 'Synopsis source',
                    )
                  }
                  accessibilityRole="button"
                >
                  <Text style={styles.link}>
                    View source{reg.source_page ? ` (page ${reg.source_page})` : ''}
                  </Text>
                </TouchableOpacity>
              ) : null}
            </View>
          ))
        )}

        {data.bathymetry && data.bathymetry.length > 0 ? (
          <View style={styles.bathySection}>
            <Text style={styles.sectionLabel}>Depth maps</Text>
            {data.bathymetry.map((survey) => (
              <TouchableOpacity
                key={survey.pdf}
                style={styles.bathyRow}
                onPress={() => onOpenPdf?.(survey)}
                accessibilityRole="button"
                accessibilityLabel={`Open depth map ${survey.title}`}
              >
                <Text style={styles.bathyIcon}>▤</Text>
                <Text style={styles.bathyTitle} numberOfLines={2}>
                  {survey.title}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
        ) : null}

        {onReportIssue ? (
          <TouchableOpacity onPress={onReportIssue} accessibilityRole="button">
            <Text style={styles.reportLink}>Report an issue with this waterbody</Text>
          </TouchableOpacity>
        ) : null}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  sheet: {
    position: 'absolute',
    left: 0,
    right: 0,
    bottom: 0,
    maxHeight: '55%',
    backgroundColor: colors.panel,
    borderTopLeftRadius: radius.lg,
    borderTopRightRadius: radius.lg,
    paddingBottom: spacing.xl,
  },
  sheetTablet: {
    position: 'absolute',
    top: 0,
    right: 0,
    bottom: 0,
    width: 400,
    backgroundColor: colors.panel,
    borderTopLeftRadius: radius.lg,
    borderBottomLeftRadius: radius.lg,
    paddingBottom: spacing.xl,
    borderLeftWidth: StyleSheet.hairlineWidth,
    borderLeftColor: colors.border,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    padding: spacing.lg,
    borderLeftWidth: 4,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.border,
  },
  headerText: { flex: 1, paddingRight: spacing.md },
  title: { color: colors.text, fontSize: typography.title, fontWeight: '700' },
  subtle: { color: colors.textMuted, fontSize: typography.small, marginTop: spacing.xs },
  closeBtn: { padding: spacing.sm },
  closeTxt: { color: colors.textMuted, fontSize: typography.title },
  body: { paddingHorizontal: spacing.lg },
  bodyContent: { paddingVertical: spacing.md, gap: spacing.md },
  muted: { color: colors.textMuted, fontSize: typography.body },
  card: {
    backgroundColor: colors.panelElevated,
    borderRadius: radius.md,
    padding: spacing.md,
  },
  ruleText: { color: colors.text, fontSize: typography.body },
  details: { color: colors.textMuted, fontSize: typography.small, marginTop: spacing.xs },
  provenance: {
    color: colors.textSubtle,
    fontSize: typography.caption,
    marginTop: spacing.xs,
    fontStyle: 'italic',
  },
  link: {
    color: colors.accent,
    fontSize: typography.small,
    marginTop: spacing.sm,
    fontWeight: '600',
  },
  bathySection: { gap: spacing.sm, marginTop: spacing.sm },
  sectionLabel: {
    color: colors.textMuted,
    fontSize: typography.caption,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  bathyRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: spacing.sm,
    backgroundColor: colors.panelElevated,
    borderRadius: radius.md,
    padding: spacing.md,
  },
  bathyIcon: { color: colors.accent, fontSize: typography.title },
  bathyTitle: { flex: 1, color: colors.text, fontSize: typography.body },
  reportLink: {
    color: colors.textMuted,
    fontSize: typography.small,
    textDecorationLine: 'underline',
    marginTop: spacing.lg,
    textAlign: 'center',
  },
});
