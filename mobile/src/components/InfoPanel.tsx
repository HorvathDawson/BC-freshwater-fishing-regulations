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

/**
 * Group expanded rules so each synopsis entry renders its parsed rule cards
 * followed by a single source block (matches the web InfoPanel grouping).
 * Synopsis rules from one entry share an `iid`; base regs stand alone.
 */
type RegGroup = { key: string; regs: Regulation[]; source?: Regulation };

function buildRegGroups(regs: Regulation[]): RegGroup[] {
  const groups: RegGroup[] = [];
  const byKey = new Map<string, RegGroup>();
  for (const reg of regs) {
    const key = reg.source === 'synopsis' ? `syn:${reg.iid ?? reg.regulation_id}` : reg.regulation_id;
    let g = byKey.get(key);
    if (!g) {
      g = { key, regs: [] };
      byKey.set(key, g);
      groups.push(g);
    }
    g.regs.push(reg);
  }
  for (const g of groups) {
    g.source = g.regs.find(
      (r) =>
        r.source === 'synopsis' &&
        Boolean(r.source_image || r.source_page || r.raw_regs || r.rule_text),
    );
  }
  return groups;
}

export function InfoPanel({
  data,
  onClose,
  isTablet = false,
  onOpenPdf,
  onOpenSource,
  onReportIssue,
}: InfoPanelProps): React.JSX.Element | null {
  const [openText, setOpenText] = React.useState<Record<string, boolean>>({});
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
          buildRegGroups(data.regulations).map((group) => {
            const src = group.source;
            const officialText = src ? src.raw_regs || src.rule_text : null;
            const isOpen = Boolean(openText[group.key]);
            return (
              <View key={group.key} style={styles.group}>
                {group.regs.map((reg) => (
                  <View key={reg.regulation_id} style={styles.card}>
                    <Text style={styles.ruleText}>{reg.rule_text}</Text>
                    {reg.restriction_details ? (
                      <Text style={styles.details}>{reg.restriction_details}</Text>
                    ) : null}
                    {reg.provenance ? (
                      <Text style={styles.provenance}>via {reg.provenance}</Text>
                    ) : null}
                  </View>
                ))}

                {src ? (
                  <View style={styles.source}>
                    {src.source_image ? (
                      <TouchableOpacity
                        style={styles.sourceImgBtn}
                        onPress={() =>
                          onOpenSource?.(
                            src.source_image as string,
                            src.source_page ? `Synopsis page ${src.source_page}` : 'Synopsis source',
                          )
                        }
                        accessibilityRole="button"
                        accessibilityLabel="View regulation in synopsis"
                      >
                        <Text style={styles.sourceImgIcon}>▤</Text>
                        <Text style={styles.sourceImgTxt}>View regulation in synopsis</Text>
                      </TouchableOpacity>
                    ) : null}

                    {officialText ? (
                      <View style={styles.sourceDetails}>
                        <TouchableOpacity
                          style={styles.sourceSummary}
                          onPress={() =>
                            setOpenText((prev) => ({ ...prev, [group.key]: !prev[group.key] }))
                          }
                          accessibilityRole="button"
                          accessibilityState={{ expanded: isOpen }}
                          accessibilityLabel={`${isOpen ? 'Hide' : 'Show'} official text`}
                        >
                          <Text style={styles.sourceChevron}>{isOpen ? '▾' : '▸'}</Text>
                          <Text style={styles.sourceSummaryTxt}>
                            Official text{src.source_page ? ` · p.${src.source_page}` : ''}
                          </Text>
                          <Text style={styles.sourceHint}>{isOpen ? 'Hide' : 'Show'}</Text>
                        </TouchableOpacity>
                        {isOpen ? <Text style={styles.sourceBody}>{officialText}</Text> : null}
                      </View>
                    ) : null}
                  </View>
                ) : null}
              </View>
            );
          })
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
  group: { gap: spacing.sm },
  source: { gap: spacing.sm, marginTop: spacing.xs },
  sourceImgBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    alignSelf: 'flex-start',
    gap: spacing.xs,
    paddingVertical: spacing.xs,
    paddingHorizontal: spacing.sm,
    borderRadius: radius.sm,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    backgroundColor: colors.panelElevated,
  },
  sourceImgIcon: { color: colors.accent, fontSize: typography.small },
  sourceImgTxt: { color: colors.accent, fontSize: typography.small, fontWeight: '700' },
  sourceDetails: {
    borderRadius: radius.sm,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    backgroundColor: colors.panelElevated,
    overflow: 'hidden',
  },
  sourceSummary: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: spacing.xs,
    paddingVertical: spacing.sm,
    paddingHorizontal: spacing.sm,
  },
  sourceChevron: { color: colors.textMuted, fontSize: typography.small, width: 14 },
  sourceSummaryTxt: {
    flex: 1,
    color: colors.textMuted,
    fontSize: typography.small,
    fontWeight: '700',
  },
  sourceHint: { color: colors.accent, fontSize: typography.small, fontWeight: '700' },
  sourceBody: {
    color: colors.text,
    fontSize: typography.small,
    lineHeight: 18,
    paddingHorizontal: spacing.sm,
    paddingBottom: spacing.sm,
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
