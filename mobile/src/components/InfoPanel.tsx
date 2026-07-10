/**
 * InfoPanel (mobile) — bottom sheet showing the tapped feature's name, aliases,
 * and its resolved regulations. This is a lean port of the web
 * `InfoPanel.tsx`; the full sectioned rule cards, PDF/source-image viewers, and
 * issue-report flow are ported incrementally from the web component.
 */
import React from 'react';
import { ScrollView, StyleSheet, Text, TouchableOpacity, View } from 'react-native';

import type { Regulation } from '../types/regulations';
import { buildAliasLines, getColorForType, type NameVariant } from '../utils/featureUtils';
import { colors, radius, spacing, typography } from '../theme/tokens';

export interface InfoPanelData {
  displayName: string;
  featureType: string;
  nameVariants: NameVariant[];
  regulations: Regulation[];
}

interface InfoPanelProps {
  data: InfoPanelData | null;
  onClose: () => void;
}

export function InfoPanel({ data, onClose }: InfoPanelProps): React.JSX.Element | null {
  if (!data) return null;

  const accent = getColorForType(data.featureType as never);
  const aliasLines = buildAliasLines(
    data.nameVariants.filter((v) => v.name.toLowerCase() !== data.displayName.toLowerCase()),
  );

  return (
    <View style={styles.sheet}>
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
            </View>
          ))
        )}
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
});
