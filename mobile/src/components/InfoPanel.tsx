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
  /** Civil dawn/dusk for the current map view center — recomputed by App.tsx
   *  on pan-threshold/date-change, not tied to the selected feature. */
  dawnDusk?: { dawn: Date | null; dusk: Date | null } | null;
}

const formatDawnDuskTime = (d: Date | null): string =>
  d ? d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }) : '—';

/** True for the indigenous-territory advisory — folded into the same
 *  "Land Use" tier as land_access (watersheds, DND land, research forests)
 *  rather than treated as its own separate provincial notice.
 *  Matched by regulation_id (a stable constant from base_regulations.json):
 *  the reg's restriction_type is just "advisory" at the data layer — the
 *  words "indigenous territory" only appear in rule_text/details, never in
 *  restriction_type, so a text match against restriction_type always missed. */
function isIndigenousAdvisory(reg: Regulation): boolean {
  return reg.regulation_id === 'PROV_ABORIGINAL_LANDS_ADVISORY';
}

/** True for the regs that get the "Land Use" badge + pinned styling:
 *  land_access advisories and the indigenous-territory advisory. */
function isLandUse(reg: Regulation): boolean {
  return reg.source === 'land_access' || isIndigenousAdvisory(reg);
}

/**
 * Sort priority for the top-pinned regulation tiers, mirroring the web
 * InfoPanel's groupPriority: municipal closures and any provincial/zone
 * "closed" reg (0) → land access / indigenous advisory ("Land Use", 1) →
 * synopsis (3) → zone (4) → provincial (5).
 */
function regulationPriority(reg: Regulation): number {
  const restrictionType = (reg.restriction_type || '').toLowerCase();
  const isClosure = restrictionType === 'closed' || restrictionType === 'closure';
  if (reg.source === 'municipal') return 0;
  if ((reg.source === 'provincial' || reg.source === 'zone') && isClosure) return 0;
  if (isLandUse(reg)) return 1;
  const sourceOrder: Record<string, number> = { synopsis: 3, zone: 4, provincial: 5 };
  return sourceOrder[reg.source] ?? 9;
}

/** Access-severity rank for the three land_access-source regs — a reach can
 *  genuinely touch a closed watershed AND a private parcel AND sit near
 *  restricted land all at once, but showing all three as separate cards
 *  reads as confusing near-duplicate notices. Only the most severe one is
 *  kept; the indigenous-territory advisory is always separate from this
 *  ranking and never gets dropped by it. */
const ACCESS_SEVERITY: Record<string, number> = {
  LAND_ACCESS_CLOSED: 0,
  LAND_ACCESS_RESTRICTED: 1,
  LAND_OWNERSHIP_PRIVATE: 2,
};

/** Collapse the "Land Use" tier down to at most two regs: the single most
 *  severe access reg (if any) plus the indigenous advisory (if present). */
function dedupLandUse(regulations: Regulation[]): Regulation[] {
  const accessRegs = regulations.filter(r => r.regulation_id in ACCESS_SEVERITY);
  if (accessRegs.length <= 1) return regulations;
  const mostSevere = [...accessRegs].sort(
    (a, b) => ACCESS_SEVERITY[a.regulation_id] - ACCESS_SEVERITY[b.regulation_id],
  )[0];
  const dropped = new Set(accessRegs.filter(r => r !== mostSevere).map(r => r.regulation_id));
  return regulations.filter(r => !dropped.has(r.regulation_id));
}

function sortRegulations(regulations: Regulation[]): Regulation[] {
  return [...dedupLandUse(regulations)].sort((a, b) => regulationPriority(a) - regulationPriority(b));
}

/** Badge text for the top-pinned tiers; undefined for regular regs (no badge shown). */
function badgeForRegulation(reg: Regulation): string | undefined {
  if (reg.source === 'municipal') return 'Closure';
  if (isLandUse(reg)) return 'Land Use';
  if (reg.source === 'zone') return 'Zone';
  if (reg.source === 'provincial') return 'Provincial';
  return undefined;
}

export function InfoPanel({ data, onClose, dawnDusk }: InfoPanelProps): React.JSX.Element | null {
  if (!data) return null;

  const accent = getColorForType(data.featureType as never);
  const aliasLines = buildAliasLines(
    data.nameVariants.filter((v) => v.name.toLowerCase() !== data.displayName.toLowerCase()),
  );
  const sortedRegulations = sortRegulations(data.regulations);

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

      {dawnDusk ? (
        <View style={styles.dawnDuskRow}>
          <Text style={styles.dawnDuskText}>
            Dawn {formatDawnDuskTime(dawnDusk.dawn)} · Dusk {formatDawnDuskTime(dawnDusk.dusk)}
          </Text>
        </View>
      ) : null}

      <ScrollView style={styles.body} contentContainerStyle={styles.bodyContent}>
        {sortedRegulations.length === 0 ? (
          <Text style={styles.muted}>No regulations found for this waterbody.</Text>
        ) : (
          sortedRegulations.map((reg) => {
            const isPinned = reg.source === 'municipal' || isLandUse(reg);
            const badge = badgeForRegulation(reg);
            return (
              <View
                key={reg.regulation_id}
                style={[styles.card, isPinned ? styles.cardPinned : null]}
              >
                {badge ? (
                  <Text style={[styles.badge, isPinned ? styles.badgePinned : null]}>
                    {badge}
                  </Text>
                ) : null}
                <Text style={styles.ruleText}>{reg.rule_text}</Text>
                {reg.restriction_details ? (
                  <Text style={styles.details}>{reg.restriction_details}</Text>
                ) : null}
                {reg.provenance ? (
                  <Text style={styles.provenance}>via {reg.provenance}</Text>
                ) : null}
              </View>
            );
          })
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
  dawnDuskRow: {
    paddingHorizontal: spacing.lg,
    paddingVertical: spacing.xs,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.border,
  },
  dawnDuskText: { color: colors.textMuted, fontSize: typography.small, fontWeight: '500' },
  body: { paddingHorizontal: spacing.lg },
  bodyContent: { paddingVertical: spacing.md, gap: spacing.md },
  muted: { color: colors.textMuted, fontSize: typography.body },
  card: {
    backgroundColor: colors.panelElevated,
    borderRadius: radius.md,
    padding: spacing.md,
  },
  cardPinned: {
    borderLeftWidth: 3,
    borderLeftColor: '#dc2626',
  },
  badge: {
    alignSelf: 'flex-start',
    fontSize: typography.caption,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    color: colors.textMuted,
    marginBottom: spacing.xs,
  },
  badgePinned: {
    color: '#dc2626',
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
