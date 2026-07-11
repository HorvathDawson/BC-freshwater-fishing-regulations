/**
 * DisambiguationMenu (mobile) — bottom-sheet overlay shown when a single map tap
 * overlaps multiple features. Faithful port of the web
 * `webapp/src/components/DisambiguationMenu.tsx`: it lists each candidate with a
 * feature-type colour dot and its display name; tapping a row selects it.
 * The web component's hover/highlight, icon glyphs, alias sub-lines, and
 * position anchoring are DOM-specific and intentionally reduced to the core
 * pick-a-feature interaction for the mobile bottom sheet.
 */
import React from 'react';
import { Modal, Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';

import { getColorForType } from '../utils/featureUtils';
import { colors, radius, spacing, typography } from '../theme/tokens';

export interface DisambiguationOption {
  id: string;
  displayName: string;
  featureType: string;
}

export interface DisambiguationMenuProps {
  options: DisambiguationOption[];
  onSelect: (option: DisambiguationOption) => void;
  onClose: () => void;
}

export function DisambiguationMenu({
  options,
  onSelect,
  onClose,
}: DisambiguationMenuProps): React.JSX.Element | null {
  if (options.length === 0) return null;

  return (
    <Modal
      visible
      transparent
      animationType="fade"
      onRequestClose={onClose}
    >
      <Pressable style={styles.backdrop} onPress={onClose} accessibilityLabel="Dismiss feature menu">
        {/* Stop backdrop press from firing when interacting with the sheet. */}
        <Pressable style={styles.sheet} onPress={() => {}}>
          <View style={styles.header}>
            <Text style={styles.headerTitle}>MULTIPLE FEATURES ({options.length})</Text>
            <Pressable
              onPress={onClose}
              accessibilityRole="button"
              accessibilityLabel="Close feature menu"
              style={styles.closeBtn}
            >
              <Text style={styles.closeTxt}>✕</Text>
            </Pressable>
          </View>

          <ScrollView style={styles.list} contentContainerStyle={styles.listContent}>
            {options.map((option) => (
              <Pressable
                key={option.id}
                onPress={() => onSelect(option)}
                accessibilityRole="button"
                accessibilityLabel={option.displayName}
                style={({ pressed }) => [styles.row, pressed && styles.rowPressed]}
              >
                <View
                  style={[styles.dot, { backgroundColor: getColorForType(option.featureType as never) }]}
                />
                <View style={styles.rowText}>
                  <Text style={styles.name} numberOfLines={2}>
                    {option.displayName}
                  </Text>
                  <Text style={styles.type}>{option.featureType}</Text>
                </View>
              </Pressable>
            ))}
          </ScrollView>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

const styles = StyleSheet.create({
  backdrop: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.4)',
    justifyContent: 'flex-end',
  },
  sheet: {
    backgroundColor: colors.panel,
    borderTopLeftRadius: radius.lg,
    borderTopRightRadius: radius.lg,
    maxHeight: '55%',
    paddingBottom: spacing.xl,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: spacing.lg,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.border,
  },
  headerTitle: {
    color: colors.textMuted,
    fontSize: typography.caption,
    fontWeight: '800',
    letterSpacing: 1,
  },
  closeBtn: { padding: spacing.sm },
  closeTxt: { color: colors.textMuted, fontSize: typography.heading },
  list: { flexGrow: 0 },
  listContent: { paddingVertical: spacing.xs },
  row: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: spacing.md,
    paddingHorizontal: spacing.lg,
    paddingVertical: spacing.md,
  },
  rowPressed: { backgroundColor: colors.panelElevated },
  dot: {
    width: 14,
    height: 14,
    borderRadius: radius.pill,
    flexShrink: 0,
  },
  rowText: { flex: 1 },
  name: { color: colors.text, fontSize: typography.body, fontWeight: '600' },
  type: {
    color: colors.textMuted,
    fontSize: typography.caption,
    textTransform: 'uppercase',
    marginTop: spacing.xs / 2,
    letterSpacing: 0.5,
  },
});
