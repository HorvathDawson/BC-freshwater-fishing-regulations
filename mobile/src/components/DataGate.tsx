/**
 * DataGate — full-screen startup gate for the offline regulations database.
 *
 * The complete DB (~1.2 GB) is far too large to ship inside the app binary, so
 * on first launch (when no dev-bundled copy exists) it must be downloaded once
 * from R2. Rather than silently pulling a gigabyte on cellular, we explain what
 * is happening and require an explicit tap, then show live download progress.
 * After the one-time download the app works fully offline.
 *
 * Rendered states:
 *   - `checking`      spinner while we probe for existing/bundled data
 *   - `needs-download`explanation + "Download data" button (user gate)
 *   - `downloading`   progress bar + percentage
 *   - `loading`       spinner while the on-device DB is opened/expanded
 *   - `error`         message + "Try again"
 */
import React from 'react';
import { ActivityIndicator, Pressable, StyleSheet, Text, View } from 'react-native';

import { colors, radius, spacing, typography } from '../theme/tokens';

/** Approximate one-time download size, shown to set expectations. */
export const DB_DOWNLOAD_SIZE_LABEL = 'about 1.2 GB';

export type DataGatePhase =
  | 'checking'
  | 'needs-download'
  | 'downloading'
  | 'loading'
  | 'error';

export interface DataGateProps {
  phase: DataGatePhase;
  /** Download progress in the range 0–1 (only used when phase is `downloading`). */
  progress?: number;
  /** Error message to display when phase is `error`. */
  message?: string;
  /** Start (or retry) the download + initialisation. */
  onDownload: () => void;
}

export function DataGate({
  phase,
  progress = 0,
  message,
  onDownload,
}: DataGateProps): React.JSX.Element {
  return (
    <View style={styles.root}>
      <View style={styles.card}>
        {phase === 'needs-download' && (
          <>
            <Text style={styles.title}>Download offline data</Text>
            <Text style={styles.body}>
              Can I Fish This? works entirely offline — no signal needed at the lake. To do
              that it needs to download the full B.C. freshwater regulations database
              ({DB_DOWNLOAD_SIZE_LABEL}) to your device.
            </Text>
            <Text style={styles.bodyMuted}>
              This is a one-time download. We recommend using Wi-Fi. Keep the app open until
              it finishes.
            </Text>
            <Pressable
              style={({ pressed }) => [styles.button, pressed && styles.buttonPressed]}
              onPress={onDownload}
              accessibilityRole="button"
              accessibilityLabel="Download offline regulations data"
            >
              <Text style={styles.buttonText}>Download data</Text>
            </Pressable>
          </>
        )}

        {phase === 'downloading' && (
          <>
            <Text style={styles.title}>Downloading data…</Text>
            <Text style={styles.body}>
              Fetching the regulations database ({DB_DOWNLOAD_SIZE_LABEL}). Please keep the
              app open and connected.
            </Text>
            <View
              style={styles.progressTrack}
              accessibilityRole="progressbar"
              accessibilityValue={{ min: 0, max: 100, now: Math.round(progress * 100) }}
            >
              <View style={[styles.progressFill, { width: `${Math.round(progress * 100)}%` }]} />
            </View>
            <Text style={styles.percent}>{Math.round(progress * 100)}%</Text>
          </>
        )}

        {(phase === 'checking' || phase === 'loading') && (
          <>
            <ActivityIndicator size="large" color={colors.accent} />
            <Text style={styles.body}>
              {phase === 'checking'
                ? 'Checking for offline data…'
                : 'Preparing regulations data…'}
            </Text>
          </>
        )}

        {phase === 'error' && (
          <>
            <Text style={styles.title}>Couldn’t load data</Text>
            <Text style={styles.error}>{message ?? 'Something went wrong.'}</Text>
            <Pressable
              style={({ pressed }) => [styles.button, pressed && styles.buttonPressed]}
              onPress={onDownload}
              accessibilityRole="button"
              accessibilityLabel="Try downloading the data again"
            >
              <Text style={styles.buttonText}>Try again</Text>
            </Pressable>
          </>
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing.xl,
    backgroundColor: colors.bg,
  },
  card: {
    width: '100%',
    maxWidth: 480,
    gap: spacing.lg,
    padding: spacing.xl,
    borderRadius: radius.lg,
    backgroundColor: colors.panel,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: 'center',
  },
  title: {
    color: colors.text,
    fontSize: typography.title,
    fontWeight: '700',
    textAlign: 'center',
  },
  body: {
    color: colors.text,
    fontSize: typography.body,
    lineHeight: typography.body * 1.4,
    textAlign: 'center',
  },
  bodyMuted: {
    color: colors.textMuted,
    fontSize: typography.small,
    lineHeight: typography.small * 1.4,
    textAlign: 'center',
  },
  button: {
    marginTop: spacing.sm,
    paddingVertical: spacing.md,
    paddingHorizontal: spacing.xl,
    borderRadius: radius.md,
    backgroundColor: colors.accent,
    alignItems: 'center',
    alignSelf: 'stretch',
  },
  buttonPressed: { opacity: 0.8 },
  buttonText: {
    color: colors.text,
    fontSize: typography.body,
    fontWeight: '600',
  },
  progressTrack: {
    alignSelf: 'stretch',
    height: 8,
    borderRadius: radius.pill,
    backgroundColor: colors.panelElevated,
    overflow: 'hidden',
  },
  progressFill: {
    height: '100%',
    borderRadius: radius.pill,
    backgroundColor: colors.accent,
  },
  percent: {
    color: colors.textMuted,
    fontSize: typography.small,
    fontVariant: ['tabular-nums'],
  },
  error: {
    color: colors.danger,
    fontSize: typography.body,
    textAlign: 'center',
  },
});
