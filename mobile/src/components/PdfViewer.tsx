/**
 * PdfViewer (mobile) — full-screen modal that renders a bathymetry depth-map PDF
 * with `react-native-pdf`. This is a faithful port of the web
 * `webapp/src/components/PdfViewer.tsx`: it reproduces the header/title/close
 * chrome, a loading indicator, an error state, and pinch-zoomable page
 * rendering. The web version rasterises pages with pdf.js and drives zoom with
 * custom wheel/drag handlers; on native we hand rendering + pinch-zoom to the
 * `Pdf` component instead, keeping the same UX.
 */
import React, { useEffect, useState } from 'react';
import {
  ActivityIndicator,
  Modal,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from 'react-native';
import Pdf from 'react-native-pdf';

import { colors, spacing, typography } from '../theme/tokens';

// Zoom bounds mirror the web viewer's MIN_ZOOM / MAX_ZOOM.
const MIN_SCALE = 0.5;
const MAX_SCALE = 4;

export interface PdfViewerProps {
  visible: boolean;
  url: string | null;
  title?: string;
  onClose: () => void;
}

type Status = 'loading' | 'ready' | 'error';

export function PdfViewer({
  visible,
  url,
  title,
  onClose,
}: PdfViewerProps): React.JSX.Element | null {
  const [status, setStatus] = useState<Status>('loading');
  const [page, setPage] = useState(1);
  const [pageCount, setPageCount] = useState(0);

  // Reset transient state whenever a new document is opened.
  useEffect(() => {
    if (!visible || !url) return;
    setStatus('loading');
    setPage(1);
    setPageCount(0);
  }, [visible, url]);

  if (!visible || !url) return null;

  return (
    <Modal
      visible={visible}
      animationType="slide"
      onRequestClose={onClose}
      transparent={false}
    >
      <View style={styles.root}>
        <View style={styles.header}>
          <Text style={styles.title} numberOfLines={1}>
            {title ?? 'Depth map'}
          </Text>
          {status === 'ready' && pageCount > 1 ? (
            <Text style={styles.pageCount}>
              {page} / {pageCount}
            </Text>
          ) : null}
          <TouchableOpacity
            onPress={onClose}
            accessibilityRole="button"
            accessibilityLabel="Close depth map"
            style={styles.closeBtn}
          >
            <Text style={styles.closeTxt}>✕</Text>
          </TouchableOpacity>
        </View>

        <View style={styles.body}>
          <Pdf
            source={{ uri: url, cache: true }}
            enablePaging={false}
            scale={1}
            minScale={MIN_SCALE}
            maxScale={MAX_SCALE}
            onLoadComplete={(numberOfPages) => {
              setPageCount(numberOfPages);
              setStatus('ready');
            }}
            onPageChanged={(pageNumber) => setPage(pageNumber)}
            onError={(err) => {
              console.error('[PdfViewer] failed to load PDF', err);
              setStatus('error');
            }}
            style={styles.pdf}
          />

          {status === 'loading' ? (
            <View style={styles.overlay} pointerEvents="none">
              <ActivityIndicator size="large" color={colors.accent} />
              <Text style={styles.overlayText}>Loading depth map…</Text>
            </View>
          ) : null}

          {status === 'error' ? (
            <View style={styles.overlay}>
              <Text style={styles.errorText}>Couldn’t display the PDF.</Text>
            </View>
          ) : null}
        </View>
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: colors.bg,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: spacing.lg,
    paddingVertical: spacing.md,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: colors.border,
    backgroundColor: colors.panel,
  },
  title: {
    flex: 1,
    color: colors.text,
    fontSize: typography.heading,
    fontWeight: '700',
    paddingRight: spacing.md,
  },
  pageCount: {
    color: colors.textMuted,
    fontSize: typography.small,
    paddingRight: spacing.md,
  },
  closeBtn: { padding: spacing.sm },
  closeTxt: { color: colors.textMuted, fontSize: typography.title },
  body: { flex: 1 },
  pdf: {
    flex: 1,
    width: '100%',
    height: '100%',
    backgroundColor: colors.bg,
  },
  overlay: {
    ...StyleSheet.absoluteFillObject,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: colors.bg,
    gap: spacing.md,
  },
  overlayText: {
    color: colors.textMuted,
    fontSize: typography.body,
  },
  errorText: {
    color: colors.danger,
    fontSize: typography.body,
    paddingHorizontal: spacing.xl,
    textAlign: 'center',
  },
});
