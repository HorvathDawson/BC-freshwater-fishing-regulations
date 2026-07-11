/**
 * SourceImageViewer (mobile) — full-screen pinch-zoom viewer for a synopsis
 * source-page PNG. Faithful port of the web
 * `webapp/src/components/SourceImageViewer.tsx`: dark full-screen modal, the
 * image centred, pinch-to-zoom + pan, and a close (X) button.
 *
 * Rendering layer only differs from the web version:
 *   - DOM/CSS transforms → React Native `Animated` transforms.
 *   - Pointer-event math → `react-native-gesture-handler` Pinch/Pan gestures.
 *
 * Reanimated is NOT installed in this app (see mobile/package.json), so the
 * gesture callbacks drive plain `Animated.Value`s. Without reanimated worklets,
 * gesture-handler v2 runs these callbacks on the JS thread, which is exactly
 * what we need to update `Animated.Value` directly.
 */
import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Animated,
  Dimensions,
  Modal,
  Pressable,
  StyleSheet,
  Text,
  View,
} from 'react-native';
import { Gesture, GestureDetector } from 'react-native-gesture-handler';

import { colors, radius, spacing, typography } from '../theme/tokens';

export interface SourceImageViewerProps {
  visible: boolean;
  imageUrl: string | null;
  pageLabel?: string;
  onClose: () => void;
}

const MIN_SCALE = 1;
const MAX_SCALE = 6;

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

export function SourceImageViewer({
  visible,
  imageUrl,
  pageLabel,
  onClose,
}: SourceImageViewerProps): React.JSX.Element | null {
  const [loaded, setLoaded] = useState(false);
  const [errored, setErrored] = useState(false);

  // Animated transform state (kept in refs so identity is stable).
  const scale = useRef(new Animated.Value(1)).current;
  const translateX = useRef(new Animated.Value(0)).current;
  const translateY = useRef(new Animated.Value(0)).current;

  // Base values captured at the start of each gesture.
  const savedScale = useRef(1);
  const savedX = useRef(0);
  const savedY = useRef(0);
  // Latest scale computed mid-pinch (folded into savedScale on gesture end).
  const liveScale = useRef(1);

  const resetTransform = useCallback(() => {
    savedScale.current = 1;
    savedX.current = 0;
    savedY.current = 0;
    liveScale.current = 1;
    scale.setValue(1);
    translateX.setValue(0);
    translateY.setValue(0);
  }, [scale, translateX, translateY]);
  useEffect(() => {
    if (visible && imageUrl) {
      setLoaded(false);
      setErrored(false);
      resetTransform();
    }
  }, [visible, imageUrl, resetTransform]);

  const pinch = Gesture.Pinch()
    .onUpdate((e) => {
      const next = clamp(savedScale.current * e.scale, MIN_SCALE, MAX_SCALE);
      liveScale.current = next;
      scale.setValue(next);
    })
    .onEnd(() => {
      // Fold the gesture's final scale into the base for the next pinch.
      savedScale.current = liveScale.current;
    });

  const pan = Gesture.Pan()
    .onUpdate((e) => {
      translateX.setValue(savedX.current + e.translationX);
      translateY.setValue(savedY.current + e.translationY);
    })
    .onEnd((e) => {
      savedX.current += e.translationX;
      savedY.current += e.translationY;
    });

  // Double-tap resets to the fitted view (mirrors the web "reset zoom" button).
  const doubleTap = Gesture.Tap()
    .numberOfTaps(2)
    .onEnd(() => {
      resetTransform();
    });

  const composed = Gesture.Simultaneous(pinch, pan, doubleTap);

  if (!visible || !imageUrl) return null;

  return (
    <Modal
      visible={visible}
      transparent
      animationType="fade"
      onRequestClose={onClose}
      statusBarTranslucent
    >
      <View style={styles.overlay}>
        {/* Header */}
        <View style={styles.header}>
          <Text style={styles.title} numberOfLines={1}>
            {pageLabel ?? 'Source image'}
          </Text>
          <Pressable
            onPress={onClose}
            accessibilityRole="button"
            accessibilityLabel="Close"
            hitSlop={8}
            style={({ pressed }) => [
              styles.closeBtn,
              pressed && styles.closeBtnPressed,
            ]}
          >
            <Text style={styles.closeTxt}>✕</Text>
          </Pressable>
        </View>

        {/* Image viewport */}
        <GestureDetector gesture={composed}>
          <View style={styles.viewport}>
            {!errored ? (
              <Animated.Image
                source={{ uri: imageUrl }}
                accessibilityLabel={
                  pageLabel ? `Synopsis source for ${pageLabel}` : 'Synopsis source image'
                }
                resizeMode="contain"
                onLoad={() => setLoaded(true)}
                onError={() => setErrored(true)}
                style={[
                  styles.image,
                  {
                    opacity: loaded ? 1 : 0,
                    transform: [{ translateX }, { translateY }, { scale }],
                  },
                ]}
              />
            ) : null}

            {!loaded && !errored ? (
              <View style={styles.centerOverlay} pointerEvents="none">
                <ActivityIndicator size="large" color={colors.text} />
              </View>
            ) : null}

            {errored ? (
              <View style={styles.centerOverlay} pointerEvents="none">
                <Text style={styles.errorText}>Couldn’t load image.</Text>
              </View>
            ) : null}
          </View>
        </GestureDetector>
      </View>
    </Modal>
  );
}

const { width: SCREEN_W, height: SCREEN_H } = Dimensions.get('window');

const styles = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.88)',
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: spacing.md,
    paddingVertical: spacing.sm,
    gap: spacing.sm,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    zIndex: 1,
  },
  title: {
    flex: 1,
    minWidth: 0,
    color: colors.text,
    fontSize: typography.small,
    fontWeight: '700',
  },
  closeBtn: {
    width: 36,
    height: 36,
    borderRadius: radius.sm,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: 'rgba(255, 80, 80, 0.25)',
  },
  closeBtnPressed: {
    backgroundColor: 'rgba(255, 80, 80, 0.45)',
  },
  closeTxt: {
    color: colors.text,
    fontSize: typography.heading,
    fontWeight: '700',
  },
  viewport: {
    flex: 1,
    overflow: 'hidden',
    alignItems: 'center',
    justifyContent: 'center',
  },
  image: {
    width: SCREEN_W,
    height: SCREEN_H,
  },
  centerOverlay: {
    ...StyleSheet.absoluteFillObject,
    alignItems: 'center',
    justifyContent: 'center',
  },
  errorText: {
    color: colors.textMuted,
    fontSize: typography.body,
  },
});
