/**
 * Disclaimer (mobile) — faithful port of the web `Disclaimer.tsx`. Presents the
 * full legal/informational disclaimer in a full-screen RN Modal with a dark
 * theme card. The web version used a close button; on mobile this is a
 * first-run acknowledgement gate, so the action button calls `onAccept`.
 * Persistence of the accepted state is owned by the parent (App.tsx).
 */
import React from 'react';
import { Linking, Modal, Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';

import { colors, radius, spacing, typography } from '../theme/tokens';

const BC_REGULATIONS_URL =
  'https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations';
const OGL_BC_URL =
  'https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc';

function openUrl(url: string): void {
  void Linking.openURL(url);
}

export interface DisclaimerProps {
  visible: boolean;
  onAccept: () => void;
}

export function Disclaimer({ visible, onAccept }: DisclaimerProps): React.JSX.Element | null {
  if (!visible) return null;

  return (
    <Modal
      visible={visible}
      transparent
      animationType="fade"
      onRequestClose={onAccept}
      statusBarTranslucent
    >
      <View style={styles.overlay}>
        <View style={styles.modal} accessibilityViewIsModal accessibilityLabel="Disclaimer">
          <Text style={styles.heading}>Disclaimer</Text>

          <ScrollView style={styles.content} contentContainerStyle={styles.contentInner}>
            <Text style={styles.paragraph}>
              This map is provided for <Text style={styles.strong}>informational purposes only</Text>{' '}
              and should not be used as a substitute for official regulations or legal advice. This
              application is not affiliated with, endorsed by, or in any way officially connected with
              the Government of British Columbia, Fisheries and Oceans Canada, or any other government
              agency.
            </Text>

            <Text style={styles.paragraph}>
              The creator and maintainers of this application do not warrant the reliability, currency,
              positional accuracy, or completeness of any data or information published in this map. The
              information is provided "as is" without any representations or warranties of any kind.
              Regulations may change without notice, and misalignment of datasets may occur due to the
              methods used to produce the original products.
            </Text>

            <Text style={styles.paragraph}>
              <Text style={styles.strong}>Always verify fishing regulations</Text> with the official{' '}
              <Text style={styles.link} onPress={() => openUrl(BC_REGULATIONS_URL)}>
                BC Fishing Regulations
              </Text>{' '}
              and any posted notices before fishing. It is your responsibility as an angler to know and
              comply with current regulations.
            </Text>

            <Text style={styles.paragraph}>
              Under no circumstances will the creator or maintainers be liable for any direct, indirect,
              special, incidental, consequential, or other loss, injury, or damage caused by use of the
              information or otherwise arising in connection with this application. This includes, without
              limitation, any fines, penalties, lost profits, or business interruption.
            </Text>

            <Text style={styles.license}>
              Contains information licensed under the{' '}
              <Text style={styles.link} onPress={() => openUrl(OGL_BC_URL)}>
                Open Government Licence – British Columbia
              </Text>
              .
            </Text>
          </ScrollView>

          <Pressable
            style={({ pressed }) => [styles.acceptBtn, pressed && styles.acceptBtnPressed]}
            onPress={onAccept}
            accessibilityRole="button"
            accessibilityLabel="Acknowledge and accept disclaimer"
          >
            <Text style={styles.acceptBtnText}>I Understand</Text>
          </Pressable>
        </View>
      </View>
    </Modal>
  );
}

/** Small link to reopen the disclaimer, styled like the web `DisclaimerLink`. */
export function DisclaimerLink({ onPress }: { onPress: () => void }): React.JSX.Element {
  return (
    <Pressable
      onPress={onPress}
      accessibilityRole="button"
      accessibilityLabel="View disclaimer"
      hitSlop={8}
    >
      <Text style={styles.disclaimerLink}>Disclaimer</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    alignItems: 'center',
    justifyContent: 'center',
    padding: spacing.xl,
  },
  modal: {
    width: '100%',
    maxWidth: 560,
    maxHeight: '80%',
    backgroundColor: colors.panel,
    borderRadius: radius.lg,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: colors.border,
    padding: spacing.xl,
  },
  heading: {
    color: colors.text,
    fontSize: typography.title,
    fontWeight: '800',
    textTransform: 'uppercase',
    letterSpacing: 1,
    paddingBottom: spacing.md,
    marginBottom: spacing.lg,
    borderBottomWidth: 2,
    borderBottomColor: colors.border,
  },
  content: {
    flexGrow: 0,
  },
  contentInner: {
    paddingBottom: spacing.sm,
  },
  paragraph: {
    color: colors.text,
    fontSize: typography.body,
    lineHeight: typography.body * 1.6,
    marginBottom: spacing.md,
  },
  strong: {
    fontWeight: '700',
  },
  link: {
    color: colors.accent,
    textDecorationLine: 'underline',
  },
  license: {
    color: colors.textMuted,
    fontSize: typography.body,
    lineHeight: typography.body * 1.6,
    paddingTop: spacing.md,
    marginTop: spacing.sm,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: colors.border,
  },
  acceptBtn: {
    marginTop: spacing.lg,
    backgroundColor: colors.accent,
    borderRadius: radius.md,
    paddingVertical: spacing.md,
    alignItems: 'center',
  },
  acceptBtnPressed: {
    opacity: 0.85,
  },
  acceptBtnText: {
    color: colors.text,
    fontSize: typography.body,
    fontWeight: '700',
  },
  disclaimerLink: {
    color: colors.textMuted,
    fontSize: typography.small,
    textDecorationLine: 'underline',
  },
});
