/**
 * Disclaimer acknowledgement persistence — the mobile analogue of the web app's
 * `localStorage` disclaimer flag. Stores a tiny marker file in the app's
 * document directory so the first-run legal modal only shows once.
 */
import * as FileSystem from 'expo-file-system';

const FLAG_URI = `${FileSystem.documentDirectory ?? ''}disclaimer_accepted`;

/** True once the user has accepted the disclaimer on this device. */
export async function hasAcceptedDisclaimer(): Promise<boolean> {
  if (!FileSystem.documentDirectory) return false;
  try {
    const info = await FileSystem.getInfoAsync(FLAG_URI);
    return info.exists;
  } catch (err) {
    // Surface the problem but fail open (re-show the disclaimer) rather than hide it.
    console.warn('disclaimerStorage: read failed', err);
    return false;
  }
}

/** Persist that the user accepted the disclaimer. */
export async function setDisclaimerAccepted(): Promise<void> {
  if (!FileSystem.documentDirectory) return;
  try {
    await FileSystem.writeAsStringAsync(FLAG_URI, '1');
  } catch (err) {
    console.warn('disclaimerStorage: write failed', err);
  }
}
