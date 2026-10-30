/**
 * (T309) Copy words from the app: the web's clipboard where there is one, else the phone's share sheet (no clipboard
 * package in production; the same way the Updates log copies). False: it failed. Long-press selection (`selectable`)
 * is the other way to copy; this is the "Copy" action on a rule row.
 */
import { Share } from "react-native";

export async function copyText(text: string): Promise<boolean> {
  try {
    const clip = (globalThis as { navigator?: { clipboard?: { writeText(t: string): Promise<void> } } }).navigator?.clipboard;
    if (clip) { await clip.writeText(text); return true; }
    await Share.share({ message: text });
    return true;
  } catch { return false; }
}
