/**
 * (T232) The development environment switch on a phone: a small file of its own (never inside the release or the spots
 * file), read synchronously at start. A JS reload applies it in a `__DEV__` build; an installed internal build is
 * closed and reopened (About says so).
 */
import { DevSettings } from "react-native";
import { Directory, File, Paths } from "expo-file-system";

const dir = () => new Directory(Paths.document, "settings");
const file = () => new File(dir(), "app-env.txt");

export const envStore = {
  read: (): string | null => { try { const f = file(); return f.exists ? f.textSync().trim() || null : null; } catch { return null; } },
  write: (env: string): void => {
    try { if (!dir().exists) dir().create({ intermediates: true }); file().write(env); } catch { /* a preference */ }
  },
};

export function restart(): boolean {
  if (!__DEV__) return false;
  DevSettings.reload();
  return true;
}
