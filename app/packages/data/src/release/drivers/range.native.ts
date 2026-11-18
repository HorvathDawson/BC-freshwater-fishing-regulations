/**
 * (T831) THE PHONE READS ITS INSTALLED ATLAS BY RANGE: `length` bytes of the file at `uri` from `offset`, read where they
 * are with expo-file-system's file handle — the install's `readAt` (`install.native.ts`) over a `file://` uri, which is
 * what the map is handed for the atlas (`Device.fileUri`). See `range.d.ts`.
 */
import { File, FileMode } from "expo-file-system";
import type { ReadAt } from "./range";

export type { ReadAt } from "./range";

export const atlasRange: ReadAt | null = async (uri, offset, length) => {
  const h = new File(uri).open(FileMode.ReadOnly);
  try {
    const n = Math.max(0, Math.min(length, (h.size ?? 0) - offset));
    if (n === 0) return new Uint8Array(0);
    h.offset = offset;
    return h.readBytesSync(n);
  } finally {
    h.close();
  }
};
