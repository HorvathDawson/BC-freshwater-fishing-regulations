/**
 * Picking a photograph for a spot in a browser (S1): an `<input type="file" accept="image/*">`, opened for the tap.
 *
 * The camera is the same input with `capture="environment"`, which a phone's browser opens as its camera and a
 * desktop one ignores (it shows the file chooser). The chosen file's bytes go straight into IndexedDB
 * (`photoStore.web.ts`): a `File` lasts only as long as the page.
 */
import type { PhotoSource, PickedPhoto } from "@app/data/spots";

/** What this platform can offer. */
export const PHOTO_SOURCES: readonly PhotoSource[] = ["library", "camera"];

/**
 * How long after the page gets its focus back a choice may still arrive. Where the input fires no "cancel" (Safari
 * before 16.4), the page regaining focus is the only sign the chooser closed; a "change" can follow the focus by a
 * moment, so a cancel is read only after this grace, and only when the input holds no file (T80 1).
 */
const PICKER_FOCUS_GRACE_MS = 1000;

/** One photo, or null when the chooser was closed without a choice. Settles exactly once. */
export function pickPhoto(from: PhotoSource): Promise<PickedPhoto | null> {
  return new Promise((resolve) => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = "image/*";
    if (from === "camera") input.setAttribute("capture", "environment");
    input.style.display = "none";
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const finish = (got: PickedPhoto | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      window.removeEventListener("focus", onFocus);
      input.remove();
      resolve(got);
    };
    const chosen = (): PickedPhoto | null => {
      const f = input.files?.[0];
      return f ? { blob: f, mimeType: f.type || null, fileName: f.name || null } : null;
    };
    function onFocus() {
      clearTimeout(timer);
      timer = setTimeout(() => finish(chosen()), PICKER_FOCUS_GRACE_MS);
    }
    input.addEventListener("change", () => finish(chosen()));
    input.addEventListener("cancel", () => finish(null));
    window.addEventListener("focus", onFocus);
    document.body.appendChild(input);
    input.click();
  });
}
