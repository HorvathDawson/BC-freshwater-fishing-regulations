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

/** One photo, or null when the chooser was closed without a choice. */
export function pickPhoto(from: PhotoSource): Promise<PickedPhoto | null> {
  return new Promise((resolve) => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = "image/*";
    if (from === "camera") input.setAttribute("capture", "environment");
    input.style.display = "none";
    const finish = (got: PickedPhoto | null) => { input.remove(); resolve(got); };
    input.addEventListener("change", () => {
      const f = input.files?.[0];
      finish(f ? { blob: f, mimeType: f.type || null, fileName: f.name || null } : null);
    });
    input.addEventListener("cancel", () => finish(null));
    document.body.appendChild(input);
    input.click();
  });
}
