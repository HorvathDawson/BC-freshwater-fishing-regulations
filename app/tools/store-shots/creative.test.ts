// (T1134) The committed App Store creative assets match what store-shots.json declares: the four files exist at the declared sizes, the
// PNGs have no alpha channel, and the push scripts' size folders do not include the creative folder (it is uploaded by hand).
import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";

const ROOT = resolve(__dirname, "../../..");
const cfg = JSON.parse(readFileSync(join(ROOT, "app/tools/store-shots.json"), "utf8"));
const dir = join(ROOT, "handoff/PLAN/app-integration/store/graphics/store/light", cfg.creative.folder);
// ffprobe is found on PATH (FFPROBE overrides, as in creative.mjs). A hosted CI runner has none and the four committed files are checked
// where it exists (this Mac: pre-push / before_pr). Without it the two video checks are SKIPPED BY NAME, with the reason printed; under
// REGS_STORE_REQUIRED=1 (scripts/test-full.sh) they fail instead, so a full local run can never pass without them.
const FFPROBE = process.env.FFPROBE ?? "ffprobe";
const probeErr = spawnSync(FFPROBE, ["-version"], { encoding: "utf8" }).error;
const HAS_FFPROBE = !probeErr;
if (probeErr) {
  const why = `T1134 video checks NOT RUN here: "${FFPROBE}" not found (${probeErr.message}); they run where ffprobe is on PATH (the Mac's pre-push)`;
  if (process.env.REGS_STORE_REQUIRED === "1") throw new Error(why + "; REGS_STORE_REQUIRED=1 refuses the skip");
  console.warn(why);
}

describe("App Store creative assets (T1134)", () => {
  it("declares the header and search slots, never as a push target", () => {
    expect(Object.keys(cfg.creative.files)).toEqual(["header", "search"]);
    expect(Object.keys(cfg.targets)).not.toContain(cfg.creative.folder);
  });
  for (const [slot, f] of Object.entries<any>(cfg.creative.files)) {
    it(`${slot}: the PNG is ${f.w}x${f.h} with no alpha channel`, () => {
      const file = join(dir, f.png); expect(existsSync(file)).toBe(true);
      const b = readFileSync(file);
      expect([b.readUInt32BE(16), b.readUInt32BE(20)]).toEqual([f.w, f.h]);
      expect([2, 3, 0]).toContain(b[25]); // truecolour / palette / grey: never 4 or 6 (alpha)
    });
    it.skipIf(!HAS_FFPROBE)(`${slot}: the video is ${f.w}x${f.h}, H.264 yuv420p, 30 fps, 5-30 s, no audio`, () => {
      const file = join(dir, f.mp4); expect(existsSync(file)).toBe(true);
      const j = JSON.parse(execFileSync(FFPROBE, ["-v", "error", "-print_format", "json", "-show_streams", "-show_format", file], { encoding: "utf8" }));
      const v = j.streams.find((s: any) => s.codec_type === "video");
      expect([v.codec_name, v.pix_fmt, v.width, v.height, v.avg_frame_rate]).toEqual(["h264", "yuv420p", f.w, f.h, "30/1"]);
      expect(Number(j.format.duration)).toBeGreaterThanOrEqual(5); expect(Number(j.format.duration)).toBeLessThanOrEqual(30);
      expect(j.streams.some((s: any) => s.codec_type === "audio")).toBe(false);
    });
  }
});
