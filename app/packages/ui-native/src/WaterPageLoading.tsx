/**
 * (T910) THE WATER'S REGULATIONS PAGE WHILE ITS CODE COMES (the web loads the page later: `part` in shell/WaterModeHost):
 * the face WaterScreen draws while its water's sheet comes — the water frame (the view strip, the name row still to
 * come) and the spinner in the body — so the two waits look the same and neither is an empty box
 * (`WaterPageLoading.test.tsx` renders both and holds them equal). No `onBack`: in the app the frame's back is the
 * Shell's (`WaterViewsContext`), as it is for the page itself.
 */
import { WATER_SCREEN_WORDS } from "@app/core";
import { LoadingState } from "./LoadingState";
import { RegsWaiting } from "./regs/NetWaiting";
import { WaterFrame, WaterTitle } from "./WaterFrame";
import type { Palette } from "./theme";

export function WaterPageLoading({ palette }: { palette: Palette }) {
  return (
    <WaterFrame palette={palette} face="regulations" title={<WaterTitle name={null} palette={palette} />}>
      <LoadingState palette={palette} size={110} label={WATER_SCREEN_WORDS.loading}
                    style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
        <RegsWaiting palette={palette} />
      </LoadingState>
    </WaterFrame>
  );
}
