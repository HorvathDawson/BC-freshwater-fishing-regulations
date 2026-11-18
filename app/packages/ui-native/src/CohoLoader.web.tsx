/**
 * (T814, owner 2026-11-17) THE COHO LOADER, on the web. `CohoLoader.native.tsx` is the phone's.
 *
 * TWO CSS ANIMATIONS AND NO JAVASCRIPT, as the web's circling fish: the sheet sits in two nested boxes behind a clip; the
 * outer steps a row down every `loopMs / rows` (`steps(rows)` over the loop), the inner steps a column across every frame
 * (`steps(columns)` over one row's time). Both start with the element, so the row and the column agree on the frame.
 * Pixels, not percentages (a percentage translate on a box wider than its clip does not step by one frame). It always
 * moves: owner 2026-11-17, "we will ignore reduced motion for this".
 *
 * The sheet is drawn by react-native-web's `Image`, which resolves the bundled file (`coho/*.webp`) as the phone does.
 *
 * (T844, owner 2026-11-17: "make way smaller for web") THE WEB'S OWN SHEET, `COHO_WEB` (cohoWeb.generated.ts, ~105 KB:
 * 40 frames at 10 fps, the colours stepped), not the phones' 0.9 MB one, which on Fast 3G came in after the map.
 */
import { useEffect } from "react";
import { Image } from "react-native";
import { SPINNER_WORDS } from "@app/core";
import { COHO_WEB as COHO } from "./cohoWeb.generated";
import { COHO_GROUND_PAD, COHO_WIDTH, cohoHeight, cohoSheet } from "./coho";
import type { Palette } from "./theme";

const STYLE_ID = "coho-loader-keyframes";

/** Injected once. Two keyframes, no per-instance cost. */
function useKeyframes() {
  useEffect(() => {
    if (document.getElementById(STYLE_ID)) return;
    const el = document.createElement("style");
    el.id = STYLE_ID;
    el.textContent =
      `@keyframes coho-row { to { transform: translateY(var(--coho-rows)); } }\n` +
      `@keyframes coho-col { to { transform: translateX(var(--coho-cols)); } }`;
    document.head.appendChild(el);
  }, []);
}

export function CohoLoader({ palette, width = COHO_WIDTH, label = SPINNER_WORDS.loading }:
  { palette: Palette; width?: number; label?: string }) {
  useKeyframes();
  const height = cohoHeight(width);
  const rowMs = COHO.loopMs / COHO.rows;

  return (
    <div role="progressbar" aria-label={label} aria-busy style={{ position: "relative", width, height }}>
      <div data-testid="coho-ground" style={{ position: "absolute", inset: -COHO_GROUND_PAD,
                                              backgroundColor: palette.coho.ground }} />
      <div style={{ position: "absolute", inset: 0, overflow: "hidden" }}>
        <div data-testid="coho-rows" style={{
          ["--coho-rows" as string]: `-${height * COHO.rows}px`,
          animation: `coho-row ${COHO.loopMs}ms steps(${COHO.rows}) infinite`,
        }}>
          <div data-testid="coho-cols" style={{
            width: width * COHO.columns, height: height * COHO.rows,
            ["--coho-cols" as string]: `-${width * COHO.columns}px`,
            animation: `coho-col ${rowMs}ms steps(${COHO.columns}) infinite`,
          }}>
            <Image testID="coho-sheet" source={cohoSheet(palette, COHO.sheets)} resizeMode="stretch"
                   style={{ width: width * COHO.columns, height: height * COHO.rows }} />
          </div>
        </div>
      </div>
    </div>
  );
}
