import { createContext } from "react";
import type { GaugePageCheck, SharePort } from "@app/ui";

/**
 * (T530) What any Share control below the Shell needs: the platform's share (the phone's sheet; the web's, else the
 * clipboard) and, for a gauge, whether the site has a page for a station (`useGaugePageCheck`: the build's own rule).
 */
export interface SharePorts { readonly port: SharePort | null; readonly hasGaugePage: GaugePageCheck | null }
export const SharePortContext = createContext<SharePorts>({ port: null, hasGaugePage: null });
