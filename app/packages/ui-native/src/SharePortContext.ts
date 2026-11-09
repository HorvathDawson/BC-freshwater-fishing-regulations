import { createContext } from "react";
import type { SharePort } from "@app/ui";

/** (T530) The platform's share (the phone's sheet; the web's, else the clipboard), for any Share control below the Shell. */
export const SharePortContext = createContext<SharePort | null>(null);
