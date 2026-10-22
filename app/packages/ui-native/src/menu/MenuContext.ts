/**
 * THE SHELL'S MENU, for what opens a menu page from deep inside a screen — a regulations tab's "Report a problem with
 * this page" (`ReportLink`). With no menu above it (a screen mounted alone, a test) such a control draws nothing.
 *
 * (T48) The hamburger that read this to open its drawer is gone: the menu's pages are listed by the bottom bar's More
 * tab (`MoreScreen`), which the Shell hands the menu directly.
 */
import { createContext } from "react";
import type { MenuControls } from "@app/ui";

export const MenuContext = createContext<MenuControls | null>(null);
