import { openBook } from "../regs/openBook";

/**
 * (T285) On a phone the official Synopsis is OUR copy, through `openBook` (the system's PDF viewer on the downloaded
 * file, offline once held, the loading card and its failure words) — Android refuses to render a PDF URL in the browser it
 * was handed to. No release copy: `openBook` opens the plain link. The web twin opens `url` (openOfficial.web.ts).
 */
export function openOfficial(_url: string): void { void openBook(1); }
