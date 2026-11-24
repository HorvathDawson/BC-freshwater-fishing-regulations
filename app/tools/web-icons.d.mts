export const ICON_SIZES: readonly number[];
export const TOUCH_SIZE: number;
export const TOUCH_PATH: string;
export const ICON_LINKS: string;
export const iconPath: (n: number) => string;
export function drawIcons(): Map<string, Uint8Array>;
export function withIconLinks(html: string): string;
