/**
 * Design tokens — ported from the webapp CSS custom properties so the mobile
 * UI is visually identical. Keep colours in sync with
 * `webapp/src/index.css` / component CSS files.
 */

export const colors = {
  // Feature-type accents (must match webapp/src/utils/featureUtils.ts getColorForType)
  stream: '#3b82f6',
  lake: '#0ea5e9',
  wetland: '#10b981',
  manmade: '#a855f7',
  ungazetted: '#f59e0b',

  // Surfaces / panels (dark theme, matches webapp overlays)
  bg: '#0b1020',
  panel: '#111827',
  panelElevated: '#1f2937',
  border: '#374151',

  // Text
  text: '#f9fafb',
  textMuted: '#9ca3af',
  textSubtle: '#6b7280',

  // Accents / states
  accent: '#3b82f6',
  focus: '#3b82f6',
  danger: '#ef4444',
  warning: '#f59e0b',
  success: '#10b981',

  // Restriction severity (matches InfoPanel.css badges)
  closed: '#ef4444',
  openWithRules: '#f59e0b',
  open: '#10b981',
  notice: '#3b82f6',
} as const;

export const spacing = {
  xs: 4,
  sm: 8,
  md: 12,
  lg: 16,
  xl: 24,
  xxl: 32,
} as const;

export const radius = {
  sm: 6,
  md: 10,
  lg: 14,
  pill: 999,
} as const;

export const typography = {
  // >= 16 avoids iOS input auto-zoom (parity with webapp media query)
  body: 16,
  small: 13,
  caption: 11,
  title: 20,
  heading: 17,
} as const;
