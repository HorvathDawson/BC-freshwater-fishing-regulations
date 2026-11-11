/** Vite's `?raw` import: a file's text (the map-props gate reads the two halves' source). */
declare module "*?raw" {
  const text: string;
  export default text;
}
