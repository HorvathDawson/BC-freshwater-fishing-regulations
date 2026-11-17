/**
 * (T814) A picture imported from a file beside the code (the coho loader's sheets, `coho/*.webp`): Metro hands back the
 * asset (a number on a phone, resolved by `Image`), vitest's Vite a URL. Either is an `Image` source; nothing else may
 * read it.
 */
declare module "*.webp" {
  const source: import("react-native").ImageSourcePropType;
  export default source;
}
