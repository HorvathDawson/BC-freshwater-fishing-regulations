/**
 * (T269) What Metro defines for every React Native bundle, for the server render of the app's components: `__DEV__`
 * (react-native-gesture-handler reads it at module load; tools/render-setup.ts does the same for the render tests).
 * Imported first by pane.tsx.
 */
(globalThis as { __DEV__?: boolean }).__DEV__ ??= false;
