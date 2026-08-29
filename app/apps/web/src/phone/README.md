Renders `@app/ui-native` via react-native-web. Add NO components here — a component that
exists only in mobile web is a component the native app does not have, which is exactly
the divergence this layout prevents. Put it in `packages/ui-native`.
