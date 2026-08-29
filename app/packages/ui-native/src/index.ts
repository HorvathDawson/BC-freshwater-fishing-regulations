/**
 * @app/ui-native — the PHONE component set, written once in React Native primitives.
 *
 * Rendered by BOTH the native app and the mobile web view (via react-native-web), so
 * the phone experience is identical on device and in a mobile browser by construction
 * rather than by discipline.
 *
 * Desktop does NOT use these — it has its own components in apps/web. That costs
 * nothing, because these components hold no logic: behaviour lives in @app/ui hooks,
 * which desktop calls too. If you find yourself wanting to share a component with
 * desktop to avoid duplicating logic, the logic is in the wrong place — move it to a
 * hook.
 */
export { PlaceholderScreen } from "./PlaceholderScreen";
