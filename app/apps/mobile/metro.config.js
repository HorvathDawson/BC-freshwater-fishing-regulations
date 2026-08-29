// Monorepo wiring. Without this, Metro watches only apps/mobile and a change in
// packages/ui-native does not trigger a reload — which looks exactly like a broken
// component and wastes an afternoon.
const path = require("node:path");
const { getDefaultConfig } = require("expo/metro-config");

const projectRoot = __dirname;
const workspaceRoot = path.resolve(projectRoot, "../.."); // app/

const config = getDefaultConfig(projectRoot);

// 1. Watch the whole workspace so edits in packages/* hot-reload.
config.watchFolders = [workspaceRoot];

// 2. Resolve from the app first, then the workspace root.
config.resolver.nodeModulesPaths = [
  path.resolve(projectRoot, "node_modules"),
  path.resolve(workspaceRoot, "node_modules"),
];

// 3. Only those two. Hierarchical lookup would let Metro walk out of app/ and pick up
//    a stray node_modules from the repo root, which is how two Reacts end up in one
//    bundle ("Invalid hook call") — the failure this whole workspace exists to avoid.
config.resolver.disableHierarchicalLookup = true;

module.exports = config;
