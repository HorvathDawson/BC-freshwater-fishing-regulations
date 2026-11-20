import { defineConfig } from "vitest/config";
// (T764) globalSetup: the run's own temp folder, removed when the run ends (app/tools/test-tmp.ts)
export default defineConfig({ test: { include: ["test/**/*.test.ts"], environment: "node", testTimeout: 120_000,
                                      globalSetup: ["../../tools/test-tmp.ts"] } });
