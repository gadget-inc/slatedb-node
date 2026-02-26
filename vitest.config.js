import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["./spec/**/*.spec.ts"],
    watch: false,
    clearMocks: true,
    hideSkippedTests: true,
    testTimeout: 20_000,
    hookTimeout: 20_000,
    env: {
      NODE_ENV: "test",
      SLATEDB_NODE_ENV: "test",
      FORCE_COLOR: "0",
    },
  },
});
