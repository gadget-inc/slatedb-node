import execa from "execa";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "vitest";

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");

describe("package exports", () => {
  it("passes are-the-types-wrong", async () => {
    await execa("pnpm", ["exec", "attw", "--pack", ".", "--ignore-rules", "cjs-resolves-to-esm"], {
      cwd: rootDir,
    });
  }, 60_000);
});
