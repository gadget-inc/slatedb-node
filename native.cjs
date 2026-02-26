const { createRequire } = require("node:module");
const { readFileSync } = require("node:fs");
const path = require("node:path");

const requireFromHere = createRequire(__filename);

/** @type {Error[]} */
const loadErrors = [];

const isMusl = () => {
  if (process.platform !== "linux") return false;
  try {
    return readFileSync("/usr/bin/ldd", "utf-8").includes("musl");
  } catch {
    return false;
  }
};

/**
 * @param {string} pathOrPackage
 * @returns {unknown}
 */
const tryRequire = (pathOrPackage) => {
  try {
    const loaded = requireFromHere(pathOrPackage);
    return /** @type {unknown} */ (loaded);
  } catch (error) {
    loadErrors.push(/** @type {Error} */ (error));
    return null;
  }
};

/** @returns {unknown} */
const loadNative = () => {
  if (process.env.NAPI_RS_NATIVE_LIBRARY_PATH) {
    const fromEnv = tryRequire(process.env.NAPI_RS_NATIVE_LIBRARY_PATH);
    if (fromEnv) return fromEnv;
  }

  const candidates = [];
  const optionalPackages = [];

  if (process.platform === "darwin") {
    if (process.arch === "arm64") {
      candidates.push("./slatedb-node.darwin-arm64.node");
      optionalPackages.push("slatedb-node-darwin-arm64");
    } else if (process.arch === "x64") {
      candidates.push("./slatedb-node.darwin-x64.node");
      optionalPackages.push("slatedb-node-darwin-x64");
    }
  } else if (process.platform === "win32") {
    if (process.arch === "arm64") {
      candidates.push("./slatedb-node.win32-arm64-msvc.node");
      optionalPackages.push("slatedb-node-win32-arm64-msvc");
    } else if (process.arch === "x64") {
      candidates.push("./slatedb-node.win32-x64-msvc.node");
      optionalPackages.push("slatedb-node-win32-x64-msvc");
    }
  } else if (process.platform === "linux") {
    if (process.arch === "arm64") {
      if (isMusl()) {
        candidates.push("./slatedb-node.linux-arm64-musl.node");
        optionalPackages.push("slatedb-node-linux-arm64-musl");
      } else {
        candidates.push("./slatedb-node.linux-arm64-gnu.node");
        optionalPackages.push("slatedb-node-linux-arm64-gnu");
      }
    } else if (process.arch === "x64") {
      if (isMusl()) {
        candidates.push("./slatedb-node.linux-x64-musl.node");
        optionalPackages.push("slatedb-node-linux-x64-musl");
      } else {
        candidates.push("./slatedb-node.linux-x64-gnu.node");
        optionalPackages.push("slatedb-node-linux-x64-gnu");
      }
    }
  }

  for (const candidate of candidates) {
    const loaded = tryRequire(path.join(__dirname, candidate));
    if (loaded) return loaded;
    const loadedRelative = tryRequire(candidate);
    if (loadedRelative) return loadedRelative;
  }

  for (const packageName of optionalPackages) {
    const loaded = tryRequire(packageName);
    if (loaded) return loaded;
  }

  const error = new Error(
    `Failed to load native binding for ${process.platform}/${process.arch}.\n` + loadErrors.map((entry) => `- ${entry.message}`).join("\n"),
  );
  error.cause = loadErrors;
  throw error;
};

module.exports = loadNative();
