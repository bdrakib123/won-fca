"use strict";

const fs = require("node:fs");
const path = require("node:path");

const packageRoot = path.resolve(__dirname, "..");
const manifestPath = path.join(packageRoot, "package.json");
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
const packageExports = manifest.exports?.["."];
const targets = {
  main: manifest.main,
  require: packageExports?.require,
  import: packageExports?.import,
  types: packageExports?.types || manifest.types
};

for (const [kind, relativePath] of Object.entries(targets)) {
  if (typeof relativePath !== "string") {
    throw new Error(`Missing package ${kind} export`);
  }
  const resolvedPath = path.resolve(packageRoot, relativePath);
  if (!resolvedPath.startsWith(`${packageRoot}${path.sep}`)) {
    throw new Error(`Package ${kind} export escapes the package directory`);
  }
  if (!fs.statSync(resolvedPath).isFile()) {
    throw new Error(`Package ${kind} export is not a file`);
  }
}

const declarationDirectory = path.dirname(path.resolve(packageRoot, targets.types));
const globalTypesPath = path.join(declarationDirectory, "global-types.d.ts");
if (!fs.existsSync(globalTypesPath)) {
  throw new Error("Package global type declarations are missing");
}
for (const declarationName of ["index.d.ts", "index.d.mts"]) {
  const declarationPath = path.join(declarationDirectory, declarationName);
  const declaration = fs.readFileSync(declarationPath, "utf8");
  if (!declaration.includes('/// <reference path="./global-types.d.ts" />')) {
    throw new Error(`${declarationName} does not include global type declarations`);
  }
}

const cjsBridge = fs.readFileSync(
  path.resolve(packageRoot, targets.require),
  "utf8"
);
if (!cjsBridge.includes('require("./index.js")')) {
  throw new Error("Package CommonJS bridge does not target dist/index.js");
}

for (const [kind, relativePath] of [
  [
    "CommonJS implementation",
    path.join(path.dirname(targets.require), "index.js")
  ],
  ["ES module implementation", targets.import]
]) {
  const source = fs.readFileSync(
    path.resolve(packageRoot, relativePath),
    "utf8"
  );
  for (const marker of [
    "serializeSqliteWrite",
    "findOrCreateRecord",
    "PRAGMA integrity_check",
    "PRAGMA busy_timeout = 10000",
    "PRAGMA journal_mode = WAL",
    "SQLite cache integrity check failed",
    "SQLITE_BUSY",
    "SQLITE_LOCKED",
    "wrapError",
    "Failed to create user"
  ]) {
    if (!source.includes(marker)) {
      throw new Error(`Package ${kind} bundle is missing ${marker}`);
    }
  }
}

process.stdout.write(`${JSON.stringify({
  package: manifest.name,
  version: manifest.version,
  targets
}, null, 2)}\n`);
