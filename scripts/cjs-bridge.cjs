"use strict";

const fs = require("fs");
const path = require("path");

const dist = path.join(__dirname, "..", "dist");
const out = path.join(dist, "cjs.cjs");

const code = `"use strict";
const m = require("./index.js");
const core = typeof m.login === "function" ? m.login : m.default;
if (typeof core !== "function") {
  throw new Error("@cexy/wonfca: expected login to be a function (check dist/index.js exports).");
}
Object.assign(core, m);
core.default = core;
module.exports = core;
`;

fs.writeFileSync(out, code, "utf8");

const globalTypesSource = path.join(__dirname, "..", "src", "global-types.d.ts");
const globalTypesTarget = path.join(dist, "global-types.d.ts");
fs.copyFileSync(globalTypesSource, globalTypesTarget);

for (const declarationName of ["index.d.ts", "index.d.mts"]) {
  const declarationPath = path.join(dist, declarationName);
  const reference = '/// <reference path="./global-types.d.ts" />\n';
  const declaration = fs.readFileSync(declarationPath, "utf8");
  if (!declaration.includes(reference.trim())) {
    fs.writeFileSync(declarationPath, `${reference}${declaration}`, "utf8");
  }
}
