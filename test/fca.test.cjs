"use strict";

const assert = require("assert");
const { EventEmitter } = require("node:events");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawnSync } = require("node:child_process");
const { Sequelize, DataTypes, UniqueConstraintError } = require("sequelize");
const sqlite3 = require("sqlite3");
const ts = require("typescript");

const fca = require("../dist/index.js");
const recovery = require("../scripts/recover-sqlite-cache.cjs");

const tests = [];
const COOKIE_ATTRIBUTE_KEYS = new Set([
  "domain",
  "path",
  "expires",
  "max-age",
  "secure",
  "httponly",
  "samesite",
  "priority",
  "partitioned"
]);

function test(name, fn) {
  tests.push({ name, fn });
}

function toCookiePair(name, value) {
  const cookieName = String(name || "").trim();
  if (!cookieName) {
    return null;
  }

  const cookieValue = value === undefined || value === null ? "" : String(value).trim();
  return `${cookieName}=${cookieValue}`;
}

function dedupeCookiePairs(pairs) {
  const latestByName = new Map();

  for (const pair of pairs) {
    const normalized = String(pair || "").trim();
    const eqIndex = normalized.indexOf("=");
    if (eqIndex <= 0) {
      continue;
    }

    const name = normalized.slice(0, eqIndex).trim();
    if (!name) {
      continue;
    }

    latestByName.set(name, `${name}=${normalized.slice(eqIndex + 1).trim()}`);
  }

  return [...latestByName.values()];
}

function parseCookieHeaderString(raw) {
  let header = String(raw || "").trim();
  if (!header) {
    return [];
  }

  if (/^cookie\s*:/i.test(header)) {
    header = header.replace(/^cookie\s*:/i, "").trim();
  }

  const pairs = [];
  for (const segment of header.split(/[;\r\n]+/)) {
    const token = segment.trim();
    const eqIndex = token.indexOf("=");
    if (eqIndex <= 0) {
      continue;
    }

    const name = token.slice(0, eqIndex).trim();
    if (!name || COOKIE_ATTRIBUTE_KEYS.has(name.toLowerCase())) {
      continue;
    }

    const value = token.slice(eqIndex + 1).trim().replace(/^"(.*)"$/, "$1");
    const pair = toCookiePair(name, value);
    if (pair) {
      pairs.push(pair);
    }
  }

  return dedupeCookiePairs(pairs);
}

function parseNetscapeCookieText(raw) {
  const pairs = [];
  const lines = String(raw || "").split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }

    const normalized = trimmed.replace(/^#HttpOnly_/, "");
    if (normalized.startsWith("#")) {
      continue;
    }

    const columns = normalized.split("\t");
    if (columns.length < 7) {
      continue;
    }

    const name = columns[5];
    const value = columns.slice(6).join("\t");
    const pair = toCookiePair(name, value);
    if (pair) {
      pairs.push(pair);
    }
  }

  return dedupeCookiePairs(pairs);
}

function normalizeCookiePayload(payload) {
  if (!payload) {
    return null;
  }

  if (typeof payload === "string") {
    const netscapePairs = parseNetscapeCookieText(payload);
    if (netscapePairs.length) {
      return netscapePairs.join("; ");
    }

    const headerPairs = parseCookieHeaderString(payload);
    if (headerPairs.length) {
      return headerPairs.join("; ");
    }

    return null;
  }

  if (Array.isArray(payload)) {
    const pairs = [];

    for (const entry of payload) {
      if (typeof entry === "string") {
        const stringPairs = parseCookieHeaderString(entry);
        if (stringPairs.length) {
          pairs.push(...stringPairs);
          continue;
        }

        const eqIndex = entry.indexOf("=");
        if (eqIndex > 0) {
          const pair = toCookiePair(entry.slice(0, eqIndex), entry.slice(eqIndex + 1));
          if (pair) {
            pairs.push(pair);
          }
        }
        continue;
      }

      if (entry && typeof entry === "object") {
        const pair = toCookiePair(entry.name || entry.key, entry.value);
        if (pair) {
          pairs.push(pair);
        }
      }
    }

    const deduped = dedupeCookiePairs(pairs);
    return deduped.length ? deduped.join("; ") : null;
  }

  if (typeof payload === "object") {
    if (payload.Cookie || payload.cookie || payload.cookies) {
      return normalizeCookiePayload(payload.Cookie || payload.cookie || payload.cookies);
    }

    if ((payload.name || payload.key) && Object.prototype.hasOwnProperty.call(payload, "value")) {
      return toCookiePair(payload.name || payload.key, payload.value);
    }

    const pairs = [];
    for (const [name, value] of Object.entries(payload)) {
      if (value === undefined || value === null || typeof value === "object") {
        continue;
      }

      const pair = toCookiePair(name, value);
      if (pair) {
        pairs.push(pair);
      }
    }

    const deduped = dedupeCookiePairs(pairs);
    return deduped.length ? deduped.join("; ") : null;
  }

  return null;
}

function loadCookieCredentials(candidate, sourceLabel) {
  if (!fs.existsSync(candidate)) {
    return null;
  }

  const raw = fs.readFileSync(candidate, "utf8").trim();
  if (!raw) {
    return null;
  }

  let payload = raw;
  if (candidate.endsWith(".json")) {
    payload = JSON.parse(raw);
  }

  const cookieHeader = normalizeCookiePayload(payload);
  if (!cookieHeader) {
    return null;
  }

  return {
    mode: "cookie",
    source: sourceLabel,
    credentials: {
      Cookie: cookieHeader
    }
  };
}

function readJsonFileIfValid(candidate) {
  if (!fs.existsSync(candidate)) {
    return null;
  }

  const raw = fs.readFileSync(candidate, "utf8");
  if (!raw.trim()) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function loadIntegrationCredentials() {
  const rootDir = process.cwd();
  const cookieCandidates = [
    path.join(rootDir, "cookie.txt"),
    path.join(rootDir, "cookies.txt"),
    path.join(rootDir, "cookie.json"),
    path.join(rootDir, "cookies.json"),
    path.join(rootDir, "test", "cookie.txt"),
    path.join(rootDir, "test", "cookie.json")
  ];

  for (const candidate of cookieCandidates) {
    const loaded = loadCookieCredentials(candidate, path.relative(rootDir, candidate) || candidate);
    if (loaded) {
      return loaded;
    }
  }

  const appStateCandidates = [
    path.join(rootDir, "appstate.json"),
    path.join(rootDir, "appState.json"),
    path.join(rootDir, "test", "appstate.json")
  ];

  for (const candidate of appStateCandidates) {
    const parsedAppState = readJsonFileIfValid(candidate);
    if (!parsedAppState) {
      continue;
    }

    return {
      mode: "appState",
      source: path.relative(rootDir, candidate) || candidate,
      credentials: {
        appState: parsedAppState
      }
    };
  }

  const configPath = path.join(rootDir, "fca-config.json");
  if (fs.existsSync(configPath)) {
    const rawConfig = readJsonFileIfValid(configPath);
    if (!rawConfig) {
      return null;
    }

    const configCookiePath =
      rawConfig.cookiePath ||
      rawConfig.cookiesPath ||
      rawConfig.cookieFile ||
      rawConfig.cookiesFile ||
      rawConfig.credentials?.cookiePath ||
      rawConfig.credentials?.cookiesPath ||
      rawConfig.credentials?.cookieFile ||
      rawConfig.credentials?.cookiesFile;

    if (configCookiePath) {
      const resolvedCookiePath = path.resolve(rootDir, configCookiePath);
      const loaded = loadCookieCredentials(
        resolvedCookiePath,
        path.relative(rootDir, resolvedCookiePath) || resolvedCookiePath
      );
      if (loaded) {
        return loaded;
      }
    }

    const configCookie =
      rawConfig.Cookie ||
      rawConfig.cookie ||
      rawConfig.cookies ||
      rawConfig.sessionCookie ||
      rawConfig.credentials?.Cookie ||
      rawConfig.credentials?.cookie ||
      rawConfig.credentials?.cookies ||
      rawConfig.credentials?.sessionCookie;

    if (configCookie) {
      const normalizedConfigCookie = normalizeCookiePayload(configCookie);
      if (normalizedConfigCookie) {
        return {
          mode: "cookie",
          source: "fca-config.json",
          credentials: {
            Cookie: normalizedConfigCookie
          }
        };
      }
    }

    const fileAppStatePath =
      rawConfig.appStatePath ||
      rawConfig.appstatePath ||
      rawConfig.appStateFile ||
      rawConfig.appstateFile;

    if (fileAppStatePath) {
      const resolvedPath = path.resolve(rootDir, fileAppStatePath);
      const parsedAppState = readJsonFileIfValid(resolvedPath);
      if (parsedAppState) {
        return {
          mode: "appState",
          source: path.relative(rootDir, resolvedPath) || resolvedPath,
          credentials: {
            appState: parsedAppState
          }
        };
      }
    }
  }

  return null;
}

test("public exports are available", () => {
  assert.strictEqual(typeof fca.login, "function");
  assert.strictEqual(typeof fca.createFcaClient, "function");
  assert.strictEqual(typeof fca.attachClientFacade, "function");
  assert.strictEqual(typeof fca.resolveConfig, "function");
  assert.strictEqual(typeof fca.loadConfig, "function");
});

require.extensions[".ts"] = function compileTypeScript(module, filename) {
  const source = fs.readFileSync(filename, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true
    },
    fileName: filename
  });
  module._compile(compiled.outputText, filename);
};

test("user and thread cache creation is idempotent under concurrency", async () => {
  const { findOrCreateRecord } = require("../src/database/helpers.ts");
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wonfca-cache-test-"));
  const sequelize = new Sequelize({
    dialect: "sqlite",
    storage: path.join(directory, "cache.sqlite"),
    logging: false,
    pool: { max: 1 }
  });
  const User = sequelize.define("User", {
    userID: { type: DataTypes.STRING, allowNull: false, unique: true },
    data: DataTypes.TEXT
  });
  const Thread = sequelize.define("Thread", {
    threadID: { type: DataTypes.STRING, allowNull: false, unique: true },
    messageCount: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
    data: DataTypes.TEXT
  });

  try {
    await sequelize.sync();
    const userResults = await Promise.all(
      Array.from({ length: 30 }, () =>
        findOrCreateRecord(User, "userID", "same-user", { data: "profile" })
      )
    );
    const threadResults = await Promise.all(
      Array.from({ length: 30 }, () =>
        findOrCreateRecord(Thread, "threadID", "same-thread", { data: "thread" })
      )
    );
    const distinctUsers = await Promise.all(
      Array.from({ length: 12 }, (_, index) =>
        findOrCreateRecord(User, "userID", `user-${index}`, { data: "profile" })
      )
    );

    assert.strictEqual(await User.count(), 13);
    assert.strictEqual(await Thread.count(), 1);
    assert.strictEqual(userResults.filter((result) => result.created).length, 1);
    assert.strictEqual(threadResults.filter((result) => result.created).length, 1);
    assert.strictEqual(distinctUsers.filter((result) => result.created).length, 12);

    const { attachThreadUpdater } = require("../src/core/state.ts");
    const context = {};
    assert.strictEqual(attachThreadUpdater(context, { Thread }, () => {}), true);
    await context._updateThreadFromMessage({ threadID: "same-thread" });
    assert.strictEqual(
      (await Thread.findOne({ where: { threadID: "same-thread" } })).messageCount,
      1
    );
    await Promise.all(
      Array.from({ length: 10 }, () =>
        context._updateThreadFromMessage({ threadID: "concurrent-thread" })
      )
    );
    assert.strictEqual(
      (await Thread.findOne({ where: { threadID: "concurrent-thread" } })).messageCount,
      10
    );

    await assert.rejects(
      sequelize.transaction(async (transaction) => {
        await User.create({ userID: "rolled-back" }, { transaction });
        throw new Error("simulate interrupted transaction");
      }),
      /simulate interrupted transaction/
    );
    assert.strictEqual(await User.findOne({ where: { userID: "rolled-back" } }), null);
  } finally {
    await sequelize.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("cache create-or-get preserves unique conflicts and surfaces SQLite failures", async () => {
  const { findOrCreateRecord, wrapError } = require("../src/database/helpers.ts");
  const cached = { get: () => ({ userID: "duplicate" }) };
  const duplicateError = new UniqueConstraintError({ message: "duplicate" });
  const duplicateModel = {
    findOrCreate: async () => { throw duplicateError; },
    findOne: async () => cached
  };
  const duplicate = await findOrCreateRecord(
    duplicateModel,
    "userID",
    "duplicate",
    {}
  );
  assert.strictEqual(duplicate.record, cached);
  assert.strictEqual(duplicate.created, false);

  for (const code of ["SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_CORRUPT"]) {
    const failure = Object.assign(new Error(`${code}: test failure`), {
      original: { code }
    });
    const model = {
      findOrCreate: async () => { throw failure; }
    };
    await assert.rejects(
      findOrCreateRecord(model, "threadID", "thread", {}),
      (error) => error === failure
    );
  }

  const corruption = Object.assign(new Error("database disk image is malformed"), {
    code: "SQLITE_CORRUPT",
    original: { code: "SQLITE_CORRUPT" }
  });
  const wrapped = wrapError("Failed to create user", corruption);
  assert.match(wrapped.message, /SQLITE_CORRUPT/);
  assert.strictEqual(wrapped.code, "SQLITE_CORRUPT");
  assert.strictEqual(wrapped.original, corruption.original);
});

test("cache initialization is shared, integrity-checked, and retryable after interruption", async () => {
  const { createCacheInitializer } = require("../src/database/initialize.ts");
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wonfca-init-test-"));
  const sequelize = new Sequelize({
    dialect: "sqlite",
    storage: path.join(directory, "cache.sqlite"),
    logging: false,
    pool: { max: 1 }
  });
  let syncCalls = 0;
  const initialize = createCacheInitializer(sequelize, async () => {
    syncCalls += 1;
    if (syncCalls === 1) {
      throw new Error("simulated interrupted initialization");
    }
    await sequelize.define("CacheRecord", {
      id: { type: DataTypes.STRING, primaryKey: true }
    }).sync();
  });

  try {
    await assert.rejects(initialize(), /simulated interrupted initialization/);
    await Promise.all([initialize(), initialize()]);
    assert.strictEqual(syncCalls, 2);
    const [busyTimeout] = await sequelize.query("PRAGMA busy_timeout");
    const [journalMode] = await sequelize.query("PRAGMA journal_mode");
    assert.strictEqual(Number(Object.values(busyTimeout[0])[0]), 10000);
    assert.strictEqual(String(Object.values(journalMode[0])[0]).toLowerCase(), "wal");
  } finally {
    await sequelize.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("corrupt cache initialization fails before schema synchronization", async () => {
  const { createCacheInitializer } = require("../src/database/initialize.ts");
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wonfca-corrupt-test-"));
  const filename = path.join(directory, "cache.sqlite");
  const setup = new Sequelize({ dialect: "sqlite", storage: filename, logging: false });
  let syncCalls = 0;
  let setupClosed = false;
  let before;
  let after;

  try {
    const User = setup.define("User", {
      userID: { type: DataTypes.STRING, unique: true }
    });
    await User.sync();
    await User.create({ userID: "preserve-me" });
    await setup.close();
    setupClosed = true;

    const corruptor = await new Promise((resolve, reject) => {
      const database = new sqlite3.Database(filename, (error) => {
        error ? reject(error) : resolve(database);
      });
    });
    await new Promise((resolve, reject) => {
      corruptor.serialize(() => {
        corruptor.run("PRAGMA writable_schema = ON");
        corruptor.run(
          "UPDATE sqlite_master SET rootpage = 99999 WHERE name = 'sqlite_autoindex_Users_1'",
          (error) => error ? reject(error) : resolve()
        );
      });
    });
    await new Promise((resolve, reject) => {
      corruptor.close((error) => error ? reject(error) : resolve());
    });
    before = crypto.createHash("sha256").update(fs.readFileSync(filename)).digest("hex");

    const damaged = new Sequelize({ dialect: "sqlite", storage: filename, logging: false });
    const initialize = createCacheInitializer(damaged, async () => {
      syncCalls += 1;
      await damaged.sync();
    });
    await assert.rejects(initialize());
    await damaged.close();
    after = crypto.createHash("sha256").update(fs.readFileSync(filename)).digest("hex");
    assert.strictEqual(syncCalls, 0);
    assert.strictEqual(after, before);
  } finally {
    if (!setupClosed) {
      await setup.close().catch(() => {});
    }
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("cache recovery operates on separate copies and never overwrites existing paths", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wonfca-recovery-test-"));
  const source = path.join(directory, "source.sqlite");
  const snapshot = path.join(directory, "snapshot.sqlite");
  const recovered = path.join(directory, "recovered.sqlite");
  const pythonDump = path.join(directory, "python-dump.sqlite");
  const rowSalvage = path.join(directory, "row-salvage.sqlite");
  const sequelize = new Sequelize({ dialect: "sqlite", storage: source, logging: false });

  try {
    const Thread = sequelize.define("Threads", {
      threadID: { type: DataTypes.STRING, primaryKey: true },
      data: DataTypes.TEXT
    });
    await Thread.sync();
    await Thread.bulkCreate([
      { threadID: "thread-1", data: "one" },
      { threadID: "thread-2", data: "two" }
    ]);
    await sequelize.close();
    const sourceHash = crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex");

    const report = await recovery.recoverCache({ source, snapshot, recovered });
    assert.strictEqual(report.verified, true);
    assert.strictEqual(report.snapshotCheck.integrityOk, true);
    assert.strictEqual(report.recoveredCheck.integrityOk, true);
    assert.deepStrictEqual(report.records.map((record) => record.missingRows), [0]);
    assert.deepStrictEqual(report.indexDifferences, []);
    assert.deepStrictEqual(report.tableDifferences, []);

    fs.closeSync(fs.openSync(pythonDump, "wx"));
    const pythonResult = spawnSync(
      "python3",
      [
        path.join(__dirname, "../scripts/sqlite-cache-dump.py"),
        source,
        pythonDump
      ],
      { encoding: "utf8" }
    );
    assert.equal(pythonResult.status, 0, pythonResult.stderr);
    const pythonCheck = await recovery.inspectDatabase(pythonDump);
    assert.equal(pythonCheck.integrityOk, true);
    assert.equal(pythonCheck.quickCheckOk, true);
    const pythonRecords = await recovery.compareRecords(source, pythonDump);
    assert.deepStrictEqual(pythonRecords.map((record) => record.missingRows), [0]);
    assert.deepStrictEqual(pythonRecords.indexDifferences, []);
    assert.deepStrictEqual(pythonRecords.tableDifferences, []);

    fs.closeSync(fs.openSync(rowSalvage, "wx"));
    const salvageResult = spawnSync(
      "python3",
      [
        path.join(__dirname, "../scripts/sqlite-cache-dump.py"),
        "--salvage-rows",
        source,
        rowSalvage
      ],
      { encoding: "utf8" }
    );
    assert.equal(salvageResult.status, 0, salvageResult.stderr);
    const salvagedRecords = await recovery.compareRecords(source, rowSalvage);
    assert.deepStrictEqual(salvagedRecords.map((record) => record.missingRows), [0]);
    assert.deepStrictEqual(salvagedRecords.map((record) => record.extraRows), [0]);
    assert.strictEqual(salvagedRecords[0].schemaMismatch, true);
    assert.strictEqual(salvagedRecords[0].indexMismatch, true);
    assert.strictEqual(
      crypto.createHash("sha256").update(fs.readFileSync(source)).digest("hex"),
      sourceHash
    );
    await assert.rejects(
      recovery.recoverCache({ source, snapshot, recovered }),
      /must not already exist/
    );

    const corruptSource = path.join(directory, "corrupt-source.sqlite");
    const corruptSnapshot = path.join(directory, "corrupt-snapshot.sqlite");
    const corruptRecovered = path.join(directory, "corrupt-recovered.sqlite");
    const damaged = new Sequelize({
      dialect: "sqlite",
      storage: corruptSource,
      logging: false
    });
    const DamagedThread = damaged.define("Threads", {
      threadID: { type: DataTypes.STRING, unique: true },
      data: DataTypes.TEXT
    });
    await DamagedThread.sync();
    await DamagedThread.create({ threadID: "keep", data: "value" });
    await damaged.close();
    const corruptor = await new Promise((resolve, reject) => {
      const database = new sqlite3.Database(corruptSource, (error) => {
        error ? reject(error) : resolve(database);
      });
    });
    await new Promise((resolve, reject) => {
      corruptor.serialize(() => {
        corruptor.run("PRAGMA writable_schema = ON");
        corruptor.run(
          "UPDATE sqlite_master SET rootpage = 99999 WHERE name = 'sqlite_autoindex_Threads_1'",
          (error) => error ? reject(error) : resolve()
        );
      });
    });
    await new Promise((resolve, reject) => {
      corruptor.close((error) => error ? reject(error) : resolve());
    });
    const corruptHash = crypto.createHash("sha256").update(fs.readFileSync(corruptSource)).digest("hex");
    const failedRecovery = await recovery.recoverCache({
      source: corruptSource,
      snapshot: corruptSnapshot,
      recovered: corruptRecovered
    });
    assert.strictEqual(failedRecovery.verified, false);
    assert.strictEqual(failedRecovery.snapshotCheck.integrityOk, false);
    assert.ok(failedRecovery.recoveryError || !failedRecovery.recoveredCheck.integrityOk);
    assert.ok(
      failedRecovery.records.some((record) => record.missingRows > 0) ||
      failedRecovery.records.some((record) => record.schemaMismatch) ||
      failedRecovery.recordComparisonError
    );
    assert.strictEqual(
      crypto.createHash("sha256").update(fs.readFileSync(corruptSource)).digest("hex"),
      corruptHash
    );
  } finally {
    await sequelize.close().catch(() => {});
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("dist/cjs.cjs default export is callable login (Mirai / classic require)", () => {
  const login = require("../dist/cjs.cjs");
  assert.strictEqual(typeof login, "function");
  assert.strictEqual(typeof login.login, "function");
  assert.strictEqual(typeof login.createMessengerBot, "function");
});

test("package export validation checks files without loading package entrypoints", () => {
  const checker = path.join(__dirname, "../scripts/check-package-exports.cjs");
  const source = fs.readFileSync(checker, "utf8");
  assert.doesNotMatch(
    source,
    /^\s*(?:const|let|var)\s+\w+\s*=\s*require\s*\(\s*["'](?:\.\.?\/|@cexy\/wonfca)/m
  );
  assert.doesNotMatch(
    source,
    /^\s*(?:const|let|var)\s+\w+\s*=\s*import\s*\(\s*["'](?:\.\.?\/|@cexy\/wonfca)/m
  );

  const result = spawnSync(process.execPath, [checker], {
    cwd: os.tmpdir(),
    encoding: "utf8"
  });
  assert.strictEqual(result.status, 0, result.stderr);
  const report = JSON.parse(result.stdout);
  assert.strictEqual(report.package, "@cexy/wonfca");
  assert.strictEqual(report.targets.require, "./dist/cjs.cjs");
  assert.strictEqual(report.targets.import, "./dist/index.mjs");
  assert.strictEqual(report.targets.types, "./dist/index.d.ts");
});

test("resolveConfig honors legacy autoUpdate alias", () => {
  const resolved = fca.resolveConfig({
    autoUpdate: false,
    checkUpdate: {
      notifyIfCurrent: true
    }
  });

  assert.strictEqual(resolved.autoUpdate, false);
  assert.strictEqual(resolved.checkUpdate.enabled, false);
  assert.strictEqual(resolved.checkUpdate.notifyIfCurrent, true);
});

test("createFcaClient reuses grouped namespaces when they already exist", () => {
  const send = () => "send-ok";
  const getInfo = () => "thread-ok";
  const listen = () => "listen-ok";

  const api = {
    messages: { send },
    threads: { getInfo },
    users: { getInfo: () => "user-ok" },
    account: { getCurrentUserID: () => "123" },
    realtime: { listen },
    http: { get: () => "http-ok" },
    scheduler: { scheduleMessage: () => "sched-ok" }
  };

  const client = fca.createFcaClient(api);

  assert.strictEqual(client.messages.send, send);
  assert.strictEqual(client.threads.getInfo, getInfo);
  assert.strictEqual(client.realtime.listen, listen);
  assert.strictEqual(client.scheduler.scheduleMessage(), "sched-ok");
});

test("attachClientFacade exposes grouped namespaces on legacy api object", () => {
  const api = {
    sendMessage() {
      return "legacy-send";
    },
    getThreadInfo() {
      return "legacy-thread";
    },
    getUserInfo() {
      return "legacy-user";
    },
    getCurrentUserID() {
      return "1000";
    },
    listenMqtt() {
      return "legacy-listen";
    },
    stopListening() {
      return "legacy-stop";
    },
    stopListeningAsync() {
      return Promise.resolve("legacy-stop-async");
    },
    useMiddleware() {
      return "legacy-use";
    },
    removeMiddleware() {
      return "legacy-remove";
    },
    clearMiddleware() {
      return "legacy-clear";
    },
    listMiddleware() {
      return [];
    },
    setMiddlewareEnabled() {
      return true;
    },
    httpGet() {
      return "legacy-http-get";
    },
    httpPost() {
      return "legacy-http-post";
    },
    postFormData() {
      return "legacy-http-form";
    }
  };

  const client = fca.attachClientFacade(api);

  assert.strictEqual(api.client, client);
  assert.strictEqual(typeof api.messages.send, "function");
  assert.strictEqual(typeof api.threads.getInfo, "function");
  assert.strictEqual(typeof api.users.getInfo, "function");
  assert.strictEqual(typeof api.account.getCurrentUserID, "function");
  assert.strictEqual(typeof api.realtime.listen, "function");
  assert.strictEqual(typeof api.http.get, "function");
});

test("MessengerBot start is idempotent and stop releases session resources", async () => {
  let listenCount = 0;
  let stopCount = 0;
  let remoteCloseCount = 0;
  let schedulerDestroyCount = 0;
  let cleaned = false;

  const api = {
    listenMqtt() {
      listenCount += 1;
      const emitter = new EventEmitter();
      emitter.stopListeningAsync = async () => {
        stopCount += 1;
      };
      return emitter;
    },
    remote: {
      close() {
        remoteCloseCount += 1;
      }
    },
    scheduler: {
      destroy() {
        schedulerDestroyCount += 1;
      }
    },
    _cleanupSessionResources() {
      if (cleaned) return;
      cleaned = true;
      remoteCloseCount += 1;
      schedulerDestroyCount += 1;
    }
  };

  const bot = new fca.MessengerBot(
    { api },
    {
      enableComposer: false,
      commandPrefix: "/",
      stopOnSignals: false,
      maxEventListeners: 64
    }
  );

  bot.startListening();
  bot.startListening();
  await bot.stop();
  await bot.stop();

  assert.strictEqual(listenCount, 1);
  assert.strictEqual(stopCount, 1);
  assert.strictEqual(remoteCloseCount, 1);
  assert.strictEqual(schedulerDestroyCount, 1);
});

test("optional live login works when credentials are provided", async () => {
  const integration = loadIntegrationCredentials();
  if (!integration) {
    return {
      skipped: true,
      reason: "add cookie.txt, cookies.txt, cookie.json, or cookiePath/Cookie in fca-config.json to enable real-cookie login"
    };
  }

  let ctx;
  try {
    ctx = await fca.login(integration.credentials, {
      online: false,
      forceLogin: false,
      autoMarkRead: false
    });
  } catch (error) {
    return {
      skipped: true,
      reason: `live login failed from ${integration.source}: ${error && error.message ? error.message : String(error)}`
    };
  }

  assert.ok(ctx);
  assert.ok(ctx.api);
  assert.strictEqual(typeof ctx.api.sendMessage, "function");
  assert.strictEqual(typeof ctx.api.listenMqtt, "function");
  assert.strictEqual(typeof ctx.api.client.messages.send, "function");
  assert.ok(ctx.userID || ctx.fbid);

  return {
    mode: integration.mode,
    source: integration.source,
    userID: ctx.userID || ctx.fbid || null
  };
});

async function main() {
  let passed = 0;
  let skipped = 0;

  for (const { name, fn } of tests) {
    try {
      const result = await fn();
      if (result && result.skipped) {
        skipped += 1;
        console.log(`SKIP ${name} - ${result.reason}`);
        continue;
      }

      passed += 1;
      if (result && result.userID) {
        console.log(`PASS ${name} (${result.mode} from ${result.source}: ${result.userID})`);
      } else {
        console.log(`PASS ${name}`);
      }
    } catch (error) {
      console.error(`FAIL ${name}`);
      throw error;
    }
  }

  console.log(`SUMMARY passed=${passed} skipped=${skipped} total=${tests.length}`);
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
