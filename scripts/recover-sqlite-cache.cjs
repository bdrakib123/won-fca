"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");
const sqlite3 = require("sqlite3");

function openDatabase(filename, mode = sqlite3.OPEN_READONLY) {
  return new Promise((resolve, reject) => {
    const database = new sqlite3.Database(filename, mode, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve(database);
      }
    });
  });
}

function closeDatabase(database) {
  return new Promise((resolve, reject) => {
    database.close((error) => error ? reject(error) : resolve());
  });
}

function all(database, sql, parameters = []) {
  return new Promise((resolve, reject) => {
    database.all(sql, parameters, (error, rows) => {
      error ? reject(error) : resolve(rows);
    });
  });
}

function quoteIdentifier(identifier) {
  return `"${String(identifier).replaceAll('"', '""')}"`;
}

async function inspectDatabase(filename) {
  let database;
  try {
    database = await openDatabase(filename);
    const integrityRows = await all(database, "PRAGMA integrity_check");
    const quickRows = await all(database, "PRAGMA quick_check");
    const tables = await all(
      database,
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    const rowCounts = {};
    for (const { name } of tables) {
      try {
        const rows = await all(database, `SELECT COUNT(*) AS count FROM ${quoteIdentifier(name)}`);
        rowCounts[name] = rows[0].count;
      } catch (error) {
        rowCounts[name] = { error: error.message };
      }
    }

    let foreignKeyErrors;
    try {
      foreignKeyErrors = await all(database, "PRAGMA foreign_key_check");
    } catch (error) {
      foreignKeyErrors = [{ error: error.message }];
    }

    const integrity = integrityRows.map((row) => Object.values(row)[0]);
    const quickCheck = quickRows.map((row) => Object.values(row)[0]);
    return {
      integrity,
      integrityOk: integrity.length === 1 && integrity[0] === "ok",
      quickCheck,
      quickCheckOk: quickCheck.length === 1 && quickCheck[0] === "ok",
      rowCounts,
      foreignKeyErrorCount: foreignKeyErrors.length,
      foreignKeyErrors: foreignKeyErrors.slice(0, 20)
    };
  } catch (error) {
    return {
      integrityOk: false,
      quickCheckOk: false,
      error: error.message
    };
  } finally {
    if (database) {
      await closeDatabase(database);
    }
  }
}

async function createVerifiedCopy(source, snapshot) {
  await fs.promises.mkdir(path.dirname(snapshot), { recursive: true });
  const reserved = await fs.promises.open(snapshot, "wx");
  await reserved.close();
  const sourceDb = await openDatabase(source);
  try {
    const backup = sourceDb.backup(snapshot);
    try {
      await new Promise((resolve, reject) => {
        backup.step(-1, (error) => error ? reject(error) : resolve());
      });
    } finally {
      await new Promise((resolve, reject) => {
        backup.finish((error) => error ? reject(error) : resolve());
      });
    }
  } finally {
    await closeDatabase(sourceDb);
  }
}

function runSqliteRecovery(snapshot, recovered, sqliteCommand) {
  return new Promise((resolve, reject) => {
    const reader = spawn(sqliteCommand, [snapshot, ".recover"], {
      stdio: ["ignore", "pipe", "pipe"]
    });
    const writer = spawn(sqliteCommand, ["-bail", recovered], {
      stdio: ["pipe", "ignore", "pipe"]
    });
    let readerError = "";
    let writerError = "";
    let readerCode;
    let writerCode;

    reader.stderr.setEncoding("utf8");
    writer.stderr.setEncoding("utf8");
    reader.stderr.on("data", (chunk) => { readerError += chunk; });
    writer.stderr.on("data", (chunk) => { writerError += chunk; });
    reader.stdout.pipe(writer.stdin);

    reader.once("error", reject);
    writer.once("error", reject);
    reader.once("close", (code) => {
      readerCode = code;
      finish();
    });
    writer.once("close", (code) => {
      writerCode = code;
      finish();
    });

    function finish() {
      if (readerCode === undefined || writerCode === undefined) {
        return;
      }
      if (readerCode !== 0 || writerCode !== 0) {
        reject(new Error(
          `sqlite3 recovery failed (recover=${readerCode}, apply=${writerCode}): ` +
          `${readerError}${writerError}`.trim().slice(0, 4000)
        ));
        return;
      }
      resolve();
    }
  });
}

function runPythonDump(snapshot, recovered, pythonCommand) {
  return new Promise((resolve, reject) => {
    const process = spawn(pythonCommand, [
      path.join(__dirname, "sqlite-cache-dump.py"),
      snapshot,
      recovered
    ], { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk) => { stderr += chunk; });
    process.once("error", reject);
    process.once("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(
          `Python SQLite dump failed (exit=${code}): ${stderr.trim().slice(0, 4000)}`
        ));
      }
    });
  });
}

function runPythonRowSalvage(snapshot, recovered, pythonCommand) {
  return new Promise((resolve, reject) => {
    const process = spawn(pythonCommand, [
      path.join(__dirname, "sqlite-cache-dump.py"),
      "--salvage-rows",
      snapshot,
      recovered
    ], { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    process.stderr.setEncoding("utf8");
    process.stderr.on("data", (chunk) => { stderr += chunk; });
    process.once("error", reject);
    process.once("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(
          `Python SQLite row salvage failed (exit=${code}): ${stderr.trim().slice(0, 4000)}`
        ));
      }
    });
  });
}

async function compareRecords(snapshot, recovered) {
  const database = await openDatabase(recovered, sqlite3.OPEN_READWRITE);
  try {
    await new Promise((resolve, reject) => {
      database.run("ATTACH DATABASE ? AS verified_source", [snapshot], (error) => {
        error ? reject(error) : resolve();
      });
    });

    const sourceTables = await all(
      database,
      "SELECT name FROM verified_source.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    const recoveredTables = await all(
      database,
      "SELECT name FROM main.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    const results = [];
    results.tableDifferences = JSON.stringify(sourceTables.map((table) => table.name)) ===
      JSON.stringify(recoveredTables.map((table) => table.name))
      ? []
      : [{
          source: sourceTables.map((table) => table.name),
          recovered: recoveredTables.map((table) => table.name)
        }];

    for (const { name } of sourceTables) {
      const quoted = quoteIdentifier(name);
      const recoveredTable = await all(
        database,
        "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = ?",
        [name]
      );

      if (!recoveredTable.length) {
        const sourceCount = await all(
          database,
          `SELECT COUNT(*) AS count FROM verified_source.${quoted}`
        );
        results.push({
          table: name,
          sourceRows: sourceCount[0].count,
          recoveredRows: 0,
          missingRows: sourceCount[0].count,
          extraRows: 0,
          changedRows: 0,
          error: null
        });
        continue;
      }

      try {
        const sourceRows = await all(database, `SELECT * FROM verified_source.${quoted}`);
        const recoveredRows = await all(database, `SELECT * FROM main.${quoted}`);
        const sourceColumns = await all(database, `PRAGMA verified_source.table_info(${quoted})`);
        const recoveredColumns = await all(database, `PRAGMA main.table_info(${quoted})`);
        const schemaSignature = (columns) => columns.map((column) => ({
          name: column.name,
          type: column.type,
          notnull: column.notnull,
          defaultValue: column.dflt_value,
          primaryKey: column.pk
        }));
        const schemaMismatch = JSON.stringify(schemaSignature(sourceColumns)) !==
          JSON.stringify(schemaSignature(recoveredColumns));
        const primaryKeys = sourceColumns
          .filter((column) => column.pk > 0)
          .sort((left, right) => left.pk - right.pk)
          .map((column) => column.name);
        const rowKey = (row) => JSON.stringify(
          primaryKeys.length
            ? primaryKeys.map((column) => row[column])
            : row
        );
        const recoveredByKey = new Map();
        for (const row of recoveredRows) {
          const key = rowKey(row);
          const rows = recoveredByKey.get(key) || [];
          rows.push(row);
          recoveredByKey.set(key, rows);
        }
        let missingRows = 0;
        let changedRows = 0;
        for (const sourceRow of sourceRows) {
          const candidates = recoveredByKey.get(rowKey(sourceRow));
          if (!candidates?.length) {
            missingRows += 1;
            continue;
          }
          const sourceValue = JSON.stringify(sourceRow);
          const matchIndex = candidates.findIndex(
            (row) => JSON.stringify(row) === sourceValue
          );
          if (matchIndex !== -1) {
            candidates.splice(matchIndex, 1);
          } else {
            changedRows += 1;
            candidates.shift();
          }
        }
        const extraRows = [...recoveredByKey.values()]
          .reduce((count, rows) => count + rows.length, 0);
        const sourceTableIndexes = await all(
          database,
          `PRAGMA verified_source.index_list(${quoted})`
        );
        const recoveredTableIndexes = await all(
          database,
          `PRAGMA main.index_list(${quoted})`
        );
        const normalizeTableIndexes = (indexes) => indexes
          .map(({ name: indexName, unique, origin, partial }) => ({
            name: indexName,
            unique,
            origin,
            partial
          }))
          .sort((left, right) => left.name.localeCompare(right.name));
        const indexMismatch = JSON.stringify(normalizeTableIndexes(sourceTableIndexes)) !==
          JSON.stringify(normalizeTableIndexes(recoveredTableIndexes));
        results.push({
          table: name,
          sourceRows: sourceRows.length,
          recoveredRows: recoveredRows.length,
          missingRows,
          extraRows,
          changedRows,
          schemaMismatch,
          indexMismatch,
          error: null
        });
      } catch (error) {
        results.push({
          table: name,
          sourceRows: null,
          recoveredRows: null,
          missingRows: null,
          extraRows: null,
          changedRows: null,
          schemaMismatch: null,
          indexMismatch: null,
          error: error.message
        });
      }
    }

    const sourceIndexes = await all(
      database,
      "SELECT name, tbl_name, sql FROM verified_source.sqlite_master " +
      "WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    const recoveredIndexes = await all(
      database,
      "SELECT name, tbl_name, sql FROM main.sqlite_master " +
      "WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    const normalizeIndexes = (indexes) => indexes.map((index) => ({
      name: index.name,
      table: index.tbl_name,
      sql: index.sql
    }));
    results.indexDifferences = JSON.stringify(normalizeIndexes(sourceIndexes)) ===
      JSON.stringify(normalizeIndexes(recoveredIndexes))
      ? []
      : [{
          source: normalizeIndexes(sourceIndexes),
          recovered: normalizeIndexes(recoveredIndexes)
        }];
    return results;
  } finally {
    await closeDatabase(database);
  }
}

async function recoverCache({
  source,
  snapshot,
  recovered,
  sqliteCommand = "sqlite3",
  pythonCommand = "python3"
}) {
  const paths = [source, snapshot, recovered].map((filename) => path.resolve(filename));
  if (new Set(paths).size !== paths.length) {
    throw new Error("Source, snapshot, and recovered database paths must be different.");
  }
  if (!fs.existsSync(paths[0])) {
    throw new Error("Source database does not exist.");
  }
  if (fs.existsSync(paths[1]) || fs.existsSync(paths[2])) {
    throw new Error("Snapshot and recovered paths must not already exist.");
  }

  await createVerifiedCopy(paths[0], paths[1]);
  const snapshotCheck = await inspectDatabase(paths[1]);
  let recoveryError = null;
  let pythonDumpError = null;
  let pythonSalvageError = null;
  let recoveryMethod = "unverified";
  let recoveredPath = paths[2];
  if (snapshotCheck.integrityOk) {
    await createVerifiedCopy(paths[1], paths[2]);
    recoveryMethod = "verified-copy";
  } else {
    await fs.promises.mkdir(path.dirname(paths[2]), { recursive: true });
    const output = await fs.promises.open(paths[2], "wx");
    await output.close();
    try {
      await runSqliteRecovery(paths[1], paths[2], sqliteCommand);
      recoveryMethod = "sqlite3-.recover";
    } catch (error) {
      recoveryError = error.message;
      recoveredPath = `${paths[2]}.python-dump`;
      if (fs.existsSync(recoveredPath)) {
        throw new Error(`Python dump output already exists: ${recoveredPath}`);
      }
      const pythonOutput = await fs.promises.open(recoveredPath, "wx");
      await pythonOutput.close();
      try {
        await runPythonDump(paths[1], recoveredPath, pythonCommand);
        recoveryMethod = "python-sql-dump";
      } catch (pythonError) {
        pythonDumpError = pythonError.message;
        recoveredPath = `${paths[2]}.python-row-salvage`;
        if (fs.existsSync(recoveredPath)) {
          throw new Error(`Python row-salvage output already exists: ${recoveredPath}`);
        }
        const salvageOutput = await fs.promises.open(recoveredPath, "wx");
        await salvageOutput.close();
        try {
          await runPythonRowSalvage(paths[1], recoveredPath, pythonCommand);
          recoveryMethod = "python-row-salvage";
        } catch (salvageError) {
          pythonSalvageError = salvageError.message;
        }
      }
    }
  }

  const recoveredCheck = await inspectDatabase(recoveredPath);
  let records = [];
  let recordComparisonError = null;
  try {
    records = await compareRecords(paths[1], recoveredPath);
  } catch (error) {
    recordComparisonError = error.message;
  }

  const tableComparisons = records.filter((record) => record.table);
  return {
    snapshotCheck,
    recoveredCheck,
    recoveryError,
    pythonDumpError,
    pythonSalvageError,
    recoveryMethod,
    recoveredPath,
    records,
    tableDifferences: records.tableDifferences || [],
    indexDifferences: records.indexDifferences || [],
    recordComparisonError,
    verified: recoveredCheck.integrityOk &&
      recoveredCheck.quickCheckOk &&
      recoveredCheck.foreignKeyErrorCount === 0 &&
      !recordComparisonError &&
      tableComparisons.every((record) =>
        !record.error &&
        record.missingRows === 0 &&
        record.extraRows === 0 &&
        record.changedRows === 0 &&
        !record.schemaMismatch &&
        !record.indexMismatch
      ) &&
      records.tableDifferences?.length === 0 &&
      records.indexDifferences?.length === 0
  };
}

module.exports = {
  compareRecords,
  inspectDatabase,
  recoverCache
};

if (require.main === module) {
  const [, , source, snapshot, recovered] = process.argv;
  if (!source || !snapshot || !recovered) {
    console.error("Usage: node scripts/recover-sqlite-cache.cjs <source> <new-snapshot> <new-recovered-copy>");
    process.exitCode = 2;
  } else {
    recoverCache({ source, snapshot, recovered })
      .then((report) => {
        console.log(JSON.stringify(report, null, 2));
        if (!report.verified) {
          process.exitCode = 1;
        }
      })
      .catch((error) => {
        console.error(`SQLite cache recovery stopped: ${error.message}`);
        process.exitCode = 1;
      });
  }
}
