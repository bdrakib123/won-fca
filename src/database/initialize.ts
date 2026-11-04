import type { Sequelize } from "sequelize";

export function createCacheInitializer(
  sequelize: Sequelize,
  syncModels: () => Promise<void>
): () => Promise<void> {
  let initialization: Promise<void> | null = null;

  return () => {
    if (!initialization) {
      initialization = (async () => {
        await sequelize.authenticate();
        const [rows] = await sequelize.query("PRAGMA integrity_check");
        const results = rows as Array<Record<string, unknown>>;
        const integrity = results.map((row) => String(Object.values(row)[0] ?? ""));
        if (integrity.length !== 1 || integrity[0] !== "ok") {
          throw new Error(`SQLite cache integrity check failed: ${integrity.join("; ") || "no result"}`);
        }

        await sequelize.query("PRAGMA busy_timeout = 10000");
        await sequelize.query("PRAGMA journal_mode = WAL");
        await syncModels();
      })().catch((error: unknown) => {
        initialization = null;
        throw error;
      });
    }
    return initialization;
  };
}
