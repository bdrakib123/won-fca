import { UniqueConstraintError } from "sequelize";

/**
 * Shared helpers for database layer (userData, threadData).
 * Keeps validation and payload normalization in one place.
 */

export const DB_NOT_INIT = "Database not initialized";
const sqliteWriteQueues = new WeakMap<object, Promise<void>>();

export async function serializeSqliteWrite<T>(Model: Loose, operation: () => Promise<T>): Promise<T> {
  const sequelize = Model.sequelize as { getDialect?: () => string } | undefined;
  const queueKey = sequelize?.getDialect?.() === "sqlite" ? sequelize : null;
  if (!queueKey) return operation();

  const previous = sqliteWriteQueues.get(queueKey);
  let release: (() => void) | undefined;
  const queued = new Promise<void>((resolve) => {
    release = resolve;
  });
  const tail = previous ? previous.catch(() => {}).then(() => queued) : queued;
  sqliteWriteQueues.set(queueKey, tail);
  await previous?.catch(() => {});

  try {
    return await operation();
  } finally {
    release?.();
    if (sqliteWriteQueues.get(queueKey) === tail) {
      sqliteWriteQueues.delete(queueKey);
    }
  }
}

export function validateId(value: Loose, fieldName = "id"): string {
  if (value == null) {
    throw new Error(`${fieldName} is required and cannot be undefined`);
  }
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error(`Invalid ${fieldName}: must be a string or number`);
  }
  return String(value);
}

export function validateData(data: Loose): void {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("Invalid data: must be a non-empty object");
  }
}

/** keys: "userID" | ["userID","data"] | null */
export function normalizeAttributes(keys: Loose): string[] | undefined {
  if (keys == null) return undefined;
  return typeof keys === "string" ? [keys] : Array.isArray(keys) ? keys : undefined;
}

/** Normalize payload: accept either { data } or raw object. */
export function normalizePayload(data: Loose, key = "data"): Record<string, Loose> {
  return Object.prototype.hasOwnProperty.call(data as object, key)
    ? (data as Record<string, Loose>)
    : { [key]: data };
}

export function wrapError(message: string, cause: Loose): Error {
  const c = cause as { message?: string } | undefined;
  const causeDetails = cause as { code?: string; original?: { code?: string } } | undefined;
  const causeCode = causeDetails?.code || causeDetails?.original?.code;
  const causeMessage = c && c.message ? c.message : String(cause);
  const detail = causeCode && !causeMessage.includes(causeCode)
    ? `${causeCode}: ${causeMessage}`
    : causeMessage;
  const wrapped = new Error(`${message}: ${detail}`);
  if (cause instanceof Error) {
    (wrapped as Error & { cause?: unknown }).cause = cause;
    const original = cause as Error & { code?: string; original?: unknown };
    if (original.code) {
      (wrapped as Error & { code?: string }).code = original.code;
    }
    if (original.original) {
      (wrapped as Error & { original?: unknown }).original = original.original;
    }
  }
  return wrapped;
}

export async function findOrCreateRecord(
  Model: Loose,
  idField: string,
  id: string,
  defaults: Record<string, Loose>,
  updateExisting?: Record<string, Loose>
): Promise<{ record: Loose; created: boolean }> {
  return serializeSqliteWrite(Model, async () => {
    const where = { [idField]: id };
    let record: Loose;
    let created: boolean;
    try {
      [record, created] = await Model.findOrCreate({ where, defaults });
    } catch (error) {
      const isUniqueConflict =
        error instanceof UniqueConstraintError ||
        (error as { name?: string })?.name === "SequelizeUniqueConstraintError";
      if (!isUniqueConflict) {
        throw error;
      }

      record = await Model.findOne({ where });
      if (!record) {
        throw error;
      }
      created = false;
    }

    if (!created && updateExisting) {
      await record.update(updateExisting);
    }
    return { record, created };
  });
}
