# SQLite cache recovery

The cache is never repaired or recreated automatically. Keep the bot stopped
while investigating so no process is writing the database. Preserve the
original database and any `-wal`/`-shm` sidecars exactly as found.

Run the recovery utility with three new, distinct output paths on a filesystem
with enough free space:

```sh
node scripts/recover-sqlite-cache.cjs \
  /path/to/Fca_Database/database.sqlite \
  /safe/recovery/cache-snapshot.sqlite \
  /safe/recovery/cache-recovered.sqlite
```

The utility uses SQLite's online backup API to create the snapshot with the
source opened read-only. If the snapshot is healthy, it makes a second clean
copy. If it is corrupt, it first asks the installed `sqlite3` CLI to reconstruct
into the separate recovered copy. If that is unavailable or fails, it attempts
a Python standard-library SQL dump from the read-only snapshot into another new
copy. A failed candidate is retained and a later method never overwrites it.

The report includes integrity/quick checks, foreign-key errors, table counts,
record-by-record differences for readable rows, and explicit-index differences.
A corrupt source snapshot may still expose readable table rows; a copy is only
marked verified if the recovered database passes both integrity checks and all
readable records, tables, indexes, and foreign-key checks match. If table data
cannot be read or compared, the report remains unverified. These checks cannot
prove that a corrupt source had no already-unreadable records.

SQLite `.recover` availability depends on how the `sqlite3` CLI was built. If the
command fails, the utility next attempts a Python SQL dump. If SQL reconstruction
fails (for example, because readable rows violate a unique constraint), it tries
a last-resort row salvage into a separate database with the readable tables and
values but without the original constraints or indexes. Row salvage is not a
production-ready cache and will be reported as unverified. It exists only to
preserve and compare readable rows; duplicate or damaged records must be
reviewed and reconciled manually. Any failed candidate is retained and never
reused as the destination of a later method.

If either integrity check or the complete schema/index/record comparison fails,
preserve all files and use a specialist recovery process; do not replace
production data. Before any manual replacement review, stop every process that
could write the cache, preserve the original database and sidecars, record the
original hash, and keep a separate verified copy. Review the report and compare
critical user/thread records and relationships. Require explicit human approval,
then replace production using a separately planned maintenance procedure with a
rollback copy; this utility has no production replacement operation.
