#!/usr/bin/env python3
import pathlib
import re
import sqlite3
import sys
import urllib.parse


def quote_identifier(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def open_source(source):
    source_path = pathlib.Path(source).resolve()
    source_uri = "file:" + urllib.parse.quote(str(source_path), safe="/") + "?mode=ro"
    connection = sqlite3.connect(source_uri, uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def open_destination(source, destination):
    source_path = pathlib.Path(source).resolve()
    destination_path = pathlib.Path(destination).resolve()
    if source_path == destination_path:
        raise ValueError("Source and destination paths must differ")
    if not destination_path.is_file() or destination_path.stat().st_size != 0:
        raise ValueError("Destination must be a new empty file")
    return sqlite3.connect(destination_path)


def dump_sql(source, destination):
    source_db = open_source(source)
    target_db = open_destination(source, destination)
    try:
        target_db.execute("PRAGMA foreign_keys=OFF")
        for statement in source_db.iterdump():
            target_db.execute(statement)
        target_db.commit()
        target_db.execute("PRAGMA foreign_keys=ON")
    except Exception:
        target_db.rollback()
        raise
    finally:
        target_db.close()
        source_db.close()


def salvage_rows(source, destination):
    source_db = open_source(source)
    target_db = open_destination(source, destination)
    try:
        target_db.execute("PRAGMA foreign_keys=OFF")
        tables = source_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        target_db.execute("BEGIN")

        for (table_name,) in tables:
            escaped_table = quote_identifier(table_name)
            columns = source_db.execute(
                f"PRAGMA table_info({escaped_table})"
            ).fetchall()
            if not columns:
                raise ValueError(f"Could not read columns for table {table_name}")

            definitions = []
            for column in columns:
                declared_type = column[2] or ""
                if not re.fullmatch(r"[A-Za-z0-9_ ()]*", declared_type):
                    declared_type = ""
                definitions.append(
                    f"{quote_identifier(column[1])} {declared_type}".rstrip()
                )
            target_db.execute(
                f"CREATE TABLE {escaped_table} ({', '.join(definitions)})"
            )

            column_names = ", ".join(
                quote_identifier(column[1])
                for column in columns
            )
            placeholders = ", ".join("?" for _ in columns)
            cursor = source_db.execute(
                f"SELECT {column_names} FROM {escaped_table}"
            )
            insert = (
                f"INSERT INTO {escaped_table} ({column_names}) "
                f'VALUES ({placeholders})'
            )
            while True:
                rows = cursor.fetchmany(500)
                if not rows:
                    break
                target_db.executemany(insert, rows)

        target_db.commit()
    except Exception:
        target_db.rollback()
        raise
    finally:
        target_db.close()
        source_db.close()


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print(
            "Usage: sqlite-cache-dump.py [--salvage-rows] "
            "<read-only-source-copy> <new-empty-destination>",
            file=sys.stderr
        )
        sys.exit(2)
    try:
        if len(sys.argv) == 4 and sys.argv[1] == "--salvage-rows":
            salvage_rows(sys.argv[2], sys.argv[3])
        elif len(sys.argv) == 3:
            dump_sql(sys.argv[1], sys.argv[2])
        else:
            raise ValueError("Unknown recovery mode")
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)
