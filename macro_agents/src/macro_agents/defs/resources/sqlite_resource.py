"""SQLite resource for accessing telemetry and user data."""

import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import dagster as dg
import polars as pl
from pydantic import Field

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Reject any string that isn't a plain SQL identifier.

    Why: identifiers can't be parameterized, so callers that interpolate
    table or column names need a strict allowlist to prevent SQL injection.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {kind}: {name!r}")
    return name


class SQLiteResource(dg.ConfigurableResource):
    """Dagster resource for managing SQLite database connections."""

    database_path: str = Field(
        description="Path to the SQLite database file",
        default="~/.economic-data/users.db",
    )

    def _find_repo_root(self) -> Path:
        """Find the repository root by searching upward for .git directory or makefile."""
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (
                (parent / ".git").exists()
                or (parent / "makefile").exists()
                or (parent / "Makefile").exists()
            ):
                return parent
        raise FileNotFoundError(
            f"Could not find repository root. Searched from {current} up to {current.parents[-1]}"
        )

    def _resolve_path(self) -> Path:
        """Resolve the database path, handling environment variables and tildes."""
        path = Path(self.database_path).expanduser()
        if not path.is_absolute():
            repo_root = self._find_repo_root()
            path = repo_root / path
        return path.resolve()

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Create a database connection context manager."""
        db_path = self._resolve_path()

        # Ensure parent directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = None
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            # Restrict the DB file to the owning user. The default umask can
            # leave 0644 on shared hosts, which would expose telemetry/user
            # rows to any local user. chmod is a no-op on Windows.
            try:
                os.chmod(db_path, 0o600)
            except OSError:
                pass
            yield conn
        finally:
            if conn:
                conn.close()

    def execute_query(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[sqlite3.Row]:
        """Execute a SQL query and return results as list of Row objects."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()

    def read_table_as_polars(
        self,
        table_name: str,
        where: tuple[str, tuple[Any, ...]] | None = None,
        order_by: tuple[str, str] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Read a SQLite table into a Polars DataFrame.

        Args:
            table_name: Name of the table to read (validated as an identifier).
            where: Optional ``(sql, params)`` pair. ``sql`` is a WHERE clause
                body using ``?`` placeholders; ``params`` are bound safely.
                Example: ``("id > ?", (last_id,))``.
            order_by: Optional ``(column, direction)`` tuple. Column is validated
                as an identifier; direction must be ``"ASC"`` or ``"DESC"``.
            limit: Optional LIMIT value (must be a positive int).
        """
        _validate_identifier(table_name, "table name")
        query_parts = [f"SELECT * FROM {table_name}"]
        params: tuple[Any, ...] = ()

        if where is not None:
            where_sql, where_params = where
            query_parts.append(f"WHERE {where_sql}")
            params = params + tuple(where_params)

        if order_by is not None:
            column, direction = order_by
            _validate_identifier(column, "order_by column")
            direction_upper = direction.upper()
            if direction_upper not in {"ASC", "DESC"}:
                raise ValueError(f"Invalid order_by direction: {direction!r}")
            query_parts.append(f"ORDER BY {column} {direction_upper}")

        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError(f"Invalid limit: {limit!r}")
            query_parts.append(f"LIMIT {limit}")

        query = " ".join(query_parts)

        with self.get_connection() as conn:
            df = pl.read_database(query, conn, execute_options={"parameters": params})

        return df

    def get_last_replicated_id(
        self, tracking_table: str, source_table: str
    ) -> int | None:
        """Get the last replicated ID from a tracking table."""
        _validate_identifier(tracking_table, "tracking_table")
        query = f"""
            SELECT last_replicated_id
            FROM {tracking_table}
            WHERE source_table = ?
            ORDER BY updated_at DESC
            LIMIT 1
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (source_table,))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_table_info(self, table_name: str) -> list[dict[str, Any]]:
        """Get schema information for a table."""
        _validate_identifier(table_name, "table name")
        query = f"PRAGMA table_info({table_name})"
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = cursor.fetchall()
            return [
                {
                    "cid": col[0],
                    "name": col[1],
                    "type": col[2],
                    "notnull": col[3],
                    "default_value": col[4],
                    "pk": col[5],
                }
                for col in columns
            ]

    def get_row_count(
        self,
        table_name: str,
        where: tuple[str, tuple[Any, ...]] | None = None,
    ) -> int:
        """Get the number of rows in a table.

        ``where`` is an optional ``(sql, params)`` pair with ``?`` placeholders.
        """
        _validate_identifier(table_name, "table name")
        query = f"SELECT COUNT(*) FROM {table_name}"
        params: tuple[Any, ...] = ()
        if where is not None:
            where_sql, where_params = where
            query += f" WHERE {where_sql}"
            params = tuple(where_params)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else 0

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (table_name,))
            return cursor.fetchone() is not None


# Create the resource instance
environment = os.getenv("ENVIRONMENT", "dev")
sqlite_db_path = os.getenv("SQLITE_DB_PATH", "~/.economic-data/users.db")

sqlite_resource = SQLiteResource(database_path=sqlite_db_path)
