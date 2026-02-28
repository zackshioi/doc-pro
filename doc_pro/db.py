from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

DB_DIR = Path("data")
DB_PATH = DB_DIR / "app.db"


def _connect() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                status TEXT NOT NULL,
                page_count INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS document_chunks (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                page INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                text_raw TEXT NOT NULL,
                text_zh TEXT,
                embedding_status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                FOREIGN KEY (document_id) REFERENCES documents(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_document_chunks_document_id ON document_chunks(document_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_document_chunks_document_page ON document_chunks(document_id, page)"
        )
        _ensure_column(
            conn,
            "documents",
            "translation_status",
            "TEXT NOT NULL DEFAULT 'pending'",
        )
        _ensure_column(
            conn,
            "documents",
            "translation_error_message",
            "TEXT",
        )


def execute(query: str, params: tuple[Any, ...] = ()) -> None:
    with _connect() as conn:
        conn.execute(query, params)
        conn.commit()


def executemany(query: str, params_list: list[tuple[Any, ...]]) -> None:
    with _connect() as conn:
        conn.executemany(query, params_list)
        conn.commit()


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def _ensure_column(
    conn: sqlite3.Connection, table_name: str, column_name: str, column_definition: str
) -> None:
    existing = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    column_names = {row[1] for row in existing}
    if column_name in column_names:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
