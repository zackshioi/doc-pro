from __future__ import annotations

import shutil
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from doc_pro import db
from doc_pro.worker import enqueue_document, enqueue_translation, start_worker

UPLOAD_DIR = Path("data/uploads")
ALLOWED_EXTENSIONS = {".pdf"}


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    db.init_db()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    start_worker()
    yield


app = FastAPI(title="doc-pro API", version="0.1.0", lifespan=lifespan)


@app.post("/api/documents")
async def create_document(
    file: UploadFile = File(...),
    user_id: str = Form(default="default-user"),
) -> dict[str, str]:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")

    document_id = str(uuid.uuid4())
    target_path = UPLOAD_DIR / f"{document_id}.pdf"
    with target_path.open("wb") as out:
        shutil.copyfileobj(file.file, out)

    now = _now_iso()
    db.execute(
        """
        INSERT INTO documents (id, user_id, filename, file_path, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document_id,
            user_id,
            file.filename or "uploaded.pdf",
            str(target_path),
            "uploaded",
            now,
            now,
        ),
    )
    enqueue_document(document_id)
    return {"document_id": document_id}


@app.get("/api/documents/{document_id}")
def get_document(document_id: str) -> dict[str, Any]:
    doc = db.fetch_one(
        """
        SELECT
            id,
            user_id,
            filename,
            status,
            page_count,
            error_message,
            translation_status,
            translation_error_message,
            created_at,
            updated_at
        FROM documents
        WHERE id = ?
        """,
        (document_id,),
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    return doc


@app.post("/api/documents/{document_id}/translate")
def translate_document(
    document_id: str,
    page: int | None = None,
) -> dict[str, Any]:
    doc = db.fetch_one("SELECT id, status FROM documents WHERE id = ?", (document_id,))
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found.")
    if doc["status"] != "ready":
        raise HTTPException(status_code=409, detail="Document is not ready for translation.")
    enqueue_translation(document_id, page)
    return {"document_id": document_id, "page": page, "translation_job": "queued"}


@app.get("/api/documents/{document_id}/chunks")
def get_document_chunks(
    document_id: str,
    page: int | None = None,
    lang: str = "raw",
) -> dict[str, Any]:
    exists = db.fetch_one("SELECT id FROM documents WHERE id = ?", (document_id,))
    if not exists:
        raise HTTPException(status_code=404, detail="Document not found.")
    if lang not in {"raw", "zh"}:
        raise HTTPException(status_code=400, detail="lang must be 'raw' or 'zh'.")

    if page is None:
        chunks = db.fetch_all(
            """
            SELECT
                id, page, chunk_index, text_raw, text_zh, embedding_status, created_at
            FROM document_chunks
            WHERE document_id = ?
            ORDER BY page ASC, chunk_index ASC
            """,
            (document_id,),
        )
    else:
        chunks = db.fetch_all(
            """
            SELECT
                id, page, chunk_index, text_raw, text_zh, embedding_status, created_at
            FROM document_chunks
            WHERE document_id = ? AND page = ?
            ORDER BY chunk_index ASC
            """,
            (document_id, page),
        )

    if lang == "zh":
        transformed_chunks: list[dict[str, Any]] = []
        for chunk in chunks:
            is_cached = bool(chunk["text_zh"])
            translated_text = chunk["text_zh"] if is_cached else chunk["text_raw"]
            transformed_chunks.append(
                {
                    "id": chunk["id"],
                    "page": chunk["page"],
                    "chunk_index": chunk["chunk_index"],
                    "text": translated_text,
                    "text_raw": chunk["text_raw"],
                    "text_zh": chunk["text_zh"],
                    "is_cached_translation": is_cached,
                    "embedding_status": chunk["embedding_status"],
                    "created_at": chunk["created_at"],
                }
            )
        chunks = transformed_chunks

    cache_count = sum(1 for c in chunks if c.get("text_zh"))
    return {
        "document_id": document_id,
        "count": len(chunks),
        "lang": lang,
        "cached_translation_count": cache_count,
        "chunks": chunks,
    }


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()
