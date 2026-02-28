from __future__ import annotations

import queue
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pypdf import PdfReader

from doc_pro.ai import translate_text_with_gen_ai
from doc_pro import db

Job = tuple[str, str, int | None]
_job_queue: "queue.Queue[Job]" = queue.Queue()
_worker_started = False
_worker_lock = threading.Lock()

CHUNK_SIZE = 800
CHUNK_OVERLAP = 160


def enqueue_document(document_id: str) -> None:
    _job_queue.put(("parse", document_id, None))


def enqueue_translation(document_id: str, page: int | None = None) -> None:
    _job_queue.put(("translate", document_id, page))


def start_worker() -> None:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        thread = threading.Thread(target=_run_worker, daemon=True)
        thread.start()
        _worker_started = True


def _run_worker() -> None:
    while True:
        job_type, document_id, page = _job_queue.get()
        try:
            if job_type == "parse":
                _process_document(document_id)
            elif job_type == "translate":
                _process_translation(document_id, page)
        finally:
            _job_queue.task_done()


def _process_document(document_id: str) -> None:
    now = _now_iso()
    db.execute(
        "UPDATE documents SET status = ?, updated_at = ?, error_message = NULL WHERE id = ?",
        ("processing", now, document_id),
    )

    document = db.fetch_one(
        "SELECT id, file_path FROM documents WHERE id = ?",
        (document_id,),
    )
    if not document:
        return

    try:
        pages = _extract_pdf_pages(Path(document["file_path"]))
        chunks = _build_chunks(document_id, pages)

        db.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
        if chunks:
            db.executemany(
                """
                INSERT INTO document_chunks (
                    id, document_id, page, chunk_index, text_raw, text_zh, embedding_status, created_at
                ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', ?)
                """,
                chunks,
            )

        db.execute(
            """
            UPDATE documents
            SET status = ?, page_count = ?, translation_status = ?, translation_error_message = ?, updated_at = ?
            WHERE id = ?
            """,
            ("ready", len(pages), "pending", None, _now_iso(), document_id),
        )
    except Exception as exc:  # noqa: BLE001
        db.execute(
            "UPDATE documents SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
            ("failed", str(exc), _now_iso(), document_id),
        )


def _process_translation(document_id: str, page: int | None) -> None:
    document = db.fetch_one(
        "SELECT id, status FROM documents WHERE id = ?",
        (document_id,),
    )
    if not document:
        return
    if document["status"] != "ready":
        db.execute(
            """
            UPDATE documents
            SET translation_status = ?, translation_error_message = ?, updated_at = ?
            WHERE id = ?
            """,
            ("failed", "Document is not ready for translation.", _now_iso(), document_id),
        )
        return

    db.execute(
        """
        UPDATE documents
        SET translation_status = ?, translation_error_message = ?, updated_at = ?
        WHERE id = ?
        """,
        ("processing", None, _now_iso(), document_id),
    )

    if page is None:
        chunks = db.fetch_all(
            """
            SELECT id, text_raw
            FROM document_chunks
            WHERE document_id = ? AND (text_zh IS NULL OR text_zh = '')
            ORDER BY page, chunk_index
            """,
            (document_id,),
        )
    else:
        chunks = db.fetch_all(
            """
            SELECT id, text_raw
            FROM document_chunks
            WHERE document_id = ? AND page = ? AND (text_zh IS NULL OR text_zh = '')
            ORDER BY chunk_index
            """,
            (document_id, page),
        )

    try:
        for chunk in chunks:
            translated = translate_text_with_gen_ai(chunk["text_raw"])
            db.execute(
                "UPDATE document_chunks SET text_zh = ? WHERE id = ?",
                (translated, chunk["id"]),
            )
    except Exception as exc:  # noqa: BLE001
        db.execute(
            """
            UPDATE documents
            SET translation_status = ?, translation_error_message = ?, updated_at = ?
            WHERE id = ?
            """,
            ("failed", str(exc), _now_iso(), document_id),
        )
        return

    _refresh_translation_status(document_id)


def _extract_pdf_pages(path: Path) -> list[str]:
    reader = PdfReader(str(path))
    pages: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(_normalize_text(text))
    return pages


def _build_chunks(document_id: str, pages: list[str]) -> list[tuple[str, str, int, int, str, str]]:
    rows: list[tuple[str, str, int, int, str, str]] = []
    for page_num, text in enumerate(pages, start=1):
        chunks = _split_text(text)
        for idx, chunk_text in enumerate(chunks):
            rows.append(
                (
                    str(uuid.uuid4()),
                    document_id,
                    page_num,
                    idx,
                    chunk_text,
                    _now_iso(),
                )
            )
    return rows


def _split_text(text: str) -> list[str]:
    if not text.strip():
        return []

    chunks: list[str] = []
    start = 0
    step = CHUNK_SIZE - CHUNK_OVERLAP
    while start < len(text):
        end = min(start + CHUNK_SIZE, len(text))
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == len(text):
            break
        start += step
    return chunks


def _normalize_text(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [line for line in lines if line]
    return "\n".join(non_empty)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _refresh_translation_status(document_id: str) -> None:
    untranslated = db.fetch_one(
        """
        SELECT COUNT(1) AS cnt
        FROM document_chunks
        WHERE document_id = ? AND (text_zh IS NULL OR text_zh = '')
        """,
        (document_id,),
    )
    remaining = untranslated["cnt"] if untranslated else 0
    next_status = "ready" if remaining == 0 else "pending"
    db.execute(
        """
        UPDATE documents
        SET translation_status = ?, translation_error_message = ?, updated_at = ?
        WHERE id = ?
        """,
        (next_status, None, _now_iso(), document_id),
    )
