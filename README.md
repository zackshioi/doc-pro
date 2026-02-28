# doc-pro

MVP feature #1: PDF upload, async parsing, and chunk storage.

## Run

```bash
uv run uvicorn doc_pro.main:app --reload
```

## API

### 1) Upload PDF

```bash
curl -X POST http://127.0.0.1:8000/api/documents \
  -F "file=@/absolute/path/to/file.pdf" \
  -F "user_id=demo-user"
```

Response:

```json
{ "document_id": "..." }
```

### 2) Check document status

```bash
curl http://127.0.0.1:8000/api/documents/{document_id}
```

### 3) Read parsed chunks

```bash
curl http://127.0.0.1:8000/api/documents/{document_id}/chunks
```

Optional page filter:

```bash
curl "http://127.0.0.1:8000/api/documents/{document_id}/chunks?page=1"
```

Read translated cache (fallback to raw text if not translated yet):

```bash
curl "http://127.0.0.1:8000/api/documents/{document_id}/chunks?page=1&lang=zh"
```

### 4) Trigger translation job

Translate full document:

```bash
curl -X POST "http://127.0.0.1:8000/api/documents/{document_id}/translate"
```

Translate a single page:

```bash
curl "http://127.0.0.1:8000/api/documents/{document_id}/chunks?page=1&lang=zh"
```
