import csv
import io
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

<<<<<<< ours
<<<<<<< ours
<<<<<<< ours
from azure.core.exceptions import AzureError, ResourceNotFoundError
=======
import pymssql
>>>>>>> theirs
=======
import pymssql
>>>>>>> theirs
=======
import pymssql
>>>>>>> theirs
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, File, HTTPException, Query, UploadFile

app = FastAPI(title="CSV Data Platform API", version="1.0.0")

SCHEMA_DDL = """
IF OBJECT_ID(N'dbo.processed_rows', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.processed_rows (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_blob NVARCHAR(512) NOT NULL,
    row_number INT NOT NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    normalized_payload NVARCHAR(MAX) NULL,
    processing_status NVARCHAR(32) NOT NULL CONSTRAINT DF_processed_rows_status DEFAULT ('processed'),
    error_message NVARCHAR(1000) NULL,
    processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_processed_rows_processed_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_processed_rows_source_row UNIQUE (source_blob, row_number)
  );
END;

IF COL_LENGTH('dbo.processed_rows', 'raw_payload') IS NULL
  ALTER TABLE dbo.processed_rows ADD raw_payload NVARCHAR(MAX) NOT NULL CONSTRAINT DF_processed_rows_raw_payload DEFAULT ('{}');

IF COL_LENGTH('dbo.processed_rows', 'processing_status') IS NULL
  ALTER TABLE dbo.processed_rows ADD processing_status NVARCHAR(32) NOT NULL CONSTRAINT DF_processed_rows_status_backfill DEFAULT ('processed');

IF COL_LENGTH('dbo.processed_rows', 'error_message') IS NULL
  ALTER TABLE dbo.processed_rows ADD error_message NVARCHAR(1000) NULL;

IF COL_LENGTH('dbo.processed_rows', 'processed_at') IS NULL
  ALTER TABLE dbo.processed_rows ADD processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_processed_rows_processed_at_backfill DEFAULT SYSUTCDATETIME();
"""


def _required_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return value


def get_blob_client() -> BlobServiceClient:
  connection_string = _required_env("BLOB_CONNECTION_STRING")
  return BlobServiceClient.from_connection_string(connection_string)


def get_sql_connection():
  import pymssql

  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
    as_dict=True,
  )


def ensure_table_exists() -> None:
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(SCHEMA_DDL)
    conn.commit()


@app.get("/health")
def health():
  return {"status": "ok", "timestamp_utc": datetime.now(timezone.utc).isoformat()}


@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
  if not file.filename.lower().endswith(".csv"):
    raise HTTPException(status_code=400, detail="Only .csv files are supported")

  content = await file.read()
  if not content:
    raise HTTPException(status_code=400, detail="Uploaded file is empty")

  try:
    decoded = content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(decoded))
    first_row = next(reader, None)
    if first_row is None:
      raise HTTPException(status_code=400, detail="CSV has no rows")
  except UnicodeDecodeError as exc:
    raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

  blob_service = get_blob_client()
  container_name = _required_env("BLOB_CONTAINER_NAME")
  blob_name = f"raw/{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid4()}-{file.filename}"

  container_client = blob_service.get_container_client(container_name)
  container_client.upload_blob(name=blob_name, data=content, overwrite=False)

  return {
    "message": "File uploaded successfully",
    "container": container_name,
    "blob": blob_name,
  }


@app.get("/records")
def get_records(
  limit: int = Query(default=100, ge=1, le=1000),
  status: str | None = Query(default=None, description="Optional status filter: processed or failed"),
):
  ensure_table_exists()

  base_query = """
    SELECT TOP (%s)
      id,
      source_blob,
      row_number,
      raw_payload,
      normalized_payload,
      processing_status,
      error_message,
      processed_at
    FROM dbo.processed_rows
  """

  where_clause = ""
  params: tuple = (limit,)
  if status:
    where_clause = " WHERE processing_status = %s"
    params = (limit, status.strip().lower())

  query = f"{base_query}{where_clause} ORDER BY id DESC"

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, params)
      rows = cursor.fetchall()

  result = []
  for row in rows:
    payload = row.get("normalized_payload") or "{}"
    raw_payload = row.get("raw_payload") or "{}"
    try:
      payload_json = json.loads(payload)
    except json.JSONDecodeError:
      payload_json = {"raw": payload}
    try:
      raw_payload_json = json.loads(raw_payload)
    except json.JSONDecodeError:
      raw_payload_json = {"raw": raw_payload}

    processed_at = row["processed_at"]
    result.append(
      {
        "id": row["id"],
        "source_blob": row["source_blob"],
        "row_number": row["row_number"],
        "status": row.get("processing_status"),
        "error_message": row.get("error_message"),
        "raw_payload": raw_payload_json,
        "payload": payload_json,
        "processed_at": processed_at.isoformat() if hasattr(processed_at, "isoformat") else str(processed_at),
      }
    )

  return {"count": len(result), "records": result}
