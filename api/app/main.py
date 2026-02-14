import csv
import io
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pymssql
from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, File, HTTPException, Query, UploadFile

app = FastAPI(title="CSV Data Platform API", version="1.0.0")


def _required_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return value


def get_blob_client() -> BlobServiceClient:
  connection_string = _required_env("BLOB_CONNECTION_STRING")
  return BlobServiceClient.from_connection_string(connection_string)


def ensure_blob_container_exists(blob_service: BlobServiceClient, container_name: str) -> None:
  container_client = blob_service.get_container_client(container_name)
  try:
    if not container_client.exists():
      container_client.create_container()
  except ResourceNotFoundError:
    container_client.create_container()
  except AzureError as exc:
    raise HTTPException(status_code=500, detail="Unable to access blob storage container") from exc


def get_sql_connection():
  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
    as_dict=True,
  )


def ensure_table_exists() -> None:
  ddl = """
  IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'dbo.processed_rows') AND type in (N'U')
  )
  BEGIN
    CREATE TABLE dbo.processed_rows (
      id INT IDENTITY(1,1) PRIMARY KEY,
      source_blob NVARCHAR(260) NOT NULL,
      row_number INT NOT NULL,
      normalized_payload NVARCHAR(MAX) NOT NULL,
      processed_at DATETIME2 NOT NULL
    )
  END
  """

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(ddl)
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

  ensure_blob_container_exists(blob_service, container_name)
  container_client = blob_service.get_container_client(container_name)
  try:
    container_client.upload_blob(name=blob_name, data=content, overwrite=False)
  except AzureError as exc:
    raise HTTPException(status_code=500, detail="Failed to upload file to blob storage") from exc

  return {
    "message": "File uploaded successfully",
    "container": container_name,
    "blob": blob_name,
  }


@app.get("/records")
def get_records(limit: int = Query(default=100, ge=1, le=1000)):
  ensure_table_exists()

  query = """
    SELECT TOP (%s) id, source_blob, row_number, normalized_payload, processed_at
    FROM dbo.processed_rows
    ORDER BY id DESC
  """

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, (limit,))
      rows = cursor.fetchall()

  result = []
  for row in rows:
    payload = row["normalized_payload"]
    try:
      payload_json = json.loads(payload)
    except json.JSONDecodeError:
      payload_json = {"raw": payload}

    processed_at = row["processed_at"]
    result.append(
      {
        "id": row["id"],
        "source_blob": row["source_blob"],
        "row_number": row["row_number"],
        "payload": payload_json,
        "processed_at": processed_at.isoformat() if hasattr(processed_at, "isoformat") else str(processed_at),
      }
    )

  return {"count": len(result), "records": result}
