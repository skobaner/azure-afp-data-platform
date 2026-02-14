import csv
import io
import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func

app = func.FunctionApp()

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


def get_sql_connection():
  import pymssql

  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
  )


def ensure_table_exists() -> None:
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(SCHEMA_DDL)
    conn.commit()


def normalize_row(row: dict) -> dict:
  normalized = {}
  for key, value in row.items():
    clean_key = key.strip() if key else "column"
    if isinstance(value, str):
      normalized[clean_key] = value.strip().upper()
    else:
      normalized[clean_key] = value
  normalized["processed_utc"] = datetime.now(timezone.utc).isoformat()
  return normalized


@app.blob_trigger(
  arg_name="input_blob",
  path="input-data/{name}",
  connection="BLOB_STORAGE_CONNECTION_STRING",
)
def process_csv(input_blob: func.InputStream):
  blob_name = input_blob.name
  logging.info("Processing blob: %s", blob_name)

  content = input_blob.read().decode("utf-8-sig")
  reader = csv.DictReader(io.StringIO(content))

  ensure_table_exists()

  insert_sql = """
    MERGE dbo.processed_rows AS target
    USING (SELECT %s AS source_blob, %s AS row_number) AS src
    ON target.source_blob = src.source_blob AND target.row_number = src.row_number
    WHEN MATCHED THEN
      UPDATE SET
        raw_payload = %s,
        normalized_payload = %s,
        processing_status = %s,
        error_message = %s,
        processed_at = %s
    WHEN NOT MATCHED THEN
      INSERT (source_blob, row_number, raw_payload, normalized_payload, processing_status, error_message, processed_at)
      VALUES (%s, %s, %s, %s, %s, %s, %s);
  """

  inserted = 0
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for idx, row in enumerate(reader, start=1):
        raw_payload = json.dumps(row)
        normalized_payload = None
        status = "processed"
        error_message = None
        processed_at = datetime.now(timezone.utc)

        try:
          normalized = normalize_row(row)
          normalized_payload = json.dumps(normalized)
        except Exception as exc:
          status = "failed"
          error_message = str(exc)[:1000]
          logging.exception("Row normalization failed for blob=%s row=%s", blob_name, idx)

        cursor.execute(
          insert_sql,
          (
            blob_name,
            idx,
            raw_payload,
            normalized_payload,
            status,
            error_message,
            processed_at,
            blob_name,
            idx,
            raw_payload,
            normalized_payload,
            status,
            error_message,
            processed_at,
          ),
        )
        inserted += 1
    conn.commit()

  logging.info("Inserted %s rows from blob %s", inserted, blob_name)
