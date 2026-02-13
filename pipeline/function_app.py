import csv
import io
import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import pymssql

app = func.FunctionApp()


def _required_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return value


def get_sql_connection():
  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
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
    INSERT INTO dbo.processed_rows (source_blob, row_number, normalized_payload, processed_at)
    VALUES (%s, %s, %s, %s)
  """

  inserted = 0
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for idx, row in enumerate(reader, start=1):
        normalized = normalize_row(row)
        cursor.execute(
          insert_sql,
          (
            blob_name,
            idx,
            json.dumps(normalized),
            datetime.now(timezone.utc),
          ),
        )
        inserted += 1
    conn.commit()

  logging.info("Inserted %s rows from blob %s", inserted, blob_name)
