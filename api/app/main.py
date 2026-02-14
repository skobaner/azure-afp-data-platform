import csv
import io
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from fastapi import FastAPI, File, HTTPException, Query, UploadFile

app = FastAPI(title="AFP Data Platform API", version="1.1.0")

SCHEMA_DDL = """
IF OBJECT_ID(N'dbo.po_limits', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.po_limits (
    po NVARCHAR(100) NOT NULL PRIMARY KEY,
    po_value DECIMAL(18,2) NOT NULL,
    total_claimed DECIMAL(18,2) NOT NULL CONSTRAINT DF_po_limits_total_claimed DEFAULT (0),
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_po_limits_updated_at DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID(N'dbo.category_limits', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.category_limits (
    category_id NVARCHAR(100) NOT NULL PRIMARY KEY,
    category_limit DECIMAL(18,2) NOT NULL,
    total_claimed DECIMAL(18,2) NOT NULL CONSTRAINT DF_category_limits_total_claimed DEFAULT (0),
    updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_category_limits_updated_at DEFAULT SYSUTCDATETIME()
  );
END;

IF OBJECT_ID(N'dbo.application_payments_processed', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.application_payments_processed (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_blob NVARCHAR(512) NOT NULL,
    row_number INT NOT NULL,
    project NVARCHAR(255) NOT NULL,
    cost_category NVARCHAR(100) NOT NULL,
    po NVARCHAR(100) NOT NULL,
    cost_amount DECIMAL(18,2) NOT NULL,
    certification NVARCHAR(32) NOT NULL,
    certified_cost DECIMAL(18,2) NOT NULL,
    po_remaining_before DECIMAL(18,2) NOT NULL,
    category_remaining_before DECIMAL(18,2) NOT NULL,
    error_message NVARCHAR(1000) NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    processed_at DATETIME2(3) NOT NULL CONSTRAINT DF_app_payments_processed_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_app_payments_source_row UNIQUE (source_blob, row_number)
  );
END;
"""


def _required_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing required environment variable: {name}")
  return value


def _to_decimal(value: str, field_name: str) -> Decimal:
  try:
    return Decimal(str(value).strip())
  except (InvalidOperation, ValueError, TypeError):
    raise HTTPException(status_code=400, detail=f"Invalid decimal value for {field_name}: {value}")


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
  import pymssql

  return pymssql.connect(
    server=_required_env("SQL_HOST"),
    user=_required_env("SQL_USER"),
    password=_required_env("SQL_PASSWORD"),
    database=_required_env("SQL_DATABASE"),
    as_dict=True,
  )


def ensure_schema_exists() -> None:
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

  ensure_blob_container_exists(blob_service, container_name)
  container_client = blob_service.get_container_client(container_name)
  try:
    container_client.upload_blob(name=blob_name, data=content, overwrite=False)
  except AzureError as exc:
    raise HTTPException(status_code=500, detail="Failed to upload file to blob storage") from exc

  return {"message": "File uploaded successfully", "container": container_name, "blob": blob_name}


@app.post("/seed/po-limits")
async def seed_po_limits(file: UploadFile = File(...)):
  ensure_schema_exists()
  content = await file.read()
  if not content:
    raise HTTPException(status_code=400, detail="Uploaded file is empty")

  decoded = content.decode("utf-8-sig")
  reader = csv.DictReader(io.StringIO(decoded))
  expected = {"PO", "PO_value", "Total_Claimed"}
  if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
    raise HTTPException(status_code=400, detail="PO limits CSV must include PO, PO_value, Total_Claimed")

  merge_sql = """
    MERGE dbo.po_limits AS target
    USING (SELECT %s AS po) AS src
    ON target.po = src.po
    WHEN MATCHED THEN
      UPDATE SET po_value = %s, total_claimed = %s, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (po, po_value, total_claimed, updated_at)
      VALUES (%s, %s, %s, SYSUTCDATETIME());
  """

  count = 0
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in reader:
        po = (row.get("PO") or "").strip()
        if not po:
          continue
        po_value = _to_decimal(row.get("PO_value"), "PO_value")
        total_claimed = _to_decimal(row.get("Total_Claimed"), "Total_Claimed")
        cursor.execute(merge_sql, (po, float(po_value), float(total_claimed), po, float(po_value), float(total_claimed)))
        count += 1
    conn.commit()

  return {"message": "PO limits seeded", "rows": count}


@app.post("/seed/category-limits")
async def seed_category_limits(file: UploadFile = File(...)):
  ensure_schema_exists()
  content = await file.read()
  if not content:
    raise HTTPException(status_code=400, detail="Uploaded file is empty")

  decoded = content.decode("utf-8-sig")
  reader = csv.DictReader(io.StringIO(decoded))
  expected = {"Category_ID", "Category_Limit", "Total_Claimed"}
  if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
    raise HTTPException(status_code=400, detail="Category limits CSV must include Category_ID, Category_Limit, Total_Claimed")

  merge_sql = """
    MERGE dbo.category_limits AS target
    USING (SELECT %s AS category_id) AS src
    ON target.category_id = src.category_id
    WHEN MATCHED THEN
      UPDATE SET category_limit = %s, total_claimed = %s, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (category_id, category_limit, total_claimed, updated_at)
      VALUES (%s, %s, %s, SYSUTCDATETIME());
  """

  count = 0
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      for row in reader:
        category_id = (row.get("Category_ID") or "").strip()
        if not category_id:
          continue
        category_limit = _to_decimal(row.get("Category_Limit"), "Category_Limit")
        total_claimed = _to_decimal(row.get("Total_Claimed"), "Total_Claimed")
        cursor.execute(
          merge_sql,
          (
            category_id,
            float(category_limit),
            float(total_claimed),
            category_id,
            float(category_limit),
            float(total_claimed),
          ),
        )
        count += 1
    conn.commit()

  return {"message": "Category limits seeded", "rows": count}


@app.get("/po-limits")
def get_po_limits():
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT po, po_value, total_claimed, updated_at FROM dbo.po_limits ORDER BY po")
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/category-limits")
def get_category_limits():
  ensure_schema_exists()
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT category_id, category_limit, total_claimed, updated_at FROM dbo.category_limits ORDER BY category_id")
      rows = cursor.fetchall()
  return {"count": len(rows), "records": rows}


@app.get("/records")
def get_records(
  limit: int = Query(default=100, ge=1, le=1000),
  certification: str | None = Query(default=None),
):
  ensure_schema_exists()

  base = """
    SELECT TOP (%s)
      id, source_blob, row_number, project, cost_category, po, cost_amount,
      certification, certified_cost, po_remaining_before, category_remaining_before,
      error_message, raw_payload, processed_at
    FROM dbo.application_payments_processed
  """
  params: tuple = (limit,)
  where = ""
  if certification:
    where = " WHERE certification = %s"
    params = (limit, certification.strip().lower())

  query = f"{base}{where} ORDER BY id DESC"
  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      cursor.execute(query, params)
      rows = cursor.fetchall()

  normalized = []
  for row in rows:
    row["raw_payload"] = row.get("raw_payload")
    normalized.append(row)
  return {"count": len(normalized), "records": normalized}
