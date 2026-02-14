import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import azure.functions as func

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

IF OBJECT_ID(N'dbo.application_payments_raw', N'U') IS NULL
BEGIN
  CREATE TABLE dbo.application_payments_raw (
    id INT IDENTITY(1,1) PRIMARY KEY,
    source_blob NVARCHAR(512) NOT NULL,
    row_number INT NOT NULL,
    project NVARCHAR(255) NULL,
    cost_category NVARCHAR(100) NULL,
    po NVARCHAR(100) NULL,
    cost_amount DECIMAL(18,2) NULL,
    raw_payload NVARCHAR(MAX) NOT NULL,
    ingested_at DATETIME2(3) NOT NULL CONSTRAINT DF_app_payments_raw_ingested_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_app_payments_raw_source_row UNIQUE (source_blob, row_number)
  );
END;
"""

REQUIRED_COLUMNS = ("project", "cost_category", "cost_amount", "PO")


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
    as_dict=True,
    autocommit=False,
  )


def ensure_schema(cursor) -> None:
  cursor.execute(SCHEMA_DDL)


def _to_decimal(value: str) -> Decimal:
  try:
    return Decimal(str(value).strip())
  except (InvalidOperation, ValueError, TypeError):
    raise ValueError(f"Invalid cost_amount value: {value}")


def _insert_processed_row(
  cursor,
  *,
  source_blob: str,
  row_number: int,
  project: str,
  cost_category: str,
  po: str,
  cost_amount: Decimal,
  certification: str,
  certified_cost: Decimal,
  po_remaining_before: Decimal,
  category_remaining_before: Decimal,
  raw_payload: dict,
  error_message: str | None = None,
) -> None:
  cursor.execute(
    """
    MERGE dbo.application_payments_processed AS target
    USING (SELECT %s AS source_blob, %s AS row_number) AS src
    ON target.source_blob = src.source_blob AND target.row_number = src.row_number
    WHEN MATCHED THEN
      UPDATE SET
        project = %s,
        cost_category = %s,
        po = %s,
        cost_amount = %s,
        certification = %s,
        certified_cost = %s,
        po_remaining_before = %s,
        category_remaining_before = %s,
        error_message = %s,
        raw_payload = %s,
        processed_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (
        source_blob, row_number, project, cost_category, po, cost_amount, certification, certified_cost,
        po_remaining_before, category_remaining_before, error_message, raw_payload, processed_at
      )
      VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, SYSUTCDATETIME()
      );
    """,
    (
      source_blob,
      row_number,
      project,
      cost_category,
      po,
      float(cost_amount),
      certification,
      float(certified_cost),
      float(po_remaining_before),
      float(category_remaining_before),
      error_message,
      json.dumps(raw_payload),
      source_blob,
      row_number,
      project,
      cost_category,
      po,
      float(cost_amount),
      certification,
      float(certified_cost),
      float(po_remaining_before),
      float(category_remaining_before),
      error_message,
      json.dumps(raw_payload),
    ),
  )


def _upsert_raw_row(
  cursor,
  *,
  source_blob: str,
  row_number: int,
  project: str | None,
  cost_category: str | None,
  po: str | None,
  cost_amount: Decimal | None,
  raw_payload: dict,
) -> None:
  cursor.execute(
    """
    MERGE dbo.application_payments_raw AS target
    USING (SELECT %s AS source_blob, %s AS row_number) AS src
    ON target.source_blob = src.source_blob AND target.row_number = src.row_number
    WHEN MATCHED THEN
      UPDATE SET
        project = %s,
        cost_category = %s,
        po = %s,
        cost_amount = %s,
        raw_payload = %s,
        ingested_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (source_blob, row_number, project, cost_category, po, cost_amount, raw_payload, ingested_at)
      VALUES (%s, %s, %s, %s, %s, %s, %s, SYSUTCDATETIME());
    """,
    (
      source_blob,
      row_number,
      project,
      cost_category,
      po,
      float(cost_amount) if cost_amount is not None else None,
      json.dumps(raw_payload),
      source_blob,
      row_number,
      project,
      cost_category,
      po,
      float(cost_amount) if cost_amount is not None else None,
      json.dumps(raw_payload),
    ),
  )


def _process_row(cursor, source_blob: str, row_number: int, row: dict) -> None:
  project = (row.get("project") or "").strip()
  cost_category = (row.get("cost_category") or "").strip()
  po = (row.get("PO") or "").strip()
  raw_cost_amount = row.get("cost_amount")
  raw_payload = dict(row)
  raw_cost_amount_decimal: Decimal | None = None

  try:
    if raw_cost_amount is not None and str(raw_cost_amount).strip() != "":
      raw_cost_amount_decimal = _to_decimal(raw_cost_amount)
  except ValueError:
    raw_cost_amount_decimal = None

  _upsert_raw_row(
    cursor,
    source_blob=source_blob,
    row_number=row_number,
    project=project or None,
    cost_category=cost_category or None,
    po=po or None,
    cost_amount=raw_cost_amount_decimal,
    raw_payload=raw_payload,
  )

  if not project or not cost_category or not po:
    _insert_processed_row(
      cursor,
      source_blob=source_blob,
      row_number=row_number,
      project=project or "(missing)",
      cost_category=cost_category or "(missing)",
      po=po or "(missing)",
      cost_amount=Decimal("0"),
      certification="deauthorized",
      certified_cost=Decimal("0"),
      po_remaining_before=Decimal("0"),
      category_remaining_before=Decimal("0"),
      raw_payload=raw_payload,
      error_message="Missing required project/cost_category/PO value",
    )
    return

  try:
    cost_amount = _to_decimal(raw_cost_amount)
    if cost_amount < 0:
      raise ValueError("cost_amount cannot be negative")
  except ValueError as exc:
    _insert_processed_row(
      cursor,
      source_blob=source_blob,
      row_number=row_number,
      project=project,
      cost_category=cost_category,
      po=po,
      cost_amount=Decimal("0"),
      certification="deauthorized",
      certified_cost=Decimal("0"),
      po_remaining_before=Decimal("0"),
      category_remaining_before=Decimal("0"),
      raw_payload=raw_payload,
      error_message=str(exc),
    )
    return

  cursor.execute(
    "SELECT po_value, total_claimed FROM dbo.po_limits WITH (UPDLOCK, ROWLOCK) WHERE po = %s",
    (po,),
  )
  po_row = cursor.fetchone()

  cursor.execute(
    "SELECT category_limit, total_claimed FROM dbo.category_limits WITH (UPDLOCK, ROWLOCK) WHERE category_id = %s",
    (cost_category,),
  )
  category_row = cursor.fetchone()

  if not po_row or not category_row:
    missing = []
    if not po_row:
      missing.append(f"PO '{po}' not found")
    if not category_row:
      missing.append(f"Category '{cost_category}' not found")
    _insert_processed_row(
      cursor,
      source_blob=source_blob,
      row_number=row_number,
      project=project,
      cost_category=cost_category,
      po=po,
      cost_amount=cost_amount,
      certification="deauthorized",
      certified_cost=Decimal("0"),
      po_remaining_before=Decimal("0"),
      category_remaining_before=Decimal("0"),
      raw_payload=raw_payload,
      error_message="; ".join(missing),
    )
    return

  po_remaining = Decimal(str(po_row["po_value"])) - Decimal(str(po_row["total_claimed"]))
  category_remaining = Decimal(str(category_row["category_limit"])) - Decimal(str(category_row["total_claimed"]))
  po_remaining = max(po_remaining, Decimal("0"))
  category_remaining = max(category_remaining, Decimal("0"))

  if po_remaining <= 0 or category_remaining <= 0:
    certification = "deauthorized"
    certified_cost = Decimal("0")
  else:
    certified_cost = min(cost_amount, po_remaining, category_remaining)
    certification = "authorized" if certified_cost == cost_amount else "partially_authorized"

  if certified_cost > 0:
    cursor.execute(
      """
      UPDATE dbo.po_limits
      SET total_claimed = total_claimed + %s,
          updated_at = SYSUTCDATETIME()
      WHERE po = %s
      """,
      (float(certified_cost), po),
    )
    cursor.execute(
      """
      UPDATE dbo.category_limits
      SET total_claimed = total_claimed + %s,
          updated_at = SYSUTCDATETIME()
      WHERE category_id = %s
      """,
      (float(certified_cost), cost_category),
    )

  _insert_processed_row(
    cursor,
    source_blob=source_blob,
    row_number=row_number,
    project=project,
    cost_category=cost_category,
    po=po,
    cost_amount=cost_amount,
    certification=certification,
    certified_cost=certified_cost,
    po_remaining_before=po_remaining,
    category_remaining_before=category_remaining,
    raw_payload=raw_payload,
    error_message=None,
  )


def main(input_blob: func.InputStream) -> None:
  source_blob = input_blob.name
  logging.info("Processing AFP blob: %s", source_blob)

  decoded = input_blob.read().decode("utf-8-sig")
  reader = csv.DictReader(io.StringIO(decoded))
  if not reader.fieldnames:
    logging.warning("Skipping blob %s because CSV headers are missing", source_blob)
    return

  missing_headers = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]
  if missing_headers:
    logging.error("Skipping blob %s due to missing headers: %s", source_blob, ",".join(missing_headers))
    return

  with get_sql_connection() as conn:
    with conn.cursor() as cursor:
      ensure_schema(cursor)
      for idx, row in enumerate(reader, start=1):
        _process_row(cursor, source_blob, idx, row)
    conn.commit()

  logging.info("Completed AFP blob processing: %s", source_blob)
