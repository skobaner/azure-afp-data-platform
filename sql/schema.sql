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
