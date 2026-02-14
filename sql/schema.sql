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
