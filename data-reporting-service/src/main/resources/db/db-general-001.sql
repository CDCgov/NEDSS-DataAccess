use NBS_ODSE;

-- Upgrade compatibility level to allow inbuilt functions such as StringSplit
ALTER DATABASE NBS_ODSE SET COMPATIBILITY_LEVEL = 130;

-- Give user `sa` authorization to enable change data capture
EXEC sp_changedbowner 'sa';

-- Enable Change Data Capture on the database
EXEC sys.sp_cdc_enable_db;

-- Enable Change Data Capture on `Person` table
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'Person',
     @role_name     = NULL,
     @supports_net_changes = 1
GO
