USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[addMissingColumn_SP]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE  PROCEDURE [dbo].[addMissingColumn_SP]
@tableName       			varchar(40)
AS
DECLARE @intFlag INT, @intCounter INT,  @stringColumnName VARCHAR(200),@stringColumnType VARCHAR(200),  @alterStag varchar(200), @alterDimen varchar(200)
SET @intFlag  = (select count(distinct rdb_column_nm) from rdb_table_metadata a where rdb_table_nm = 'D_'+@tableName and a.rdb_column_nm  not in (
	SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_CATALOG = 'RDB_MODERN' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'D_'+@tableName) 
	and data_type in ('NUMERIC', 'CODED', 'DATETIME', 'DATE', 'Text')) ;
set @intCounter  = @intFlag ;
	PRINT  @intCounter;
	PRINT 'D_'+@tableName
	WHILE (@intFlag >0)

BEGIN
	SET @intFlag  = (select count(distinct rdb_column_nm) from rdb_table_metadata a where rdb_table_nm = 'D_'+@tableName and a.rdb_column_nm  not in (
	SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_CATALOG = 'RDB_MODERN' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'D_'+@tableName)
	and data_type in ('NUMERIC', 'CODED', 'DATETIME', 'DATE', 'Text')) ;

	set @stringColumnName =(select top 1 rdb_column_nm from rdb_table_metadata a where rdb_table_nm = 'D_'+@tableName and a.rdb_column_nm  not in (
		SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_CATALOG = 'RDB_MODERN' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'D_'+@tableName 
	)and data_type in ('NUMERIC', 'CODED', 'DATETIME', 'DATE', 'Text') );


	set @stringColumnType =(select top 1 data_type from rdb_table_metadata where rdb_column_nm =@stringColumnName);

	
	if(@stringColumnType = 'Date/Time'OR @stringColumnType = 'DATETIME' OR @stringColumnType = 'DATE') 
	BEGIN
		set @alterStag = 'ALTER TABLE D_'+@tableName+' ADD ' + @stringColumnName +' DATETIME null';
		PRINT @alterStag
		exec (@alterStag);
		set @alterStag = 'ALTER TABLE S_'+@tableName+' ADD ' + @stringColumnName +' DATETIME null';
		PRINT @alterStag
		exec (@alterStag);
		PRINT 'DATETIME update'
	END

	if(@stringColumnType = 'text' OR @stringColumnType is null) 
	BEGIN
		set @alterStag = 'ALTER TABLE S_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(2000) null';
		PRINT @alterStag
		exec (@alterStag);
		set @alterDimen = 'ALTER TABLE D_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(2000) null';
		PRINT @alterDimen
		exec (@alterDimen);
		PRINT 'text update'
	END

	if(@stringColumnType = 'Coded') 
	BEGIN
		set @alterStag = 'ALTER TABLE S_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(4000) null';
		PRINT @alterStag
		exec (@alterStag);
		set @alterDimen = 'ALTER TABLE D_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(4000) null';
		PRINT @alterDimen
		exec (@alterDimen);
		PRINT 'Coded update'
	END
	 
if(@stringColumnType = 'Numeric' OR @stringColumnType = 'NUMERIC') 
	BEGIN
		set @alterStag = 'ALTER TABLE S_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(2000) null';
		PRINT @alterStag
		exec (@alterStag);
		set @alterDimen = 'ALTER TABLE D_'+@tableName+' ADD ' + @stringColumnName +' VARCHAR(2000) null';
		PRINT @alterDimen
		exec (@alterDimen);
		PRINT 'Numeric update'
	END
 

END
Return @intCounter;
PRINT 'Process executed with updates to S_'+@tableName+' AND D_'+@tableName+' table';

GO
