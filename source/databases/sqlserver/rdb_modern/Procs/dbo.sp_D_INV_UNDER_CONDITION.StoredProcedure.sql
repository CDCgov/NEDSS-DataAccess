USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_D_INV_UNDER_CONDITION]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_D_INV_UNDER_CONDITION] 
	   @Batch_id bigint
AS
BEGIN
	DECLARE @RowCount_no int;
	DECLARE @Proc_Step_no float= 0;
	DECLARE @Proc_Step_Name varchar(200)= '';
	DECLARE @batch_start_time datetime2(7)= NULL;
	DECLARE @batch_end_time datetime2(7)= NULL;

	BEGIN TRY

		SET @Proc_Step_no = 1;
		SET @Proc_Step_Name = 'SP_Start';

		BEGIN TRANSACTION;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_UNDER_CONDITION', 'D_INV_UNDER_CONDITION', 'START', @Proc_Step_no, @Proc_Step_Name, 0 );

		COMMIT TRANSACTION;

		SELECT @batch_start_time = batch_start_dttm, @batch_end_time = batch_end_dttm
		FROM [dbo].[job_batch_log]
		WHERE status_type = 'start' and type_code='MasterETL';

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 2;
		SET @Proc_Step_Name = ' Add new columns to D_INV_UNDER_CONDITION'; 

		-- create table rdb_ui_metadata_INV_UNDER_CONDITION as 

		DECLARE @Temp_Query_Table TABLE
		( 
										ID int IDENTITY(1, 1), QUERY_stmt varchar(5000)
		);
		DECLARE @column_query varchar(5000);
		DECLARE @Max_Query_No int;
		DECLARE @Curr_Query_No int;
		DECLARE @ColumnList varchar(5000);

		INSERT INTO @Temp_Query_Table
			   SELECT 'ALTER TABLE dbo.D_INV_UNDER_CONDITION ADD ['+COLUMN_NAME+'] '+DATA_TYPE+CASE
																								   WHEN DATA_TYPE IN( 'char', 'varchar', 'nchar', 'nvarchar' ) THEN ' ('+COALESCE(CAST(NULLIF(CHARACTER_MAXIMUM_LENGTH, -1) AS varchar(10)), CAST(CHARACTER_MAXIMUM_LENGTH as varchar(10)))+')'
																								   ELSE ''
																								   END+CASE
																									   WHEN IS_NULLABLE = 'NO' THEN ' NOT NULL'
																									   ELSE ' NULL'
																									   END
			   FROM INFORMATION_SCHEMA.COLUMNS AS c
			   WHERE TABLE_NAME = 'S_INV_UNDER_CONDITION' AND 
					 NOT EXISTS
			   (
				   SELECT 1
				   FROM INFORMATION_SCHEMA.COLUMNS
				   WHERE TABLE_NAME = 'D_INV_UNDER_CONDITION' AND 
						 COLUMN_NAME = c.COLUMN_NAME
			   ) AND 
					 LOWER(COLUMN_NAME) NOT IN( LOWER('PAGE_CASE_UID'), 'last_chg_time' );


		SET @Max_Query_No =
		(
			SELECT MAX(ID)
			FROM @Temp_Query_Table AS t
		);

		SET @Curr_Query_No = 0;

		WHILE @Max_Query_No > @Curr_Query_No

		BEGIN
			SET @Curr_Query_No = @Curr_Query_No + 1;

			SET @column_query =
			(
				SELECT QUERY_stmt
				FROM @Temp_Query_Table AS t
				WHERE ID = @Curr_Query_No
			);

			--SELECT @column_query;

			EXEC (@column_query);

		END;

		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_UNDER_CONDITION', 'D_INV_UNDER_CONDITION', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 3;
		SET @Proc_Step_Name = ' Inserting data in to D_INV_UNDER_CONDITION';

		IF NOT EXISTS
		(
			SELECT d_inv_UNDER_CONDITION_key
			FROM [dbo].[D_INV_UNDER_CONDITION]
			WHERE d_inv_UNDER_CONDITION_key = 1
		)
		BEGIN
			INSERT INTO [dbo].[D_INV_UNDER_CONDITION]( [D_INV_UNDER_CONDITION_KEY] )
			VALUES( 1 );
		END;

		DECLARE @insert_query nvarchar(max);

		SET @insert_query =
		(
			SELECT 'INSERT INTO  [dbo].[D_INV_UNDER_CONDITION]( [D_INV_UNDER_CONDITION_KEY] ,'+STUFF(
																										(
																											SELECT ', ['+name+']'
																											FROM syscolumns
																											WHERE id = OBJECT_ID('S_INV_UNDER_CONDITION') AND 
																												  LOWER(NAME) NOT IN( LOWER('PAGE_CASE_UID'), 'last_chg_time' )
																											FOR XML PATH('')
																										), 1, 1, '')+' ) select [D_INV_UNDER_CONDITION_KEY] , '+STUFF(
																																									 (
																																										 SELECT ', ['+name+']'
																																										 FROM syscolumns
																																										 WHERE id = OBJECT_ID('S_INV_UNDER_CONDITION') AND 
																																											   LOWER(NAME) NOT IN( LOWER('PAGE_CASE_UID'), 'last_chg_time' )
																																										 FOR XML PATH('')
																																									 ), 1, 1, '')+' 
	   FROM  dbo.L_INV_UNDER_CONDITION_INC LINV 
	   INNER JOIN dbo.S_INV_UNDER_CONDITION SINV ON SINV.PAGE_CASE_UID=LINV.PAGE_CASE_UID 
	    where linv.D_INV_UNDER_CONDITION_KEY != 1'
		);

		SELECT @insert_query;

		EXEC sp_executesql @insert_query;

		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_UNDER_CONDITION', 'D_INV_UNDER_CONDITION', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 999;
		SET @Proc_Step_Name = 'SP_COMPLETE';

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_UNDER_CONDITION', 'D_INV_UNDER_CONDITION', 'COMPLETE', @Proc_Step_no, @Proc_Step_name, @RowCount_no );

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH

		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION;
		END;

		DECLARE @ErrorNumber int= ERROR_NUMBER();
		DECLARE @ErrorLine int= ERROR_LINE();
		DECLARE @ErrorMessage nvarchar(4000)= ERROR_MESSAGE();
		DECLARE @ErrorSeverity int= ERROR_SEVERITY();
		DECLARE @ErrorState int= ERROR_STATE();

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_UNDER_CONDITION', 'D_INV_UNDER_CONDITION', 'ERROR', @Proc_Step_no, 'Step -'+CAST(@Proc_Step_no AS varchar(3))+' -'+CAST(@ErrorMessage AS varchar(500)), 0 );

		RETURN -1;
	END CATCH;

END;

GO
