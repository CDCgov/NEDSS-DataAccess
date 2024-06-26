USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_L_INV_SYMPTOM]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_L_INV_SYMPTOM] 
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
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, 0 );

		COMMIT TRANSACTION;



		BEGIN TRANSACTION;

		SET @Proc_Step_no = 2;
		SET @Proc_Step_Name = 'CREATE TABLE LOOKUP_TABLE1_INV_SYMPTOM'; 


		IF OBJECT_ID('dbo.LOOKUP_TABLE1_INV_SYMPTOM', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.LOOKUP_TABLE1_INV_SYMPTOM;
		END;

		IF OBJECT_ID('dbo.S_INV_SYMPTOM', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[S_INV_SYMPTOM]
			( 
						 [PAGE_CASE_UID] [numeric](20, 0) NULL
			)
			ON [PRIMARY];

			CREATE INDEX S_INV_SYMPTOM
ON [dbo].S_INV_SYMPTOM
			( PAGE_CASE_UID
			);
		END;
		IF OBJECT_ID('dbo.L_INV_SYMPTOM', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[L_INV_SYMPTOM]
			( 
						 [D_INV_SYMPTOM_KEY] [float] NULL, [PAGE_CASE_UID] [float] NULL
			)
			ON [PRIMARY];

			CREATE INDEX L_INV_SYMPTOM
ON [dbo].L_INV_SYMPTOM
			( PAGE_CASE_UID
			);
			CREATE NONCLUSTERED INDEX [L_RDB_PERF_04082021_01]
ON [dbo].[L_INV_SYMPTOM]
			( [D_INV_SYMPTOM_KEY]
			) 
				   INCLUDE( [PAGE_CASE_UID] );
		END;
		WITH lst
			 AS (SELECT PAGE_CASE_UID
				 FROM dbo.PHC_UIDS
				 EXCEPT
				 (
					 SELECT PAGE_CASE_UID
					 FROM dbo.S_INV_SYMPTOM
					 UNION
					 SELECT PAGE_CASE_UID
					 FROM dbo.L_INV_SYMPTOM
				 ))
			 SELECT *, 1 AS D_NE_KEY
			 INTO dbo.LOOKUP_TABLE1_INV_SYMPTOM
			 FROM lst;



		SELECT @RowCount_no = @@ROWCOUNT;


		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;



		BEGIN TRANSACTION;

		SET @Proc_Step_no = 3;
		SET @Proc_Step_Name = ' ADD TO  TABLE LOOKUP_TABLE1_INV_SYMPTOM';






		INSERT INTO dbo.LOOKUP_TABLE1_INV_SYMPTOM
			   SELECT pcuid.PAGE_CASE_UID, ladmin.D_INV_SYMPTOM_KEY
			   FROM dbo.PHC_UIDS AS pcuid, dbo.L_INV_SYMPTOM AS ladmin
			   WHERE pcuid.PAGE_CASE_UID = ladmin.PAGE_CASE_UID AND 
					 ladmin.D_INV_SYMPTOM_KEY != 1;



		SELECT @RowCount_no = @@ROWCOUNT;


		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;




		BEGIN TRANSACTION;

		SET @Proc_Step_no = 4;
		SET @Proc_Step_Name = 'CREATE TABLE LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM'; 


		IF OBJECT_ID('dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM;
		END;



		SELECT pcuid.PAGE_CASE_UID, ladmin.D_INV_SYMPTOM_KEY
		INTO dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM
		FROM dbo.PHC_UIDS AS pcuid, dbo.L_INV_SYMPTOM AS ladmin
		WHERE pcuid.PAGE_CASE_UID = ladmin.PAGE_CASE_UID AND 
			  ladmin.D_INV_SYMPTOM_KEY != 1;






		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [L_RDB_PREF_INTERNAL_03]
ON [dbo].[LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM]
		( [D_INV_SYMPTOM_KEY]
		);

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;




		BEGIN TRANSACTION;

		SET @Proc_Step_no = 5;
		SET @Proc_Step_Name = ' CREATE TABLE LOOKUP_TABLE_N_INV_SYMPTOM'; 


		--CREATE TABLE LOOKUP_TABLE_N_INV_SYMPTOM 


		DELETE FROM dbo.LOOKUP_TABLE_N_INV_SYMPTOM;


		INSERT INTO dbo.LOOKUP_TABLE_N_INV_SYMPTOM
			   SELECT PAGE_CASE_UID
			   FROM dbo.PHC_UIDS
			   EXCEPT
			   SELECT PAGE_CASE_UID
			   FROM [dbo].[LOOKUP_TABLE1_INV_SYMPTOM];






		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 6;
		SET @Proc_Step_Name = 'CREATE TABLE L_INV_SYMPTOM_INC'; 

		--CREATE TABLE L_INV_SYMPTOM_INC AS 

		IF OBJECT_ID('dbo.L_INV_SYMPTOM_INC', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.L_INV_SYMPTOM_INC;
		END;



		SELECT ltn.PAGE_CASE_UID AS PAGE_CASE_UID_N, lt1.PAGE_CASE_UID AS PAGE_CASE_UID_NE, ltn.D_INV_SYMPTOM_KEY AS D_INV_SYMPTOM_KEY_N, lt1.D_NE_KEY, CAST(NULL AS bigint) AS PAGE_CASE_UID, CAST(NULL AS bigint) AS D_INV_SYMPTOM_KEY
		INTO dbo.L_INV_SYMPTOM_INC
		FROM dbo.LOOKUP_TABLE1_INV_SYMPTOM AS lt1
			 FULL JOIN
			 dbo.LOOKUP_TABLE_N_INV_SYMPTOM AS ltn
			 ON lt1.PAGE_CASE_UID = ltn.PAGE_CASE_UID;




		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [L_RDB_PERF_INTERNAL_01]
ON [dbo].[L_INV_SYMPTOM_INC]
		( [PAGE_CASE_UID]
		);
		CREATE NONCLUSTERED INDEX [L_RDB_PERF_INTERNAL_02]
ON [dbo].[L_INV_SYMPTOM_INC]
		( [D_INV_SYMPTOM_KEY]
		);

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;


		BEGIN TRANSACTION;

		SET @Proc_Step_no = 7;
		SET @Proc_Step_Name = 'UPDATE TABLE L_INV_SYMPTOM_INC';

		UPDATE dbo.L_INV_SYMPTOM_INC
		  SET PAGE_CASE_UID = COALESCE(PAGE_CASE_UID_N, PAGE_CASE_UID_NE);



		UPDATE dbo.L_INV_SYMPTOM_INC
		  SET D_INV_SYMPTOM_KEY = COALESCE(D_INV_SYMPTOM_KEY_N, D_NE_KEY);



		SELECT @RowCount_no = -1;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;


		BEGIN TRANSACTION;


		SET @Proc_Step_no = 8;
		SET @Proc_Step_Name = 'Delete from L_INV_SYMPTOM where existing default entry';

		DELETE ladmin
		FROM dbo.L_INV_SYMPTOM ladmin
			 JOIN
			 dbo.L_INV_SYMPTOM_INC ladminI
			 ON ladmin.PAGE_CASE_UID = ladminI.PAGE_CASE_UID
		WHERE ladmin.D_INV_SYMPTOM_KEY = 1;


		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;
   
		--    select 'I am here ';

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 9;
		SET @Proc_Step_Name = 'Insert into L_INV_SYMPTOM new default key entry';

		INSERT INTO [dbo].L_INV_SYMPTOM( [PAGE_CASE_UID], [D_INV_SYMPTOM_KEY] )
			   SELECT PAGE_CASE_UID, D_INV_SYMPTOM_KEY
			   FROM dbo.L_INV_SYMPTOM_INC
			   WHERE D_INV_SYMPTOM_KEY = 1;


		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;
   
		--    select 'I am here ';

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 10;
		SET @Proc_Step_Name = 'Insert into L_INV_SYMPTOM new key entry';

		INSERT INTO [dbo].L_INV_SYMPTOM( [PAGE_CASE_UID], [D_INV_SYMPTOM_KEY] )
			   SELECT PAGE_CASE_UID, D_INV_SYMPTOM_KEY
			   FROM dbo.LOOKUP_TABLE_N_INV_SYMPTOM;



		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;



		BEGIN TRANSACTION;


		SET @Proc_Step_no = 11;
		SET @Proc_Step_Name = 'Delete from D_INV_SYMPTOM where existing default entry';

		IF OBJECT_ID('dbo.D_INV_SYMPTOM', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[D_INV_SYMPTOM]
			( 
						 [D_INV_SYMPTOM_KEY] [float] NULL
			)
			ON [PRIMARY];
		END;

		DELETE dadmin
		FROM dbo.D_INV_SYMPTOM dadmin
			 JOIN
			 dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM ladminI
			 ON dadmin.d_inv_SYMPTOM_key = ladminI.d_inv_SYMPTOM_key;



		SELECT @RowCount_no = @@ROWCOUNT;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;


		IF OBJECT_ID('dbo.LOOKUP_TABLE1_INV_SYMPTOM', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.LOOKUP_TABLE1_INV_SYMPTOM;
		END;

		IF OBJECT_ID('dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.LOOKUP_TABLE_D_REMOVE_INV_SYMPTOM;
		END;


		DELETE FROM [dbo].[LOOKUP_TABLE_N_INV_SYMPTOM];


		--    select 'I am here ';

		BEGIN TRANSACTION;

		SET @Proc_Step_no = 999;
		SET @Proc_Step_Name = 'SP_COMPLETE';


		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'COMPLETE', @Proc_Step_no, @Proc_Step_name, @RowCount_no );


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
		VALUES( @Batch_id, 'INV_SYMPTOM', 'L_INV_SYMPTOM', 'ERROR', @Proc_Step_no, 'Step -' + CAST(@Proc_Step_no AS varchar(3)) + ' -' + CAST(@ErrorMessage AS varchar(500)), 0 );


		RETURN -1;
	END CATCH;

END;


 
GO
