USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_CLEAR_INV_SUMMART_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE  PROCEDURE [dbo].[sp_CLEAR_INV_SUMMART_DATAMART]
@Batch_id BIGINT
as


BEGIN
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	DECLARE @batch_start_time datetime2(7) = null ;
	DECLARE @batch_end_time datetime2(7) = null ;
  
  BEGIN TRY
    
	SET @Proc_Step_no = 1;
	SET @Proc_Step_Name = 'SP_Start';

	
	BEGIN TRANSACTION;
	
    INSERT INTO [dbo].[job_flow_log] (
	        batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @Batch_id
           ,'INV_SUMMART_DATAMART'
           ,'sp_CLEAR_INV_SUMMART_DATAMART'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	
	
	select @batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
	from [dbo].[job_batch_log]
	 where status_type = 'start'
     ;


BEGIN TRANSACTION ;



				IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT', 'U') IS NOT NULL   
				 drop table dbo.TMP_S_PATIENT_LOCATION_KEYS_INIT;---1a

				IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_BASE', 'U') IS NOT NULL   
				drop table dbo.TMP_S_CONFIRMATION_METHOD_BASE  ;----3

				IF OBJECT_ID('dbo.TMP_S_CONFIRMATION_METHOD_PIVOT', 'U') IS NOT NULL
				DROP TABLE dbo.TMP_S_CONFIRMATION_METHOD_PIVOT;---4

				IF OBJECT_ID('dbo.TMP_S_PATIENT_LOCATION_KEY', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENT_LOCATION_KEY  ;---5

				IF OBJECT_ID('dbo.TMP_S_PATIENTS_INFO', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENTS_INFO  ;----6

				IF OBJECT_ID('dbo.TMP_S_PHYSICIANS_INFO', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PHYSICIANS_INFO  ;---7

				IF OBJECT_ID('dbo.TMP_S_PATIENTS_DETAIL', 'U') IS NOT NULL   
				drop table dbo.TMP_S_PATIENTS_DETAIL  ;-----8

				IF OBJECT_ID('dbo.TMP_S_INV_WITH_USER', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_WITH_USER  ;------9

				IF OBJECT_ID('dbo.TMP_S_INV_SUMM_DATAMART_INITI', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_SUMM_DATAMART_INITI  ;---10

				IF OBJECT_ID('dbo.TMP_InvLab', 'U') IS NOT NULL   
				drop table dbo.TMP_InvLab ;----11

				IF OBJECT_ID('dbo.TMP_Lab', 'U') IS NOT NULL   
				drop table dbo.TMP_Lab ;----12

				IF OBJECT_ID(' dbo.TMP_BothTable', 'U') IS NOT NULL   
				drop table  dbo.TMP_BothTable ;---13

				IF OBJECT_ID('dbo.TMP_Inv2Labs', 'U') IS NOT NULL   
				 drop table dbo.TMP_Inv2Labs ;----14

				IF OBJECT_ID('dbo.TMP_SPECIMEN_COLLECTION', 'U') IS NOT NULL   
				 drop table dbo.TMP_SPECIMEN_COLLECTION ;----15

				IF OBJECT_ID(' dbo.TMP_CASE_LAB_DATAMART_MODIFIED', 'U') IS NOT NULL   
				drop table  dbo.TMP_CASE_LAB_DATAMART_MODIFIED ;---16

				IF OBJECT_ID('dbo.TMP_INV_SUMM_DATAMART', 'U') IS NOT NULL   
				drop table dbo.TMP_INV_SUMM_DATAMART ;----17				
				IF OBJECT_ID('dbo.TMP_S_INV_SUMM_DATAMART_INIT', 'U') IS NOT NULL   
				drop table dbo.TMP_S_INV_SUMM_DATAMART_INIT ;----18
		

COMMIT TRANSACTION ;

    BEGIN TRANSACTION ;
	
	SET @Proc_Step_no = 999;
	SET @Proc_Step_Name = 'SP_COMPLETE'; 


	INSERT INTO [dbo].[job_flow_log] (
		    batch_id
			,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[row_count]
           )
		   VALUES
           (
		   @Batch_id,
           'INV_SUMMART_DATAMART'
           ,'sp_CLEAR_INV_SUMMART_DATAMART'
		   ,'COMPLETE'
		   ,@Proc_Step_no
		   ,@Proc_Step_name
           ,@RowCount_no
		   );
  
	
	COMMIT TRANSACTION;
  END TRY

  BEGIN CATCH
  
     
     IF @@TRANCOUNT > 0   ROLLBACK TRANSACTION;
 
  
	
	DECLARE @ErrorNumber INT = ERROR_NUMBER();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
 
	
    INSERT INTO [dbo].[job_flow_log] (
		    batch_id
		   ,[Dataflow_Name]
		   ,[package_Name]
		    ,[Status_Type] 
           ,[step_number]
           ,[step_name]
           ,[Error_Description]
		   ,[row_count]
           )
		   VALUES
           (
           @Batch_id
           ,'INV_SUMMART_DATAMART'
           ,'sp_CLEAR_INV_SUMMART_DATAMART'
		   ,'ERROR'
		   ,@Proc_Step_no
		   ,'ERROR - '+ @Proc_Step_name
           , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
           ,0
		   );
  

      return -1 ;

	END CATCH
	
END

;






GO
