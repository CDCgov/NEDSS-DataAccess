USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_CLEAR_CASE_LAB_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE  PROCEDURE [dbo].[sp_CLEAR_CASE_LAB_DATAMART]
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
           ,'CLEAR_CASE_LAB_DATAMART'
           ,'CLEAR_CASE_LAB_DATAMART'
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



	
		IF OBJECT_ID('dbo.TMP_CASE_LAB_DATAMART_FINAL', 'U') IS NOT NULL 
								 drop table  TMP_CASE_LAB_DATAMART_FINAL ;
	

		IF OBJECT_ID('dbo.TMP_CASE_LAB_DATAMART_FINAL', 'U') IS NOT NULL 
								 drop table  TMP_CASE_LAB_DATAMART_FINAL ;
	
	
		IF OBJECT_ID('dbo.TMP_CLDM_All_Case', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_All_Case; 

		IF OBJECT_ID('dbo.TMP_CLDM_GEN_PATIENT_ADD', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_GEN_PATIENT_ADD ;

		IF OBJECT_ID('dbo.TMP_CLDM_GEN_PAT_ADD_INV', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_GEN_PAT_ADD_INV 

		IF OBJECT_ID('dbo.TMP_CLDM_GEN_PATCOMPL_INV_PROVIDER', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_GEN_PATCOMPL_INV_PROVIDER 

		IF OBJECT_ID('dbo.TMP_CLDM_GEN_PATCOMPL_INV_INVESTIGATOR', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_GEN_PATCOMPL_INV_INVESTIGATOR 

		IF OBJECT_ID('dbo.TMP_CLDM_GEN_PATINFO_INV_PHY_RPTSRC_COND', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_GEN_PATINFO_INV_PHY_RPTSRC_COND 

		IF OBJECT_ID('dbo.TMP_CLDM_CASE_LAB_DATAMART', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_CASE_LAB_DATAMART

		IF OBJECT_ID('dbo.TMP_CLDM_invlab', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_invlab ;

  	    IF OBJECT_ID('dbo.TMP_CLDM_lab', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_lab ;

		IF OBJECT_ID('dbo.TMP_CLDM_both', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_both 

		IF OBJECT_ID('dbo.TMP_CLDM_inv2labs', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_inv2labs 

		IF OBJECT_ID('dbo.TMP_CLDM_invmorb', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_invmorb 

		IF OBJECT_ID('dbo.TMP_CLDM_morbResults', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_morbResults 

		IF OBJECT_ID('dbo.TMP_CLDM_morbLabResults', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_morbLabResults ;

		IF OBJECT_ID('dbo.TMP_CLDM_Inv2labs_final', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_Inv2labs_final ;

		IF OBJECT_ID('dbo.TMP_CLDM_sample1', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample1;

		IF OBJECT_ID('dbo.TMP_CLDM_sample2', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample2;

		
		IF OBJECT_ID('dbo.TMP_CLDM_sample21', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample21;

		IF OBJECT_ID('dbo.TMP_CLDM_sample3', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample3 ;

		IF OBJECT_ID('dbo.TMP_CLDM_sample4', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample4 ;

		IF OBJECT_ID('dbo.TMP_CLDM_sample5', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_sample5 ;

		IF OBJECT_ID('dbo.TMP_CLDM_CASE_LAB_DATAMART_FINAL', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CLDM_CASE_LAB_DATAMART_FINAL ;

		IF OBJECT_ID('dbo.TMP_SPECIMEN_COLLECTION_TABLE', 'U') IS NOT NULL 
								 drop table  dbo.TMP_SPECIMEN_COLLECTION_TABLE ;

		IF OBJECT_ID('dbo.TMP_CASE_LAB_DATAMART_MODIFIED', 'U') IS NOT NULL 
								 drop table  dbo.TMP_CASE_LAB_DATAMART_MODIFIED;
						


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
           'CLEAR_CASE_LAB_DATAMART'
           ,'CLEAR_CASE_LAB_DATAMART'
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
           ,'CLEAR_CASE_LAB_DATAMART'
           ,'CASE_LAB_DATAMART'
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
