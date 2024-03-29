USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_CLEAR_D_LabTest_Results]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE  PROCEDURE [dbo].[sp_CLEAR_D_LabTest_Results]
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
           ,'LabTest_Results'
           ,'CLEAR_D_LabTest_Results'
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

   --IF OBJECT_ID('dbo.TMP_D_LabTest_Results_N', 'U') IS  NOT NULL 
			--		 drop table   dbo.TMP_D_LabTest_Results_N ; 

         IF OBJECT_ID('dbo.TMP_lab_test_resultInit', 'U') IS NOT NULL 
			         drop table   dbo.TMP_lab_test_resultInit ; 
		  
         IF OBJECT_ID('dbo.TMP_Lab_Result_Val', 'U') IS NOT NULL 
			         drop table   dbo.TMP_Lab_Result_Val ; 

		 IF OBJECT_ID('dbo.TMP_Result_And_R_Result', 'U') IS NOT NULL 
			         drop table   dbo.TMP_Result_And_R_Result ; 




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
           'LabTest_Results'
           ,'CLEAR_D_LabTest_Results'
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
           ,'LabTest_Results'
           ,'D_LabTest_Results'
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
