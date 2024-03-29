USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_SLD_ALL_TEST]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create  PROCEDURE [dbo].[sp_SLD_ALL_TEST]
as


BEGIN
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
    DECLARE @batch_id BIGINT ;
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
		   @batch_id
           ,'SLD_ALL'
           ,'SLD_ALL'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	

	DECLARE	@return_value int = 0 ;
	DECLARE	@return_value_ret int;
	declare @ret int;

     EXEC	@return_value = [dbo].[sp_nbs_batch_start];

 
      SELECT	'Return Value TEST ' = @return_value;

	  set @return_value_ret = @return_value;

	  if ( @return_value_ret <> 0 ) 
	    begin
		   select 'no more processing'
	       return @return_value_ret ;
        end
	
		select @batch_id = batch_id,@batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
		from [dbo].[job_batch_log]
		 where status_type = 'start';


		--EXEC  [dbo].sp_CLEAR_INV_ADMINISTRATIVE @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_CLINICAL @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_COMPLICATION @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_CONTACT @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_DEATH @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_EPIDEMIOLOGY @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_HIV @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_ISOLATE_TRACKING @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_LAB_FINDING @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_MEDICAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_MOTHER @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_OTHER @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_PATIENT_OBS @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_PREGNANCY_BIRTH @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_RESIDENCY @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_RISK_FACTOR @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_SOCIAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_SYMPTOM @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_TRAVEL @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_TREATMENT @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_UNDER_CONDITION @batch_id;
		--EXEC  [dbo].sp_CLEAR_INV_VACCINATION @batch_id;

      
		--EXEC  [dbo].sp_S_INV_ADMINISTRATIVE @batch_id;
		--EXEC  [dbo].sp_S_INV_CLINICAL @batch_id;
		--EXEC  [dbo].sp_S_INV_COMPLICATION @batch_id;
		--EXEC  [dbo].sp_S_INV_CONTACT @batch_id;
		--EXEC  [dbo].sp_S_INV_DEATH @batch_id;
		--EXEC  [dbo].sp_S_INV_EPIDEMIOLOGY @batch_id;
		--EXEC  [dbo].sp_S_INV_HIV @batch_id;
		--EXEC  [dbo].sp_S_INV_ISOLATE_TRACKING @batch_id;
		--EXEC  [dbo].sp_S_INV_LAB_FINDING @batch_id;
		--EXEC  [dbo].sp_S_INV_MEDICAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_S_INV_MOTHER @batch_id;
		--EXEC  [dbo].sp_S_INV_OTHER @batch_id;
		--EXEC  [dbo].sp_S_INV_PATIENT_OBS @batch_id;
		--EXEC  [dbo].sp_S_INV_PREGNANCY_BIRTH @batch_id;
		--EXEC  [dbo].sp_S_INV_RESIDENCY @batch_id;
		--EXEC  [dbo].sp_S_INV_RISK_FACTOR @batch_id;
		--EXEC  [dbo].sp_S_INV_SOCIAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_S_INV_SYMPTOM @batch_id;
		--EXEC  [dbo].sp_S_INV_TRAVEL @batch_id;
		--EXEC  [dbo].sp_S_INV_TREATMENT @batch_id;
		--EXEC  [dbo].sp_S_INV_UNDER_CONDITION @batch_id;
		--EXEC  [dbo].sp_S_INV_VACCINATION @batch_id;

 
		--EXEC  [dbo].sp_L_INV_ADMINISTRATIVE @batch_id;
		--EXEC  [dbo].sp_L_INV_CLINICAL @batch_id;
		--EXEC  [dbo].sp_L_INV_COMPLICATION @batch_id;
		--EXEC  [dbo].sp_L_INV_CONTACT @batch_id;
		--EXEC  [dbo].sp_L_INV_DEATH @batch_id;
		--EXEC  [dbo].sp_L_INV_EPIDEMIOLOGY @batch_id;
		--EXEC  [dbo].sp_L_INV_HIV @batch_id;
		--EXEC  [dbo].sp_L_INV_ISOLATE_TRACKING @batch_id;
		--EXEC  [dbo].sp_L_INV_LAB_FINDING @batch_id;
		--EXEC  [dbo].sp_L_INV_MEDICAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_L_INV_MOTHER @batch_id;
		--EXEC  [dbo].sp_L_INV_OTHER @batch_id;
		--EXEC  [dbo].sp_L_INV_PATIENT_OBS @batch_id;
		--EXEC  [dbo].sp_L_INV_PREGNANCY_BIRTH @batch_id;
		--EXEC  [dbo].sp_L_INV_RESIDENCY @batch_id;
		--EXEC  [dbo].sp_L_INV_RISK_FACTOR @batch_id;
		--EXEC  [dbo].sp_L_INV_SOCIAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_L_INV_SYMPTOM @batch_id;
		--EXEC  [dbo].sp_L_INV_TRAVEL @batch_id;
		--EXEC  [dbo].sp_L_INV_TREATMENT @batch_id;
		--EXEC  [dbo].sp_L_INV_UNDER_CONDITION @batch_id;
		--EXEC  [dbo].sp_L_INV_VACCINATION @batch_id;


 
		--EXEC  [dbo].sp_D_INV_ADMINISTRATIVE @batch_id;
		--EXEC  [dbo].sp_D_INV_CLINICAL @batch_id;
		--EXEC  [dbo].sp_D_INV_COMPLICATION @batch_id;
		--EXEC  [dbo].sp_D_INV_CONTACT @batch_id;
		--EXEC  [dbo].sp_D_INV_DEATH @batch_id;
		--EXEC  [dbo].sp_D_INV_EPIDEMIOLOGY @batch_id;
		--EXEC  [dbo].sp_D_INV_HIV @batch_id;
		--EXEC  [dbo].sp_D_INV_ISOLATE_TRACKING @batch_id;
		--EXEC  [dbo].sp_D_INV_LAB_FINDING @batch_id;
		--EXEC  [dbo].sp_D_INV_MEDICAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_D_INV_MOTHER @batch_id;
		--EXEC  [dbo].sp_D_INV_OTHER @batch_id;
		--EXEC  [dbo].sp_D_INV_PATIENT_OBS @batch_id;
		--EXEC  [dbo].sp_D_INV_PREGNANCY_BIRTH @batch_id;
		--EXEC  [dbo].sp_D_INV_RESIDENCY @batch_id;
		--EXEC  [dbo].sp_D_INV_RISK_FACTOR @batch_id;
		--EXEC  [dbo].sp_D_INV_SOCIAL_HISTORY @batch_id;
		--EXEC  [dbo].sp_D_INV_SYMPTOM @batch_id;
		--EXEC  [dbo].sp_D_INV_TRAVEL @batch_id;
		--EXEC  [dbo].sp_D_INV_TREATMENT @batch_id;
		--EXEC  [dbo].sp_D_INV_UNDER_CONDITION @batch_id;
		--EXEC  [dbo].sp_D_INV_VACCINATION @batch_id;

  --      EXEC [dbo].[sp_SLD_INVESTIGATION_REPEAT] @batch_id;

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
		   @batch_id,
           'SLD_ALL'
           ,'SLD_ALL'
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
           @batch_id
           ,'SLD_ALL'
           ,'SLD_ALL'
		   ,'ERROR'
		   ,@Proc_Step_no
		   ,'ERROR - '+ @Proc_Step_name
           , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
           ,0
		   );
  

      return -1 ;

	END CATCH
	
END;
GO
