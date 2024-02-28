USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_nbs_batch_complete]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_nbs_batch_complete]
@typeCode varchar(100)
as

BEGIN
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no INT = 0 ;
    DECLARE @batch_id BIGINT = 0 ;
	DECLARE @BATCH_COMPLETE_time datetime2(7) = current_timestamp ;
	DECLARE @batch_end_time datetime2(7) = current_timestamp ;
	 DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	
  BEGIN TRY
    
	
   SELECT @batch_id = cast((format(getdate(),'yyMMddHHmmss')) as bigint)
	
	
	BEGIN TRANSACTION;

	
	
	  SET @Proc_Step_no = 1


	
	select @batch_id = batch_id
	from [dbo].[job_batch_log]
	 where status_type = 'start';

	 update [dbo].[job_batch_log]
			  set status_type = 'error', update_dttm = CURRENT_TIMESTAMP
	   where (
				select count(*)
				from  [dbo].[job_flow_log]
				where batch_id = (select  batch_id
					from [dbo].[job_batch_log]
					 where status_type = 'start' 
					 and type_code=@typeCode)
		 			 and lower(Status_Type) = 'error'
		    		) > 0 
		 and  status_type = 'start'
				 ;

	 
	 update [dbo].[job_batch_log]
			  set status_type = 'complete', update_dttm = CURRENT_TIMESTAMP
	   where (
				select count(*)
				from  [dbo].[job_flow_log]
				where batch_id = (select  batch_id
					from [dbo].[job_batch_log]
					 where status_type = 'start' 
					 and type_code=@typeCode)
		 			 and lower(Status_Type) = 'error'
					 and type_code=@typeCode
		    		) = 0 
		 and  status_type = 'start'
		 and type_code=@typeCode
				 ;

	 
	 select @RowCount_no = @@ROWCOUNT;
	 if @typeCode='MasterETL'
	 begin
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
           ,'BATCH_COMPLETE'
           ,'nbs_BATCH_COMPLETE'
		   ,'start'
		   ,@Proc_Step_no
		   ,'Creating PHC_CASE_UIDS' 
           ,@RowCount_no
		   );
	 end
     COMMIT TRANSACTION;
  
	
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
           'BATCH_COMPLETE'
           ,'sp_nbs_batch_complete'
		   ,'COMPLETE'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
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
           ,[row_count]
           )
		   VALUES
           (
           @batch_id
           ,'BATCH_COMPLETE'
           ,'BATCH_COMPLETE'
		   ,'ERROR'
		   ,@Proc_Step_no
		   , 'Step -' +CAST(@Proc_Step_no AS VARCHAR(3))+' -' +CAST(@ErrorMessage AS VARCHAR(500))
           ,0
		   );
  
       return -1;

	END CATCH
	

END




SET ANSI_PADDING ON


GO
