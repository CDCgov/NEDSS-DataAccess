USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_ETL_PART1_Start]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 CREATE  PROCEDURE [dbo].[sp_ETL_PART1_Start] 
 as

 BEGIN
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
    DECLARE @batch_id BIGINT ;
	DECLARE @typeCode VARCHAR(100) = 'MasterETL' ;
    DECLARE @typeDescrption VARCHAR(100) = 'MasterETL' ;
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

     EXEC	@return_value = [dbo].[sp_nbs_batch_start]
	 @typeCode, @typeDescrption;

 
      SELECT	'Return Value TEST ' = @return_value;

	  set @return_value_ret = @return_value;

	  if ( @return_value_ret <> 0 ) 
	    begin
		   select 'no more processing'
	       return @return_value_ret ;
        end
	
		select @batch_id = batch_id,@batch_start_time = batch_start_dttm,@batch_end_time = batch_end_dttm
		from [dbo].[job_batch_log] where type_code='MasterETL'
		 and status_type = 'start';

		EXEC  [dbo].[sp_nbs_batch_start_activity] @batch_id;

		EXEC  [dbo].sp_CLEAR_PATIENT @batch_id;
		
		 
		 print @batch_id
		
		 /*  TODO: Ravi 
		 EXEC  [dbo].SP_PRE_RUN @batch_id;
		EXEC  [dbo].[sp_nbs_batch_log_activity]  @batch_id,  @package_Name = N'PRE_RUN';
		print @batch_id
		  TODO: Ravi */
		 /* Repalced from sp_pre_run */
		INSERT INTO [dbo].[ACTIVITY_LOG_MASTER]
           ([activity_log_master_uid]
           ,[start_date]
           ,[refresh_ind]
           ,[REFRESH_DESCRIPTION]
			)
     VALUES
          ((select isNull(max(activity_log_master_uid)+1, 1) from [ACTIVITY_LOG_MASTER] )
           ,getDate()
		   ,'F'
           ,'ETL process initiated.');
        		 /* Repalced from sp_pre_run */
  
		EXEC  [dbo].sp_D_PATIENT @batch_id;
		EXEC  [dbo].[sp_nbs_batch_log_activity]  @batch_id,  @package_Name = N'D_PATIENT';
		print @batch_id
		 /*   TODO: Ravi 
		EXEC  [dbo].sp_D_PROVIDER @batch_id;
		EXEC  [dbo].[sp_nbs_batch_log_activity]  @batch_id,  @package_Name = N'D_PROVIDER' ;
		print @batch_id
		EXEC  [dbo].sp_D_ORGANIZATION @batch_id;
		
		print @batch_id
		EXEC  [dbo].sp_EVENT_METRIC_DATAMART @batch_id;
		EXEC  [dbo].[sp_nbs_batch_log_activity]  @batch_id,  @package_Name = N'sp_EVENT_METRIC_DATAMART' ;
		
		print @batch_id
		PRINT 'CLEAR_USER_PROFILE CALLED'
		EXEC  [dbo].sp_CLEAR_USER_PROFILE @batch_id;
		PRINT 'USER_PROFILE CALLED' 
		EXEC  [dbo].sp_USER_PROFILE @batch_id;
		
		EXEC  [dbo].[sp_nbs_batch_log_activity]  @batch_id,  @package_Name = N'D_USER_PROFILE' ;
		  TODO: Ravi   */
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
	
END

;


GO
