USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_nbs_batch_log_activity]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_nbs_batch_log_activity] @batch_id     BIGINT, 
                                                   @package_Name VARCHAR(500)
AS
    BEGIN
        DECLARE @activity_log_detail_uid BIGINT;
        SELECT @activity_log_detail_uid = ISNULL(MAX(activity_log_detail_uid), 1)
        FROM ACTIVITY_LOG_DETAIL;
        INSERT INTO ACTIVITY_LOG_DETAIL
        (activity_log_detail_uid, 
         activity_log_master_uid, 
         module_name, 
         db_object_name, 
         subStep_status, 
         subStep_number, 
         subStep_description, 
         row_count_insert, 
         subStep_start_date, 
         subStep_end_date
        )
               SELECT 
               --@activity_log_detail_uid+(ROW_NUMBER() OVER (ORDER BY [record_id]))
               @activity_log_detail_uid + record_id, 
               batch_id, 
               [Dataflow_Name], 
               [package_Name], 
               [Status_Type], 
               [step_number], 
               [step_name], 
               [row_count], 
               create_dttm, 
               update_dttm
               FROM job_flow_log
               WHERE package_Name = @package_Name
                     AND batch_id = @batch_id
               ORDER BY [record_id];
    END;
GO
