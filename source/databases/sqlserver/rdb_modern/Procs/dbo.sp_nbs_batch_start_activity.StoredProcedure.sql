USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_nbs_batch_start_activity]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_nbs_batch_start_activity] @batch_id BIGINT
AS
    BEGIN
        INSERT INTO [dbo].[ACTIVITY_LOG_MASTER]
        ([activity_log_master_uid], 
         [start_date], 
         [end_date], 
         [refresh_ind], 
         [REFRESH_DESCRIPTION], 
         [extended_refresh_description], 
         [batch_start_date], 
         [batch_end_date]
        )
               SELECT [batch_id], 
                      [batch_start_dttm], 
                      [batch_end_dttm], 
                      [Status_Type], 
                      [Msg_Description1], 
                      [Msg_Description2], 
                      [create_dttm], 
                      [update_dttm]
               FROM [dbo].[job_batch_log]
               WHERE batch_id = @batch_id;
    END;
GO
