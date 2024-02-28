USE [rdb_modern]
GO
/****** Object:  View [dbo].[v_ETL_ACTIVITY_LOG_LAST_RUN]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[v_ETL_ACTIVITY_LOG_LAST_RUN] as 
SELECT ACTIVITY_LOG_MASTER.activity_log_master_uid AS ETL_RUN_ID, ACTIVITY_LOG_MASTER.start_date AS ETL_START_TIME, 
                  ACTIVITY_LOG_MASTER.end_date AS ETL_END_TIME, CASE WHEN ACTIVITY_LOG_MASTER.refresh_ind='T' THEN 'SUCCESS' ELSE 'FAILURE' END AS ETL_STATUS, 
                  ACTIVITY_LOG_MASTER.REFRESH_DESCRIPTION AS ETL_NOTES, ETL_PROCESS.process_name AS ETL_PROCESS, ACTIVITY_LOG_DETAIL.start_date AS START_TIME, 
                  ACTIVITY_LOG_DETAIL.end_date AS END_TIME, ACTIVITY_LOG_DETAIL.admin_comment AS NOTES, 
                  ACTIVITY_LOG_DETAIL.source_row_count AS SOURCE_ROW_COUNT, ACTIVITY_LOG_DETAIL.destination_row_count AS DESTINATION_ROW_COUNT, 
                  ACTIVITY_LOG_DETAIL.row_count_insert AS INSERT_COUNT, ACTIVITY_LOG_DETAIL.row_count_update AS UPDATE_COUNT
FROM     ACTIVITY_LOG_DETAIL INNER JOIN
                  ACTIVITY_LOG_MASTER ON ACTIVITY_LOG_DETAIL.activity_log_master_uid = ACTIVITY_LOG_MASTER.activity_log_master_uid INNER JOIN
                  ETL_PROCESS ON ACTIVITY_LOG_DETAIL.process_uid = ETL_PROCESS.process_uid
WHERE  (ACTIVITY_LOG_MASTER.activity_log_master_uid IN
                      (SELECT MAX(activity_log_master_uid) AS Expr1
                       FROM      ACTIVITY_LOG_MASTER AS ACTIVITY_LOG_MASTER_1))

GO
