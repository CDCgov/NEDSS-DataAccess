USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_CLEAR_INV_OTHER]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_CLEAR_INV_OTHER] @Batch_id BIGINT
AS
    BEGIN
        DECLARE @RowCount_no INT;
        DECLARE @Proc_Step_no FLOAT= 0;
        DECLARE @Proc_Step_Name VARCHAR(200)= '';
        DECLARE @batch_start_time DATETIME2(7)= NULL;
        DECLARE @batch_end_time DATETIME2(7)= NULL;
        BEGIN TRY
            SET @Proc_Step_no = 1;
            SET @Proc_Step_Name = 'SP_Start';
            BEGIN TRANSACTION;
            INSERT INTO [dbo].[job_flow_log]
            (batch_id, 
             [Dataflow_Name], 
             [package_Name], 
             [Status_Type], 
             [step_number], 
             [step_name], 
             [row_count]
            )
            VALUES
            (@Batch_id, 
             'INV_OTHER', 
             'CLEAR_INV_OTHER', 
             'START', 
             @Proc_Step_no, 
             @Proc_Step_Name, 
             0
            );
            COMMIT TRANSACTION;
            SELECT @batch_start_time = batch_start_dttm, 
                   @batch_end_time = batch_end_dttm
            FROM [dbo].[job_batch_log]
            WHERE status_type = 'start';
            BEGIN TRANSACTION;
            IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_OTHER_TEMP', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_OTHER_TEMP;
            END;
            IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_COUNTY_TABLE_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_DATA_INV_OTHER_out', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_DATA_INV_OTHER_out;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_DESC_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_OTHER_TEMP', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_DESC_INV_OTHER_TEMP;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_MERGED_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_OTHER_EMPTY_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_OTHER_EMPTY_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_OTHER_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_OTHER_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_OTHER_NONEMPTY_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_OTHER_NONEMPTY_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_SNTEMP_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_STD_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_STD_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.CODED_TABLE_TEMP_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.CODED_TABLE_TEMP_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.DATE_DATA_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.DATE_DATA_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.DATE_DATA_INV_OTHER_out', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.DATE_DATA_INV_OTHER_out;
            END;
            IF OBJECT_ID('dbo.NUMERIC_BASE_DATA_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_BASE_DATA_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_2_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_2_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_MERGED_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_MERGED_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_OUT_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_OUT_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_PIVOT_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_PIVOT_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_TRANS_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS1_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.NUMERIC_DATA_TRANS1_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.PAGE_DATE_TABLE_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.PAGE_DATE_TABLE_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.RDB_UI_METADATA_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_OTHER_TEMP', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.RDB_UI_METADATA_INV_OTHER_TEMP;
            END;
            IF OBJECT_ID('dbo.Stageing_key_metadata_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.Stageing_key_metadata_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.STAGING_KEY_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.STAGING_KEY_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.STAGING_KEY_INV_OTHER_FINAL', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.STAGING_KEY_INV_OTHER_FINAL;
            END;
            IF OBJECT_ID('dbo.text_data_INV_OTHER', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.text_data_INV_OTHER;
            END;
            IF OBJECT_ID('dbo.text_data_INV_OTHER_out', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE dbo.text_data_INV_OTHER_out;
            END;
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            SET @Proc_Step_no = 999;
            SET @Proc_Step_Name = 'SP_COMPLETE';
            INSERT INTO [dbo].[job_flow_log]
            (batch_id, 
             [Dataflow_Name], 
             [package_Name], 
             [Status_Type], 
             [step_number], 
             [step_name], 
             [row_count]
            )
            VALUES
            (@Batch_id, 
             'INV_OTHER', 
             'S_INV_OTHER', 
             'COMPLETE', 
             @Proc_Step_no, 
             @Proc_Step_name, 
             @RowCount_no
            );
            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            DECLARE @ErrorNumber INT= ERROR_NUMBER();
            DECLARE @ErrorLine INT= ERROR_LINE();
            DECLARE @ErrorMessage NVARCHAR(4000)= ERROR_MESSAGE();
            DECLARE @ErrorSeverity INT= ERROR_SEVERITY();
            DECLARE @ErrorState INT= ERROR_STATE();
            INSERT INTO [dbo].[job_flow_log]
            (batch_id, 
             [Dataflow_Name], 
             [package_Name], 
             [Status_Type], 
             [step_number], 
             [step_name], 
             [Error_Description], 
             [row_count]
            )
            VALUES
            (@Batch_id, 
             'INV_OTHER', 
             'S_INV_OTHER', 
             'ERROR', 
             @Proc_Step_no, 
             'ERROR - ' + @Proc_Step_name, 
             'Step -' + CAST(@Proc_Step_no AS VARCHAR(3)) + ' -' + CAST(@ErrorMessage AS VARCHAR(500)), 
             0
            );
            RETURN -1;
        END CATCH;
    END;
GO
