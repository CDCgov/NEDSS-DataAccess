USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[sp_S_INV_TRAVEL]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_S_INV_TRAVEL] 
				 @Batch_id bigint
AS
BEGIN
	DECLARE @RowCount_no int;
	DECLARE @Proc_Step_no float= 0;
	DECLARE @Proc_Step_Name varchar(200)= '';
	DECLARE @batch_start_time datetime2(7)= NULL;
	DECLARE @batch_end_time datetime2(7)= NULL;
	BEGIN TRY
		SET @Proc_Step_no = 1;
		SET @Proc_Step_Name = 'SP_Start';

		BEGIN TRANSACTION;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, 0 );

		COMMIT TRANSACTION;

		SELECT @batch_start_time = batch_start_dttm, @batch_end_time = batch_end_dttm
		FROM [dbo].[job_batch_log]
		WHERE status_type = 'start' AND 
			  type_code = 'MasterETL';
		
		/** TODO: (Upasana) Commented for Change Data Capture- Start: Processing logic **/
		SELECT cdc_topic_updated_datetime, nbs_case_answer_uid, cdc_id 
		INTO #TMP_CDC_NBS_case_answer
		FROM nbs_changedata.dbo.NBS_case_answer
		WHERE cdc_status <> 1 AND cdc_topic_updated_datetime<=@batch_end_time;
	
		SELECT cdc_topic_updated_datetime, public_health_case_uid, cdc_id 
		INTO #TMP_CDC_Public_health_case
		FROM nbs_changedata.dbo.Public_health_case
		WHERE cdc_status <> 1 AND cdc_topic_updated_datetime<=@batch_end_time;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 2;
		SET @Proc_Step_Name = ' Generating rdb_ui_metadata_INV_TRAVEL'; 
		-- create table rdb_ui_metadata_INV_TRAVEL as 
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;

		SELECT NRDBM.RDB_COLUMN_NM, NUIM.NBS_QUESTION_UID, NUIM.CODE_SET_GROUP_ID, NUIM.INVESTIGATION_FORM_CD,
		--CODE_SET_GROUP_ID,
		QUESTION_GROUP_SEQ_NBR, DATA_TYPE
		INTO dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), NBS_ODSE.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE( NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			   NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			   QUESTION_GROUP_SEQ_NBR IS NULL AND 
			   UPPER(DATA_TYPE) = 'TEXT'
			 ) OR 
			 ( NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			   NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			   QUESTION_GROUP_SEQ_NBR IS NULL AND 
			   RDB_COLUMN_NM LIKE '%_CD'
			 );
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;

		SELECT *
		INTO [dbo].RDB_UI_METADATA_INV_TRAVEL
		FROM
		(
			SELECT *, ROW_NUMBER() OVER(PARTITION BY NBS_QUESTION_UID
			ORDER BY NBS_QUESTION_UID) AS rowid
			FROM [dbo].RDB_UI_METADATA_INV_TRAVEL_TEMP
		) AS Der
		WHERE rowid = 1;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 3;
		SET @Proc_Step_Name = ' Generating text_data_INV_TRAVEL'; 
		-- CREATE TABLE text_data_INV_TRAVEL AS
		IF OBJECT_ID('dbo.text_data_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.text_data_INV_TRAVEL;
		END;

		SELECT DISTINCT 
			   PA.NBS_CASE_ANSWER_UID, meta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(REPLACE(ANSWER_TXT, CHAR(13) + CHAR(10), ' ') AS varchar(2000)) AS ANSWER_TXT, COALESCE(ACT_UID, 1) AS PAGE_CASE_UID_TEXT, PA.RECORD_STATUS_CD, meta.NBS_QUESTION_UID
		INTO dbo.text_data_INV_TRAVEL
		FROM dbo.RDB_UI_METADATA_INV_TRAVEL AS meta WITH(NOLOCK) LEFT
			 OUTER JOIN
			 nbs_changedata.dbo.NBS_CASE_ANSWER AS PA WITH(NOLOCK)
			 ON meta.nbs_question_uid = PA.nbs_question_uid AND 
				pa.ANSWER_GROUP_SEQ_NBR IS NULL LEFT
																	 OUTER JOIN
																	 dbo.PHC_UIDS WITH(NOLOCK)
																	 ON PHC_UIDS.PAGE_CASE_UID = PA.act_uid
												INNER JOIN
												NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
												ON UPPER(CVG.CODE) = UPPER(meta.DATA_TYPE)
		WHERE CVG.CODE_SET_NM = 'NBS_DATA_TYPE' AND 
			  CODE IN( 'CODED', 'TEXT' )
			  AND (PA.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer)
								)  
		--ORDER BY ACT_UID,NBS_CASE_ANSWER_UID, meta.CODE_SET_GROUP_ID
		ORDER BY PA.NBS_CASE_ANSWER_UID, meta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(REPLACE(ANSWER_TXT, CHAR(13) + CHAR(10), ' ') AS varchar(2000)), COALESCE(ACT_UID, 1), PA.RECORD_STATUS_CD, meta.NBS_QUESTION_UID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 4;
		SET @Proc_Step_Name = ' Generating text_data_INV_TRAVEL_out';
		IF OBJECT_ID('dbo.text_data_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.text_data_INV_TRAVEL_out;
		END;
		DECLARE @columns nvarchar(max);
		DECLARE @sql nvarchar(max);
		SET @columns = N'';
		SELECT @columns+=N', p.' + QUOTENAME(LTRIM(RTRIM([RDB_COLUMN_NM])))
		FROM
		(
			SELECT [RDB_COLUMN_NM]
			FROM [dbo].text_data_INV_TRAVEL AS p
			GROUP BY [RDB_COLUMN_NM]
		) AS x;
		SET @sql = N'
SELECT [PAGE_CASE_UID_text] as PAGE_CASE_UID_text, ' + STUFF(@columns, 1, 2, '') + ' into dbo.text_data_INV_TRAVEL_out ' + 'FROM (
SELECT [PAGE_CASE_UID_text], [answer_txt] , [RDB_COLUMN_NM] 
 FROM [dbo].text_data_INV_TRAVEL
	group by [PAGE_CASE_UID_text], [answer_txt] , [RDB_COLUMN_NM] 
		) AS j PIVOT (max(answer_txt) FOR [RDB_COLUMN_NM] in 
	   (' + STUFF(REPLACE(@columns, ', p.[', ',['), 1, 1, '') + ')) AS p;';
		PRINT @sql;
		EXEC sp_executesql @sql;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;
	
		---********************************************

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 5;
		SET @Proc_Step_Name = ' Generating CODED rdb_ui_metadata_INV_TRAVEL'; 
	
		--create table rdb_ui_metadata as 
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;

		SELECT DISTINCT 
			   NUIM.NBS_QUESTION_UID, NRDBM.RDB_COLUMN_NM, NUIM.CODE_SET_GROUP_ID, NUIM.unit_value, NUIM.INVESTIGATION_FORM_CD, CODE_SET_GROUP_ID AS CODE_SET_GROUP_ID1, QUESTION_GROUP_SEQ_NBR, DATA_TYPE, OTHER_VALUE_IND_CD
		INTO dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), nbs_odse.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			  NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			  QUESTION_GROUP_SEQ_NBR IS NULL AND 
			  ( UPPER(DATA_TYPE) = 'CODED' OR 
				UPPER(UNIT_TYPE_CD) = 'CODED' OR 
				mask = 'NUM_TEMP'
			  ) AND 
			  RDB_COLUMN_NM NOT LIKE '%_CD'
		ORDER BY NUIM.NBS_QUESTION_UID, NRDBM.RDB_COLUMN_NM, NUIM.CODE_SET_GROUP_ID, NUIM.unit_value DESC;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;

		SELECT *
		INTO [dbo].RDB_UI_METADATA_INV_TRAVEL
		FROM
		(
			SELECT DISTINCT 
				   *, ROW_NUMBER() OVER(PARTITION BY INVESTIGATION_FORM_CD, NBS_QUESTION_UID
				   ORDER BY NBS_QUESTION_UID, unit_value DESC, other_value_ind_cd DESC) AS rowid
			FROM [dbo].RDB_UI_METADATA_INV_TRAVEL_TEMP
		) AS Der
		WHERE rowid = 1;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			UPDATE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
			  SET data_type = 'CODED', CODE_SET_GROUP_ID = unit_value
			WHERE CODE_SET_GROUP_ID IS NULL AND 
				  ISNUMERIC(unit_value) = 1;
		END;

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 6;
		SET @Proc_Step_Name = ' Generating CODED rdb_ui_metadata_INV_TRAVEL'; 
		-- CREATE TABLE CODED_TABLE AS
		IF OBJECT_ID('dbo.CASE_ANSWER_PHC_UIDS', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CASE_ANSWER_PHC_UIDS;
		END;

		SELECT DISTINCT 
			   NBS_CASE_ANSWER.NBS_CASE_ANSWER_UID, NBS_CASE_ANSWER.RECORD_STATUS_CD, PHC_UIDS.PAGE_CASE_UID AS 'PAGE_CASE_UID', PHC_UIDS.INVESTIGATION_FORM_CD, ANSWER_TXT, NBS_CASE_ANSWER.nbs_question_uid
		INTO dbo.CASE_ANSWER_PHC_UIDS
		FROM nbs_changedata.dbo.NBS_CASE_ANSWER NBS_CASE_ANSWER
			 INNER JOIN
			 NBS_ODSE.dbo.NBS_UI_METADATA
			 ON NBS_UI_METADATA.NBS_QUESTION_UID = NBS_CASE_ANSWER.NBS_QUESTION_UID
			 INNER JOIN
			 dbo.PHC_UIDS
			 ON PHC_UIDS.PAGE_CASE_UID = NBS_CASE_ANSWER.ACT_UID 
 		 INNER JOIN NBS_ODSE.DBO.NBS_RDB_METADATA ON NBS_RDB_METADATA.NBS_UI_METADATA_UID = NBS_ui_metadata.NBS_UI_METADATA_UID
		WHERE UPPER(data_type) = 'CODED' AND 
			  PHC_UIDS.INVESTIGATION_FORM_CD = NBS_UI_METADATA.INVESTIGATION_FORM_CD AND 
	                 ANSWER_GROUP_SEQ_NBR IS NULL  and rdb_table_nm = 'D_INV_TRAVEL' 
	                AND (NBS_CASE_ANSWER.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer)
								)
	                 ;

			  
			  

		CREATE NONCLUSTERED INDEX [idx_CASE_ANSWER_PHC_UIDS]
          ON [dbo].[CASE_ANSWER_PHC_UIDS] ([INVESTIGATION_FORM_CD],[nbs_question_uid])
          INCLUDE ([NBS_CASE_ANSWER_UID],[RECORD_STATUS_CD],[PAGE_CASE_UID],[ANSWER_TXT])
		;

		IF OBJECT_ID('dbo.CODED_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_INV_TRAVEL;
		END;
		SELECT DISTINCT 
			   NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(ANSWER_TXT AS varchar(2000)) AS ANSWER_TXT, PAGE_CASE_UID, PA.RECORD_STATUS_CD, rmeta.NBS_QUESTION_UID, OTHER_VALUE_IND_CD, CAST(NULL AS [varchar](256)) AS ANSWER_OTH, CAST(NULL AS [varchar](256)) AS ANSWER_TXT1, PA.INVESTIGATION_FORM_CD
		INTO dbo.CODED_TABLE_INV_TRAVEL
		FROM dbo.RDB_UI_METADATA_INV_TRAVEL AS rmeta WITH(NOLOCK) LEFT
			 OUTER JOIN
			 dbo.CASE_ANSWER_PHC_UIDS AS PA WITH(NOLOCK)
			 ON rmeta.nbs_question_uid = PA.nbs_question_uid AND 
				PA.INVESTIGATION_FORM_CD = rmeta.investigation_form_cd
																	  INNER JOIN
																	  NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
																	  ON UPPER(CVG.CODE) = UPPER(rmeta.DATA_TYPE)
		WHERE CVG.CODE_SET_NM = 'NBS_DATA_TYPE' 
		--AND UPPER(data_type) = 'CODED'
		ORDER BY PAGE_CASE_UID, NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID;

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 7;
		SET @Proc_Step_Name = ' Update CODED rdb_ui_metadata_INV_TRAVEL';
		UPDATE dbo.coded_table_INV_TRAVEL
		  SET ANSWER_OTH = SUBSTRING(ANSWER_TXT, CHARINDEX('^', ANSWER_TXT) + 1, LEN(RTRIM(LTRIM(ANSWER_TXT))))
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.coded_table_INV_TRAVEL
		  SET ANSWER_TXT = SUBSTRING(ANSWER_TXT, 1, ( CHARINDEX('^', ANSWER_TXT) - 1 ))
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.coded_table_INV_TRAVEL
		  SET ANSWER_TXT = 'OTH'
		WHERE UPPER(ANSWER_TXT) LIKE 'OTH^%';
		SELECT @RowCount_no = -1;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 8;
		SET @Proc_Step_Name = ' Create table CODED_TABLE_OTHER_EMPTY_INV_TRAVEL'; 
		--CREATE TABLE 	CODED_TABLE_OTHER_EMPTY 
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL;
		END;

		SELECT DISTINCT 
			   CODED.CODE_SET_GROUP_ID, PAGE_CASE_UID, NBS_QUESTION_UID, RDB_COLUMN_NM, NBS_CASE_ANSWER_UID, '' AS ANSWER_TXT, ANSWER_OTH AS ANSWER_DESC11, OTHER_VALUE_IND_CD
		INTO dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL
		FROM dbo.CODED_TABLE_INV_TRAVEL AS CODED
		WHERE OTHER_VALUE_IND_CD = 'T' AND 
			  ( COALESCE(ANSWER_TXT, '') <> 'OTH' OR 
				LEN(COALESCE(answer_txt, '')) = 0
			  );
		--ORDER BY NBS_CASE_ANSWER_UID, RDB_COLUMN_NM
		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [RDB_PERF_INTERNAL_03]
ON [dbo].[CODED_TABLE_OTHER_EMPTY_INV_TRAVEL]
		( [PAGE_CASE_UID] ASC, [RDB_COLUMN_NM] ASC
		);

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 9;
		SET @Proc_Step_Name = ' create table CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL'; 
		-- CREATE TABLE CODED_TABLE_OTHER_NONEMPTY 
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL;
		END;

		SELECT DISTINCT 
			   CODED.CODE_SET_GROUP_ID, PAGE_CASE_UID, NBS_QUESTION_UID, RDB_COLUMN_NM, NBS_CASE_ANSWER_UID, ANSWER_TXT, ANSWER_OTH AS ANSWER_DESC11, OTHER_VALUE_IND_CD
		INTO dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL
		FROM dbo.CODED_TABLE_INV_TRAVEL AS CODED
		WHERE OTHER_VALUE_IND_CD = 'T' AND 
			  ( ANSWER_OTH IS NOT NULL OR 
				ANSWER_TXT LIKE 'OTH^%'
			  )
		ORDER BY NBS_CASE_ANSWER_UID, RDB_COLUMN_NM;
		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [RDB_PERF_INTERNAL_03]
ON [dbo].CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL
		( [PAGE_CASE_UID] ASC, [RDB_COLUMN_NM] ASC
		);

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 10;
		SET @Proc_Step_Name = ' delete from CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL';
		DELETE CODED_TABLE_OTHER_EMPTY_INV_TRAVEL
		FROM dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL A
			 INNER JOIN
			 dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL B
			 ON A.PAGE_CASE_UID = B.PAGE_CASE_UID AND 
				A.RDB_COLUMN_NM = B.RDB_COLUMN_NM;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 11;
		SET @Proc_Step_Name = ' create table CODED_TABLE_OTHER_INV_TRAVEL';
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_INV_TRAVEL;
		END;

		SELECT COALESCE(cne.[NBS_QUESTION_UID], ce.[NBS_QUESTION_UID]) AS [NBS_QUESTION_UID], COALESCE(cne.[RDB_COLUMN_NM], ce.[RDB_COLUMN_NM]) AS [RDB_COLUMN_NM], COALESCE(cne.[CODE_SET_GROUP_ID], ce.[CODE_SET_GROUP_ID]) AS [CODE_SET_GROUP_ID], COALESCE(cne.[PAGE_CASE_UID], ce.[PAGE_CASE_UID]) AS [PAGE_CASE_UID], COALESCE(cne.[NBS_CASE_ANSWER_UID], ce.[NBS_CASE_ANSWER_UID]) AS [NBS_CASE_ANSWER_UID], COALESCE(cne.[ANSWER_TXT], ce.[ANSWER_TXT]) AS [ANSWER_TXT], COALESCE(cne.[ANSWER_DESC11], ce.[ANSWER_DESC11]) AS [ANSWER_DESC11], COALESCE(cne.[OTHER_VALUE_IND_CD], ce.[OTHER_VALUE_IND_CD]) AS [OTHER_VALUE_IND_CD], CAST(NULL AS [varchar](30)) AS RDB_COLUMN_NM2
		INTO dbo.CODED_TABLE_OTHER_INV_TRAVEL
		FROM [dbo].[CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL] AS cne
			 FULL OUTER JOIN
			 [dbo].[CODED_TABLE_OTHER_EMPTY_INV_TRAVEL] AS ce
			 ON cne.NBS_CASE_ANSWER_UID = ce.NBS_CASE_ANSWER_UID AND 
				cne.[RDB_COLUMN_NM] = ce.[RDB_COLUMN_NM];
		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [RDB_PERF_INTERNAL_01]
ON [dbo].[CODED_TABLE_OTHER_INV_TRAVEL]
		( [OTHER_VALUE_IND_CD]
		) 
			   INCLUDE( [RDB_COLUMN_NM] );

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 12;
		SET @Proc_Step_Name = ' update table CODED_TABLE_OTHER_INV_TRAVEL';
		UPDATE dbo.CODED_TABLE_OTHER_INV_TRAVEL
		  SET RDB_COLUMN_NM = REPLACE(SUBSTRING(RDB_COLUMN_NM, 1, 26), ' ', '') + '_OTH'
		WHERE OTHER_VALUE_IND_CD = 'T';
		UPDATE dbo.CODED_TABLE_OTHER_INV_TRAVEL
		  SET ANSWER_TXT = ''
		WHERE( OTHER_VALUE_IND_CD = 'T' AND 
			   ANSWER_TXT <> 'OTH'
			 );
		SELECT @RowCount_no = -1;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 12;
		SET @Proc_Step_Name = ' create table CODED_TABLE_STD_INV_TRAVEL'; 
		--CREATE TABLE 	CODED_TABLE_STD 
		IF OBJECT_ID('dbo.CODED_TABLE_STD_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_STD_INV_TRAVEL;
		END;
		SELECT DISTINCT 
			   CODED.CODE_SET_GROUP_ID, PAGE_CASE_UID, coded.NBS_QUESTION_UID, NBS_CASE_ANSWER_UID, ANSWER_TXT, METADATA.CODE_SET_NM, RDB_COLUMN_NM, ANSWER_OTH, METADATA.CODE, CODE_SHORT_DESC_TXT AS ANSWER_TXT1
		INTO dbo.CODED_TABLE_STD_INV_TRAVEL
		FROM dbo.coded_table_INV_TRAVEL AS CODED WITH(NOLOCK) LEFT
			 OUTER JOIN
			 REF_FORMCODE_TRANSLATION AS METADATA WITH(NOLOCK)
			 ON METADATA.INVESTIGATION_FORM_CD = CODED.INVESTIGATION_FORM_CD AND 
				METADATA.CODE_SET_GROUP_ID = CODED.CODE_SET_GROUP_ID AND 
				METADATA.CODE = CODED.ANSWER_TXT and METADATA.NBS_QUESTION_UID=coded.NBS_QUESTION_UID

		UNION
		SELECT DISTINCT 
			   CODED.CODE_SET_GROUP_ID, PAGE_CASE_UID, NBS_QUESTION_UID, NBS_CASE_ANSWER_UID, ANSWER_TXT, 'COUNTY_CCD' AS CODE_SET_NM, RDB_COLUMN_NM, ANSWER_OTH, CODED.ANSWER_TXT, '' AS ANSWER_TXT1
		--INTO dbo.CODED_TABLE_STD_INV_TRAVEL
		FROM dbo.coded_table_INV_TRAVEL AS CODED WITH(NOLOCK)
		WHERE CODED.CODE_SET_GROUP_ID IN
		(
			SELECT code_set_group_id
			FROM nbs_srte..codeset
			WHERE CLASS_CD = 'V_State_county_code_value'
		)
		ORDER BY NBS_CASE_ANSWER_UID, RDB_COLUMN_NM;

		DELETE FROM dbo.CODED_TABLE_STD_INV_TRAVEL
		WHERE ANSWER_TXT IS NOT NULL AND 
			  ANSWER_TXT1 IS NULL AND 
			  CODE_SET_NM IS NULL;

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 13;
		SET @Proc_Step_Name = ' create table RDB_UI_METADATA_INV_TRAVEL'; 
 
		--CREATE TABLE RDB_UI_METADATA AS 
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;

		SELECT DISTINCT 
			   NUIM.NBS_QUESTION_UID, NRDBM.RDB_COLUMN_NM, NUIM.CODE_SET_GROUP_ID, NUIM.UNIT_VALUE, NUIM.INVESTIGATION_FORM_CD, MASK, CODE_SET_GROUP_ID AS CODE_SET_GROUP_ID1, QUESTION_GROUP_SEQ_NBR, DATA_TYPE, UNIT_VALUE AS UNIT_VALUE1, UNIT_TYPE_CD
		INTO dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), nbs_odse.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			  NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			  QUESTION_GROUP_SEQ_NBR IS NULL AND 
			  ( ( UPPER(DATA_TYPE) = 'NUMERIC' AND 
				  UNIT_VALUE IS NOT NULL AND 
				  unit_type_cd != 'LITERAL'
				) OR 
				( UPPER(DATA_TYPE) = 'NUMERIC' AND 
				  UPPER(mask) = 'NUM' AND 
				  unit_type_cd = 'LITERAL'
				)
			  ) AND 
			  RDB_COLUMN_NM NOT LIKE '%_CD';
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;

		SELECT *
		INTO [dbo].RDB_UI_METADATA_INV_TRAVEL
		FROM
		(
			SELECT DISTINCT 
				   *, ROW_NUMBER() OVER(PARTITION BY NBS_QUESTION_UID
				   ORDER BY NBS_QUESTION_UID) AS rowid
			FROM [dbo].RDB_UI_METADATA_INV_TRAVEL_TEMP
		) AS Der;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 14;
		SET @Proc_Step_Name = ' Update  RDB_UI_METADATA_INV_TRAVEL';
		UPDATE dbo.RDB_UI_METADATA_INV_TRAVEL
		  SET CODE_SET_GROUP_ID = unit_value
		WHERE RTRIM(CODE_SET_GROUP_ID) IS NULL AND 
			  UNIT_TYPE_CD = 'CODED';
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 15;
		SET @Proc_Step_Name = ' Create table CODED_TABLE_SNTEMP_INV_TRAVEL'; 
		-- CREATE TABLE CODED_TABLE_SNTEMP AS
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_INV_TRAVEL;
		END;

		SELECT PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(ANSWER_TXT AS varchar(2000)) AS ANSWER_TXT, ACT_UID AS 'PAGE_CASE_UID', PA.RECORD_STATUS_CD, rmeta.NBS_QUESTION_UID, MASK, CAST(NULL AS [varchar](2000)) AS ANSWER_TXT_CODE, CAST(NULL AS [varchar](2000)) AS ANSWER_VALUE, PHC_UIDS.INVESTIGATION_FORM_CD
		INTO dbo.CODED_TABLE_SNTEMP_INV_TRAVEL
		FROM dbo.RDB_UI_METADATA_INV_TRAVEL AS rmeta WITH(NOLOCK) LEFT
			 OUTER JOIN
			 nbs_changedata.dbo.NBS_CASE_ANSWER AS PA WITH(NOLOCK)
			 ON rmeta.nbs_question_uid = PA.nbs_question_uid AND 
				pa.ANSWER_GROUP_SEQ_NBR IS NULL LEFT
																	  OUTER JOIN
																	  PHC_UIDS
																	  ON PHC_UIDS.PAGE_CASE_UID = PA.act_uid
												INNER JOIN
												NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
												ON UPPER(CVG.CODE) = UPPER(rmeta.DATA_TYPE)
		WHERE CVG.CODE_SET_NM = 'NBS_DATA_TYPE' AND 
			  UPPER(data_type) = 'NUMERIC'
			  AND (PA.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer)
								)
		ORDER BY ACT_UID, PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 17;
		SET @Proc_Step_Name = ' Ceate TABLE   CODED_TABLE_SNTEMP_INV_TRAVEL';
		UPDATE dbo.CODED_TABLE_SNTEMP_INV_TRAVEL
		  SET ANSWER_TXT_CODE = SUBSTRING(ANSWER_TXT, CHARINDEX('^', ANSWER_TXT) + 1, LEN(RTRIM(ANSWER_TXT))), ANSWER_VALUE = REPLACE(SUBSTRING(ANSWER_TXT, 1, ( CHARINDEX('^', ANSWER_TXT) - 1 )), ',', '')
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.CODED_TABLE_SNTEMP_INV_TRAVEL
		  SET ANSWER_VALUE = answer_txt
		WHERE ISNUMERIC(answer_txt) = 1;

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;
	BEGIN TRANSACTION;
		SET @Proc_Step_no = 18;
		SET @Proc_Step_Name = ' LOG Invalid Numeric data INTO ETL_DQ_LOG';
	
		INSERT INTO dbo.ETL_DQ_LOG( EVENT_TYPE, EVENT_LOCAL_ID, EVENT_UID, DQ_ISSUE_CD, DQ_ISSUE_DESC_TXT, DQ_ISSUE_QUESTION_IDENTIFIER, DQ_ISSUE_ANSWER_TXT, DQ_ISSUE_RDB_LOCATION, JOB_BATCH_LOG_UID, DQ_ETL_PROCESS_TABLE, DQ_ETL_PROCESS_COLUMN, DQ_STATUS_TIME, DQ_ISSUE_SOURCE_LOCATION, DQ_ISSUE_SOURCE_QUESTION_LABEL )
		(
		SELECT DISTINCT 'INVESTIGATION', PUBLIC_HEALTH_CASE.LOCAL_ID, PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID, 
		'INVALID_NUMERIC_VALUE', 'BAD NUMERIC VALUE: A non-numeric value exists in a field expecting a numeric value and requires update. Please correct the bad numeric value so that it can be properly written to the reporting database during the next ETL run', 
		NBS_UI_METADATA.QUESTION_IDENTIFIER, ANSWER_VALUE, NBS_ui_metadata.DATA_LOCATION, @Batch_id, NBS_rdb_metadata.rdb_table_nm, NBS_rdb_metadata.RDB_COLUMN_NM, 
		GETDATE(), NBS_ui_metadata.DATA_LOCATION, QUESTION_LABEL
					FROM dbo.CODED_TABLE_SNTEMP_INV_TRAVEL
			 INNER JOIN
				 nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE
				 ON CODED_TABLE_SNTEMP_INV_TRAVEL.page_case_uid= PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID
				 INNER JOIN
				 NBS_SRTE.DBO.CONDITION_CODE
				 ON CONDITION_CODE.CONDITION_CD = PUBLIC_HEALTH_CASE.CD
				  INNER JOIN
				 nbs_odse.dbo.NBS_rdb_metadata
				 ON NBS_rdb_metadata.rdb_column_nm = CODED_TABLE_SNTEMP_INV_TRAVEL.rdb_column_nm
				 INNER JOIN
				 NBS_ODSE.DBO.NBS_UI_METADATA
				 ON NBS_rdb_metadata.NBS_UI_METADATA_UID = NBS_ui_metadata.NBS_UI_METADATA_UID AND 
					CONDITION_CODE.INVESTIGATION_FORM_CD = NBS_ui_metadata.INVESTIGATION_FORM_CD
			WHERE (isNumeric(ANSWER_VALUE) != 1) AND 
				  ANSWER_VALUE IS NOT NULL
				 AND (PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID in (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case)
								));
		
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );
		COMMIT TRANSACTION;
		BEGIN TRANSACTION;
		SET @Proc_Step_no = 19;
		SET @Proc_Step_Name = ' Update  CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL'; 
		--CREATE TABLE CODED_TABLE_SNTEMP_TRANS_A
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL;
		END;

		SELECT 
		--CODED.CODE_SET_GROUP_ID, 
		  PAGE_CASE_UID, ANSWER_TXT_CODE, ANSWER_VALUE, NBS_CASE_ANSWER_UID, METADATA.CODE_SET_NM, RDB_COLUMN_NM, METADATA.CODE, CODE_SHORT_DESC_TXT AS 'ANSWER_TXT2', MASK, coded.NBS_QUESTION_UID, CAST(NULL AS varchar(2000)) AS ANSWER_TXT, METADATA.INVESTIGATION_FORM_CD
		INTO dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
		FROM dbo.CODED_TABLE_SNTEMP_INV_TRAVEL AS CODED WITH(NOLOCK) LEFT
			 JOIN
			 REF_FORMCODE_TRANSLATION AS METADATA WITH(NOLOCK)
			 ON METADATA.INVESTIGATION_FORM_CD = CODED.INVESTIGATION_FORM_CD AND 
				METADATA.CODE_SET_GROUP_ID = CODED.CODE_SET_GROUP_ID AND 
				METADATA.CODE = CODED.ANSWER_TXT_CODE
			 ORDER BY NBS_CASE_ANSWER_UID, RDB_COLUMN_NM;
		SELECT @RowCount_no = @@ROWCOUNT;
		UPDATE CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL SET ANSWER_VALUE=NULL WHERE  ISNUMERIC(ANSWER_VALUE)!=1;
		UPDATE CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
		  SET CODE_SET_NM = a.CODE_SET_NM, ANSWER_TXT_CODE = '', ANSWER_TXT2 = '', code = ''
		FROM
		(
			SELECT TOP 1 CODE_SET_NM, NBS_QUESTION_UID
			FROM CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
			WHERE CODE_SET_NM IS NOT NULL
		) AS a
		WHERE CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL.NBS_QUESTION_UID = a.NBS_QUESTION_UID AND 
			  CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL.code_set_nm IS NULL;

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;


		BEGIN TRANSACTION;
		SET @Proc_Step_no = 20;
		SET @Proc_Step_Name = ' UPDATE TABLE  CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL';

		UPDATE dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
		  SET ANSWER_TXT = REPLACE(ANSWER_VALUE, ' ', '')
		WHERE LEN(mask) > 0;
		UPDATE dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
		  SET ANSWER_TXT = REPLACE(ANSWER_VALUE, ' ', '') + ' ' + REPLACE(ANSWER_TXT2, ' ', '')
		WHERE LEN(mask) = 0;
		SELECT @RowCount_no = -1;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 21;
		SET @Proc_Step_Name = ' UPDATE TABLE  CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL';
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL;
		END;

		SELECT *
		INTO dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL
		FROM dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 22;
		SET @Proc_Step_Name = ' UPDATE TABLE  CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL';
		UPDATE dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL
		  SET RDB_COLUMN_NM = REPLACE(SUBSTRING(RDB_COLUMN_NM, 1, 25) + '_UNIT', ' ', ''), ANSWER_TXT = REPLACE(ANSWER_TXT2, '  ', ' ')
		WHERE LEN(mask) > 0;
		--alter table  dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL     drop column CODE_SET_GROUP_ID	;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 23;
		SET @Proc_Step_Name = ' CREATE TABLE  CODED_TABLE_INV_TRAVEL';
		IF OBJECT_ID('dbo.CODED_TABLE_TEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_TEMP_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_INV_TRAVEL;
		END;

		SELECT COALESCE(csnta.NBS_CASE_ANSWER_UID, csntc.NBS_CASE_ANSWER_UID) AS NBS_CASE_ANSWER_UID, COALESCE(csnta.RDB_COLUMN_NM, csntc.RDB_COLUMN_NM) AS RDB_COLUMN_NM, COALESCE(csnta.PAGE_CASE_UID, csntc.PAGE_CASE_UID) AS PAGE_CASE_UID, COALESCE(csnta.ANSWER_TXT_CODE, csntc.ANSWER_TXT_CODE) AS ANSWER_TXT_CODE, COALESCE(csnta.ANSWER_VALUE, csntc.ANSWER_VALUE) AS ANSWER_VALUE, COALESCE(csnta.CODE_SET_NM, csntc.CODE_SET_NM) AS CODE_SET_NM, COALESCE(csnta.CODE, csntc.CODE) AS CODE, COALESCE(csnta.ANSWER_TXT2, csntc.ANSWER_TXT2) AS ANSWER_TXT2, COALESCE(csnta.MASK, csntc.MASK) AS MASK, COALESCE(csnta.ANSWER_TXT, csntc.ANSWER_TXT) AS ANSWER_TXT, COALESCE(csnta.nbs_question_uid, csntc.nbs_question_uid) AS nbs_question_uid
		INTO dbo.CODED_TABLE_TEMP_INV_TRAVEL
		FROM [dbo].CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL AS csnta
			 FULL OUTER JOIN
			 [dbo].CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL AS csntc
			 ON csnta.NBS_CASE_ANSWER_UID = csntc.NBS_CASE_ANSWER_UID AND 
				csnta.[RDB_COLUMN_NM] = csntc.[RDB_COLUMN_NM];
		SELECT CODE_SET_GROUP_ID, COALESCE(csnta.PAGE_CASE_UID, csntt.[PAGE_CASE_UID]) AS [PAGE_CASE_UID], COALESCE(csnta.NBS_QUESTION_UID, csntt.NBS_QUESTION_UID) AS [NBS_QUESTION_UID], COALESCE(csnta.NBS_QUESTION_UID, csntt.[NBS_CASE_ANSWER_UID]) AS [NBS_CASE_ANSWER_UID], COALESCE(csnta.ANSWER_TXT, csntt.[ANSWER_TXT]) AS [ANSWER_TXT], COALESCE(csnta.CODE_SET_NM, csntt.[CODE_SET_NM]) AS [CODE_SET_NM], COALESCE(csnta.RDB_COLUMN_NM, csntt.[RDB_COLUMN_NM]) AS [RDB_COLUMN_NM], ANSWER_OTH, COALESCE(csnta.CODE, csntt.[CODE]) AS [CODE], csnta.ANSWER_TXT1, csntt.[ANSWER_TXT_CODE], csntt.[ANSWER_VALUE], csntt.[ANSWER_TXT2], csntt.[MASK]
		INTO dbo.CODED_TABLE_INV_TRAVEL
		FROM [dbo].CODED_TABLE_STD_INV_TRAVEL AS csnta
			 FULL OUTER JOIN
			 [dbo].CODED_TABLE_TEMP_INV_TRAVEL AS csntt
			 ON csnta.NBS_CASE_ANSWER_UID = csntt.NBS_CASE_ANSWER_UID AND 
				csnta.[RDB_COLUMN_NM] = csntt.[RDB_COLUMN_NM];
		SELECT @RowCount_no = @@ROWCOUNT;
		CREATE NONCLUSTERED INDEX [RDB_PERF_INTERNAL_02]
ON [dbo].[CODED_TABLE_INV_TRAVEL]
		( [CODE_SET_GROUP_ID]
		) 
			   INCLUDE( [PAGE_CASE_UID], [NBS_QUESTION_UID], [NBS_CASE_ANSWER_UID], [ANSWER_TXT], [RDB_COLUMN_NM], [ANSWER_OTH] );

		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 24;
		SET @Proc_Step_Name = ' UPDATE TABLE  CODED_TABLE_INV_TRAVEL';
		UPDATE dbo.CODED_TABLE_INV_TRAVEL
		  SET ANSWER_TXT1 = ANSWER_TXT
		WHERE RTRIM(ANSWER_TXT1) = '';
		IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_DESC_INV_TRAVEL_TEMP;
		END;

		SELECT p1.PAGE_CASE_UID, p1.NBS_QUESTION_UID, STUFF(
		(
			SELECT TOP 10 ' | ' + ANSWER_TXT1
			FROM [dbo].[CODED_TABLE_INV_TRAVEL] AS p2
			WHERE p2.PAGE_CASE_UID = p1.PAGE_CASE_UID AND 
				  p2.nbs_question_uid = p1.NBS_QUESTION_UID
			ORDER BY PAGE_CASE_UID, NBS_QUESTION_UID, NBS_CASE_ANSWER_UID FOR XML PATH(''), TYPE
		).value( '.', 'varchar(2000)' ), 1, 3, '') AS ANSWER_DESC11
		INTO [dbo].[CODED_TABLE_DESC_INV_TRAVEL_TEMP]
		FROM [dbo].[CODED_TABLE_INV_TRAVEL] AS p1
		--where  nbs_question_uid is not null
		GROUP BY PAGE_CASE_UID, RDB_COLUMN_NM, NBS_QUESTION_UID;
		IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_DESC_INV_TRAVEL;
		END;

		SELECT ct.*, COALESCE(ctt.answer_desc11, ct.answer_txt1) AS answer_desc11
		INTO dbo.CODED_TABLE_DESC_INV_TRAVEL
		FROM [dbo].[CODED_TABLE_INV_TRAVEL] AS ct LEFT
			 OUTER JOIN
			 [CODED_TABLE_DESC_INV_TRAVEL_TEMP] AS ctt
			 ON ct.PAGE_CASE_UID = ctt.PAGE_CASE_UID AND 
				ct.NBS_QUESTION_UID = ctt.NBS_QUESTION_UID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 26;
		SET @Proc_Step_Name = 'CREATE TABLE  CODED_COUNTY_TABLE_INV_TRAVEL'; 
		--CREATE TABLE 	CODED_COUNTY_TABLE 
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_INV_TRAVEL;
		END;

		SELECT CODED.CODE_SET_GROUP_ID, PAGE_CASE_UID, NBS_QUESTION_UID, NBS_CASE_ANSWER_UID, ANSWER_TXT, CVG.CODE_SET_NM, RDB_COLUMN_NM, ANSWER_OTH, CVG.CODE, CODE_SHORT_DESC_TXT AS 'ANSWER_TXT1'
		INTO dbo.CODED_COUNTY_TABLE_INV_TRAVEL
		FROM dbo.CODED_TABLE_INV_TRAVEL AS CODED WITH(NOLOCK) LEFT
			 JOIN
			 NBS_SRTE.dbo.CODESET_GROUP_METADATA AS METADATA WITH(NOLOCK)
			 ON METADATA.CODE_SET_GROUP_ID = CODED.CODE_SET_GROUP_ID LEFT
																  JOIN
																  NBS_SRTE.dbo.V_STATE_COUNTY_CODE_VALUE AS CVG WITH(NOLOCK)
																  ON CVG.CODE_SET_NM = METADATA.CODE_SET_NM AND 
																	 CVG.CODE = CODED.ANSWER_TXT
		WHERE METADATA.CODE_SET_NM = 'COUNTY_CCD';
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP;
		END;

		SELECT p1.PAGE_CASE_UID, p1.NBS_QUESTION_UID, STUFF(
		(
			SELECT TOP 10 ' |' + ANSWER_TXT1
			FROM [dbo].[CODED_COUNTY_TABLE_INV_TRAVEL] AS p2
			WHERE p2.PAGE_CASE_UID = p1.PAGE_CASE_UID AND 
				  p2.nbs_question_uid = p1.NBS_QUESTION_UID
			ORDER BY PAGE_CASE_UID, NBS_QUESTION_UID, NBS_CASE_ANSWER_UID FOR XML PATH(''), TYPE
		).value( '.', 'varchar(2000)' ), 1, 2, '') AS ANSWER_DESC11
		INTO [dbo].[CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP]
		FROM [dbo].[CODED_COUNTY_TABLE_INV_TRAVEL] AS p1
		GROUP BY PAGE_CASE_UID, NBS_QUESTION_UID;
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL;
		END;

		SELECT cct.*, cctt.answer_desc11
		INTO [dbo].[CODED_COUNTY_TABLE_DESC_INV_TRAVEL]
		FROM [dbo].[CODED_COUNTY_TABLE_INV_TRAVEL] AS cct LEFT
			 OUTER JOIN
			 dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP AS cctt
			 ON cct.PAGE_CASE_UID = cctt.PAGE_CASE_UID AND 
				cct.NBS_QUESTION_UID = cctt.NBS_QUESTION_UID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 27;
		SET @Proc_Step_Name = 'CREATE TABLE CODED_TABLE_MERGED_INV_TRAVEL';
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_TEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_MERGED_TEMP_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_MERGED_INV_TRAVEL;
		END;

		SELECT temp_tbl.*
		INTO dbo.CODED_TABLE_MERGED_INV_TRAVEL
		FROM
		(
			SELECT [CODE_SET_GROUP_ID], [PAGE_CASE_UID], [NBS_QUESTION_UID], [NBS_CASE_ANSWER_UID], [ANSWER_TXT], [CODE_SET_NM], [RDB_COLUMN_NM], [ANSWER_OTH], [CODE], [ANSWER_TXT1], [answer_desc11], [ANSWER_TXT_CODE], [ANSWER_VALUE], [ANSWER_TXT2], [MASK], NULL AS OTHER_VALUE_IND_CD, NULL AS RDB_COLUMN_NM2
			FROM [dbo].[CODED_TABLE_DESC_INV_TRAVEL]
			UNION ALL
			SELECT [CODE_SET_GROUP_ID], [PAGE_CASE_UID], [NBS_QUESTION_UID], [NBS_CASE_ANSWER_UID], [ANSWER_TXT], [CODE_SET_NM], [RDB_COLUMN_NM], [ANSWER_OTH], [CODE], [ANSWER_TXT1], [answer_desc11], NULL, NULL, NULL, NULL, NULL, NULL
			FROM [dbo].[CODED_COUNTY_TABLE_DESC_INV_TRAVEL]
			UNION ALL
			SELECT [CODE_SET_GROUP_ID], [PAGE_CASE_UID], [NBS_CASE_ANSWER_UID], [NBS_QUESTION_UID], [ANSWER_TXT], NULL, [RDB_COLUMN_NM], NULL, NULL, NULL, [ANSWER_DESC11], NULL, NULL, NULL, NULL, [OTHER_VALUE_IND_CD], [RDB_COLUMN_NM2]
			FROM [dbo].[CODED_TABLE_OTHER_INV_TRAVEL]
		) AS temp_tbl;
		CREATE NONCLUSTERED INDEX [RDB_PERF_INTERNAL_04]
ON [dbo].[CODED_TABLE_MERGED_INV_TRAVEL]
		( [RDB_COLUMN_NM]
		);

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 30;
		SET @Proc_Step_Name = ' CREATE TABLE  RDB_UI_METADATA_INV_TRAVEL'; 
		-- create table rdb_ui_metadata as 
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;

		SELECT DISTINCT 
			   NRDBM.RDB_COLUMN_NM, NUIM.NBS_QUESTION_UID, NUIM.CODE_SET_GROUP_ID, NUIM.INVESTIGATION_FORM_CD, CODE_SET_GROUP_ID AS CODE_SET_GROUP_ID1, QUESTION_GROUP_SEQ_NBR, DATA_TYPE
		INTO dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), nbs_odse.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			  NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			  QUESTION_GROUP_SEQ_NBR IS NULL AND 
			  DATA_TYPE IN( 'Date/Time', 'Date', 'DATETIME', 'DATE' );
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;

		SELECT *
		INTO [dbo].RDB_UI_METADATA_INV_TRAVEL
		FROM
		(
			SELECT *, ROW_NUMBER() OVER(PARTITION BY NBS_QUESTION_UID
			ORDER BY NBS_QUESTION_UID) AS rowid
			FROM [dbo].RDB_UI_METADATA_INV_TRAVEL_TEMP
		) AS Der
		WHERE rowid = 1;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 31;
		SET @Proc_Step_Name = ' CREATE TABLE DATE_DATA_INV_TRAVEL'; 
		-- CREATE TABLE DATE_DATA AS
		IF OBJECT_ID('dbo.DATE_DATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.DATE_DATA_INV_TRAVEL;
		END;

		SELECT PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, ( CASE
																			  WHEN ISDATE(ANSWER_TXT) = 1 THEN ANSWER_TXT
																			  ELSE NULL
																			  END ) AS ANSWER_TXT1, ACT_UID AS PAGE_CASE_UID, PA.RECORD_STATUS_CD, rmeta.NBS_QUESTION_UID, ( CASE
																																											 WHEN ISDATE(ANSWER_TXT) = 1 THEN ANSWER_TXT
																																											 ELSE NULL
																																											 END ) AS ANSWER_TXT
		INTO dbo.DATE_DATA_INV_TRAVEL
		FROM dbo.RDB_UI_METADATA_INV_TRAVEL AS rmeta WITH(NOLOCK) LEFT
			 OUTER JOIN
			 nbs_changedata.dbo.NBS_CASE_ANSWER AS PA WITH(NOLOCK)
			 ON rmeta.nbs_question_uid = PA.nbs_question_uid AND 
				pa.ANSWER_GROUP_SEQ_NBR IS NULL LEFT
																	  OUTER JOIN
																	  dbo.PHC_UIDS WITH(NOLOCK)
																	  ON PHC_UIDS.PAGE_CASE_UID = PA.act_uid LEFT
												OUTER JOIN
												NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
												ON UPPER(CVG.CODE) = UPPER(rmeta.DATA_TYPE)
		WHERE CVG.CODE_SET_NM = 'NBS_DATA_TYPE' AND 
			  CODE IN( 'DATETIME', 'DATE' )
			  AND (PA.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer)
								)
		ORDER BY ACT_UID, PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID;

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		INSERT INTO dbo.ETL_DQ_LOG( EVENT_TYPE, EVENT_LOCAL_ID, EVENT_UID, DQ_ISSUE_CD, DQ_ISSUE_DESC_TXT, DQ_ISSUE_QUESTION_IDENTIFIER, DQ_ISSUE_ANSWER_TXT, DQ_ISSUE_RDB_LOCATION, JOB_BATCH_LOG_UID, DQ_ETL_PROCESS_TABLE, DQ_ETL_PROCESS_COLUMN, DQ_STATUS_TIME, DQ_ISSUE_SOURCE_LOCATION, DQ_ISSUE_SOURCE_QUESTION_LABEL )
		(
			SELECT 'INVESTIGATION', PUBLIC_HEALTH_CASE.LOCAL_ID, PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID, 'INVALID_DATE', 'BAD DATE: A poorly formatted date exists and requires update. Please correct the bad date so that it can be properly written to the reporting database during the next ETL run.', NBS_ui_metadata.QUESTION_IDENTIFIER, ANSWER_TXT, NBS_ui_metadata.DATA_LOCATION, @Batch_id, NBS_rdb_metadata.rdb_table_nm, NBS_rdb_metadata.RDB_COLUMN_NM, GETDATE(), NBS_ui_metadata.DATA_LOCATION, QUESTION_LABEL
			FROM dbo.PHC_UIDS
				 INNER JOIN
				 nbs_changedata.dbo.NBS_CASE_ANSWER NBS_case_answer
				 ON NBS_case_answer.act_uid = PHC_UIDS.PAGE_CASE_UID
				 INNER JOIN
				 nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE
				 ON NBS_CASE_ANSWER.ACT_UID = PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID
				 INNER JOIN
				 NBS_SRTE.DBO.CONDITION_CODE
				 ON CONDITION_CODE.CONDITION_CD = PUBLIC_HEALTH_CASE.CD
				 INNER JOIN
				 NBS_ODSE.DBO.NBS_UI_METADATA
				 ON NBS_CASE_ANSWER.NBS_QUESTION_UID = NBS_UI_METADATA.NBS_QUESTION_UID AND 
					CONDITION_CODE.INVESTIGATION_FORM_CD = NBS_UI_METADATA.INVESTIGATION_FORM_CD
				 INNER JOIN
				 NBS_ODSE.DBO.NBS_RDB_METADATA
				 ON NBS_RDB_METADATA.NBS_UI_METADATA_UID = NBS_UI_METADATA.NBS_UI_METADATA_UID
			WHERE DATA_TYPE IN( 'Date/Time', 'Date', 'DATETIME', 'DATE' ) AND 
				  (ISDATE(ANSWER_TXT) != 1) AND 
				  UPPER(NBS_UI_METADATA.DATA_LOCATION) = 'NBS_CASE_ANSWER.ANSWER_TXT' AND 
				  ANSWER_TXT IS NOT NULL AND 
				  NBS_rdb_metadata.rdb_table_nm = 'D_INV_TRAVEL'
				  AND (NBS_case_answer.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer) OR 
				  PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID in (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case)
								)
		);

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 32;
		SET @Proc_Step_Name = ' UPDATE TABLE date_data_INV_TRAVEL';
		UPDATE dbo.DATE_DATA_INV_TRAVEL
		  SET ANSWER_TXT1 = FORMAT(CAST([ANSWER_TXT] AS date), 'MM/dd/yy') + ' 00:00:00';
		--SET ANSWER_TXT=dhms(input(ANSWER_TXT1,MMDDYY10.),0,0,0); 
		--DROP ANSWER_TXT;
		--RENAME ANSWER_TXT=ANSWER_TXT1;   
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 36;
		SET @Proc_Step_Name = ' CREATE TABLE PAGE_DATE_TABLE_INV_TRAVEL';
		IF OBJECT_ID('dbo.PAGE_DATE_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.PAGE_DATE_TABLE_INV_TRAVEL;
		END;
		CREATE TABLE dbo.PAGE_DATE_TABLE_INV_TRAVEL
		( 
					 NBS_CASE_ANSWER_UID bigint, CODE_SET_GROUP_ID bigint, RDB_COLUMN_NM char(40), ANSWER_TXT1 date, PAGE_CASE_UID bigint, LAST_CHG_TIME date, RECORD_STATUS_CD char(40)
		);
 
		--IF PAGE_CASE_UID=. THEN PAGE_CASE_UID= 1;
		UPDATE dbo.DATE_DATA_INV_TRAVEL
		  SET PAGE_CASE_UID = 1
		WHERE PAGE_CASE_UID IS NULL;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 37;
		SET @Proc_Step_Name = ' UPDATE TABLE  PAGE_DATE_TABLE_INV_TRAVEL'; 
		--%DBLOAD (PAGE_DATE_TABLE, DATE_DATA); 
		INSERT INTO dbo.PAGE_DATE_TABLE_INV_TRAVEL( NBS_CASE_ANSWER_UID, CODE_SET_GROUP_ID, RDB_COLUMN_NM, ANSWER_TXT1, PAGE_CASE_UID, RECORD_STATUS_CD )
			   SELECT NBS_CASE_ANSWER_UID, CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(ANSWER_TXT1 AS datetime), PAGE_CASE_UID, 
			   --	LAST_CHG_TIME , 
			   RECORD_STATUS_CD
			   FROM dbo.DATE_DATA_INV_TRAVEL;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 39;
		SET @Proc_Step_Name = ' UPDATE TABLE  PAGE_DATE_TABLE_INV_TRAVEL';
		IF OBJECT_ID('dbo.date_data_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.date_data_INV_TRAVEL_out;
		END;
		SET @columns = N'';
		SELECT @columns+=N', p.' + QUOTENAME(LTRIM(RTRIM([RDB_COLUMN_NM])))
		FROM
		(
			SELECT [RDB_COLUMN_NM]
			FROM [dbo].PAGE_DATE_TABLE_INV_TRAVEL AS p
			GROUP BY [RDB_COLUMN_NM]
		) AS x;
		SET @sql = N'
SELECT [PAGE_CASE_UID] as PAGE_CASE_UID_date, ' + STUFF(@columns, 1, 2, '') + ' into dbo.DATE_DATA_INV_TRAVEL_out ' + 'FROM (
SELECT [PAGE_CASE_UID], [answer_txt1] , [RDB_COLUMN_NM] 
 FROM [dbo].PAGE_DATE_TABLE_INV_TRAVEL
	group by [PAGE_CASE_UID], [answer_txt1] , [RDB_COLUMN_NM] 
		) AS j PIVOT (max(answer_txt1) FOR [RDB_COLUMN_NM] in 
	   (' + STUFF(REPLACE(@columns, ', p.[', ',['), 1, 1, '') + ')) AS p;';
		PRINT @sql;
		EXEC sp_executesql @sql;
		--select * from dbo.DATE_DATA_INV_TRAVEL_out;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 40;
		SET @Proc_Step_Name = ' UPDATE TABLE  RDB_UI_METADATA_INV_TRAVEL'; 
		--create table rdb_ui_metadata as 
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;

		SELECT DISTINCT 
			   NRDBM.RDB_COLUMN_NM, NUIM.NBS_QUESTION_UID, NUIM.CODE_SET_GROUP_ID, NUIM.INVESTIGATION_FORM_CD, CODE_SET_GROUP_ID AS CODE_SET_GROUP_ID1, QUESTION_GROUP_SEQ_NBR, DATA_TYPE
		INTO dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), nbs_odse.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			  NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			  nuim.QUESTION_GROUP_SEQ_NBR IS NULL AND 
			  nuim.DATA_TYPE IN( 'Numeric', 'NUMERIC' ) AND 
			  NUIM.nbs_question_uid NOT IN
		(
			SELECT nbs_question_uid
			FROM CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL
		);

		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;

		SELECT *
		INTO [dbo].RDB_UI_METADATA_INV_TRAVEL
		FROM
		(
			SELECT *, ROW_NUMBER() OVER(PARTITION BY NBS_QUESTION_UID
			ORDER BY NBS_QUESTION_UID) AS rowid
			FROM [dbo].RDB_UI_METADATA_INV_TRAVEL_TEMP
		) AS Der
		WHERE rowid = 1;
 
		--CREATE TABLE NUMERIC_BASE_DATA_INV_TRAVEL AS
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 41;
		SET @Proc_Step_Name = ' CREATE TABLE  NUMERIC_BASE_DATA_INV_TRAVEL';
		IF OBJECT_ID('dbo.NUMERIC_BASE_DATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE NUMERIC_BASE_DATA_INV_TRAVEL;
		END;

		SELECT PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID, RDB_COLUMN_NM, CAST(ANSWER_TXT AS varchar(2000)) AS ANSWER_TXT, ACT_UID AS PAGE_CASE_UID, PA.RECORD_STATUS_CD, rmeta.NBS_QUESTION_UID, LEN(RTRIM(ANSWER_TXT)) AS TXT_LEN, CAST(NULL AS [varchar](2000)) AS ANSWER_UNIT, CAST(NULL AS int) AS LENCODED, CAST(NULL AS [varchar](2000)) AS ANSWER_CODED, CAST(NULL AS [varchar](2000)) AS UNIT_VALUE1, CAST(NULL AS [varchar](30)) AS RDB_COLUMN_NM2
		INTO dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		FROM dbo.RDB_UI_METADATA_INV_TRAVEL AS rmeta WITH(NOLOCK) LEFT
			 OUTER JOIN
			 nbs_changedata.dbo.NBS_CASE_ANSWER AS PA WITH(NOLOCK)
			 ON rmeta.nbs_question_uid = PA.nbs_question_uid AND 
				pa.ANSWER_GROUP_SEQ_NBR IS NULL LEFT
																	  OUTER JOIN
																	  dbo.PHC_UIDS
																	  ON PHC_UIDS.PAGE_CASE_UID = PA.act_uid
												INNER JOIN
												NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
												ON UPPER(CVG.CODE) = UPPER(rmeta.DATA_TYPE)
		WHERE CVG.CODE_SET_NM = 'NBS_DATA_TYPE' AND 
			  CODE IN( 'Numeric', 'NUMERIC' )
			  AND (PA.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer))
		ORDER BY ACT_UID, PA.NBS_CASE_ANSWER_UID, rmeta.CODE_SET_GROUP_ID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 43;
		SET @Proc_Step_Name = ' UPDATE TABLE  NUMERIC_BASE_DATA_INV_TRAVEL'; 
		--update dbo.NUMERIC_BASE_DATA_INV_TRAVEL set TXT_LEN=LEN(RTRIM(ANSWER_TXT));
		UPDATE dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		  SET ANSWER_UNIT = SUBSTRING(ANSWER_TXT, 1, ( CHARINDEX('^', ANSWER_TXT) - 1 ))
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		  SET LENCODED = LEN(RTRIM(ANSWER_UNIT))
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		  SET ANSWER_CODED = SUBSTRING(ANSWER_TXT, ( LENCODED + 2 ), TXT_LEN)
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		  SET UNIT_VALUE1 = REPLACE(ANSWER_UNIT, ',', '')
		WHERE CHARINDEX('^', ANSWER_TXT) > 0;
		UPDATE dbo.NUMERIC_BASE_DATA_INV_TRAVEL
		  SET RDB_COLUMN_NM2 = SUBSTRING(RTRIM(RDB_COLUMN_NM), 1, 25) + ' UNIT'
		WHERE LEN(RTRIM(ANSWER_CODED)) > 0;
		IF OBJECT_ID('dbo.NUMERIC_DATA_2_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE NUMERIC_DATA_2_INV_TRAVEL;
		END;

		SELECT *
		INTO dbo.NUMERIC_DATA_2_INV_TRAVEL
		FROM dbo.NUMERIC_BASE_DATA_INV_TRAVEL;
		UPDATE dbo.NUMERIC_DATA_2_INV_TRAVEL
		  SET RDB_COLUMN_NM = RDB_COLUMN_NM2
		WHERE LEN(RTRIM(RDB_COLUMN_NM2)) > 0;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 44;
		SET @Proc_Step_Name = ' CREATE TABLE  NUMERIC_DATA_MERGED_INV_TRAVEL';
		IF OBJECT_ID('dbo.NUMERIC_DATA_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_MERGED_INV_TRAVEL;
		END;

		SELECT COALESCE(nbd.NBS_CASE_ANSWER_UID, nd2.NBS_CASE_ANSWER_UID) AS NBS_CASE_ANSWER_UID, COALESCE(nbd.[RDB_COLUMN_NM2], nd2.[RDB_COLUMN_NM2]) AS [RDB_COLUMN_NM2], COALESCE(nbd.[CODE_SET_GROUP_ID], nd2.[CODE_SET_GROUP_ID]) AS [CODE_SET_GROUP_ID], COALESCE(nbd.[RDB_COLUMN_NM], nd2.[RDB_COLUMN_NM]) AS [RDB_COLUMN_NM], COALESCE(nbd.[ANSWER_TXT], nd2.[ANSWER_TXT]) AS [ANSWER_TXT], COALESCE(nbd.[PAGE_CASE_UID], nd2.[PAGE_CASE_UID]) AS [PAGE_CASE_UID], COALESCE(nbd.[RECORD_STATUS_CD], nd2.[RECORD_STATUS_CD]) AS [RECORD_STATUS_CD], COALESCE(nbd.[NBS_QUESTION_UID], nd2.[NBS_QUESTION_UID]) AS [NBS_QUESTION_UID], COALESCE(nbd.[TXT_LEN], nd2.[TXT_LEN]) AS [TXT_LEN], COALESCE(nbd.[ANSWER_UNIT], nd2.[ANSWER_UNIT]) AS [ANSWER_UNIT], COALESCE(nbd.[LENCODED], nd2.[LENCODED]) AS [LENCODED], COALESCE(nbd.[ANSWER_CODED], nd2.[ANSWER_CODED]) AS [ANSWER_CODED], COALESCE(nbd.[UNIT_VALUE1], nd2.[UNIT_VALUE1]) AS [UNIT_VALUE1]
		INTO dbo.NUMERIC_DATA_MERGED_INV_TRAVEL
		FROM [dbo].NUMERIC_BASE_DATA_INV_TRAVEL AS nbd
			 FULL OUTER JOIN
			 [dbo].NUMERIC_DATA_2_INV_TRAVEL AS nd2
			 ON nbd.NBS_CASE_ANSWER_UID = nd2.NBS_CASE_ANSWER_UID AND 
				nbd.[RDB_COLUMN_NM] = nd2.[RDB_COLUMN_NM];
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 45;
		SET @Proc_Step_Name = ' CREATE TABLE  NUMERIC_DATA_TRANS_INV_TRAVEL'; 
		--CREATE TABLE 	NUMERIC_DATA_TRANS  AS 
		IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_TRANS_INV_TRAVEL;
		END;

		SELECT PAGE_CASE_UID, NBS_QUESTION_UID, NBS_CASE_ANSWER_UID, ANSWER_UNIT, ANSWER_CODED, CVG.CODE_SET_NM, RDB_COLUMN_NM, ANSWER_TXT, CODE, CODE_SHORT_DESC_TXT AS UNIT, CAST(NULL AS [varchar](2000)) AS ANSWER_TXT_F
		INTO dbo.NUMERIC_DATA_TRANS_INV_TRAVEL
		FROM dbo.NUMERIC_DATA_MERGED_INV_TRAVEL AS CODED WITH(NOLOCK) LEFT
			 JOIN
			 NBS_SRTE.dbo.CODESET_GROUP_METADATA AS METADATA WITH(NOLOCK)
			 ON METADATA.CODE_SET_GROUP_ID = CODED.CODE_SET_GROUP_ID LEFT
																		  JOIN
																		  NBS_SRTE.dbo.CODE_VALUE_GENERAL AS CVG WITH(NOLOCK)
																		  ON CVG.CODE_SET_NM = METADATA.CODE_SET_NM
		WHERE CVG.CODE = CODED.ANSWER_CODED OR 
			  ANSWER_CODED IS NULL
		ORDER BY PAGE_CASE_UID;
		UPDATE dbo.NUMERIC_DATA_TRANS_INV_TRAVEL
		  SET PAGE_CASE_UID = COALESCE(PAGE_CASE_UID, 1), ANSWER_TXT_F = CASE
																		 WHEN COALESCE(RTRIM(UNIT), '') = '' THEN ANSWER_TXT
																		 WHEN CHARINDEX(' UNIT', RDB_COLUMN_NM) > 0 THEN UNIT
																		 ELSE ANSWER_UNIT
																		 END;

		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 48;
		SET @Proc_Step_Name = ' UPDATE TABLE  CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL'; 
		--CREATE TABLE NUMERIC_DATA_TRANS1_INV_TRAVEL AS 
		IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			( 
						 [RDB_COLUMN_NM] [varchar](30) NULL, [ANSWER_TXT] [varchar](2000) NULL
			)
			ON [PRIMARY];
		END;

		SELECT DISTINCT 
			   ndtis.PAGE_CASE_UID, ndtis.RDB_COLUMN_NM, ANSWER_UNIT, ANSWER_TXT_F AS ANSWER_TXT
		INTO dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL
		FROM dbo.NUMERIC_DATA_TRANS_INV_TRAVEL AS ndtis LEFT
			 OUTER JOIN
			 dbo.CODED_TABLE_MERGED_INV_TRAVEL AS ctmis
			 ON ndtis.RDB_COLUMN_NM = ctmis.RDB_COLUMN_NM AND 
				ctmis.answer_txt IS NOT NULL AND 
				ctmis.RDB_COLUMN_NM IS NULL;


		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		WITH lst
			 AS (SELECT ndtis.rdb_column_nm,
							  CASE
							  WHEN ndtis.answer_txt IS NOT NULL THEN 1
							  ELSE 0
							  END AS Ans_null
				 FROM [dbo].[NUMERIC_DATA_TRANS1_INV_TRAVEL] AS ndtis LEFT
					  OUTER JOIN
					  dbo.CODED_TABLE_MERGED_INV_TRAVEL AS ctmis
					  ON ndtis.RDB_COLUMN_NM = ctmis.RDB_COLUMN_NM AND 
						 ctmis.answer_txt IS NOT NULL AND 
						 ctmis.RDB_COLUMN_NM IS NULL)
			 DELETE FROM dbo.CODED_TABLE_MERGED_INV_TRAVEL
			 WHERE RDB_COLUMN_NM IN
			 (
				 SELECT rdb_column_nm
				 FROM lst
				 WHERE ans_null != 0
			 );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		WITH lst
			 AS (SELECT rdb_column_nm,
						CASE
						WHEN answer_txt IS NOT NULL THEN 1
						ELSE 0
						END AS Ans_null
				 FROM [dbo].[NUMERIC_DATA_TRANS1_INV_TRAVEL]
				 WHERE RDB_COLUMN_NM IN
				 (
					 SELECT RDB_COLUMN_NM
					 FROM dbo.CODED_TABLE_MERGED_INV_TRAVEL
					 GROUP BY rdb_column_nm
				 )
				 GROUP BY rdb_column_nm,
						  CASE
						  WHEN answer_txt IS NOT NULL THEN 1
						  ELSE 0
						  END)
			 DELETE FROM [dbo].[NUMERIC_DATA_TRANS1_INV_TRAVEL]
			 WHERE rdb_column_nm IN
			 (
				 SELECT rdb_column_nm
				 FROM lst
				 WHERE ans_null = 0
				 EXCEPT
				 SELECT rdb_column_nm
				 FROM lst
				 WHERE ans_null = 1
			 );
		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 49;
		SET @Proc_Step_Name = ' CREATE TABLE  CODED_DATA_INV_TRAVEL_out';
		IF OBJECT_ID('dbo.CODED_DATA_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_DATA_INV_TRAVEL_out;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = NULL
			WHERE answer_desc11 = '';
		END;

		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = ANSWER_VALUE
			WHERE ANSWER_OTH IS NOT NULL AND 
				  ANSWER_VALUE IS NOT NULL AND 
				  answer_desc11 IS NULL AND 
				  answer_oth IS NOT NULL;
		END;

		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = ANSWER_TXT
			WHERE ANSWER_OTH IS NULL AND 
				  ANSWER_TXT2 IS NOT NULL AND 
				  ( answer_desc11 IS NULL
				  ) AND 
				  answer_oth IS NULL AND 
				  LEN(ANSWER_TXT2) > 0;
		END;

		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = ANSWER_TXT
			WHERE ANSWER_OTH IS NULL AND 
				  ANSWER_TXT2 IS NOT NULL AND 
				  ( answer_desc11 IS NULL
				  ) AND 
				  answer_oth IS NULL AND 
				  LEN(ANSWER_TXT) > 0;
		END;



		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = ANSWER_VALUE
			WHERE ANSWER_OTH IS NULL AND 
				  ANSWER_VALUE IS NOT NULL AND 
				  answer_desc11 IS NULL AND 
				  answer_txt IS NOT NULL AND 
				  code = '';
		END;

		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			UPDATE [dbo].CODED_TABLE_MERGED_INV_TRAVEL
			  SET answer_desc11 = ANSWER_TXT
			WHERE answer_desc11 IS NULL AND 
				  ANSWER_OTH IS NULL AND 
				  ANSWER_TXT2 IS NULL AND 
				  answer_oth IS NULL AND 
				  ANSWER_TXT IS NOT NULL AND 
				  ANSWER_TXT <> ''
		END;

		SET @columns = N'';
		SELECT @columns+=N', p.' + QUOTENAME(LTRIM(RTRIM([RDB_COLUMN_NM])))
		FROM
		(
			SELECT [RDB_COLUMN_NM]
			FROM [dbo].CODED_TABLE_MERGED_INV_TRAVEL AS p
			GROUP BY [RDB_COLUMN_NM]
		) AS x;
		SET @sql = N'
SELECT [PAGE_CASE_UID] as PAGE_CASE_UID_coded, ' + STUFF(@columns, 1, 2, '') + ' into dbo.CODED_DATA_INV_TRAVEL_out ' + 'FROM (
SELECT [PAGE_CASE_UID], [ANSWER_DESC11] , [RDB_COLUMN_NM] 
 FROM [dbo].CODED_TABLE_MERGED_INV_TRAVEL
	group by [PAGE_CASE_UID], [ANSWER_DESC11] , [RDB_COLUMN_NM] 
		) AS j PIVOT (max(ANSWER_DESC11) FOR [RDB_COLUMN_NM] in 
	   (' + STUFF(REPLACE(@columns, ', p.[', ',['), 1, 1, '') + ')) AS p;';
		PRINT @sql;
		EXEC sp_executesql @sql;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;
		BEGIN TRANSACTION;
		SET @Proc_Step_no = 49;
		SET @Proc_Step_Name = ' LOG Invalid Numeric data INTO ETL_DQ_LOG';
	
		INSERT INTO dbo.ETL_DQ_LOG( EVENT_TYPE, EVENT_LOCAL_ID, EVENT_UID, DQ_ISSUE_CD, DQ_ISSUE_DESC_TXT, DQ_ISSUE_QUESTION_IDENTIFIER, DQ_ISSUE_ANSWER_TXT, DQ_ISSUE_RDB_LOCATION, JOB_BATCH_LOG_UID, DQ_ETL_PROCESS_TABLE, DQ_ETL_PROCESS_COLUMN, DQ_STATUS_TIME, DQ_ISSUE_SOURCE_LOCATION, DQ_ISSUE_SOURCE_QUESTION_LABEL )
		(
		SELECT DISTINCT 'INVESTIGATION', PUBLIC_HEALTH_CASE.LOCAL_ID, PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID, 
		'INVALID_NUMERIC_VALUE', 'BAD NUMERIC VALUE: A non-numeric value exists in a field expecting a numeric value and requires update. Please correct the bad numeric value so that it can be properly written to the reporting database during the next ETL run', 
		NBS_UI_METADATA.QUESTION_IDENTIFIER, ANSWER_TXT, NBS_ui_metadata.DATA_LOCATION, @Batch_id, NBS_rdb_metadata.rdb_table_nm, NBS_rdb_metadata.RDB_COLUMN_NM, 
		GETDATE(), NBS_ui_metadata.DATA_LOCATION, QUESTION_LABEL
					FROM dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL
			 INNER JOIN
				 nbs_changedata.dbo.PUBLIC_HEALTH_CASE PUBLIC_HEALTH_CASE
				 ON NUMERIC_DATA_TRANS1_INV_TRAVEL.page_case_uid= PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID
				 INNER JOIN
				 NBS_SRTE.DBO.CONDITION_CODE
				 ON CONDITION_CODE.CONDITION_CD = PUBLIC_HEALTH_CASE.CD
				  INNER JOIN
				 NBS_ODSE.DBO.NBS_RDB_METADATA
				 ON NBS_rdb_metadata.rdb_column_nm = NUMERIC_DATA_TRANS1_INV_TRAVEL.rdb_column_nm
				 INNER JOIN
				 NBS_ODSE.DBO.NBS_UI_METADATA
				 ON NBS_rdb_metadata.NBS_UI_METADATA_UID = NBS_ui_metadata.NBS_UI_METADATA_UID AND 
					CONDITION_CODE.INVESTIGATION_FORM_CD = NBS_ui_metadata.INVESTIGATION_FORM_CD
			WHERE (isNumeric(ANSWER_TXT) != 1) AND 
				  ANSWER_TXT IS NOT NULL
				 AND (PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID in (Select PUBLIC_HEALTH_CASE_UID from #TMP_CDC_Public_health_case)
				));
		
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;
		BEGIN TRANSACTION;
		SET @Proc_Step_no = 50;
		SET @Proc_Step_Name = ' CREATE TABLE  NUMERIC_DATA_PIVOT_INV_TRAVEL';
		IF OBJECT_ID('dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL;
		END;
		SET @columns = N'';
		SELECT @columns+=N', p.' + QUOTENAME(LTRIM(RTRIM([RDB_COLUMN_NM])))
		FROM
		(
			SELECT [RDB_COLUMN_NM]
			FROM [dbo].NUMERIC_DATA_TRANS1_INV_TRAVEL AS p
			GROUP BY [RDB_COLUMN_NM]
		) AS x;
		SET @sql = N'
SELECT [PAGE_CASE_UID] as PAGE_CASE_UID_NUMERIC, ' + STUFF(@columns, 1, 2, '') + ' into dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL ' + 'FROM (
SELECT [PAGE_CASE_UID], [answer_txt] , [RDB_COLUMN_NM] 
 FROM [dbo].NUMERIC_DATA_TRANS1_INV_TRAVEL
  WHERE (isNumeric(ANSWER_TXT) = 1) AND  ANSWER_TXT IS NOT NULL 
	group by [PAGE_CASE_UID], [answer_txt] , [RDB_COLUMN_NM] 
		) AS j PIVOT (max(answer_txt) FOR [RDB_COLUMN_NM] in 
	   (' + STUFF(REPLACE(@columns, ', p.[', ',['), 1, 1, '') + ')) AS p;';
		PRINT @sql;
		EXEC sp_executesql @sql;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );


		COMMIT TRANSACTION;


		BEGIN TRANSACTION;
		SET @Proc_Step_no = 51;
		SET @Proc_Step_Name = ' CREATE TABLE NUMERIC_DATA_OUT_INV_TRAVEL';
		IF OBJECT_ID('dbo.NUMERIC_DATA_OUT_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_OUT_INV_TRAVEL;
		END;
		--CREATE TABLE NUMERIC_DATA_OUT 
		IF OBJECT_ID('dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			SELECT *
			INTO dbo.NUMERIC_DATA_OUT_INV_TRAVEL
			FROM dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL
			WHERE 
			---??? LEN(RTRIM(_LABEL_))>0    AND 
			PAGE_CASE_UID_NUMERIC > 0;
		END;
		---************************************************
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 52;
		SET @Proc_Step_Name = ' Generating Stageing_key_metadata_INV_TRAVEL';
		IF OBJECT_ID('dbo.Stageing_key_metadata_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.Stageing_key_metadata_INV_TRAVEL;
		END;
		--create table Stageing_key_metadata_INV_TRAVEL as 
		SELECT DISTINCT 
			   NRDBM.RDB_COLUMN_NM, NUIM.NBS_QUESTION_UID, NUIM.CODE_SET_GROUP_ID, NUIM.INVESTIGATION_FORM_CD, data_type, CODE_SET_GROUP_ID AS CODE_SET_GROUP_ID1, QUESTION_GROUP_SEQ_NBR, DATA_TYPE AS DATA_TYPE1
		INTO dbo.Stageing_key_metadata_INV_TRAVEL
		FROM nbs_odse.dbo.NBS_rdb_metadata AS NRDBM WITH(NOLOCK), nbs_odse.dbo.NBS_ui_metadata AS NUIM WITH(NOLOCK)
		WHERE NRDBM.NBS_UI_METADATA_UID = NUIM.NBS_UI_METADATA_UID AND 
			  NRDBM.RDB_TABLE_NM = 'D_INV_TRAVEL' AND 
			  QUESTION_GROUP_SEQ_NBR IS NULL;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 53;
		SET @Proc_Step_Name = ' Generating STAGING_KEY_INV_TRAVEL'; 
		--create table STAGING_KEY AS 
		IF OBJECT_ID('dbo.STAGING_KEY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.STAGING_KEY_INV_TRAVEL;
		END;

		SELECT ACT_UID AS PAGE_CASE_UID, PA.NBS_CASE_ANSWER_UID, PHC_UIDS.LAST_CHG_TIME
		INTO dbo.STAGING_KEY_INV_TRAVEL
		FROM dbo.Stageing_key_metadata_INV_TRAVEL AS NUIM WITH(NOLOCK)
			 INNER JOIN
			 nbs_changedata.dbo.NBS_CASE_ANSWER AS PA WITH(NOLOCK)
			 ON NUIM.NBS_QUESTION_UID = PA.NBS_QUESTION_UID
			 INNER JOIN
			 dbo.PHC_UIDS WITH(NOLOCK)
			 ON PHC_UIDS.PAGE_CASE_UID = PA.ACT_UID
		WHERE ANSWER_GROUP_SEQ_NBR IS NULL
		AND (PA.nbs_case_answer_uid in (Select nbs_case_answer_uid from #TMP_CDC_NBS_case_answer))
		GROUP BY ACT_UID, PA.NBS_CASE_ANSWER_UID, PHC_UIDS.LAST_CHG_TIME;
		IF OBJECT_ID('dbo.STAGING_KEY_INV_TRAVEL_FINAL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.STAGING_KEY_INV_TRAVEL_FINAL;
		END;

		SELECT *
		INTO [dbo].[STAGING_KEY_INV_TRAVEL_FINAL]
		FROM
		(
			SELECT *, ROW_NUMBER() OVER(PARTITION BY [page_case_uid]
			ORDER BY [page_case_uid], NBS_CASE_ANSWER_UID) AS rowid
			FROM [dbo].[STAGING_KEY_INV_TRAVEL]
		) AS Der
		WHERE rowid = 1;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 54;
		SET @Proc_Step_Name = ' Creating Table S_INV_TRAVEL'; 
    
	
		--CREATE TABLE 	S_INV_TRAVEL 
		IF OBJECT_ID('dbo.NUMERIC_DATA_OUT_INV_TRAVEL', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[NUMERIC_DATA_OUT_INV_TRAVEL]
			( 
						 [PAGE_CASE_UID_numeric] [bigint] NULL,
			)
			ON [PRIMARY];
		END;
		IF OBJECT_ID('dbo.date_data_INV_TRAVEL_out', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[date_data_INV_TRAVEL_out]
			( 
						 [PAGE_CASE_UID_date] [bigint] NULL,
			)
			ON [PRIMARY];
		END;
		IF OBJECT_ID('dbo.CODED_DATA_INV_TRAVEL_out', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[CODED_DATA_INV_TRAVEL_out]
			( 
						 [PAGE_CASE_UID_coded] [bigint] NULL,
			)
			ON [PRIMARY];
		END;
		IF OBJECT_ID('dbo.text_data_INV_TRAVEL_out', 'U') IS NULL
		BEGIN
			CREATE TABLE [dbo].[text_data_INV_TRAVEL_out]
			( 
						 [PAGE_CASE_UID_text] [bigint] NULL,
			)
			ON [PRIMARY];
		END;
		IF OBJECT_ID('dbo.S_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.S_INV_TRAVEL;
		END;
		--select * from 	 dbo.CODED_DATA_INV_TRAVEL_out;
		SELECT sk.*, ndo.*, ddo.*, cdo.*, tdo.*
		INTO dbo.S_INV_TRAVEL
		FROM dbo.STAGING_KEY_INV_TRAVEL_FINAL AS sk LEFT
			 OUTER JOIN
			 dbo.NUMERIC_DATA_OUT_INV_TRAVEL AS ndo
			 ON ndo.PAGE_CASE_UID_NUMERIC = sk.PAGE_CASE_UID LEFT
														OUTER JOIN
														dbo.date_data_INV_TRAVEL_out AS ddo
														ON ddo.PAGE_CASE_UID_date = sk.PAGE_CASE_UID LEFT
															 OUTER JOIN
															 dbo.CODED_DATA_INV_TRAVEL_out AS cdo
															 ON cdo.PAGE_CASE_UID_coded = sk.PAGE_CASE_UID LEFT
																									 OUTER JOIN
																									 dbo.text_data_INV_TRAVEL_OUT AS tdo
																									 ON tdo.PAGE_CASE_UID_text = sk.PAGE_CASE_UID;
		SELECT @RowCount_no = @@ROWCOUNT;
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'START', @Proc_Step_no, @Proc_Step_Name, @RowCount_no );

		COMMIT TRANSACTION;
	
		/** TODO: (Upasana) Commented for Change Data Capture- End: Processing logic **/
		UPDATE landing
		SET landing.cdc_status = 2,
		landing.cdc_processed_datetime = GETDATE(),
		landing.cdc_status_desc = 'S_INV_TRAVEL'
		FROM nbs_changedata.dbo.NBS_case_answer landing
			INNER JOIN #TMP_CDC_NBS_case_answer session_table ON landing.nbs_case_answer_uid = session_table.nbs_case_answer_uid AND landing.cdc_id = session_table.cdc_id
		
		UPDATE landing
		SET landing.cdc_status = 2, 
		landing.cdc_processed_datetime = GETDATE(),
		landing.cdc_status_desc = 'S_INV_TRAVEL'
		FROM nbs_changedata.dbo.Public_health_case landing
			INNER JOIN #TMP_CDC_Public_health_case session_table ON landing.public_health_case_uid = session_table.public_health_case_uid AND landing.cdc_id = session_table.cdc_id
			
	
	
		ALTER TABLE dbo.S_INV_TRAVEL DROP COLUMN rowid;
		ALTER TABLE dbo.S_INV_TRAVEL DROP COLUMN PAGE_CASE_UID_numeric;
		ALTER TABLE dbo.S_INV_TRAVEL DROP COLUMN PAGE_CASE_UID_date;
		ALTER TABLE dbo.S_INV_TRAVEL DROP COLUMN PAGE_CASE_UID_coded;
		ALTER TABLE dbo.S_INV_TRAVEL DROP COLUMN PAGE_CASE_UID_text;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.text_data_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.text_data_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.text_data_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.text_data_INV_TRAVEL_out;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CASE_ANSWER_PHC_UIDS', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CASE_ANSWER_PHC_UIDS;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_EMPTY_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_NONEMPTY_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_OTHER_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_OTHER_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_STD_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_STD_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_A_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_SNTEMP_TRANS_CODE_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_TEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_TEMP_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_DESC_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_DESC_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_DESC_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_COUNTY_TABLE_DESC_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_TEMP_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_MERGED_TEMP_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_TABLE_MERGED_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.DATE_DATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.DATE_DATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.PAGE_DATE_TABLE_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.PAGE_DATE_TABLE_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE NBS_PAGE_DATE_TABLE;
		END;
		IF OBJECT_ID('dbo.date_data_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.date_data_INV_TRAVEL_out;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL_TEMP;
		END;
		IF OBJECT_ID('dbo.RDB_UI_METADATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.RDB_UI_METADATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_BASE_DATA_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE NUMERIC_BASE_DATA_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_2_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE NUMERIC_DATA_2_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_MERGED_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_TRANS_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_TRANS1_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.CODED_DATA_INV_TRAVEL_out', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.CODED_DATA_INV_TRAVEL_out;
		END;
		IF OBJECT_ID('dbo.CODED_TABLE_MERGED_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			IF OBJECT_ID('dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL', 'U') IS NOT NULL
			BEGIN
				DROP TABLE dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL;
			END;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_OUT_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_OUT_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.NUMERIC_DATA_PIVOT_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.Stageing_key_metadata_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.Stageing_key_metadata_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.STAGING_KEY_INV_TRAVEL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.STAGING_KEY_INV_TRAVEL;
		END;
		IF OBJECT_ID('dbo.STAGING_KEY_INV_TRAVEL_FINAL', 'U') IS NOT NULL
		BEGIN
			DROP TABLE dbo.STAGING_KEY_INV_TRAVEL_FINAL;
		END;

		BEGIN TRANSACTION;
		SET @Proc_Step_no = 999;
		SET @Proc_Step_Name = 'SP_COMPLETE';
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'COMPLETE', @Proc_Step_no, @Proc_Step_name, @RowCount_no );

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION;
		END;
		DECLARE @ErrorNumber int= ERROR_NUMBER();
		DECLARE @ErrorLine int= ERROR_LINE();
		DECLARE @ErrorMessage nvarchar(4000)= ERROR_MESSAGE();
		DECLARE @ErrorSeverity int= ERROR_SEVERITY();
		DECLARE @ErrorState int= ERROR_STATE();
		INSERT INTO [dbo].[job_flow_log]( batch_id, [Dataflow_Name], [package_Name], [Status_Type], [step_number], [step_name], [Error_Description], [row_count] )
		VALUES( @Batch_id, 'INV_TRAVEL', 'S_INV_TRAVEL', 'ERROR', @Proc_Step_no, 'ERROR - ' + @Proc_Step_name, 'Step -' + CAST(@Proc_Step_no AS varchar(3)) + ' -' + CAST(@ErrorMessage AS varchar(500)), 0 );
		RETURN -1;
	END CATCH;
END;
GO
