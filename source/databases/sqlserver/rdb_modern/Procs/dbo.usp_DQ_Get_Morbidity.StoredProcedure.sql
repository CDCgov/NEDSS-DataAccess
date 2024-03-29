USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[usp_DQ_Get_Morbidity]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Hong Zhang
-- Create date: 20090119 (new version)
-- Description:	This procedure collects errorneous morbidity data and insert to a data validation table.
--							It returns subsets of data based on the params the user specified.
--							The procedure is a subprocedure of [usp_DQ_Main]
-- Modify Date: 20100805 (R4.0.2)
-- Modified By: Hong Zhang
-- =============================================
CREATE PROCEDURE .[dbo].[usp_DQ_Get_Morbidity]
@need_error			tinyint,
@need_warning	 tinyint,
@min_last_chg_time	datetime=null
AS
BEGIN

	SET NOCOUNT ON
	PRINT 'Morbidity Report Validation Checks Insert Started...'+ CONVERT(varchar(20), getdate(),120) 

	/* currenlty morbidity report has just a warning */
	IF @need_warning = 1 
	BEGIN
		/* morbidity report  data validation */
		INSERT INTO DATA_VALIDATION
			(MESSAGE_CD, MESSAGE_TYPE, MESSAGE_DESC, 
			PATIENT_LOCAL_ID, PATIENT_LAST_NM, PATIENT_FIRST_NM, EVENT_LOCAL_ID, EVENT_TYPE, EVENT_LAST_CHG_TIME, 
			JURISDICTION_NM, MMWR_WEEK, MMWR_YEAR, CONDITION_NM, PROGRAM_JURISDICTION_OID,PROGRAM_AREA_NM,
			PROGRAM_AREA_CD,CONDITION_CD,JURISDICTION_CD,INVESTIGATOR_NM,INVESTIGATION_STATUS,INV_CASE_STATUS,
			ELR_IND)
		SELECT	
						t2.data_validation_message_cd, t2.data_validation_message_type, t2.data_validation_message_desc, 
						t6.PATIENT_LOCAL_ID, t6.PATIENT_LAST_NAME, t6.PATIENT_FIRST_NAME, t5.local_id,'Morbidity Report', t5.last_chg_time, 
						t3.jurisdiction_nm, NULL, NULL, t5.condition_desc_txt, t5.program_jurisdiction_oid, t5.prog_area_desc_txt, 
						t5.prog_area_cd, t5.condition_cd, t3.jurisdiction_cd, NULL, NULL, NULL, 
						NULL
		FROM MORBIDITY_REPORT t3 LEFT JOIN MORBIDITY_REPORT_EVENT t4
					ON t3.morb_rpt_key = t4.morb_rpt_key
					LEFT JOIN D_PATIENT t6
					ON t6.patient_key = t4.patient_key
					LEFT JOIN EVENT_METRIC t5 
					ON t3.morb_rpt_local_id = t5.local_id  -- both varchar
					CROSS JOIN data_validation_message	 t2
		WHERE ( 
--							COALESCE(t3.record_status_cd,'') <> 'INACTIVE'  -- added null fields. I commented out on 01/27/09 as the "active" condition already includes this condition.
							t5.last_chg_time IS NOT NULL -- added this clause per Jit to remove nullls 01/27/09 
							AND t5.last_chg_time >= COALESCE(@min_last_chg_time, t5.last_chg_time)
							)
					AND  (
								 t2.data_validation_message_id = 28 AND (t4.investigation_key = 1AND t3.record_status_cd = 'ACTIVE')  -- Orphaned Morb Report (Morb without an investigation) MRB100 Warning
								) 
	END
	PRINT 'Morbidity Report Validation Checks Insert Completed...'+ CONVERT(varchar(20), getdate(),120) 

END
GO
