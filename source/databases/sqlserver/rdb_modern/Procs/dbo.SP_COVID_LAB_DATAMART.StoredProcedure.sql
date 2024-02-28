USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[SP_COVID_LAB_DATAMART]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SP_COVID_LAB_DATAMART]
AS
    BEGIN
        DECLARE @REFRESH_TIME AS DATETIME;
        DECLARE @COVID_DM_REFRESH_TIME AS DATETIME;
        DECLARE @seedValue AS BIGINT, @stateSiteCd VARCHAR(20);
        SET @REFRESH_TIME = SYSDATETIME();
        SET @COVID_DM_REFRESH_TIME = SYSDATETIME();
        SET @seedValue =
        (
            SELECT CAST(config_value AS BIGINT)
            FROM nbs_odse.dbo.NBS_configuration
            WHERE config_key = 'SEED_VALUE'
        );
        SET @stateSiteCd =
        (
            SELECT config_value
            FROM nbs_odse.dbo.NBS_configuration
            WHERE config_key = 'STATE_SITE_CODE'
        );
        SET @REFRESH_TIME =
        (
            SELECT MAX(REFRESH_TIME)
            FROM dbo.DATAMART_REFRESH_ACTIVITY_LOG
            WHERE STORED_PROCEDURE_NM = 'SP_COVID_LAB_DATAMART'
        );
        SET @COVID_DM_REFRESH_TIME =
        (
            SELECT MAX(REFRESH_TIME)
            FROM dbo.DATAMART_REFRESH_ACTIVITY_LOG
            WHERE STORED_PROCEDURE_NM = 'SP_COVID_LAB_DATAMART'
        );
        DECLARE @RowCount_no BIGINT;
        BEGIN TRY
            BEGIN TRANSACTION;
            PRINT 'BEGIN LIST LOINC RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('COVID_LOINC_LIST_FROM_DM') IS NOT NULL
                DROP TABLE COVID_LOINC_LIST_FROM_DM;
            SELECT Resulted_Test_Cd
            INTO COVID_LOINC_LIST_FROM_DM
            FROM COVID_LAB_DATAMART WITH(NOLOCK) group by Resulted_Test_Cd;
            PRINT 'END LIST LOINC RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'BEGIN LIST MISSING LOINC RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('COVID_MISSING_LOINC_LIST') IS NOT NULL
                DROP TABLE COVID_MISSING_LOINC_LIST;
            SELECT observation_uid
            INTO COVID_MISSING_LOINC_LIST
            FROM nbs_changedata.dbo.observation WITH(NOLOCK)
            WHERE obs_domain_cd_st_1 = 'Result'
                  AND cd IN
            (
                SELECT loinc_cd
                FROM nbs_srte..Loinc_condition
                WHERE condition_cd = '11065'
                      AND loinc_cd NOT IN
                (
                    SELECT Resulted_Test_Cd
                    FROM COVID_LOINC_LIST_FROM_DM
                )
            );
            PRINT 'END LIST MISSING LOINC RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'BEGIN LIST MERGED RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('COVID_MERGED_LAB_LIST', 'U') IS NOT NULL
                DROP TABLE COVID_MERGED_LAB_LIST;
            BEGIN
                SELECT *
                INTO COVID_MERGED_LAB_LIST
                FROM
                (
                    SELECT Observation.observation_uid, 
                           o1.observation_uid AS result_observation_uid, 
                           Observation.local_id AS Lab_Local_ID, 
                           Person.local_id AS Patient_Local_ID, 
                           person.last_chg_time
                    FROM nbs_changedata.dbo.Observation Observation WITH(NOLOCK)
                         INNER JOIN nbs_changedata.dbo.act_relationship ar WITH(NOLOCK) ON Observation.observation_uid = ar.target_act_uid
                                                                                     AND ar.type_cd = 'COMP'
                         INNER JOIN nbs_changedata.dbo.observation o1 WITH(NOLOCK) ON ar.source_act_uid = o1.observation_uid
                                                                                AND o1.obs_domain_cd_st_1 = 'Result'
                         INNER JOIN nbs_changedata.dbo.Participation Participation WITH(NOLOCK) ON Participation.act_uid = Observation.observation_uid
                         INNER JOIN nbs_changedata.dbo.person person WITH(NOLOCK) ON Participation.subject_entity_uid = person.person_uid
                    WHERE Participation.type_cd = 'PATSBJ'
                          AND (Person.last_chg_time > @COVID_DM_REFRESH_TIME
                               OR Observation.last_chg_time > @COVID_DM_REFRESH_TIME)
                          AND (o1.cd IN
                    (
                        SELECT loinc_cd
                        FROM nbs_srte..Loinc_condition
                        WHERE condition_cd = '11065'
                    )
                               OR o1.cd IN(''))--replace '' with the local codes seperated by comma
                         AND o1.cd NOT IN
                    (
                        SELECT loinc_cd
                        FROM nbs_srte..Loinc_code
                        WHERE time_aspect = 'Pt'
                              AND system_cd = '^Patient'
                    )
                    UNION
                    SELECT Observation.observation_uid, 
                           o1.observation_uid AS result_observation_uid, 
                           Observation.local_id AS Lab_Local_ID, 
                           Person.local_id AS Patient_Local_ID, 
                           person.last_chg_time
                    FROM nbs_changedata.dbo.Observation Observation WITH(NOLOCK)
                         INNER JOIN nbs_changedata.dbo.act_relationship ar WITH(NOLOCK) ON Observation.observation_uid = ar.target_act_uid
                                                                                     AND ar.type_cd = 'COMP'
                         INNER JOIN nbs_changedata.dbo.observation o1 WITH(NOLOCK) ON ar.source_act_uid = o1.observation_uid
                                                                                AND o1.obs_domain_cd_st_1 = 'Result'
                         INNER JOIN nbs_changedata.dbo.Participation Participation WITH(NOLOCK) ON Participation.act_uid = Observation.observation_uid
                         INNER JOIN nbs_changedata.dbo.person person WITH(NOLOCK) ON Participation.subject_entity_uid = person.person_uid
                         INNER JOIN COVID_MISSING_LOINC_LIST missing_loinc WITH(NOLOCK) ON missing_LOINC.observation_uid = o1.observation_uid
                ) AS COVID_INC;
            END;
            SET @REFRESH_TIME =
            (
                SELECT ISNULL(MAX(last_chg_time), @REFRESH_TIME)
                FROM dbo.COVID_MERGED_LAB_LIST
            );
            PRINT 'END LIST MERGED RECORDS FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'BEGIN COVID LAB DATAMART RECORDS FOR TEXT/COMMENTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_TEXT_RESULT_LIST') IS NOT NULL
                DROP TABLE COVID_TEXT_RESULT_LIST;
            BEGIN
                SELECT COVID_MERGED_LAB_LIST.observation_uid, 
                       result_observation_uid, 
                       Lab_Local_ID, 
                       Patient_Local_ID, 
                       last_chg_time, 
                       replace(replace(obs_text_results.text_results, CHAR(13), ' '), CHAR(10), ' ') AS 'Text_Result_Desc', 
                       replace(replace(obs_result_comment.result_comments, CHAR(13), ' '), CHAR(10), ' ') AS 'Result_Comments'
                INTO COVID_TEXT_RESULT_LIST
                FROM COVID_MERGED_LAB_LIST
                     LEFT OUTER JOIN
                (
                    SELECT DISTINCT 
                           observation_uid, 
                           LTRIM(STUFF(
                    (
                        SELECT '; ' + value_txt
                        FROM nbs_changedata.dbo.Obs_value_txt ovt WITH(NOLOCK)
                        WHERE ovt.observation_uid = Obs_value_txt.observation_uid
                              AND (ovt.txt_type_cd = 'O'
                                   OR ovt.txt_type_cd IS NULL) FOR XML PATH('')
                    ), 1, 1, '')) text_results
                    FROM nbs_changedata.dbo.Obs_value_txt WITH(NOLOCK)
                    GROUP BY observation_uid, 
                             value_txt
                ) obs_text_results ON obs_text_results.observation_uid = result_observation_uid
                     LEFT OUTER JOIN
                (
                    SELECT DISTINCT 
                           observation_uid, 
                           LTRIM(STUFF(
                    (
                        SELECT '; ' + value_txt
                        FROM nbs_changedata.dbo.Obs_value_txt ovt WITH(NOLOCK)
                        WHERE ovt.observation_uid = Obs_value_txt.observation_uid
                              AND ovt.txt_type_cd = 'N' FOR XML PATH('')
                    ), 1, 1, '')) result_comments
                    FROM nbs_changedata.dbo.Obs_value_txt WITH(NOLOCK)
                    GROUP BY observation_uid, 
                             value_txt
                ) obs_result_comment ON obs_result_comment.observation_uid = result_observation_uid;
            END;
            PRINT 'END COVID LAB DATAMART RECORDS FOR TEXT/COMMENTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'BEGIN UPDATE COVID LAB DATAMART RECORDS FOR MERGED PATIENTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_DATAMART') IS NOT NULL
                BEGIN
                    UPDATE COVID_LAB_DATAMART
                      SET 
                          COVID_LAB_DATAMART.Patient_Local_ID = COVID_MERGED_LAB_LIST.Patient_Local_ID
                    FROM COVID_LAB_DATAMART
                         INNER JOIN COVID_MERGED_LAB_LIST ON COVID_LAB_DATAMART.Observation_Uid = COVID_MERGED_LAB_LIST.observation_uid;
            END;
            PRINT 'END UPDATE COVID LAB DATAMART RECORDS FOR MERGED PATIENTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;

            --performing facility Data
            BEGIN TRANSACTION;
            PRINT 'BEGIN PERFORMING FACILITY RECORDS FOR LAB RESULTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_PERFORMING_FACILITY', 'U') IS NOT NULL
                DROP TABLE DBO.COVID_PERFORMING_FACILITY;
            BEGIN
                SELECT o1.result_observation_uid, 
                       nm5.nm_txt AS 'Perform_Facility_Name', 
                       postal.street_addr1 AS 'Testing_lab_Address_One', 
                       postal.street_addr2 AS 'Testing_lab_Address_Two', 
                       postal.cntry_cd AS 'Testing_lab_Country', 
                       postal.cnty_cd AS 'Testing_lab_county', 
                       county.code_desc_txt AS 'Testing_lab_county_Desc', 
                       postal.city_desc_txt AS 'Testing_lab_City', 
                       postal.state_cd AS 'Testing_lab_State_Cd', 
                       state.state_NM AS 'Testing_lab_State', 
                       postal.zip_cd AS 'Testing_lab_Zip_Cd'
                INTO COVID_PERFORMING_FACILITY
                FROM COVID_TEXT_RESULT_LIST o1
                     LEFT JOIN nbs_changedata.dbo.participation part5 WITH(NOLOCK) ON o1.observation_uid = part5.act_uid
                                                                                AND part5.type_cd = 'PRF'
                                                                                AND part5.subject_class_cd = 'ORG'
                     LEFT JOIN nbs_changedata.dbo.organization_name NM5 WITH(NOLOCK) ON part5.subject_entity_uid = NM5.organization_uid
                     OUTER APPLY
                (
                    SELECT TOP (1) *
                    FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                    WHERE Entity_locator_participation.entity_uid = NM5.organization_uid
                          AND Entity_locator_participation.class_cd = 'PST'
                          AND Entity_locator_participation.use_cd = 'WP'
                          AND Entity_locator_participation.cd = 'O'
                          AND Entity_locator_participation.status_cd = 'A'
                    ORDER BY Entity_locator_participation.locator_uid DESC
                ) AS entity_part1
                     LEFT JOIN nbs_changedata.dbo.postal_locator postal WITH(NOLOCK) ON entity_part1.locator_uid = postal.postal_locator_uid
                     LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value county WITH(NOLOCK) ON county.code = postal.cnty_cd
                     LEFT OUTER JOIN nbs_srte.dbo.State_code state WITH(NOLOCK) ON state.state_cd = postal.state_cd;
            END;
            PRINT 'END PERFORMING FACILITY RECORDS FOR LAB RESULTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            ---LAB COMMENTS

            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_COMMENT_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_COMMENT_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_COMMENT_DATA;
            BEGIN
                SELECT o.observation_uid, 
                       obs_lab_comment.lab_comments
                INTO dbo.COVID_LAB_COMMENT_DATA
                FROM COVID_TEXT_RESULT_LIST o
                     LEFT OUTER JOIN
                (
                    SELECT DISTINCT 
                           observation_uid, 
                           LTRIM(STUFF(
                    (
                        SELECT '; ' + value_txt
                        FROM nbs_changedata.dbo.Obs_value_txt ovt WITH(NOLOCK)
                             INNER JOIN nbs_changedata.dbo.observation o2 WITH(NOLOCK) ON o2.observation_uid = ovt.observation_uid
                                                                                 AND o2.cd = 'LAB214'
                                                                                 AND o2.obs_domain_cd_st_1 = 'C_Result'
                             INNER JOIN nbs_changedata.dbo.Act_relationship a2 WITH(NOLOCK) ON a2.source_act_uid = o2.observation_uid
                             INNER JOIN nbs_changedata.dbo.Act_relationship a1 WITH(NOLOCK) ON a1.source_act_uid = a2.target_act_uid
                        WHERE a1.target_act_uid = observation.observation_uid FOR XML PATH('')
                    ), 1, 1, '')) lab_comments
                    FROM nbs_changedata.dbo.observation WITH(NOLOCK)
                    GROUP BY observation_uid
                ) obs_lab_comment ON obs_lab_comment.observation_uid = o.observation_uid
                WHERE obs_lab_comment.lab_comments IS NOT NULL
                      AND obs_lab_comment.lab_comments != ''
                GROUP BY o.observation_uid, 
                         lab_comments;
            END;
            PRINT 'END PERFORMING FACILITY RECORDS FOR LAB RESULTS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;

            -- Core Lab Data
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_CORE_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_CORE_DATA;
            SELECT DISTINCT 
                   o.Observation_uid AS 'Observation_UID', 
                   o.local_id AS 'Lab_Local_ID', 
                   o.record_status_cd, 
                   o.cd AS 'Ordered_Test_Cd', 
                   o.cd_desc_txt AS 'Ordered_Test_Desc', 
                   o.cd_system_cd AS 'Ordered_Test_Code_System', 
                   o.electronic_ind AS 'Electronic_Ind', 
                   o.prog_area_cd AS 'Program_Area_Cd', 
                   o.jurisdiction_cd AS 'Jurisdiction_Cd', 
                   o.program_jurisdiction_oid AS 'program_jurisdiction_oid', 
                   o.activity_to_time AS 'Lab_Report_Dt', 
                   o.RPT_TO_STATE_TIME AS 'Lab_Rpt_Received_By_PH_Dt', 
                   o.activity_from_time AS 'ORDER_TEST_DATE', 
                   o.target_site_cd AS 'SPECIMEN_SOURCE_SITE_CD', 
                   o.target_site_desc_txt AS 'SPECIMEN_SOURCE_SITE_DESC', 
                   cvg1.code_short_desc_txt AS 'Order_result_status', 
                   Jurisdiction_code.code_short_desc_txt AS 'Jurisdiction_Nm', 
                   mart.cd AS 'Specimen_Cd', 
                   mart.cd_desc_txt AS 'Specimen_Desc', 
                   mart.description AS 'Specimen_type_free_text',
                   CASE
                       WHEN mei.root_extension_txt IS NOT NULL
                            AND mei.root_extension_txt != ''
                       THEN mei.root_extension_txt
                       WHEN ei.root_extension_txt IS NOT NULL
                            AND ei.root_extension_txt != ''
                       THEN ei.root_extension_txt
                       ELSE o.local_id
                   END AS 'Specimen_Id',
                   CASE
                       WHEN ei.root_extension_txt IS NULL
                            OR ei.root_extension_txt = ''
                       THEN o.local_id
                       ELSE ei.root_extension_txt
                   END AS 'Testing_Lab_Accession_Number', 
                   o.add_time AS 'Lab_Added_Dt', 
                   o.last_chg_time AS 'Lab_Update_Dt', 
                   o.effective_from_time AS 'Specimen_Coll_Dt', 
                   o1.observation_uid AS 'COVID_LAB_DATAMART_KEY', 
                   o1.cd AS 'Resulted_Test_Cd', 
                   o1.cd_desc_txt AS 'Resulted_Test_Desc', 
                   o1.cd_system_cd AS 'Resulted_Test_Code_System', 
                   eii.root_extension_txt AS 'DEVICE_INSTANCE_ID_1', 
                   eii1.root_extension_txt AS 'DEVICE_INSTANCE_ID_2', 
                   cvg2.code_short_desc_txt AS 'Test_result_status', 
                   o1.method_desc_txt AS 'Test_Method_Desc', 
                   devices.value('/Devices[1]/Device[1]', 'varchar(199)') AS Device_Type_Id_1, 
                   devices.value('/Devices[1]/Device[2]', 'varchar(199)') AS Device_Type_Id_2, 
                   Perform_Facility_Name, 
                   Testing_lab_Address_One, 
                   Testing_lab_Address_Two, 
                   Testing_lab_Country, 
                   Testing_lab_county, 
                   Testing_lab_county_Desc, 
                   Testing_lab_City, 
                   Testing_lab_State_Cd, 
                   Testing_lab_State, 
                   Testing_lab_Zip_Cd, 
                   ovc.code AS 'Result_Cd', 
                   ovc.code_system_cd AS 'Result_Cd_Sys', 
                   ovc.display_name AS 'Result_Desc', 
                   Text_Result_Desc, 
                   ovn.comparator_cd_1 AS 'Numeric_Comparator_Cd', 
                   ovn.numeric_value_1 AS 'Numeric_Value_1', 
                   ovn.numeric_value_2 AS 'Numeric_Value_2', 
                   ovn.numeric_unit_cd AS 'Numeric_Unit_Cd', 
                   ovn.low_range AS 'Numeric_Low_Range', 
                   ovn.high_range AS 'Numeric_High_Range', 
                   ovn.separator_cd AS 'Numeric_Separator_Cd', 
                   intrep.interpretation_cd AS 'Interpretation_Cd', 
                   intrep.interpretation_desc_txt AS 'Interpretation_Desc', 
                   Result_Comments, 
                   COVID_LAB_COMMENT_DATA.lab_comments AS 'Lab_Comments', 
                   LTRIM(ISNULL(ovc.display_name, '') + ' ' + ISNULL(Text_Result_Desc, '') + ' ' + ISNULL(Result_Comments, ' ')) AS 'Result'
            INTO dbo.COVID_LAB_CORE_DATA
            FROM nbs_changedata.dbo.observation o WITH(NOLOCK)
                 INNER JOIN COVID_TEXT_RESULT_LIST WITH(NOLOCK) ON o.observation_uid = COVID_TEXT_RESULT_LIST.observation_uid
                 INNER JOIN nbs_changedata.dbo.participation p WITH(NOLOCK) ON o.observation_uid = p.act_uid
                                                                         AND p.type_cd = 'AUT'
                 INNER JOIN nbs_changedata.dbo.organization_name NM WITH(NOLOCK) ON P.subject_entity_uid = NM.organization_uid
                 INNER JOIN nbs_changedata.dbo.act_relationship ar WITH(NOLOCK) ON o.observation_uid = ar.target_act_uid
                                                                             AND ar.type_cd = 'COMP'
                 INNER JOIN nbs_changedata.dbo.observation o1 WITH(NOLOCK) ON ar.source_act_uid = o1.observation_uid
                                                                        AND o1.obs_domain_cd_st_1 = 'Result'
                                                                        AND o1.observation_uid = COVID_TEXT_RESULT_LIST.result_observation_uid
                 LEFT OUTER JOIN nbs_changedata.dbo.Observation_interp intrep ON intrep.observation_uid = o1.observation_uid
                 LEFT OUTER JOIN nbs_changedata.dbo.obs_value_coded ovc WITH(NOLOCK) ON o1.observation_uid = ovc.observation_uid
                 LEFT OUTER JOIN nbs_srte.dbo.Jurisdiction_code ON Jurisdiction_code.code = o.jurisdiction_cd
                 LEFT OUTER JOIN nbs_srte..Code_value_general cvg1 ON cvg1.code = o.STATUS_CD
                                                                      AND cvg1.code_set_nm = 'ACT_OBJ_ST'
                 LEFT OUTER JOIN nbs_srte..Code_value_general cvg2 ON cvg2.code = o1.STATUS_CD
                                                                      AND cvg2.code_set_nm = 'ACT_OBJ_ST'
                 LEFT OUTER JOIN nbs_changedata.dbo.Obs_value_numeric ovn WITH(NOLOCK) ON o1.observation_uid = ovn.observation_uid
                 LEFT JOIN nbs_changedata.dbo.participation part3 WITH(NOLOCK) ON o.observation_uid = part3.act_uid
                                                                            AND part3.type_cd = 'SPC'
                 LEFT JOIN nbs_changedata.dbo.material mart WITH(NOLOCK) ON part3.subject_entity_uid = mart.material_uid
                 LEFT JOIN nbs_changedata.dbo.Entity_id mei WITH(NOLOCK) ON mart.material_uid = mei.entity_uid
                                                                      AND mei.type_cd = 'SPC'
                 LEFT JOIN nbs_changedata.dbo.Act_id ei WITH(NOLOCK) ON o.observation_uid = ei.act_uid
                                                                  AND ei.type_cd = 'FN'
                 LEFT JOIN nbs_changedata.dbo.Act_id eii WITH(NOLOCK) ON o1.observation_uid = eii.act_uid
                                                                   AND eii.type_cd = 'EII'
                                                                   AND eii.act_id_seq = 3
                 LEFT JOIN nbs_changedata.dbo.Act_id eii1 WITH(NOLOCK) ON o1.observation_uid = eii1.act_uid
                                                                    AND eii1.type_cd = 'EII'
                                                                    AND eii1.act_id_seq = 4
                 LEFT JOIN COVID_PERFORMING_FACILITY ON COVID_PERFORMING_FACILITY.result_observation_uid = o1.observation_uid
                 LEFT OUTER JOIN COVID_LAB_COMMENT_DATA ON COVID_LAB_COMMENT_DATA.observation_uid = o.observation_uid
                 OUTER APPLY
            (
                SELECT CONVERT(XML, '<Devices><Device>' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(o1.method_cd, '&', '&#38;'), '<', '&#60;'), '''', '&#39;'), '"', '&#34;'), '>', '&#62;'), '**', '</Device><Device>') + '</Device></Devices>') AS devices
                FROM nbs_changedata.dbo.Observation WITH(NOLOCK)
                WHERE Observation.observation_uid = o1.observation_uid
                      AND method_cd IS NOT NULL
            ) methodCd;
            SET @REFRESH_TIME =
            (
                SELECT ISNULL(MAX(Lab_Update_Dt), @REFRESH_TIME)
                FROM dbo.COVID_LAB_CORE_DATA WITH(NOLOCK)
            );
            PRINT 'End COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin DELETE UPDATED RECORDS into COVID_LAB_DATAMART: ' + CONVERT(VARCHAR, GETDATE(), 9);
            DELETE FROM dbo.COVID_LAB_DATAMART
            WHERE Observation_uid IN
            (
                SELECT Observation_UID
                FROM COVID_LAB_CORE_DATA WITH(NOLOCK)
            );
            PRINT 'End DELETE UPDATED RECORDS into COVID_LAB_DATAMART: ' + CONVERT(VARCHAR, GETDATE(), 9);
            PRINT 'Begin DELETE FROM COVID_LAB_CORE_DATA for deleted records: ' + CONVERT(VARCHAR, GETDATE(), 9);
            DELETE FROM dbo.COVID_LAB_CORE_DATA
            WHERE record_status_cd = 'LOG_DEL';
            PRINT 'End DELETE FROM COVID_LAB_CORE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_AOE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('nbs_changedata.dbo.LOOKUP_QUESTION', 'U') IS NULL
                BEGIN
                    DECLARE @createTableSQL NVARCHAR(250);
                    SET @createTableSQL = 'CREATE TABLE dbo.COVID_LAB_AOE_DATA ( AOE_Observation_uid bigint null, FIRST_TEST varchar(50) NULL)';
                    IF OBJECT_ID('dbo.COVID_LAB_AOE_DATA', 'U') IS NOT NULL
                        DROP TABLE dbo.COVID_LAB_AOE_DATA;
                    EXEC sp_executesql 
                         @createTableSQL;
            END;
                ELSE
                BEGIN
                    IF OBJECT_ID('dbo.COVID_LAB_AOE_ST', 'U') IS NOT NULL
                        DROP TABLE dbo.COVID_LAB_AOE_ST;
                    SELECT DISTINCT 
                           o.observation_uid AS 'AOE_Observation_uid', 
                           o1.cd, 
                           rdb_column_nm,
                           CASE
                               WHEN Obs_value_numeric.numeric_value_1 IS NOT NULL
                               THEN CAST(numeric_value_1 AS VARCHAR(20)) + '^' + Obs_value_numeric.numeric_unit_cd
                               WHEN Obs_value_txt.value_txt IS NOT NULL
                               THEN Obs_value_txt.value_txt
                               WHEN Obs_value_coded.code IS NOT NULL
                               THEN Code_value_general.code_short_desc_txt
                           END AS Answer_Txt
                    INTO COVID_LAB_AOE_ST
                    FROM nbs_changedata.dbo.LOOKUP_QUESTION lq
                         LEFT OUTER JOIN nbs_changedata.dbo.observation o1 WITH(NOLOCK) ON o1.cd = lq.from_question_identifier
                         LEFT OUTER JOIN nbs_changedata.dbo.act_relationship ar WITH(NOLOCK) ON ar.source_act_uid = o1.observation_uid
                                                                                          AND o1.obs_domain_cd_st_1 = 'Result'
                         LEFT OUTER JOIN nbs_changedata.dbo.observation o WITH(NOLOCK) ON o.observation_uid = ar.target_act_uid
                                                                                 AND ar.type_cd = 'COMP'
                         LEFT OUTER JOIN nbs_changedata.dbo.obs_value_coded obs_value_coded WITH(NOLOCK) ON obs_value_coded.observation_uid = o1.observation_uid
                         LEFT OUTER JOIN nbs_changedata.dbo.Obs_value_txt Obs_value_txt WITH(NOLOCK) ON Obs_value_txt.observation_uid = o1.observation_uid
                                                                                 AND (Obs_value_txt.txt_type_cd = 'O'
                                                                                      OR Obs_value_txt.txt_type_cd IS NULL)
                         LEFT OUTER JOIN nbs_changedata.dbo.Obs_value_numeric Obs_value_numeric WITH(NOLOCK) ON Obs_value_numeric.observation_uid = o1.observation_uid
                         LEFT OUTER JOIN nbs_srte..Code_value_general ON Code_value_general.code_set_nm = lq.FROM_CODE_SET
                                                                         AND Obs_value_coded.code = Code_value_general.code
                    WHERE O.observation_uid IN
                    (
                        SELECT COVID_MERGED_LAB_LIST.observation_uid
                        FROM COVID_MERGED_LAB_LIST
                        GROUP BY observation_uid
                    );
                    IF OBJECT_ID('dbo.COVID_LAB_AOE_DATA', 'U') IS NOT NULL
                        DROP TABLE dbo.COVID_LAB_AOE_DATA;
                    DECLARE @columns NVARCHAR(MAX);
                    DECLARE @sql NVARCHAR(MAX);
                    SET @columns = N'';
                    SELECT @columns+=N', p.' + QUOTENAME(LTRIM(RTRIM([RDB_COLUMN_NM])))
                    FROM
                    (
                        SELECT DISTINCT 
                               [rdb_column_nm]
                        FROM nbs_changedata.dbo.LOOKUP_QUESTION AS p WITH(NOLOCK)
                        WHERE FROM_FORM_CD = 'LAB_REPORT'
                    ) AS x;
                    --PRINT @columns;
                    SET @sql = N'SELECT [AOE_Observation_uid] , ' + STUFF(@columns, 1, 2, '') + ' into dbo.COVID_LAB_AOE_DATA ' + 'FROM (
						SELECT [AOE_Observation_uid], answer_txt, [rdb_column_nm] 
						from COVID_LAB_AOE_ST
						AS p with (nolock)
						group by [AOE_Observation_uid], [answer_txt] , [rdb_column_nm]
							) AS j PIVOT (max(answer_txt) FOR [rdb_column_nm] in 
						(' + STUFF(REPLACE(@columns, ', p.[', ',['), 1, 1, '') + ')) AS p;';
                    --PRINT @sql;
                    EXEC sp_executesql 
                         @sql;
            END;
            PRINT 'End COVID_LAB_AOE_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin Alter Datamart Columns for AOE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            DECLARE @Temp_Query_Table TABLE
            (ID         INT IDENTITY(1, 1), 
             QUERY_stmt VARCHAR(5000)
            );
            DECLARE @column_query VARCHAR(5000);
            DECLARE @Max_Query_No INT;
            DECLARE @Curr_Query_No INT;
            DECLARE @ColumnList VARCHAR(5000);
            INSERT INTO @Temp_Query_Table
                   SELECT 'ALTER TABLE dbo.COVID_LAB_DATAMART ADD [' + COLUMN_NAME + '] ' + DATA_TYPE + CASE
                                                                                                                WHEN DATA_TYPE IN('char', 'varchar', 'nchar', 'nvarchar')
                                                                                                                THEN ' (' + COALESCE(CAST(NULLIF(CHARACTER_MAXIMUM_LENGTH, -1) AS VARCHAR(10)), CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10))) + ')'
                                                                                                                ELSE ''
                                                                                                            END + CASE
                                                                                                                      WHEN IS_NULLABLE = 'NO'
                                                                                                                      THEN ' NOT NULL'
                                                                                                                      ELSE ' NULL'
                                                                                                                  END
                   FROM INFORMATION_SCHEMA.COLUMNS AS c
                   WHERE TABLE_NAME = 'COVID_LAB_AOE_DATA'
                         AND NOT EXISTS
                   (
                       SELECT COLUMN_NAME
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_NAME = 'COVID_LAB_DATAMART'
                             AND COLUMN_NAME = c.COLUMN_NAME
                   )
                         AND COLUMN_NAME NOT IN('AOE_Observation_uid');
            --SELECT *
            --FROM @Temp_Query_Table;
            SET @Max_Query_No =
            (
                SELECT MAX(ID)
                FROM @Temp_Query_Table AS t
            );
            SET @Curr_Query_No = 0;
            WHILE @Max_Query_No > @Curr_Query_No
                BEGIN
                    SET @Curr_Query_No = @Curr_Query_No + 1;
                    SET @column_query =
                    (
                        SELECT QUERY_stmt
                        FROM @Temp_Query_Table AS t
                        WHERE ID = @Curr_Query_No
                    );
                    --SELECT @column_query;
                    EXEC (@column_query);
                END;
            PRINT 'End Alter Datamart Columns for AOE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_RSLT_TYPE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_RSLT_TYPE', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_RSLT_TYPE;
            SELECT COVID_LAB_CORE_DATA.Observation_UID AS 'RT_Observation_UID', 
                   --COVID_LAB_CORE_DATA.Lab_Local_ID, 
                   COVID_LAB_CORE_DATA.Result AS 'RT_Result',
                   CASE
                                                 -- Modify the logic (add additional variables) to determine negative labs
                       WHEN result IN('NEGATIVE', 'Negative:  SARS-CoV-2 virus is NOT detected', 'PAN SARS RNA:    NEGATIVE', 'PRESUMPTIVE NEGATIVE', 'SARS COV 2 RNA:  NEGATIVE', 'Not Detected', 'Not detected (qualifier value)', 'OVERALL RESULT:  NOT DETECTED', 'Undetected', 'SARS-CoV-2 RNA was not present in the specimen')
                            OR result LIKE '%Negative%'
                            OR result LIKE '%Presumptive Negative%'
                            OR result LIKE 'the specimen is negative for sars-cov%'
                            OR result LIKE '%not detected%'
                            OR result LIKE 'undetected%'
                       THEN 'Negative'
                                                 -- Modify the logic (add additional variables) to determine positive labs
                       WHEN result IN('***DETECTED***', 'Presum-Pos', 'present')
                            OR result LIKE 'abnormal%'
                            OR (result LIKE '%detected%'
                                AND result NOT LIKE '%not detected%'
                                AND result NOT LIKE '%undetected%')
                            OR result LIKE 'positive%'
                            OR result LIKE '%positive%'
                            OR result LIKE 'presumptive pos%'
                            OR result LIKE 'the specimen is positive for sars-cov%'
                       THEN 'Positive'
                                                 -- Modify the logic (add additional variables) to determine Indeterminate labs
                       WHEN result IN('Inconclusive', 'Indeterminate', 'Invalid', 'not det', 'Not Performed', 'pendingPUI', 'unknown', 'unknowninconclusive')
                            OR result LIKE '%INCONCLUSIVE by RT%'
                            OR result LIKE '%Inconclusive%'
                            OR result LIKE '%Indeterminate%'
                            OR result LIKE '%unresolved%'
                       THEN 'Indeterminate'
                       ELSE NULL
                   END AS Result_Category
            INTO dbo.COVID_LAB_RSLT_TYPE
            FROM dbo.COVID_LAB_CORE_DATA WITH(NOLOCK)
            WHERE Result != '';
            PRINT 'End COVID_LAB_RSLT_TYPE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_PATIENT_RACE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_PATIENT_RACE', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_PATIENT_RACE;
            SELECT subject_entity_uid, 
                   race
            INTO COVID_PATIENT_RACE
            FROM COVID_LAB_CORE_DATA a
                 INNER JOIN nbs_changedata.dbo.participation WITH(NOLOCK) ON a.observation_uid = participation.act_uid
                                                                    AND type_cd = 'PATSBJ'
                 LEFT OUTER JOIN
            (
                SELECT DISTINCT 
                       person_uid, 
                       LTRIM(STUFF(
                (
                    SELECT '; ' + code_short_desc_txt
                    FROM nbs_changedata.dbo.person_race pr WITH(NOLOCK)
                         INNER JOIN nbs_srte.dbo.Code_value_general ON pr.race_cd = Code_value_general.code
                                                                       AND Code_value_general.code_set_nm = 'PHVS_RACECATEGORY_CDC_NULLFLAVOR'
                    WHERE pr.person_uid = Person_race.person_uid FOR XML PATH('')
                ), 1, 1, '')) [Race]
                FROM nbs_changedata.dbo.Person_race WITH(NOLOCK)
                GROUP BY person_uid, 
                         race_cd
            ) patient_race ON patient_race.person_uid = participation.subject_entity_uid;
            PRINT 'End COVID_PATIENT_RACE: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_PATIENT_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_PATIENT_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_PATIENT_DATA;
            -- Patient Data
            SELECT DISTINCT 
                   o.Observation_uid AS 'Pat_Observation_UID', 
                   patient_name.last_nm AS 'Last_Name', 
                   patient_name.middle_nm AS 'Middle_Name', 
                   patient_name.first_nm AS 'First_Name', 
                   pers.local_id AS 'Patient_Local_ID', 
                   (CAST(SUBSTRING(pers.local_id, CHARINDEX('PSN', pers.local_id) + 3, CHARINDEX(@stateSiteCd, pers.local_id) - 4) AS BIGINT)) - @seedValue AS 'PATIENT_ID', 
                   pers.curr_sex_cd AS 'Current_Sex_Cd', 
                   pers.age_reported AS 'Age_Reported', 
                   pers.age_reported_unit_cd AS 'Age_Unit_Cd', 
                   pers.birth_time AS 'Birth_Dt', 
                   pers.deceased_time AS 'PATIENT_DEATH_DATE', 
                   pers.deceased_ind_cd AS 'PATIENT_DEATH_IND', 
                   tele.phone_nbr_txt AS 'Phone_Number', 
                   postal.street_addr1 AS 'Address_One', 
                   postal.street_addr2 AS 'Address_Two', 
                   postal.city_desc_txt AS 'City', 
                   postal.state_cd AS 'State_Cd', 
                   state.state_NM AS 'State', 
                   postal.zip_cd AS 'Zip_Code', 
                   postal.cnty_cd AS 'County_Cd', 
                   county.code_desc_txt AS 'County_Desc', 
                   COVID_PATIENT_RACE.Race AS 'PATIENT_RACE_CALC', 
                   pers.ETHNIC_GROUP_IND AS 'PATIENT_ETHNICITY'
            INTO dbo.COVID_LAB_PATIENT_DATA
            FROM dbo.COVID_LAB_CORE_DATA o WITH(NOLOCK)
                 INNER JOIN nbs_changedata.dbo.act_relationship ar WITH(NOLOCK) ON o.observation_uid = ar.target_act_uid
                                                                             AND ar.type_cd = 'COMP'
                 INNER JOIN nbs_changedata.dbo.observation o1 WITH(NOLOCK) ON ar.source_act_uid = o1.observation_uid
                                                                        AND o1.obs_domain_cd_st_1 = 'Result'
                 INNER JOIN nbs_changedata.dbo.participation part_2 WITH(NOLOCK) ON o.observation_uid = part_2.act_uid
                                                                              AND part_2.type_cd = 'PATSBJ'
                 INNER JOIN nbs_changedata.dbo.person pers WITH(NOLOCK) ON part_2.subject_entity_uid = pers.person_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.person_name p_name WITH(NOLOCK)
                WHERE part_2.subject_entity_uid = p_name.person_uid
                      AND part_2.type_cd = 'PATSBJ'
                      AND p_name.nm_use_cd = 'L'
                      AND p_name.status_cd = 'A'
                ORDER BY p_name.person_uid DESC
            ) AS patient_name
                 LEFT OUTER JOIN COVID_PATIENT_RACE ON covid_patient_race.subject_entity_uid = pers.person_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = pers.person_uid
                      AND Entity_locator_participation.class_cd = 'TELE'
                      AND Entity_locator_participation.use_cd = 'H'
                      AND Entity_locator_participation.cd = 'PH'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS entity_part
                 LEFT JOIN nbs_changedata.dbo.tele_locator tele WITH(NOLOCK) ON tele.tele_locator_uid = entity_part.locator_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = pers.person_uid
                      AND Entity_locator_participation.class_cd = 'PST'
                      AND Entity_locator_participation.use_cd = 'H'
                      AND Entity_locator_participation.cd = 'H'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS entity_part1
                 LEFT JOIN nbs_changedata.dbo.postal_locator postal WITH(NOLOCK) ON entity_part1.locator_uid = postal.postal_locator_uid
                 LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value county ON county.code = postal.cnty_cd
                 LEFT OUTER JOIN nbs_srte.dbo.State_code state ON state.state_cd = postal.state_cd;
            PRINT 'End COVID_LAB_PATIENT_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_ENTITIES_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_ENTITIES_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_ENTITIES_DATA;
            --Lab Entities Data

            SELECT DISTINCT 
                   o.Observation_UID AS 'Entity_Observation_uid', 
                   nm.nm_txt AS 'Reporting_Facility_Name', 
                   postal2.street_addr1 AS 'Reporting_Facility_Address_One', 
                   postal2.street_addr2 AS 'Reporting_Facility_Address_Two', 
                   postal2.cntry_cd AS 'Reporting_Facility_Country', 
                   postal2.cnty_cd AS 'Reporting_Facility_County', 
                   county2.code_desc_txt AS 'Reporting_Facility_County_Desc', 
                   postal2.city_desc_txt AS 'Reporting_Facility_City', 
                   postal2.state_cd AS 'Reporting_Facility_State_Cd', 
                   state2.state_NM AS 'Reporting_Facility_State', 
                   postal2.zip_cd AS 'Reporting_Facility_Zip_Cd', 
                   OrgEntity.root_extension_txt AS 'Reporting_Facility_Clia', 
                   tele2.phone_nbr_txt AS 'Reporting_Facility_Phone_Nbr', 
                   tele2.extension_txt AS 'Reporting_Facility_Phone_Ext', 
                   nm4.nm_txt AS 'Ordering_Facility_Name', 
                   postal4.street_addr1 AS 'Ordering_Facility_Address_One', 
                   postal4.street_addr2 AS 'Ordering_Facility_Address_Two', 
                   postal4.cntry_cd AS 'Ordering_Facility_Country', 
                   postal4.cnty_cd AS 'Ordering_Facility_County', 
                   county4.code_desc_txt AS 'Ordering_Facility_County_Desc', 
                   postal4.city_desc_txt AS 'Ordering_Facility_City', 
                   postal4.state_cd AS 'Ordering_Facility_State_Cd', 
                   state4.state_NM AS 'Ordering_Facility_State', 
                   postal4.zip_cd AS 'Ordering_Facility_Zip_Cd', 
                   tele4.phone_nbr_txt AS 'Ordering_Facility_Phone_Nbr', 
                   tele4.extension_txt AS 'Ordering_Facility_Phone_Ext', 
                   nm6.first_nm AS 'Ordering_Provider_First_Name', 
                   nm6.last_nm AS 'Ordering_Provider_Last_Name', 
                   postal6.street_addr1 AS 'Ordering_Provider_Address_One', 
                   postal6.street_addr2 AS 'Ordering_Provider_Address_Two', 
                   postal6.cntry_cd AS 'Ordering_Provider_Country', 
                   postal6.cnty_cd AS 'Ordering_Provider_County', 
                   county6.code_desc_txt AS 'Ordering_Provider_County_Desc', 
                   postal6.city_desc_txt AS 'Ordering_Provider_City', 
                   postal6.state_cd AS 'Ordering_Provider_State_Cd', 
                   state6.state_NM AS 'Ordering_Provider_State', 
                   postal6.zip_cd AS 'Ordering_Provider_Zip_Cd', 
                   tele6.phone_nbr_txt AS 'Ordering_Provider_Phone_Nbr', 
                   tele6.extension_txt AS 'Ordering_Provider_Phone_Ext', 
                   OrdProviderID.root_extension_txt AS 'ORDERING_PROVIDER_ID'
            INTO dbo.COVID_LAB_ENTITIES_DATA
            FROM dbo.COVID_LAB_CORE_DATA o WITH(NOLOCK)
                 INNER JOIN nbs_changedata.dbo.participation p WITH(NOLOCK) ON o.observation_uid = p.act_uid
                                                                         AND p.type_cd = 'AUT'
                 INNER JOIN nbs_changedata.dbo.organization_name NM WITH(NOLOCK) ON P.subject_entity_uid = NM.organization_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = p.subject_entity_uid
                      AND Entity_locator_participation.class_cd = 'PST'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'O'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS entity_part2
                 LEFT OUTER JOIN nbs_changedata.dbo.postal_locator postal2 WITH(NOLOCK) ON entity_part2.locator_uid = postal2.postal_locator_uid
                 LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value county2 ON County2.code = postal2.cnty_cd
                 LEFT OUTER JOIN nbs_srte.dbo.State_code state2 ON state2.state_cd = postal2.state_cd
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = p.subject_entity_uid
                      AND Entity_locator_participation.class_cd = 'TELE'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'PH'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS entity_part_tele2
                 LEFT OUTER JOIN nbs_changedata.dbo.Tele_locator tele2 WITH(NOLOCK) ON entity_part_tele2.locator_uid = tele2.tele_locator_uid
                 -- LEFT OUTER JOIN nbs_changedata.dbo.Entity_id orgID WITH(NOLOCK) ON p.subject_entity_uid = orgID.entity_uid
                 --                                                             AND orgID.type_cd in('FI', 'CLIA')
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_id orgID WITH(NOLOCK)
                WHERE p.subject_entity_uid = orgID.entity_uid
                      AND orgID.type_cd IN('FI', 'CLIA')
            ) AS OrgEntity
                 LEFT JOIN nbs_changedata.dbo.participation part4 WITH(NOLOCK) ON o.observation_uid = part4.act_uid
                                                                            AND part4.type_cd = 'ORD'
                                                                            AND part4.subject_class_cd = 'ORG'
                 LEFT JOIN nbs_changedata.dbo.organization_name NM4 WITH(NOLOCK) ON part4.subject_entity_uid = NM4.organization_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = NM4.organization_uid
                      AND Entity_locator_participation.class_cd = 'PST'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'O'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS OrdFacilityPSTELP
                 LEFT JOIN nbs_changedata.dbo.postal_locator postal4 WITH(NOLOCK) ON OrdFacilityPSTELP.locator_uid = postal4.postal_locator_uid
                 LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value county4 ON county4.code = postal4.cnty_cd
                 LEFT OUTER JOIN nbs_srte.dbo.State_code state4 ON state4.state_cd = postal4.state_cd
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = NM4.organization_uid
                      AND Entity_locator_participation.class_cd = 'TELE'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'PH'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS OrdFacilityTELEELP
                 LEFT JOIN nbs_changedata.dbo.tele_locator tele4 WITH(NOLOCK) ON OrdFacilityTELEELP.locator_uid = tele4.tele_locator_uid
                 LEFT JOIN nbs_changedata.dbo.participation part6 WITH(NOLOCK) ON o.observation_uid = part6.act_uid
                                                                            AND part6.type_cd = 'ORD'
                                                                            AND part6.subject_class_cd = 'PSN'
                 LEFT JOIN nbs_changedata.dbo.Person_name NM6 WITH(NOLOCK) ON part6.subject_entity_uid = NM6.person_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = NM6.person_uid
                      AND Entity_locator_participation.class_cd = 'PST'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'O'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS OrdProviderPSTELP
                 LEFT JOIN nbs_changedata.dbo.postal_locator postal6 WITH(NOLOCK) ON OrdProviderPSTELP.locator_uid = postal6.postal_locator_uid
                 LEFT OUTER JOIN nbs_srte.dbo.State_county_code_value county6 ON county6.code = postal6.cnty_cd
                 LEFT OUTER JOIN nbs_srte.dbo.State_code state6 ON state6.state_cd = postal6.state_cd
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.Entity_locator_participation WITH(NOLOCK)
                WHERE Entity_locator_participation.entity_uid = NM6.person_uid
                      AND Entity_locator_participation.class_cd = 'TELE'
                      AND Entity_locator_participation.use_cd = 'WP'
                      AND Entity_locator_participation.cd = 'PH'
                      AND Entity_locator_participation.status_cd = 'A'
                ORDER BY Entity_locator_participation.locator_uid DESC
            ) AS OrdProviderTELEELP
                 LEFT JOIN nbs_changedata.dbo.tele_locator tele6 WITH(NOLOCK) ON OrdProviderTELEELP.locator_uid = tele6.tele_locator_uid
                 OUTER APPLY
            (
                SELECT TOP (1) *
                FROM nbs_changedata.dbo.ENTITY_ID WITH(NOLOCK)
                WHERE ENTITY_ID.entity_uid = NM6.person_uid
                      AND ENTITY_ID.type_cd = 'NPI'
                      AND ENTITY_ID.status_cd = 'A'
                ORDER BY ENTITY_ID.as_of_date DESC
            ) AS OrdProviderID;
            PRINT 'End COVID_LAB_ENTITIES_DATA: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_ASSOCIATIONS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_LAB_ASSOCIATIONS', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_ASSOCIATIONS;
            SELECT DISTINCT 
                   COVID_LAB_CORE_DATA.Observation_UID AS 'ASSOC_OBSERVATION_UID', 
                   ASSOCIATIONS.Associated_Case_ID
            INTO dbo.COVID_LAB_ASSOCIATIONS
            FROM dbo.COVID_LAB_CORE_DATA WITH(NOLOCK)
                 INNER JOIN nbs_changedata.dbo.Act_relationship WITH(NOLOCK) ON Act_relationship.source_act_uid = COVID_LAB_CORE_DATA.Observation_UID
                 LEFT OUTER JOIN
            (
                SELECT DISTINCT 
                       SOURCE_ACT_UID, 
                       LTRIM(STUFF(
                (
                    SELECT ', ' + SSM.LOCAL_ID
                    FROM nbs_changedata.dbo.Public_health_case SSM
                         INNER JOIN nbs_changedata.dbo.Act_relationship SUB ON SUB.source_act_uid = Act_relationship.source_act_uid
                    WHERE SSM.public_health_case_uid = SUB.target_act_uid
                    ORDER BY CASE
                                 WHEN SSM.cd LIKE '11065'
                                 THEN 0
                                 ELSE 1
                             END, 
                             SSM.add_time ASC FOR XML PATH('')
                ), 1, 1, '')) AS Associated_Case_ID
                FROM nbs_changedata.dbo.Act_relationship WITH(NOLOCK)
                     INNER JOIN dbo.COVID_LAB_CORE_DATA ON Act_relationship.source_act_uid = COVID_LAB_CORE_DATA.observation_uid
            ) ASSOCIATIONS ON ASSOCIATIONS.source_act_uid = COVID_LAB_CORE_DATA.Observation_UID;
            PRINT 'End COVID_LAB_ASSOCIATIONS: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            DECLARE @insert_query NVARCHAR(MAX);
            SET @insert_query =
            (
                SELECT 'INSERT INTO  [dbo].[COVID_LAB_DATAMART]( ' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_CORE_DATA')
                          AND NAME NOT IN('record_status_cd') FOR XML PATH('')
                ), 1, 1, '') + ', [Result_Category], ' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_PATIENT_DATA')
                          AND NAME NOT IN('Pat_Observation_UID') FOR XML PATH('')
                ), 1, 1, '') + ',' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_ENTITIES_DATA')
                          AND NAME NOT IN('Entity_Observation_uid') FOR XML PATH('')
                ), 1, 1, '') + ',' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_AOE_DATA')
                          AND NAME NOT IN('AOE_Observation_uid') FOR XML PATH('')
                ), 1, 1, '') + ', [Associated_Case_ID]' + ' ) select distinct ' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_CORE_DATA')
                          AND NAME NOT IN('record_status_cd') FOR XML PATH('')
                ), 1, 1, '') + ', [Result_Category], ' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_PATIENT_DATA')
                          AND NAME NOT IN('Pat_Observation_UID') FOR XML PATH('')
                ), 1, 1, '') + ',' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_ENTITIES_DATA')
                          AND NAME NOT IN('Entity_Observation_uid') FOR XML PATH('')
                ), 1, 1, '') + ',' + STUFF(
                (
                    SELECT ', [' + name + ']'
                    FROM syscolumns
                    WHERE id = OBJECT_ID('COVID_LAB_AOE_DATA')
                          AND NAME NOT IN('AOE_Observation_uid') FOR XML PATH('')
                ), 1, 1, '') + ', [Associated_Case_ID] ' + ' 
	             FROM dbo.COVID_LAB_CORE_DATA 
                 LEFT OUTER JOIN dbo.COVID_LAB_RSLT_TYPE ON COVID_LAB_CORE_DATA.Observation_UID = COVID_LAB_RSLT_TYPE.RT_Observation_UID
                                                                AND COVID_LAB_CORE_DATA.Result = COVID_LAB_RSLT_TYPE.RT_Result
                 INNER JOIN dbo.COVID_LAB_PATIENT_DATA ON COVID_LAB_CORE_DATA.Observation_UID = COVID_LAB_PATIENT_DATA.Pat_Observation_UID
                 LEFT OUTER JOIN dbo.COVID_LAB_ENTITIES_DATA ON COVID_LAB_CORE_DATA.Observation_UID = COVID_LAB_ENTITIES_DATA.Entity_Observation_uid
				 LEFT OUTER JOIN dbo.COVID_LAB_AOE_DATA ON COVID_LAB_CORE_DATA.Observation_UID = COVID_LAB_AOE_DATA.AOE_Observation_uid
                 LEFT OUTER JOIN dbo.COVID_LAB_ASSOCIATIONS ON COVID_LAB_CORE_DATA.Observation_UID = COVID_LAB_ASSOCIATIONS.ASSOC_OBSERVATION_UID;'
            );
            --SELECT @insert_query;
            EXEC sp_executesql 
                 @insert_query;
            SELECT @RowCount_no = @@ROWCOUNT;
            INSERT INTO dbo.DATAMART_REFRESH_ACTIVITY_LOG
            (STORED_PROCEDURE_NM, 
             DATAMART_NM, 
             DATAMART_ROW_COUNT, 
             REFRESH_TIME
            )
            VALUES
            ('SP_COVID_LAB_DATAMART', 
             'COVID_LAB_DATAMART', 
             @RowCount_no, 
             @REFRESH_TIME
            );
            PRINT 'End COVID_LAB_DATAMART: ' + CONVERT(VARCHAR, GETDATE(), 9);
            COMMIT TRANSACTION;
            BEGIN TRANSACTION;
            PRINT 'Begin COVID_LAB_DATAMART cleanup: ' + CONVERT(VARCHAR, GETDATE(), 9);
            IF OBJECT_ID('dbo.COVID_PERFORMING_FACILITY', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_PERFORMING_FACILITY;
            IF OBJECT_ID('dbo.COVID_LAB_CORE_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_CORE_DATA;
            IF OBJECT_ID('COVID_MERGED_LAB_LIST', 'U') IS NOT NULL
                DROP TABLE COVID_MERGED_LAB_LIST;
			IF OBJECT_ID('COVID_LOINC_LIST_FROM_DM', 'U') IS NOT NULL
                DROP TABLE COVID_LOINC_LIST_FROM_DM;	
            IF OBJECT_ID('COVID_MISSING_LOINC_LIST', 'U') IS NOT NULL
                DROP TABLE COVID_MISSING_LOINC_LIST;
            IF OBJECT_ID('COVID_TEXT_RESULT_LIST', 'U') IS NOT NULL
                DROP TABLE COVID_TEXT_RESULT_LIST;
            IF OBJECT_ID('dbo.COVID_LAB_COMMENT_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_COMMENT_DATA;
            IF OBJECT_ID('dbo.COVID_LAB_PATIENT_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_PATIENT_DATA;
            IF OBJECT_ID('dbo.COVID_PATIENT_RACE', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_PATIENT_RACE;
            IF OBJECT_ID('dbo.COVID_LAB_ENTITIES_DATA', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_ENTITIES_DATA;
            IF OBJECT_ID('dbo.COVID_LAB_ASSOCIATIONS', 'U') IS NOT NULL
                DROP TABLE dbo.COVID_LAB_ASSOCIATIONS;
            IF OBJECT_ID('nbs_changedata.dbo.LOOKUP_QUESTION', 'U') IS NULL
                ALTER TABLE dbo.COVID_LAB_DATAMART DROP COLUMN FIRST_TEST;
            PRINT 'End COVID_LAB_DATAMART cleanup: ' + CONVERT(VARCHAR, GETDATE(), 9);
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
            SELECT ERROR_NUMBER() AS ErrorNumber, 
                   ERROR_SEVERITY() AS ErrorSeverity, 
                   ERROR_STATE() AS ErrorState, 
                   ERROR_PROCEDURE() AS ErrorProcedure, 
                   ERROR_LINE() AS ErrorLine, 
                   ERROR_MESSAGE() AS ErrorMessage;
            RETURN -1;
        END CATCH;
    END;
GO
