use NBS_ODSE
go;
Create or Alter PROCEDURE [dbo].[sp_PROVIDER_EVENT] @user_id_list varchar(max)
as
BEGIN

    --UPDATE ACTIVITY_LOG_DETAIL SET
    --START_DATE=DATETIME();

    DECLARE @Proc_Step_no FLOAT = 0;
    DECLARE @Proc_Step_Name VARCHAR(200) = '';

    BEGIN TRY

        -- Step 2

        SET NOCOUNT ON;
        SET @PROC_STEP_NO = 2;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_INIT';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_INIT', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_INIT;


        SELECT PERSON.PERSON_UID        'PROVIDER_UID',
               PERSON.LOCAL_ID          'PROVIDER_LOCAL_ID',
               PERSON.DESCRIPTION       'PROVIDER_GENERAL_COMMENTS',
               PERSON.ELECTRONIC_IND    'PROVIDER_ENTRY_METHOD',
               PERSON.PERSON_PARENT_UID 'PROVIDER_MPR_UID',
               PERSON.LAST_CHG_TIME     'PROVIDER_LAST_CHANGE_TIME',
               PERSON.ADD_TIME          'PROVIDER_ADD_TIME',
               PERSON.RECORD_STATUS_CD  'PROVIDER_RECORD_STATUS',
               PERSON.ADD_USER_ID,
               PERSON.LAST_CHG_USER_ID
        into NBS_ODSE.dbo.TMP_S_PROVIDER_INIT
        FROM NBS_ODSE.dbo.Person PERSON with (nolock)
        WHERE PERSON.CD = 'PRV'
          and person_uid in (SELECT value FROM STRING_SPLIT(@user_id_list, ','));

        -- Step 3
        --CREATE TABLE PROVIDER_UID_COLL AS

        SET @PROC_STEP_NO = 3;
        SET @PROC_STEP_NAME = ' GENERATING TMP_PROVIDER_UID_COLL';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL;


        SELECT PROVIDER_UID
        into NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL
        FROM NBS_ODSE.dbo.TMP_S_PROVIDER_INIT with (nolock);

        -- Step 4
        -- CREATE TABLE  S_INITPROVIDER AS

        SET @PROC_STEP_NO = 4;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPROVIDER';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_INITPROVIDER', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_INITPROVIDER;

        SELECT A.*,
               B.user_first_nm             AS 'ADD_USER_FIRST_NAME',
               B.user_last_nm              AS 'ADD_USER_LAST_NAME',
               C.user_first_nm             AS 'CHG_USER_FIRST_NAME',
               C.user_last_nm              AS 'CHG_USER_LAST_NAME',
               Cast(null as [varchar](50)) as PROVIDER_ADDED_BY,
               Cast(null as [varchar](50)) as PROVIDER_LAST_UPDATED_BY
        into NBS_ODSE.dbo.TMP_S_INITPROVIDER
        FROM NBS_ODSE.dbo.TMP_S_PROVIDER_INIT A with (nolock)
                 LEFT OUTER JOIN NBS_ODSE.dbo.Auth_user B with (nolock) ON A.ADD_USER_ID = B.NEDSS_ENTRY_ID
                 LEFT OUTER JOIN NBS_ODSE.dbo.Auth_user C with (nolock) ON A.ADD_USER_ID = C.NEDSS_ENTRY_ID;

        update NBS_ODSE.dbo.TMP_S_INITPROVIDER set PROVIDER_RECORD_STATUS = 'ACTIVE' where PROVIDER_RECORD_STATUS = '';
        update NBS_ODSE.dbo.TMP_S_INITPROVIDER
        set PROVIDER_RECORD_STATUS = 'INACTIVE'
        where PROVIDER_RECORD_STATUS = 'SUPERCEDED';
        update NBS_ODSE.dbo.TMP_S_INITPROVIDER
        set PROVIDER_RECORD_STATUS = 'INACTIVE'
        where PROVIDER_RECORD_STATUS = 'LOG_DEL';

        update NBS_ODSE.dbo.TMP_S_INITPROVIDER
        set PROVIDER_ADDED_BY = CAST((Case
                                          when len(rtrim(ADD_USER_FIRST_NAME)) > 0 and
                                               len(rtrim(ADD_USER_LAST_NAME)) > 0
                                              then rtrim(ADD_USER_LAST_NAME) + ', ' + rtrim(ADD_USER_FIRST_NAME)
                                          when len(rtrim(ADD_USER_FIRST_NAME)) > 0
                                              then rtrim(ADD_USER_FIRST_NAME)
                                          when len(rtrim(ADD_USER_LAST_NAME)) > 0
                                              then rtrim(ADD_USER_LAST_NAME)
                                          else ''
            END
            ) as varchar(50));

        update NBS_ODSE.dbo.TMP_S_INITPROVIDER
        set PROVIDER_LAST_UPDATED_BY = CAST((Case
                                                 when len(rtrim(CHG_USER_FIRST_NAME)) > 0 and
                                                      len(rtrim(CHG_USER_LAST_NAME)) > 0
                                                     then rtrim(CHG_USER_LAST_NAME) + ', ' + rtrim(CHG_USER_FIRST_NAME)
                                                 when len(rtrim(CHG_USER_FIRST_NAME)) > 0
                                                     then rtrim(CHG_USER_FIRST_NAME)
                                                 when len(rtrim(CHG_USER_LAST_NAME)) > 0
                                                     then rtrim(CHG_USER_LAST_NAME)
                                                 else ''
            END
            ) as varchar(50));

        -- Step 5
        -- CREATE TABLE S_PROVIDER_NAME AS


        SET @PROC_STEP_NO = 5;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_NAME';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_NAME', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_NAME;


        SELECT FIRST_NM,
               LAST_NM,
               MIDDLE_NM,
               NM_SUFFIX,
               NM_PREFIX,
               NM_DEGREE,
               NM_USE_CD,
               pn.PERSON_UID AS            'PROVIDER_UID_NAME',
               Cast(null as [varchar](50)) PROVIDER_FIRST_NAME,
               Cast(null as [varchar](50)) PROVIDER_LAST_NAME,
               Cast(null as [varchar](50)) PROVIDER_MIDDLE_NAME,
               Cast(null as [varchar](50)) PROVIDER_ALIAS_NICKNAME,
               Cast(null as [varchar](50)) PROVIDER_NAME_SUFFIX,
               Cast(null as [varchar](50)) PROVIDER_NAME_PREFIX,
               Cast(null as [varchar](50)) PROVIDER_NAME_DEGREE
        into NBS_ODSE.dbo.TMP_S_PROVIDER_NAME
        FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL tpuc WITH (NOLOCK)
                 INNER JOIN NBS_ODSE.dbo.PERSON_NAME pn WITH (NOLOCK) ON tpuc.PROVIDER_UID = pn.PERSON_UID
        WHERE NM_USE_CD = 'L';


        update NBS_ODSE.[dbo].TMP_S_PROVIDER_NAME
        SET PROVIDER_FIRST_NAME=FIRST_NM,
            PROVIDER_LAST_NAME=LAST_NM,
            PROVIDER_MIDDLE_NAME=MIDDLE_NM
        where NM_USE_CD = 'L';

        update NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME]
        set NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_SUFFIX = SUBSTRING(cvg.[code_short_desc_txt], 1, 50)
        FROM nbs_odse.dbo.NBS_question nq with (nolock),
             [NBS_SRTE].dbo.[Codeset] cd with (nolock),
             [NBS_SRTE].dbo.[Code_value_general] cvg with (nolock),
             NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME] sir
        where nq.question_identifier = ('DEM107')
          and cd.code_set_group_id = nq.code_set_group_id
          and cvg.code_set_nm = cd.code_set_nm
          and sir.NM_SUFFIX = cvg.code
          and sir.NM_SUFFIX is not null;


        update NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME]
        set NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_PREFIX = SUBSTRING(cvg.[code_short_desc_txt], 1, 50)
        FROM nbs_odse.dbo.NBS_question nq with (nolock),
             [NBS_SRTE].dbo.[Codeset] cd with (nolock),
             [NBS_SRTE].dbo.[Code_value_general] cvg with (nolock),
             NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME] sir
        where nq.question_identifier = ('DEM101')
          and cd.code_set_group_id = nq.code_set_group_id
          and cvg.code_set_nm = cd.code_set_nm
          and sir.NM_PREFIX = cvg.code
          and sir.NM_PREFIX is not null;

        update NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME]
        set NBS_ODSE.dbo.[TMP_S_PROVIDER_NAME].PROVIDER_NAME_DEGREE = NM_DEGREE
        where NM_DEGREE is not null;

-- Step 6

        SET @PROC_STEP_NO = 6;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_WITH_NM';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM;


        SELECT si.*, spn.*
        into NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM
        FROM NBS_ODSE.dbo.TMP_S_INITPROVIDER si
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_NAME spn with (nolock)
                                 ON si.PROVIDER_UID = spn.PROVIDER_UID_NAME;

        SET @PROC_STEP_NO = 7;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_POSTAL_LOCATOR';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR;

        with lst as (SELECT distinct pl.CITY_DESC_TXT                    AS 'PROVIDER_CITY',
                                     pl.CNTRY_CD                         AS 'PROVIDER_COUNTRY',
                                     pl.CNTY_CD                          AS 'PROVIDER_COUNTY_CODE',
                                     pl.STATE_CD                         AS 'PROVIDER_STATE_CODE',
                                     rtrim(pl.STREET_ADDR1)              AS 'PROVIDER_STREET_ADDRESS_1',
                                     rtrim(pl.STREET_ADDR2)              AS 'PROVIDER_STREET_ADDRESS_2',
                                     pl.ZIP_CD                           AS 'PROVIDER_ZIP',
                                     sc.CODE_DESC_TXT                    AS 'PROVIDER_STATE_DESC',
                                     substring(ccv.CODE_DESC_TXT, 1, 50) AS 'PROVIDER_COUNTY_DESC',
                                     cc.CODE_SHORT_DESC_TXT              AS 'PROVIDER_COUNTRY_DESC',
                                     elp.LOCATOR_DESC_TXT                AS 'PROVIDER_ADDRESS_COMMENTS',
                                     elp.ENTITY_UID                      as ENTITY_UID_POSTAL,
                                     Cast(null as [varchar](50))            PROVIDER_STATE,
                                     Cast(null as [varchar](50))            PROVIDER_COUNTY
                             ,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY elp.ENTITY_UID
                                         ORDER BY pl.POSTAL_LOCATOR_UID DESC
                                         )                               AS [ROWNO]

                     FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc
                              LEFT OUTER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock)
                                              ON puc.PROVIDER_UID = elp.ENTITY_UID and elp.record_status_cd = 'ACTIVE'
                              LEFT OUTER JOIN NBS_ODSE.dbo.POSTAL_LOCATOR pl with (nolock)
                                              ON elp.LOCATOR_UID = pl.POSTAL_LOCATOR_UID
                              LEFT OUTER JOIN NBS_SRTE.dbo.STATE_CODE sc with (nolock) ON sc.STATE_CD = pl.STATE_CD
                              LEFT OUTER JOIN NBS_SRTE.dbo.COUNTRY_CODE cc with (nolock) ON cc.CODE = pl.CNTRY_CD
                              LEFT OUTER JOIN NBS_SRTE.dbo.STATE_COUNTY_CODE_VALUE ccv with (nolock)
                                              ON ccv.CODE = pl.CNTY_CD
                     WHERE elp.USE_CD = 'WP'
                       AND elp.CD = 'O'
                       AND elp.CLASS_CD = 'PST')
        select *
        into NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
        from lst
        where rowno = 1;


        alter table NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
            drop column ROWNO;

        ALTER TABLE NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
            ALTER COLUMN PROVIDER_COUNTRY VARCHAR(50) NULL;
        ALTER TABLE NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
            ALTER COLUMN PROVIDER_COUNTY_DESC VARCHAR(50) NULL;


        update NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
        set PROVIDER_STATE=PROVIDER_STATE_DESC
        where len(rtrim(PROVIDER_STATE_DESC)) >= 1;
        update NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
        set PROVIDER_COUNTY=PROVIDER_COUNTY_DESC
        where len(rtrim(PROVIDER_COUNTY_DESC)) >= 1;
        update NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR
        set PROVIDER_COUNTRY=PROVIDER_COUNTRY_DESC
        where len(rtrim(PROVIDER_COUNTRY_DESC)) >= 1;


        --PROC SORT DATA=S_POSTAL_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;

        -- CREATE TABLE S_PROVIDER_TELE_LOCATOR_OFFICE AS

        SET @PROC_STEP_NO = 8;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_TELE_LOCATOR_OFFICE';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE;


        with lst as (SELECT DISTINCT elp.ENTITY_UID       as ENTITY_UID_OFFICE,
                                     tl.EXTENSION_TXT     AS 'PROVIDER_PHONE_EXT_WORK',
                                     tl.PHONE_NBR_TXT     AS 'PROVIDER_PHONE_WORK',
                                     tl.EMAIL_ADDRESS     AS 'PROVIDER_EMAIL_WORK',
                                     elp.LOCATOR_DESC_TXT AS 'PROVIDER_PHONE_COMMENTS'
                             ,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY elp.ENTITY_UID
                                         ORDER BY tl.TELE_LOCATOR_UID DESC
                                         )                AS [ROWNO]
                     FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc
                              INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock)
                                         ON puc.PROVIDER_UID = elp.ENTITY_UID and elp.record_status_cd = 'ACTIVE'
                              INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR tl with (nolock)
                                         ON elp.LOCATOR_UID = tl.TELE_LOCATOR_UID
                     WHERE elp.USE_CD = 'WP'
                       AND elp.CD = 'O'
                       AND elp.CLASS_CD = 'TELE'
                       AND elp.RECORD_STATUS_CD = 'ACTIVE')
        select ENTITY_UID_OFFICE,
               PROVIDER_PHONE_EXT_WORK,
               PROVIDER_PHONE_WORK,
               PROVIDER_EMAIL_WORK,
               PROVIDER_PHONE_COMMENTS
        into NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE
        from lst
        where rowno = 1;


        --PROC SORT DATA=S_PROVIDER_TELE_LOCATOR_OFFICE NODUPKEY; BY ENTITY_UID; RUN;


        --CREATE TABLE S_PROVIDER_TELE_LOCATOR_CELL AS

        SET @PROC_STEP_NO = 9;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_TELE_LOCATOR_CELL';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL;


        with lst as (SELECT DISTINCT elp.ENTITY_UID   as ENTITY_UID_CELL,
                                     tl.PHONE_NBR_TXT AS 'PROVIDER_PHONE_CELL'
                             ,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY elp.ENTITY_UID
                                         ORDER BY tl.TELE_LOCATOR_UID DESC
                                         )            AS [ROWNO]
                     FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc with (nolock)
                              INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock)
                                         ON puc.PROVIDER_UID = elp.ENTITY_UID
                              INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR tl with (nolock)
                                         ON elp.LOCATOR_UID = tl.TELE_LOCATOR_UID
                     WHERE elp.CD = 'CP'
                       AND elp.CLASS_CD = 'TELE')
        select *
        into NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL
        from lst
        where rowno = 1;


        alter table NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL
            drop column ROWNO;


        --		PROC SORT DATA=S_PROVIDER_TELE_LOCATOR_CELL NODUPKEY; BY ENTITY_UID; RUN;
        --		CREATE TABLE S_PROVIDER_LOCATOR AS

        SET @PROC_STEP_NO = 10;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_LOCATOR';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR;


        SELECT S_pl.*, tlo.*, tlc.*, puc.PROVIDER_UID as PROVIDER_UID_LOCATOR
        into NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR
        FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE tlo
                                 ON puc.PROVIDER_UID = tlo.ENTITY_UID_OFFICE
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL tlc
                                 ON puc.PROVIDER_UID = tlc.ENTITY_UID_CELL
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR S_pl
                                 ON puc.PROVIDER_UID = S_pl.ENTITY_UID_POSTAL;


        /*

        PROC SORT DATA=S_PROVIDER_LOCATOR NODUPKEY; BY PROVIDER_UID; RUN;
        PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_POSTAL_LOCATOR S_PROVIDER_TELE_LOCATOR_OFFICE S_PROVIDER_TELE_LOCATOR_CELL;RUN;
    */

        --CREATE TABLE QEC_ENTITY_ID AS


        SET @PROC_STEP_NO = 11;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_QEC_ENTITY_ID';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID;


        SELECT DISTINCT PROVIDER_UID as PROVIDER_UID_QEC,
                        ROOT_EXTENSION_TXT,
                        ASSIGNING_AUTHORITY_CD
        into NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID
        FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc
                 LEFT OUTER JOIN NBS_ODSE.dbo.ENTITY_ID with (nolock)
                                 ON puc.PROVIDER_UID = ENTITY_ID.ENTITY_UID and Entity_id.record_status_cd = 'ACTIVE'
                                     AND ENTITY_ID.TYPE_CD = 'QEC';


        --PROC SORT DATA=QEC_ENTITY_ID NODUPKEY; BY PROVIDER_UID; RUN;

        -- CREATE TABLE PRN_ENTITY_ID AS

        SET @PROC_STEP_NO = 12;
        SET @PROC_STEP_NAME = ' GENERATING TMP_PRN_ENTITY_ID';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_PRN_ENTITY_ID', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_PRN_ENTITY_ID;

        with lst as (SELECT DISTINCT PROVIDER_UID as PROVIDER_UID_PRN,
                                     ROOT_EXTENSION_TXT,
                                     ASSIGNING_AUTHORITY_CD
                             ,
                                     ROW_NUMBER() OVER (
                                         PARTITION BY puc.PROVIDER_UID
                                         ORDER BY entity_id_seq DESC
                                         )        AS [ROWNO]

                     FROM NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL puc
                              LEFT OUTER JOIN NBS_ODSE.dbo.ENTITY_ID with (nolock)
                                              ON puc.PROVIDER_UID = ENTITY_ID.ENTITY_UID and
                                                 ENTITY_ID.record_status_cd = 'ACTIVE'
                                                  AND ENTITY_ID.TYPE_CD = 'PRN')

        select PROVIDER_UID_PRN,
               ROOT_EXTENSION_TXT,
               ASSIGNING_AUTHORITY_CD
        into NBS_ODSE.dbo.TMP_PRN_ENTITY_ID
        from lst
        where rowno = 1;

        --PROC SORT DATA=PRN_ENTITY_ID NODUPKEY; BY PROVIDER_UID; RUN;

        --CREATE TABLE S_PROVIDER_FINAL AS

        SET @PROC_STEP_NO = 14;
        SET @PROC_STEP_NAME = ' GENERATING TMP_S_PROVIDER_FINAL';

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL;


        SELECT distinct pwm.*,
                        spl.*,
                        qe.ROOT_EXTENSION_TXT     AS 'PROVIDER_QUICK_CODE',
                        pe.ROOT_EXTENSION_TXT     AS 'PROVIDER_REGISTRATION_NUM',
                        pe.ASSIGNING_AUTHORITY_CD AS 'PROVIDER_REGISRATION_NUM_AUTH'
        into NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        FROM NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM pwm
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR spl ON pwm.PROVIDER_UID = spl.PROVIDER_UID_LOCATOR
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID qe ON pwm.PROVIDER_UID = qe.PROVIDER_UID_QEC
                 LEFT OUTER JOIN NBS_ODSE.dbo.TMP_PRN_ENTITY_ID pe ON pwm.PROVIDER_UID = pe.PROVIDER_UID_PRN;

        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_QUICK_CODE= null
        where rtrim(ltrim(PROVIDER_QUICK_CODE)) = ''

        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_REGISRATION_NUM_AUTH= null
        where rtrim(ltrim(PROVIDER_REGISRATION_NUM_AUTH)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_STREET_ADDRESS_1= null
        where rtrim(ltrim(PROVIDER_STREET_ADDRESS_1)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_STREET_ADDRESS_2= null
        where rtrim(ltrim(PROVIDER_STREET_ADDRESS_2)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_COUNTY_CODE= null
        where rtrim(ltrim(PROVIDER_COUNTY_CODE)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_ADDRESS_COMMENTS= null
        where rtrim(ltrim(PROVIDER_ADDRESS_COMMENTS)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_PHONE_WORK= null
        where rtrim(ltrim(PROVIDER_PHONE_WORK)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_PHONE_EXT_WORK= null
        where rtrim(ltrim(PROVIDER_PHONE_EXT_WORK)) = ''
        update NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        set PROVIDER_PHONE_COMMENTS= null
        where rtrim(ltrim(PROVIDER_PHONE_COMMENTS)) = ''

        --PROC DATASETS LIBRARY = WORK NOLIST;DELETE L_PROVIDER_E L_PROVIDER_N D_PROVIDER_E D_PROVIDER_N S_PROVIDER_FINAL;RUN;


        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_INIT', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_INIT;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_PROVIDER_UID_COLL;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_INITPROVIDER', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_INITPROVIDER;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_NAME', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_NAME;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_WITH_NM;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_POSTAL_LOCATOR;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_OFFICE;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_TELE_LOCATOR_CELL;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_LOCATOR;

        IF OBJECT_ID('NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID', 'U') IS NOT NULL
            drop table NBS_ODSE.dbo.TMP_S_PROVIDER_QEC_ENTITY_ID;


        SET NOCOUNT OFF;

        select distinct [PROVIDER_UID]
                      , [PROVIDER_LOCAL_ID]
                      , [PROVIDER_RECORD_STATUS]
                      , [PROVIDER_NAME_PREFIX]
                      , [PROVIDER_FIRST_NAME]
                      , [PROVIDER_MIDDLE_NAME]
                      , [PROVIDER_LAST_NAME]
                      , [PROVIDER_NAME_SUFFIX]
                      , [PROVIDER_NAME_DEGREE]
                      , [PROVIDER_GENERAL_COMMENTS]
                      , [PROVIDER_QUICK_CODE]
                      , [PROVIDER_REGISTRATION_NUM]
                      , [PROVIDER_REGISRATION_NUM_AUTH]
                      , [PROVIDER_STREET_ADDRESS_1]
                      , [PROVIDER_STREET_ADDRESS_2]
                      , [PROVIDER_CITY]
                      , [PROVIDER_STATE]
                      , [PROVIDER_STATE_CODE]
                      , [PROVIDER_ZIP]
                      , [PROVIDER_COUNTY]
                      , [PROVIDER_COUNTY_CODE]
                      , [PROVIDER_COUNTRY]
                      , [PROVIDER_ADDRESS_COMMENTS]
                      , [PROVIDER_PHONE_WORK]
                      , [PROVIDER_PHONE_EXT_WORK]
                      , [PROVIDER_EMAIL_WORK]
                      , [PROVIDER_PHONE_COMMENTS]
                      , [PROVIDER_PHONE_CELL]
                      , [PROVIDER_ENTRY_METHOD]
                      , [PROVIDER_LAST_CHANGE_TIME]
                      , [PROVIDER_ADD_TIME]
                      , [PROVIDER_ADDED_BY]
                      , [PROVIDER_LAST_UPDATED_BY]
        from NBS_ODSE.dbo.TMP_S_PROVIDER_FINAL
        where [PROVIDER_UID] in (SELECT value FROM STRING_SPLIT(@user_id_list, ','));
        return;
    END TRY
    BEGIN CATCH
        return -1;
    END CATCH
END ;
go

