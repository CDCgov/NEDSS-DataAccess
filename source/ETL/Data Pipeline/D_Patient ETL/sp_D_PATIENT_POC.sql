CREATE   PROCEDURE [dbo].[sp_D_PATIENT_POC]
  @batch_id BIGINT
 as


  BEGIN
  
  --
--UPDATE ACTIVITY_LOG_DETAIL SET 
--START_DATE=DATETIME();
  -- DECLARE @batch_id INT =999 ;
 
    DECLARE @RowCount_no INT ;
    DECLARE @Proc_Step_no FLOAT = 0 ;
    DECLARE @Proc_Step_Name VARCHAR(200) = '' ;
	DECLARE @batch_start_time datetime2(7) = null ;
	DECLARE @batch_end_time datetime2(7) = null ;
 
 BEGIN TRY
    
	SET @Proc_Step_no = 1;
	SET @Proc_Step_Name = 'SP_Start';

	

	
	BEGIN TRANSACTION;
	
    INSERT INTO rdb.[dbo].[job_flow_log] (
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
           ,'D_PATIENT'
           ,'RDB.D_PATIENT'
		   ,'START'
		   ,@Proc_Step_no
		   ,@Proc_Step_Name
           ,0
		   );
  
    COMMIT TRANSACTION;
	
	
	SELECT @batch_start_time = batch_start_dttm, 
		   @batch_end_time = batch_end_dttm
	FROM [dbo].[job_batch_log]
	WHERE type_code = 'MasterETL'
		  AND status_type = 'start';



	BEGIN TRANSACTION;

		
	
			SET @PROC_STEP_NO = 1;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPATIENT_TEMP'; 

		
		IF OBJECT_ID('NBS_ChangeData.dbo.TMP_CDC_Person', 'U') IS NOT NULL   
 			 drop table NBS_ChangeData.dbo.TMP_CDC_Person  ;
 			
 		-- Temporary solution for session table.
 		-- Case: Never processed.
		SELECT __cdc_update_time, person_uid, cdc_id 
		INTO NBS_ChangeData.dbo.TMP_CDC_Person
		FROM NBS_ChangeData.dbo.[_Person]
		WHERE cdc_process_status = 0 AND cdc_processed_time IS NULL
		UNION
		-- Case after sproc run: Processed at least once but needs re-processing. 
		SELECT __cdc_update_time, person_uid, cdc_id 
		FROM NBS_ChangeData.dbo.[_Person]
		WHERE DATEADD(MINUTE , [__cdc_update_time]/60000, '1970-01-01 00:00:00.0') > cdc_processed_time AND cdc_process_status = 1
		UNION
		-- Case new topic during sproc run: Processed at least once but needs re-processing.
		SELECT __cdc_update_time, person_uid, cdc_id 
		FROM NBS_ChangeData.dbo.[_Person]
		WHERE cdc_processed_time IS NOT NULL AND cdc_process_status = 0;
						
		
		IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT_TEMP', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_INITPATIENT_TEMP  ;


		SELECT 
			PERSON.PERSON_UID AS  'PATIENT_UID',
			PERSON.LOCAL_ID AS  'PATIENT_LOCAL_ID',              
			PERSON.AGE_REPORTED,                 
			PERSON.AGE_REPORTED_UNIT_CD,
			PERSON.BIRTH_GENDER_CD,             
			PERSON.BIRTH_TIME AS  'PATIENT_DOB',  
			PERSON.CURR_SEX_CD,
			PERSON.DECEASED_IND_CD,
			PERSON.ADD_USER_ID,  
			PERSON.LAST_CHG_USER_ID,
			SPEAKS_ENGLISH_CD,
			ADDITIONAL_GENDER_CD AS 'PATIENT_ADDL_GENDER_INFO',
			ETHNIC_UNK_REASON_CD,
			SEX_UNK_REASON_CD,
			PREFERRED_GENDER_CD,
			PERSON.DECEASED_TIME AS  'PATIENT_DECEASED_DATE',
			RTRIM(LTRIM(Replace(Replace(PERSON.[description],CHAR(10),' '),CHAR(13),' '))) as 'PATIENT_GENERAL_COMMENTS' , -- VS TRANSLATE(PERSON.DESCRIPTION,' ' ,'0D0A'x)	as 'PATIENT_GENERAL_COMMENTS' ,
			PERSON.ELECTRONIC_IND AS  'PATIENT_ENTRY_METHOD',
			PERSON.ETHNIC_GROUP_IND,             
			PERSON.MARITAL_STATUS_CD,
			PERSON.PERSON_PARENT_UID AS  'PATIENT_MPR_UID', 
			PERSON.LAST_CHG_TIME AS  'PATIENT_LAST_CHANGE_TIME',
			PERSON.ADD_TIME AS  'PATIENT_ADD_TIME',
			PERSON.RECORD_STATUS_CD AS  'PATIENT_RECORD_STATUS',
			PERSON.OCCUPATION_CD,
			PERSON.PRIM_LANG_CD,
			PARTICIPATION.ACT_UID AS   'PATIENT_EVENT_UID',
			PARTICIPATION.TYPE_CD AS  'PATIENT_EVENT_TYPE'
		into  rdb..TMP_S_INITPATIENT_TEMP  
			FROM NBS_ChangeData.dbo._Person PERSON with ( nolock ), NBS_ChangeData.dbo._Participation PARTICIPATION with ( nolock ), NBS_ChangeData.dbo.TMP_CDC_Person TMP_PERSON
			WHERE 
		PERSON.PERSON_UID=PARTICIPATION.SUBJECT_ENTITY_UID
		AND PERSON.CD='PAT' AND PERSON.PERSON_UID=TMP_PERSON.person_uid
		

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 2;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPATIENT'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_INITPATIENT  ;

	SELECT   distinct * 
		into rdb.dbo.TMP_S_INITPATIENT
		FROM rdb.dbo.TMP_S_INITPATIENT_TEMP t1
		WHERE PATIENT_EVENT_UID = (SELECT max(PATIENT_EVENT_UID) from rdb.dbo.TMP_S_INITPATIENT_TEMP WHERE [PATIENT_UID] = t1.[PATIENT_UID])
		;


		
--CREATE TABLE  S_INITPATIENT_REV AS 

   		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 3;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPATIENT_REV'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT_REV', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_INITPATIENT_REV ;

   SELECT distinct A.*, 
	B.user_first_nm AS  'ADD_USER_FIRST_NAME',
	 B.user_last_nm AS  'ADD_USER_LAST_NAME', 
	C.user_first_nm AS  'CHG_USER_FIRST_NAME',
	 C.user_last_nm AS  'CHG_USER_LAST_NAME',
	 cast(null as varchar(50)) as PATIENT_ADDED_BY,
	 cast(null as varchar(50)) as PATIENT_LAST_UPDATED_BY,
	 cast( null as varchar(100)) as	PATIENT_AGE_REPORTED_UNIT	,
     cast( null as varchar(100)) as	PATIENT_CURRENT_SEX	,
     cast( null as varchar(100)) as	PATIENT_BIRTH_SEX	,
     cast( null as varchar(100)) as	PATIENT_MARITAL_STATUS	,
     cast( null as varchar(100)) as	PATIENT_DECEASED_INDICATOR	,
     cast( null as varchar(100)) as	PATIENT_ETHNICITY	,
     cast( null as varchar(100)) as	PATIENT_SPEAKS_ENGLISH	,
     cast( null as varchar(100)) as	PATIENT_UNK_ETHNIC_RSN 	,
     cast( null as varchar(100)) as	PATIENT_CURR_SEX_UNK_RSN	,
     cast( null as varchar(100)) as	PATIENT_PREFERRED_GENDER	,
     cast( null as varchar(100)) as	PATIENT_PRIMARY_LANGUAGE 	,
     cast( null as varchar(100)) as	PATIENT_PRIMARY_OCCUPATION  ,
     A.PATIENT_UID as	PATIENT_PATIENT_MPR_UID	,
     cast( null as varchar(100)) as	PATIENT_AGE_REPORTED	
	 , '' as TEMP
	  --,	 cast('' as varchar(50)) as PATIENT_EVENT_TYPE 
  into  rdb.dbo.TMP_S_INITPATIENT_REV 
  FROM	rdb.dbo.TMP_S_INITPATIENT A 
  LEFT OUTER JOIN NBS_ODSE.dbo.Auth_user B ON A.ADD_USER_ID=B.NEDSS_ENTRY_ID
  LEFT OUTER JOIN NBS_ODSE.dbo.Auth_user C ON A.LAST_CHG_USER_ID=C.NEDSS_ENTRY_ID
  ;

  /*
DATA S_INITPATIENT_REV;
SET S_INITPATIENT_REV;
	LENGTH PATIENT_ADDED_BY $50;
	LENGTH PATIENT_LAST_UPDATED_BY $50;
*/

	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_ADDED_BY= CAST((RTRIM(ADD_USER_LAST_NAME)+ ', ' +RTRIM(ADD_USER_FIRST_NAME)) as varchar(50))
	where  LEN(replace(ADD_USER_FIRST_NAME,' ',''))> 0 AND LEN(replace(ADD_USER_LAST_NAME,' ',''))>0 
	;

		
	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_ADDED_BY= CAST(RTRIM(ADD_USER_FIRST_NAME) as varchar(50))
	where  LEN(replace(ADD_USER_FIRST_NAME,' ',''))> 0 AND LEN(replace(ADD_USER_LAST_NAME,' ',''))<=0 
	;

	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_ADDED_BY= CAST(RTRIM(ADD_USER_LAST_NAME) as varchar(50))
	where  LEN(replace(ADD_USER_FIRST_NAME,' ',''))<= 0 AND LEN(replace(ADD_USER_LAST_NAME,' ',''))>0 
	;


	
	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_LAST_UPDATED_BY= CAST(RTRIM(CHG_USER_LAST_NAME)+ ', ' +RTRIM(CHG_USER_FIRST_NAME) as varchar(50))
	where  LEN(replace(CHG_USER_FIRST_NAME,' ',''))> 0 AND LEN(replace(CHG_USER_LAST_NAME,' ',''))>0 
	;

		
	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_LAST_UPDATED_BY= CAST(RTRIM(CHG_USER_FIRST_NAME) as varchar(50))
	where  LEN(replace(CHG_USER_FIRST_NAME,' ',''))> 0 AND LEN(replace(CHG_USER_LAST_NAME,' ',''))<=0 
	;

	update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set  PATIENT_LAST_UPDATED_BY= CAST(RTRIM(CHG_USER_LAST_NAME) as varchar(50))
	where  LEN(replace(CHG_USER_FIRST_NAME,' ',''))<= 0 AND LEN(replace(CHG_USER_LAST_NAME,' ',''))>0 
	;

/*
RUN;



--  PROC SORT DATA=S_INITPATIENT_REV NODUPKEY; BY PATIENT_UID; RUN;
PROC SQL;
*/

 		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 4;
			SET @PROC_STEP_NAME = ' GENERATING TMP_PATIENT_REV_UID'; 

		IF OBJECT_ID('rdb.dbo.TMP_PATIENT_REV_UID', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_PATIENT_REV_UID ;


 SELECT DISTINCT PATIENT_UID  
  into rdb.dbo.TMP_PATIENT_REV_UID
  FROM rdb.dbo.TMP_S_INITPATIENT_REV
  ;


/*
QUIT;
DATA S_INITPATIENT_REV;
SET  S_INITPATIENT_REV;
LENGTH PATIENT_EVENT_TYPE $50;
*/

/* 
	PATIENT_AGE_REPORTED_UNIT=PUT(AGE_REPORTED_UNIT_CD,$DEM218F.);
	PATIENT_CURRENT_SEX=PUT(CURR_SEX_CD,$DEM113F.);
	PATIENT_BIRTH_SEX=PUT(BIRTH_GENDER_CD, $DEM114F.);
	PATIENT_MARITAL_STATUS=PUT(MARITAL_STATUS_CD ,$DEM140F.);
	PATIENT_DECEASED_INDICATOR=PUT(DECEASED_IND_CD, $DEM127F.);
	PATIENT_ETHNICITY=PUT(ETHNIC_GROUP_IND, $DEM155F.);

*/
	
	
	  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_AGE_REPORTED_UNIT = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM218')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.AGE_REPORTED_UNIT_CD = cvg.code
  and   sir.AGE_REPORTED_UNIT_CD is not null
 ;


  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_CURRENT_SEX  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM113')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.CURR_SEX_CD  = cvg.code
  and   sir.CURR_SEX_CD  is not null
  ;

  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_BIRTH_SEX  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM114')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.BIRTH_GENDER_CD  = cvg.code
  and   sir.BIRTH_GENDER_CD  is not null
;

update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_MARITAL_STATUS  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM140')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.MARITAL_STATUS_CD  = cvg.code
  and   sir.MARITAL_STATUS_CD  is not null
;


  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_DECEASED_INDICATOR   = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM127')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.DECEASED_IND_CD  = cvg.code
  and   sir.DECEASED_IND_CD  is not null
;


  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_ETHNICITY  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM155')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.ETHNIC_GROUP_IND   = cvg.code
  and   sir.ETHNIC_GROUP_IND   is not null
;

  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set  PATIENT_ETHNICITY  = ETHNIC_GROUP_IND
  where   ETHNIC_GROUP_IND   is not null
  and PATIENT_ETHNICITY is null 
;



/*
PATIENT_SPEAKS_ENGLISH= PUT(SPEAKS_ENGLISH_CD,$YNU.); -- NBS214
PATIENT_UNK_ETHNIC_RSN =PUT(ETHNIC_UNK_REASON_CD,$ETHN_UN.); -- NBS273
PATIENT_CURR_SEX_UNK_RSN=PUT(SEX_UNK_REASON_CD,$UNK_SEX.); -- NBS272
PATIENT_PREFERRED_GENDER=PUT(PREFERRED_GENDER_CD,$TRANSGN.); -- 54899_0
PATIENT_PRIMARY_LANGUAGE = PUT(PRIM_LANG_CD,$P_LANG.);-- DEM142
PATIENT_PRIMARY_OCCUPATION = PUT(OCCUPATION_CD,$PT_OCUP.);-- DEM139

*/
  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_SPEAKS_ENGLISH  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'NBS214')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.SPEAKS_ENGLISH_CD   = cvg.code
  and   sir.SPEAKS_ENGLISH_CD   is not null
;

  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_UNK_ETHNIC_RSN    = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'NBS273')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.ETHNIC_UNK_REASON_CD    = cvg.code
  and   sir.ETHNIC_UNK_REASON_CD    is not null
;

  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_CURR_SEX_UNK_RSN   = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'NBS272')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.SEX_UNK_REASON_CD    = cvg.code
  and   sir.SEX_UNK_REASON_CD    is not null
;


  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_PREFERRED_GENDER   = cvg.[code_short_desc_txt]

   FROM  [NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
    where   cvg.code_set_nm =  'NBS_STD_GENDER_PARPT'
    and   sir.PREFERRED_GENDER_CD    = cvg.code
    and   sir.PREFERRED_GENDER_CD    is not null
	  ;
  
  	
  update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_PRIMARY_LANGUAGE    = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].[language_code] cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM142')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.PRIM_LANG_CD   = cvg.code
  and   sir.PRIM_LANG_CD    is not null
;

 update rdb.dbo.TMP_S_INITPATIENT_REV 
   set rdb.dbo.TMP_S_INITPATIENT_REV.PATIENT_PRIMARY_OCCUPATION  = cvg.[code_short_desc_txt]
  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
       [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
	    [NBS_SRTE].[dbo].NAICS_Industry_code cvg with (nolock),
         rdb.dbo.TMP_S_INITPATIENT_REV sir 
  where nq.question_identifier = ( 'DEM139')
  and   cd.code_set_group_id = nq.code_set_group_id
  and   cvg.code_set_nm = cd.code_set_nm
  and   sir.OCCUPATION_CD     = cvg.code
  and   sir.OCCUPATION_CD     is not null
;

	
/*
	update  rdb.dbo.TMP_S_INITPATIENT_REV 
     set PATIENT_AGE_REPORTED = FORMAT(cast([AGE_REPORTED] as numeric), N'N0')
	 ;
*/

	 update  rdb.dbo.TMP_S_INITPATIENT_REV 
    set PATIENT_AGE_REPORTED = cast([AGE_REPORTED] as numeric)
	where isnumeric([AGE_REPORTED]) = 1
	 ;
	 
	 /*
IF PATIENT_RECORD_STATUS = '' THEN PATIENT_RECORD_STATUS = 'ACTIVE';
IF PATIENT_RECORD_STATUS = 'SUPERCEDED' THEN PATIENT_RECORD_STATUS = 'INACTIVE' ;
IF PATIENT_RECORD_STATUS = 'LOG_DEL' THEN PATIENT_RECORD_STATUS = 'INACTIVE' ; 

*/

update  rdb.dbo.TMP_S_INITPATIENT_REV 
set PATIENT_RECORD_STATUS = 'ACTIVE'
where PATIENT_RECORD_STATUS = '' or PATIENT_RECORD_STATUS is null
;

update  rdb.dbo.TMP_S_INITPATIENT_REV 
set PATIENT_RECORD_STATUS = 'INACTIVE'
where PATIENT_RECORD_STATUS in ( 'SUPERCEDED','LOG_DEL' )
;




ALTER TABLE rdb.dbo.TMP_S_INITPATIENT_REV 
  DROP COLUMN BIRTH_GENDER_CD ,
              CURR_SEX_CD,
			   DECEASED_IND_CD,
			    MARITAL_STATUS_CD,
				 ETHNIC_GROUP_IND
				 ;


/*
RUN;
PROC SQL;
CREATE TABLE S_PERSON_RACE AS 
*/

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 5;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_RACE  
       ;

			SELECT 
				PERSON_RACE.PERSON_UID AS   'PATIENT_UID',
				RACE_CD,
				RACE_CODE.CODE_DESC_TXT, 
				RACE_CATEGORY_CD,
				RACE_CODE.PARENT_IS_CD
			into rdb.dbo.TMP_S_PERSON_RACE
			FROM  rdb.dbo.TMP_PATIENT_REV_UID 
			INNER JOIN NBS_ODSE.dbo.PERSON_RACE with (nolock)   ON   PERSON_RACE.PERSON_UID=TMP_PATIENT_REV_UID.PATIENT_UID
			LEFT OUTER JOIN NBS_SRTE.dbo.RACE_CODE with (nolock)    ON   PERSON_RACE.RACE_CD = RACE_CODE.CODE
			LEFT OUTER JOIN NBS_SRTE.dbo.RACE_CODE RT with (nolock) ON   PERSON_RACE.RACE_CATEGORY_CD = RT.CODE
				ORDER BY PATIENT_UID, CODE_DESC_TXT
				;


--CREATE TABLE rdb.dbo.TMP_PERSON_ROOT_RACE AS

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =6;
			SET @PROC_STEP_NAME = ' GENERATING TMP_PERSON_ROOT_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_PERSON_ROOT_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_PERSON_ROOT_RACE 


SELECT * 
into rdb.dbo.TMP_PERSON_ROOT_RACE
FROM rdb.dbo.TMP_S_PERSON_RACE
WHERE PARENT_IS_CD='ROOT'
	  --ORDER BY PATIENT_UID
	  ;

--CREATE TABLE rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE AS
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =7;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_AMER_INDIAN_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE ;


				SELECT *  
				into rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE
				FROM rdb.dbo.TMP_S_PERSON_RACE
				WHERE RACE_CATEGORY_CD='1002-5'
				AND RACE_CD <> RACE_CATEGORY_CD
					  --ORDER BY PATIENT_UID
					  ;

--CREATE TABLE rdb.dbo.TMP_S_PERSON_BLACK_RACE AS
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =8;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_BLACK_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_BLACK_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_BLACK_RACE ;


			SELECT *  
			into rdb.dbo.TMP_S_PERSON_BLACK_RACE
			FROM rdb.dbo.TMP_S_PERSON_RACE
			WHERE RACE_CATEGORY_CD='2054-5'
			AND RACE_CD <> RACE_CATEGORY_CD
				  --ORDER BY PATIENT_UID
				  ;

--CREATE TABLE rdb.dbo.TMP_S_PERSON_WHITE_RACE

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =9;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_WHITE_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_WHITE_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_WHITE_RACE ;

		SELECT *  
		into rdb.dbo.TMP_S_PERSON_WHITE_RACE
		FROM rdb.dbo.TMP_S_PERSON_RACE
		WHERE RACE_CATEGORY_CD='2106-3'
		AND RACE_CD <> RACE_CATEGORY_CD
			  --ORDER BY PATIENT_UID
			  ;

--CREATE TABLE rdb.dbo.TMP_S_PERSON_ASIAN_RACE AS

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =10;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_ASIAN_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_ASIAN_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_ASIAN_RACE ;

		SELECT *  
		into rdb.dbo.TMP_S_PERSON_ASIAN_RACE
		FROM rdb.dbo.TMP_S_PERSON_RACE
		WHERE RACE_CATEGORY_CD='2028-9'
		AND RACE_CD <> RACE_CATEGORY_CD
			  --ORDER BY PATIENT_UID
			  ;


--CREATE TABLE rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE AS

		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO =11;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_HAWAIIAN_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE ;

			SELECT *  
			into rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE
			FROM rdb.dbo.TMP_S_PERSON_RACE
			WHERE RACE_CATEGORY_CD='2076-8'
			AND RACE_CD <> RACE_CATEGORY_CD
				  --ORDER BY PATIENT_UID
				  ;



/*  -- VS
QUIT;
DATA S_PERSON_ROOT_RACE; 
LENGTH PATIENT_RACE_CALCULATED $2000; 
LENGTH PATIENT_RACE_CALC_DETAILS $4000;
LENGTH PATIENT_RACE_ALL $4000;

SET  PERSON_ROOT_RACE; BY  PATIENT_UID; 

RETAIN PATIENT_RACE_CALC_DETAILS PATIENT_RACE_ALL;

IF  FIRST. PATIENT_UID THEN PATIENT_RACE_CALC_DETAILS=' '  ; 
IF  FIRST. PATIENT_UID THEN PATIENT_RACE_ALL=' '; 

IF RACE_CATEGORY_CD not in ('PHC1175' ,'NASK','U')     THEN 
PATIENT_RACE_CALC_DETAILS=CATX(' | ',PATIENT_RACE_CALC_DETAILS,CODE_DESC_TXT); 
PATIENT_RACE_ALL=CATX(' | ',PATIENT_RACE_ALL,CODE_DESC_TXT); 

IF LAST. PATIENT_UID; 

IF LENGTHN(PATIENT_RACE_CALC_DETAILS)<1 THEN PATIENT_RACE_CALC_DETAILS='Unknown';

PATIENT_RACE_CALCULATED=PATIENT_RACE_CALC_DETAILS;
X=INDEX(PATIENT_RACE_CALCULATED,'|');
IF X>0 THEN PATIENT_RACE_CALCULATED='Multi-Race';
RUN;

*/


		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 12;
			SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_ROOT_RACE'; 

		IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_ROOT_RACE', 'U') IS NOT NULL   
 			 drop table rdb.dbo.TMP_S_PERSON_ROOT_RACE;

				select *
				into  rdb.dbo.TMP_S_PERSON_ROOT_RACE 
				from rdb.dbo.TMP_PERSON_ROOT_RACE
				;

				ALTER TABLE rdb.dbo.TMP_S_PERSON_ROOT_RACE 
				 ADD  PATIENT_RACE_CALCULATED VARCHAR(2000), 
					  PATIENT_RACE_CALC_DETAILS varchar(4000),
					  PATIENT_RACE_ALL varchar(4000)
					  ;

				with cte as (
				select patient_uid, 
				(    STUFF((SELECT ' | ' + CAST(sppr2.CODE_DESC_TXT AS varchar(2000))
						   FROM rdb..TMP_S_PERSON_ROOT_RACE sppr2
						   WHERE sppr2.patient_uid = sppr.patient_uid
						   FOR XML PATH('')), 2 ,1, '') ) AS CODE_DESC_TXT_List
				from rdb..TMP_S_PERSON_ROOT_RACE sppr
				where CODE_DESC_TXT is not null 
				--and RACE_CATEGORY_CD not in ('PHC1175' ,'NASK','U')
				group by patient_uid 
				--having count(*) > 1

				)
				update sppr
				set sppr.PATIENT_RACE_ALL = ltrim(cte1.CODE_DESC_TXT_List)
				from  rdb.[dbo].TMP_S_PERSON_ROOT_RACE sppr, 
					 cte cte1
				where  sppr.PATIENT_UID = cte1.PATIENT_UID
				--and cte1.rn = 1
				 ;


 
				with cte as (
				select patient_uid, 
				(    STUFF((SELECT ' | ' + CAST(sppr2.CODE_DESC_TXT AS varchar (2000))
						   FROM rdb..TMP_S_PERSON_ROOT_RACE sppr2
						   WHERE sppr2.patient_uid = sppr.patient_uid
						  and RACE_CATEGORY_CD not in ('PHC1175' ,'NASK','U')
						   FOR XML PATH('')), 2 ,1, '') ) AS CODE_DESC_TXT_List
				from rdb..TMP_S_PERSON_ROOT_RACE sppr
				where CODE_DESC_TXT is not null 
				--and RACE_CATEGORY_CD not in ('PHC1175' ,'NASK','U')
				group by patient_uid 
				--having count(*) > 1

				)
				update sppr
				set sppr.PATIENT_RACE_CALC_DETAILS = ltrim(cte1.CODE_DESC_TXT_List)
				from  rdb.[dbo].TMP_S_PERSON_ROOT_RACE sppr, 
					 cte cte1
				where  sppr.PATIENT_UID = cte1.PATIENT_UID
				--and cte1.rn = 1
				 ;
				 
				update rdb.[dbo].TMP_S_PERSON_ROOT_RACE 
				set   PATIENT_RACE_CALCULATED='Unknown',
				PATIENT_RACE_CALC_DETAILS='Unknown'
				where len(PATIENT_RACE_CALC_DETAILS) <1 OR PATIENT_RACE_CALC_DETAILS is null
				;
				update rdb.[dbo].TMP_S_PERSON_ROOT_RACE 
				set   PATIENT_RACE_CALCULATED='Multi-Race'
				where CHARINDEX('|',PATIENT_RACE_CALC_DETAILS) > 0
				;

				 update rdb.[dbo].TMP_S_PERSON_ROOT_RACE 
				set   PATIENT_RACE_CALCULATED=PATIENT_RACE_CALC_DETAILS
				where CHARINDEX('|',PATIENT_RACE_CALC_DETAILS) = 0
				;


				/* 
				DATA  S_PERSON_AMER_INDIAN_RACE; 
				LENGTH PATIENT_RACE_AMER_IND_ALL $2000;
				LENGTH PATIENT_RACE_AMER_IND_1 $50;
				LENGTH PATIENT_RACE_AMER_IND_2 $50;
				LENGTH PATIENT_RACE_AMER_IND_3 $50;
				LENGTH PATIENT_RACE_AMER_IND_4 $50;
					SET  S_PERSON_AMER_INDIAN_RACE; BY  PATIENT_UID; 
				RETAIN PATIENT_RACE_AMER_IND_ALL;
				RETAIN PATIENT_RACE_AMER_IND_1;
				RETAIN PATIENT_RACE_AMER_IND_2;
				RETAIN PATIENT_RACE_AMER_IND_3;
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_ALL=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_1=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_2=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_3=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_AMER_IND_4=' ';
					PATIENT_RACE_AMER_IND_ALL=CATX(' | ',PATIENT_RACE_AMER_IND_ALL,CODE_DESC_TXT); 
					IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_1))=0 THEN  PATIENT_RACE_AMER_IND_1=CODE_DESC_TXT;
					ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_2))=0 THEN  PATIENT_RACE_AMER_IND_2=CODE_DESC_TXT;
					ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_3))=0 THEN  PATIENT_RACE_AMER_IND_3=CODE_DESC_TXT;
					ELSE IF LENGTHN(TRIM(PATIENT_RACE_AMER_IND_4))=0 THEN  PATIENT_RACE_AMER_IND_4=CODE_DESC_TXT;
				IF LAST.PATIENT_UID; 
				IF LENGTHN(COMPRESS(PATIENT_RACE_AMER_IND_4))>0  THEN PATIENT_RACE_AMER_IND_GT3_IND='TRUE';
				ELSE PATIENT_RACE_AMER_IND_GT3_IND='FALSE';
				RUN;

				 */


		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 121;
			SET @PROC_STEP_NAME = ' UPDATING TMP_S_PERSON_AMER_INDIAN_RACE'; 



				ALTER TABLE rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE
				 ADD   
						PATIENT_RACE_AMER_IND_ALL varchar(2000 ),
						PATIENT_RACE_AMER_IND_1 varchar(50 ),
						PATIENT_RACE_AMER_IND_2 varchar(50 ),
						PATIENT_RACE_AMER_IND_3 varchar(50 ),
						PATIENT_RACE_AMER_IND_4 varchar(50 ),
						PATIENT_RACE_AMER_IND_GT3_IND varchar(10 )
				 ;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_AMER_IND_1] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 1
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_AMER_IND_2] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 2
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
			    group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_AMER_IND_3] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 3
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_AMER_IND_4] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 4
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				update rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] 
				set [PATIENT_RACE_AMER_IND_GT3_IND] = 'TRUE'
				where [PATIENT_RACE_AMER_IND_4] is not null
				;

				update rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] 
				set [PATIENT_RACE_AMER_IND_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_AMER_IND_4] is  null
				;

				update rdb.[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] 
				set [PATIENT_RACE_AMER_IND_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_AMER_IND_4] is  null
				;
				/*
				update  [RDB].[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE]
  
				set  [PATIENT_RACE_AMER_IND_ALL] =
					   SUBSTRING (
						( coalesce(  ' | ' + PATIENT_RACE_AMER_IND_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_4 ,'') ),
						3, 
						LEN(coalesce(  ' | ' + PATIENT_RACE_AMER_IND_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_AMER_IND_4 ,''))
						)
				 ;
				 */
				 IF OBJECT_ID('rdb.dbo.TEMP_AMER_IND_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_AMER_IND_RACE_ALL ; 


				SELECT distinct patient_uid, STUFF((
					SELECT distinct ' | ' + code_desc_txt    FROM [TMP_S_PERSON_AMER_INDIAN_RACE ] t1
					where t1.patient_uid=t2.patient_uid
					FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') as PATIENT_RACE_AMER_IND_ALL 
					into rdb..TEMP_AMER_IND_RACE_ALL
					from rdb..[TMP_S_PERSON_AMER_INDIAN_RACE ] t2 
				;

				update p
				SET
					p.PATIENT_RACE_AMER_IND_ALL =  SUBSTRING(ps.PATIENT_RACE_AMER_IND_ALL, 2, LEN(ps.PATIENT_RACE_AMER_IND_ALL))
				   from 
					  [RDB].[dbo].[TMP_S_PERSON_AMER_INDIAN_RACE] p
						INNER JOIN TEMP_AMER_IND_RACE_ALL ps 
						on p.PATIENT_UID = ps.PATIENT_UID		         
				--and prs.RACE_CD = cte1.race_cd
				;

 

				 /*
				DATA  S_PERSON_BLACK_RACE; 
				LENGTH PATIENT_RACE_BLACK_ALL $2000;
				LENGTH PATIENT_RACE_BLACK_1 $50;
				LENGTH PATIENT_RACE_BLACK_2 $50;
				LENGTH PATIENT_RACE_BLACK_3 $50;
				LENGTH PATIENT_RACE_BLACK_4 $50;
				LENGTH PATIENT_RACE_BLACK_GT3_IND $10;
				SET  S_PERSON_BLACK_RACE; BY  PATIENT_UID;  
				RETAIN PATIENT_RACE_BLACK_ALL;
				RETAIN PATIENT_RACE_BLACK_1;
				RETAIN PATIENT_RACE_BLACK_2;
				RETAIN PATIENT_RACE_BLACK_3;
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_ALL=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_1=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_2=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_3=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_BLACK_4=' ';
				PATIENT_RACE_BLACK_ALL=CATX(' | ',PATIENT_RACE_BLACK_ALL,CODE_DESC_TXT); 
				IF LENGTHN(TRIM(PATIENT_RACE_BLACK_1))=0 THEN  PATIENT_RACE_BLACK_1=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_2))=0 THEN  PATIENT_RACE_BLACK_2=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_3))=0 THEN  PATIENT_RACE_BLACK_3=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_BLACK_4))=0 THEN  PATIENT_RACE_BLACK_4=CODE_DESC_TXT;
				IF LAST.PATIENT_UID; 

				*/

				
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 122;
			SET @PROC_STEP_NAME = ' UPDATING TMP_S_PERSON_BLACK_RACE'; 


				ALTER TABLE rdb.dbo.TMP_S_PERSON_BLACK_RACE
				 ADD   
						PATIENT_RACE_BLACK_ALL varchar(2000 ),
						PATIENT_RACE_BLACK_1 varchar(50 ),
						PATIENT_RACE_BLACK_2 varchar(50 ),
						PATIENT_RACE_BLACK_3 varchar(50 ),
						PATIENT_RACE_BLACK_4 varchar(50 ),
						PATIENT_RACE_BLACK_GT3_IND varchar(10 )
				 ;

				 ; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_BLACK_1] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
					 cte cte1
				where cte1.rn = 1
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_BLACK_2] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
					 cte cte1
				where cte1.rn = 2
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_BLACK_3] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
					 cte cte1
				where cte1.rn = 3
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_BLACK_4] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] prs, 
					 cte cte1
				where cte1.rn = 4
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				update rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] 
				set [PATIENT_RACE_BLACK_GT3_IND] = 'TRUE'
				where [PATIENT_RACE_BLACK_4] is not null
				;

				update rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] 
				set [PATIENT_RACE_BLACK_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_BLACK_4] is null
				;

				update rdb.[dbo].[TMP_S_PERSON_BLACK_RACE] 
				set [PATIENT_RACE_BLACK_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_BLACK_4] is  null
				;

				/*
				update  [RDB].[dbo].[TMP_S_PERSON_BLACK_RACE]
  
				set  [PATIENT_RACE_BLACK_ALL] =
					   SUBSTRING (
						( coalesce(  ' | ' + PATIENT_RACE_BLACK_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_4 ,'') ),
						3, 
						LEN(coalesce(  ' | ' + PATIENT_RACE_BLACK_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_BLACK_4 ,''))
						)
				 ;
				 */

				 	IF OBJECT_ID('rdb.dbo.TEMP_BLACK_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_BLACK_RACE_ALL ; 


				SELECT distinct patient_uid, STUFF((
					SELECT distinct ' | ' + code_desc_txt    FROM [TMP_S_PERSON_BLACK_RACE] t1
					where t1.patient_uid=t2.patient_uid
					FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') as PATIENT_RACE_BLACK_ALL 
					into rdb..TEMP_BLACK_RACE_ALL
					from rdb..[TMP_S_PERSON_BLACK_RACE] t2 
				
				update p
				SET
					p.PATIENT_RACE_BLACK_ALL =  SUBSTRING(ps.PATIENT_RACE_BLACK_ALL, 2, LEN(ps.PATIENT_RACE_BLACK_ALL))
				   from 
					  [RDB].[dbo].[TMP_S_PERSON_BLACK_RACE] p
						INNER JOIN TEMP_BLACK_RACE_ALL ps 
						on p.PATIENT_UID = ps.PATIENT_UID		         
				--and prs.RACE_CD = cte1.race_cd
				;

 
				/*

				DATA  S_PERSON_WHITE_RACE; 
				LENGTH PATIENT_RACE_WHITE_ALL $2000;
				LENGTH PATIENT_RACE_WHITE_1 $50;
				LENGTH PATIENT_RACE_WHITE_2 $50;
				LENGTH PATIENT_RACE_WHITE_3 $50;
				LENGTH PATIENT_RACE_WHITE_4 $50;
				LENGTH PATIENT_RACE_WHITE_GT3_IND $10;
				SET  S_PERSON_WHITE_RACE; BY  PATIENT_UID; 
				RETAIN PATIENT_RACE_WHITE_ALL;
				RETAIN PATIENT_RACE_WHITE_1;
				RETAIN PATIENT_RACE_WHITE_2;
				RETAIN PATIENT_RACE_WHITE_3;
				IF  FIRST.PATIENT_UID THEN RACE_WHITE_ALL=' ';
				IF  FIRST.PATIENT_UID THEN RACE_WHITE_1=' ';
				IF  FIRST.PATIENT_UID THEN RACE_WHITE_2=' ';
				IF  FIRST.PATIENT_UID THEN RACE_WHITE_3=' ';
				IF  FIRST.PATIENT_UID THEN RACE_WHITE_4=' ';
				PATIENT_RACE_WHITE_ALL=CATX(' | ',PATIENT_RACE_WHITE_ALL,CODE_DESC_TXT); 
				IF LENGTHN(TRIM(PATIENT_RACE_WHITE_1))=0 THEN  PATIENT_RACE_WHITE_1=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_2))=0 THEN  PATIENT_RACE_WHITE_2=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_3))=0 THEN  PATIENT_RACE_WHITE_3=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_WHITE_4))=0 THEN  PATIENT_RACE_WHITE_4=CODE_DESC_TXT;
				IF LAST.PATIENT_UID; 
				IF LENGTHN(COMPRESS(PATIENT_RACE_WHITE_4))>0  THEN PATIENT_RACE_WHITE_GT3_IND='TRUE';
				ELSE PATIENT_RACE_WHITE_GT3_IND='FALSE';
				RUN; 
				*/


					
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 123;
			SET @PROC_STEP_NAME = ' UPDATING TMP_S_PERSON_WHITE_RACE'; 

				ALTER TABLE RDB.dbo.TMP_S_PERSON_WHITE_RACE 
				 ADD
				 PATIENT_RACE_WHITE_ALL varchar(2000),
				 PATIENT_RACE_WHITE_1 varchar(50),
				 PATIENT_RACE_WHITE_2 varchar(50),
				 PATIENT_RACE_WHITE_3 varchar(50),
				 PATIENT_RACE_WHITE_4 varchar(50),
				 PATIENT_RACE_WHITE_GT3_IND varchar(10)
				 ;

				 ; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_WHITE_1] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
					 cte cte1
				where cte1.rn = 1
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_WHITE_2] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
					 cte cte1
				where cte1.rn = 2
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_WHITE_3] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
					 cte cte1
				where cte1.rn = 3
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_WHITE_4] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] prs, 
					 cte cte1
				where cte1.rn = 4
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				update rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] 
				set [PATIENT_RACE_WHITE_GT3_IND] = 'TRUE'
				where [PATIENT_RACE_WHITE_4] is not null
				;

				update rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] 
				set [PATIENT_RACE_WHITE_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_WHITE_4] is  null
				;

				update rdb.[dbo].[TMP_S_PERSON_WHITE_RACE] 
				set [PATIENT_RACE_WHITE_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_WHITE_4] is  null
				;

				/*
				update  [RDB].[dbo].[TMP_S_PERSON_WHITE_RACE]
  
				set  [PATIENT_RACE_WHITE_ALL] =
					   SUBSTRING (
						( coalesce(  ' | ' + PATIENT_RACE_WHITE_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_4 ,'') ),
						3, 
						LEN(coalesce(  ' | ' + PATIENT_RACE_WHITE_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_WHITE_4 ,''))
						)
				 ;
				 */
				 
				IF OBJECT_ID('rdb.dbo.TEMP_WHITE_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_WHITE_RACE_ALL ; 


				SELECT distinct patient_uid, STUFF((
					SELECT distinct ' | ' + code_desc_txt    FROM [TMP_S_PERSON_WHITE_RACE] t1
					where t1.patient_uid=t2.patient_uid
					FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') as PATIENT_RACE_WHITE_ALL 
					into rdb..TEMP_WHITE_RACE_ALL
					from rdb..[TMP_S_PERSON_WHITE_RACE] t2 
				;

				update p
				SET
					p.PATIENT_RACE_WHITE_ALL =  SUBSTRING(ps.PATIENT_RACE_WHITE_ALL, 2, LEN(ps.PATIENT_RACE_WHITE_ALL))
				   from 
					  [RDB].[dbo].[TMP_S_PERSON_WHITE_RACE] p
						INNER JOIN TEMP_WHITE_RACE_ALL ps 
						on p.PATIENT_UID = ps.PATIENT_UID		         
				--and prs.RACE_CD = cte1.race_cd
				;


 

				/*
				PROC SORT DATA=S_PERSON_RACE NODUPKEY; BY PATIENT_UID; RUN;
				
				DATA  S_PERSON_ASIAN_RACE; 
				LENGTH PATIENT_RACE_ASIAN_ALL $2000;
				LENGTH PATIENT_RACE_ASIAN_1 $50;
				LENGTH PATIENT_RACE_ASIAN_2 $50;
				LENGTH PATIENT_RACE_ASIAN_3 $50;
				LENGTH PATIENT_RACE_ASIAN_4 $50;
				LENGTH PATIENT_RACE_ASIAN_GT3_IND $10;
				SET  S_PERSON_ASIAN_RACE; BY  PATIENT_UID; 
				RETAIN PATIENT_RACE_ASIAN_ALL;
				RETAIN PATIENT_RACE_ASIAN_1;
				RETAIN PATIENT_RACE_ASIAN_2;
				RETAIN PATIENT_RACE_ASIAN_3;
				IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_ALL=' ';
				IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_1=' ';
				IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_2=' ';
				IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_3=' ';
				IF FIRST.PATIENT_UID THEN PATIENT_RACE_ASIAN_4=' ';
				PATIENT_RACE_ASIAN_ALL=CATX(' | ',PATIENT_RACE_ASIAN_ALL,CODE_DESC_TXT); 
				IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_1))=0 THEN PATIENT_RACE_ASIAN_1=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_2))=0 THEN  PATIENT_RACE_ASIAN_2=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_3))=0 THEN  PATIENT_RACE_ASIAN_3=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_ASIAN_4))=0 THEN  PATIENT_RACE_ASIAN_4=CODE_DESC_TXT;
				IF LAST.PATIENT_UID; 
				IF LENGTHN(COMPRESS(PATIENT_RACE_ASIAN_4))>0 THEN PATIENT_RACE_ASIAN_GT3_IND='TRUE';
				ELSE PATIENT_RACE_ASIAN_GT3_IND='FALSE';
				RUN; 
				*/
					
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 124;
			SET @PROC_STEP_NAME = ' UPDATING TMP_S_PERSON_ASIAN_RACE'; 

				ALTER TABLE RDB.dbo.TMP_S_PERSON_ASIAN_RACE 
				 ADD
				 PATIENT_RACE_ASIAN_ALL varchar(2000),
				 PATIENT_RACE_ASIAN_1 varchar(50),
				 PATIENT_RACE_ASIAN_2 varchar(50),
				 PATIENT_RACE_ASIAN_3 varchar(50),
				 PATIENT_RACE_ASIAN_4 varchar(50),
				 PATIENT_RACE_ASIAN_GT3_IND varchar(10)
				 ;
			
				 ; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt]
				      , row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_ASIAN_1] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 1
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
	       
				)
				update prs
				set prs.[PATIENT_RACE_ASIAN_2] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 2
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_ASIAN_3] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 3
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_ASIAN_4] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 4
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				update rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] 
				set [PATIENT_RACE_ASIAN_GT3_IND] = 'TRUE'
				where [PATIENT_RACE_ASIAN_4] is not null
				;

				update rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] 
				set [PATIENT_RACE_ASIAN_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_ASIAN_4] is  null
				;

				update rdb.[dbo].[TMP_S_PERSON_ASIAN_RACE] 
				set [PATIENT_RACE_ASIAN_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_ASIAN_4] is  null
				;
				


				IF OBJECT_ID('rdb.dbo.TEMP_ASIAN_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_ASIAN_RACE_ALL ; 


				SELECT distinct patient_uid, STUFF((
					SELECT distinct ' | ' + code_desc_txt    FROM [TMP_S_PERSON_ASIAN_RACE] t1
					where t1.patient_uid=t2.patient_uid
					FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') as PATIENT_RACE_ASIAN_ALL 
					into rdb..TEMP_ASIAN_RACE_ALL
					from rdb..[TMP_S_PERSON_ASIAN_RACE] t2 
				
				update p
				SET
					p.PATIENT_RACE_ASIAN_ALL =  SUBSTRING(ps.PATIENT_RACE_ASIAN_ALL, 2, LEN(ps.PATIENT_RACE_ASIAN_ALL))
				   from 
					  [RDB].[dbo].[TMP_S_PERSON_ASIAN_RACE] p
						INNER JOIN TEMP_ASIAN_RACE_ALL ps 
						on p.PATIENT_UID = ps.PATIENT_UID		         
				--and prs.RACE_CD = cte1.race_cd
				;

/*				update  [RDB].[dbo].[TMP_S_PERSON_ASIAN_RACE]
  
				set  [PATIENT_RACE_ASIAN_ALL] =
					   SUBSTRING (
						( coalesce(  ' | ' + PATIENT_RACE_ASIAN_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_4 ,'') ),
						3, 
						LEN(coalesce(  ' | ' + PATIENT_RACE_ASIAN_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_ASIAN_4 ,''))
						)
				 ;
				 */
 


				/*
				DATA  S_PERSON_HAWAIIAN_RACE; 
				LENGTH PATIENT_RACE_NAT_HI_ALL $2000;
				LENGTH PATIENT_RACE_NAT_HI_1 $50;
				LENGTH PATIENT_RACE_NAT_HI_2 $50;
				LENGTH PATIENT_RACE_NAT_HI_3 $50;
				LENGTH PATIENT_RACE_NAT_HI_4 $50;
				LENGTH PATIENT_RACE_NAT_HI_GT3_IND $10;
				SET  S_PERSON_HAWAIIAN_RACE; BY  PATIENT_UID; 
				RETAIN PATIENT_RACE_NAT_HI_ALL;
				RETAIN PATIENT_RACE_NAT_HI_1;
				RETAIN PATIENT_RACE_NAT_HI_2;
				RETAIN PATIENT_RACE_NAT_HI_3;
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_ALL=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_1=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_2=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_3=' ';
				IF  FIRST.PATIENT_UID THEN PATIENT_RACE_NAT_HI_4=' ';
				PATIENT_RACE_NAT_HI_ALL=CATX(' | ',PATIENT_RACE_NAT_HI_ALL,CODE_DESC_TXT); 
				IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_1))=0 THEN  PATIENT_RACE_NAT_HI_1=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_2))=0 THEN  PATIENT_RACE_NAT_HI_2=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_3))=0 THEN  PATIENT_RACE_NAT_HI_3=CODE_DESC_TXT;
				ELSE IF LENGTHN(TRIM(PATIENT_RACE_NAT_HI_4))=0 THEN  PATIENT_RACE_NAT_HI_4=CODE_DESC_TXT;
				IF LAST.PATIENT_UID; 
				IF LENGTHN(COMPRESS(PATIENT_RACE_NAT_HI_4))>0  THEN PATIENT_RACE_NAT_HI_GT3_IND='TRUE';
				ELSE PATIENT_RACE_NAT_HI_GT3_IND='FALSE';
				RUN; 
				PROC SQL;

				*/

					
		     SELECT @ROWCOUNT_NO = @@ROWCOUNT;

		     INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
				(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
				VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

			COMMIT TRANSACTION;
		
			BEGIN TRANSACTION;

			SET @PROC_STEP_NO = 125;
			SET @PROC_STEP_NAME = ' UPDATING TMP_S_PERSON_HAWIIAN_RACE'; 

				ALTER TABLE RDB.dbo.TMP_S_PERSON_HAWAIIAN_RACE 
				 ADD
				 PATIENT_RACE_NAT_HI_ALL varchar(2000),
				 PATIENT_RACE_NAT_HI_1 varchar(50),
				 PATIENT_RACE_NAT_HI_2 varchar(50),
				 PATIENT_RACE_NAT_HI_3 varchar(50),
				 PATIENT_RACE_NAT_HI_4 varchar(50),
				 PATIENT_RACE_NAT_HI_GT3_IND varchar(10)
				 ;


				 ; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], 
				          row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 

				)
				update prs
				set prs.[PATIENT_RACE_NAT_HI_1] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 1
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt] 
				)
				update prs
				set prs.[PATIENT_RACE_NAT_HI_2] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 2
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;


				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt]
				)
				update prs
				set prs.[PATIENT_RACE_NAT_HI_3] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 3
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				; with cte as (
				select pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt], row_number() OVER (PARTITION BY pr.person_uid ORDER BY pr.person_uid) AS rn
				from rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
				  NBS_ODSE.dbo.person_race  pr with (nolock),  
				[NBS_SRTE].[dbo].[Race_code] rc with (nolock)
				where prs.PATIENT_UID = PR.person_uid 
				and pr.race_cd =  pr.race_category_cd
				and rc.code  = pr.race_category_cd
				group by  pr.person_uid,prs.race_cd, prs.race_category_cd, prs.[code_desc_txt]
				)
				update prs
				set prs.[PATIENT_RACE_NAT_HI_4] = cte1.code_desc_txt
				from  rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] prs, 
					 cte cte1
				where cte1.rn = 4
				and prs.PATIENT_UID = cte1.person_uid
				--and prs.RACE_CD = cte1.race_cd
				;

				update rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] 
				set [PATIENT_RACE_NAT_HI_GT3_IND] = 'TRUE'
				where [PATIENT_RACE_NAT_HI_4] is not null
				;

				update rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] 
				set [PATIENT_RACE_NAT_HI_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_NAT_HI_4] is  null
				;

				update rdb.[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] 
				set [PATIENT_RACE_NAT_HI_GT3_IND] = 'FALSE'
				where [PATIENT_RACE_NAT_HI_4] is  null
				;

				/*

				update  [RDB].[dbo].[TMP_S_PERSON_HAWAIIAN_RACE]
  
				set  [PATIENT_RACE_NAT_HI_ALL] =
					   SUBSTRING (
						( coalesce(  ' | ' + PATIENT_RACE_NAT_HI_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_4 ,'') ),
						3, 
						LEN(coalesce(  ' | ' + PATIENT_RACE_NAT_HI_1 ,'')
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_2 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_3 ,'') 
						+coalesce(  ' | ' + PATIENT_RACE_NAT_HI_4 ,''))
						)
				 ;
				 */

				 
				IF OBJECT_ID('rdb.dbo.TEMP_HAWAIIAN_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_HAWAIIAN_RACE_ALL ; 


				SELECT distinct patient_uid, STUFF((
					SELECT distinct ' | ' + code_desc_txt    FROM [TMP_S_PERSON_HAWAIIAN_RACE] t1
					where t1.patient_uid=t2.patient_uid
					FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') as PATIENT_RACE_NAT_HI_ALL 
					into rdb..TEMP_HAWAIIAN_RACE_ALL
					from rdb..[TMP_S_PERSON_HAWAIIAN_RACE] t2 
				
				update p
				SET
					p.PATIENT_RACE_NAT_HI_ALL =  SUBSTRING(ps.PATIENT_RACE_NAT_HI_ALL, 2, LEN(ps.PATIENT_RACE_NAT_HI_ALL))
				   from 
					  [RDB].[dbo].[TMP_S_PERSON_HAWAIIAN_RACE] p
						INNER JOIN TEMP_HAWAIIAN_RACE_ALL ps 
						on p.PATIENT_UID = ps.PATIENT_UID		         
				--and prs.RACE_CD = cte1.race_cd
				;

 


				--CREATE TABLE S_PERSON_RACE_OUT AS
							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =13;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_RACE_OUT'; 

						IF OBJECT_ID('RDB.DBO.TMP_S_PERSON_RACE_OUT', 'U') IS NOT NULL   
 							 DROP TABLE RDB.dbo.TMP_S_PERSON_RACE_OUT ;

							 /*
				SELECT distinct
					PATIENT_RACE_CALCULATED,PATIENT_RACE_CALC_DETAILS,
					PATIENT_RACE_ALL,
					PATIENT_RACE_NAT_HI_1,   PATIENT_RACE_NAT_HI_2, PATIENT_RACE_NAT_HI_3,PATIENT_RACE_NAT_HI_GT3_IND, PATIENT_RACE_NAT_HI_ALL,
					PATIENT_RACE_ASIAN_1, PATIENT_RACE_ASIAN_2, PATIENT_RACE_ASIAN_ALL,PATIENT_RACE_ASIAN_3,PATIENT_RACE_ASIAN_GT3_IND,
					PATIENT_RACE_AMER_IND_1, PATIENT_RACE_AMER_IND_2, PATIENT_RACE_AMER_IND_3, PATIENT_RACE_AMER_IND_GT3_IND, PATIENT_RACE_AMER_IND_ALL, 
					PATIENT_RACE_BLACK_1, PATIENT_RACE_BLACK_2, PATIENT_RACE_BLACK_3, PATIENT_RACE_BLACK_GT3_IND, PATIENT_RACE_BLACK_ALL,
					PATIENT_RACE_WHITE_1, PATIENT_RACE_WHITE_2, PATIENT_RACE_WHITE_3, PATIENT_RACE_WHITE_GT3_IND, PATIENT_RACE_WHITE_ALL, 
					spr.PATIENT_UID as PATIENT_UID_RACE_OUT
				INTO RDB.dbo.TMP_S_PERSON_RACE_OUT 
				FROM RDB.dbo.TMP_S_PERSON_ROOT_RACE spr
				LEFT OUTER JOIN RDB.dbo.TMP_S_PERSON_AMER_INDIAN_RACE sai ON spr.PATIENT_UID=sai.PATIENT_UID
				LEFT OUTER JOIN RDB.dbo.TMP_S_PERSON_BLACK_RACE spb ON spr.PATIENT_UID=spb.PATIENT_UID
				LEFT OUTER JOIN RDB.dbo.TMP_S_PERSON_WHITE_RACE spw ON spr.PATIENT_UID=spw.PATIENT_UID
				LEFT OUTER JOIN RDB.dbo.TMP_S_PERSON_HAWAIIAN_RACE sph ON  spr.PATIENT_UID=sph.PATIENT_UID
				LEFT OUTER JOIN RDB.dbo.TMP_S_PERSON_ASIAN_RACE spa ON spr.PATIENT_UID=spa.PATIENT_UID
				--ORDER BY spr.PATIENT_UID
				;

				*/
							
				
				SELECT distinct
					PATIENT_RACE_CALCULATED,PATIENT_RACE_CALC_DETAILS,
					PATIENT_RACE_ALL
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_NAT_HI_1
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_NAT_HI_2
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_NAT_HI_3
					,  CAST( NULL as VARCHAR(10)) AS  PATIENT_RACE_NAT_HI_GT3_IND
					,  CAST( NULL as VARCHAR(2000)) AS   PATIENT_RACE_NAT_HI_ALL
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_ASIAN_1
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_ASIAN_2
					,  CAST( NULL as VARCHAR(2000)) AS   PATIENT_RACE_ASIAN_ALL
					,  CAST( NULL as VARCHAR(50)) AS  PATIENT_RACE_ASIAN_3
					,  CAST( NULL as VARCHAR(10)) AS  PATIENT_RACE_ASIAN_GT3_IND
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_AMER_IND_1
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_AMER_IND_2
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_AMER_IND_3
					,  CAST( NULL as VARCHAR(10)) AS   PATIENT_RACE_AMER_IND_GT3_IND
					,  CAST( NULL as VARCHAR(2000)) AS   PATIENT_RACE_AMER_IND_ALL
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_BLACK_1
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_BLACK_2
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_BLACK_3
					,  CAST( NULL as VARCHAR(10)) AS   PATIENT_RACE_BLACK_GT3_IND
					,  CAST( NULL as VARCHAR(2000)) AS   PATIENT_RACE_BLACK_ALL
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_WHITE_1
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_WHITE_2
					,  CAST( NULL as VARCHAR(50)) AS   PATIENT_RACE_WHITE_3
					,  CAST( NULL as VARCHAR(10)) AS   PATIENT_RACE_WHITE_GT3_IND
					,  CAST( NULL as VARCHAR(2000)) AS   PATIENT_RACE_WHITE_ALL
					, spr.PATIENT_UID as PATIENT_UID_RACE_OUT
				INTO RDB.dbo.TMP_S_PERSON_RACE_OUT 
				FROM RDB.dbo.TMP_S_PERSON_ROOT_RACE spr
				;





					UPDATE spr   
					   SET spr.PATIENT_RACE_AMER_IND_1 = sai.PATIENT_RACE_AMER_IND_1 ,
						   spr.PATIENT_RACE_AMER_IND_2 = sai.PATIENT_RACE_AMER_IND_2 ,
						   spr.PATIENT_RACE_AMER_IND_3 = sai.PATIENT_RACE_AMER_IND_3 ,
						   spr.PATIENT_RACE_AMER_IND_GT3_IND = sai.PATIENT_RACE_AMER_IND_GT3_IND ,
						   spr.PATIENT_RACE_AMER_IND_ALL = sai.PATIENT_RACE_AMER_IND_ALL 
					  FROM RDB.dbo.TMP_S_PERSON_RACE_OUT spr
						 JOIN RDB.dbo.TMP_S_PERSON_AMER_INDIAN_RACE sai ON spr.PATIENT_UID_RACE_OUT=sai.PATIENT_UID
					   ;

   
					UPDATE spr   
					   SET spr.PATIENT_RACE_NAT_HI_1 = sai.PATIENT_RACE_NAT_HI_1 ,
						   spr.PATIENT_RACE_NAT_HI_2 = sai.PATIENT_RACE_NAT_HI_2 ,
						   spr.PATIENT_RACE_NAT_HI_3 = sai.PATIENT_RACE_NAT_HI_3 ,
						   spr.PATIENT_RACE_NAT_HI_GT3_IND = sai.PATIENT_RACE_NAT_HI_GT3_IND ,
						   spr.PATIENT_RACE_NAT_HI_ALL = sai.PATIENT_RACE_NAT_HI_ALL 
					  FROM RDB.dbo.TMP_S_PERSON_RACE_OUT spr
						 JOIN RDB.dbo.TMP_S_PERSON_HAWAIIAN_RACE sai ON spr.PATIENT_UID_RACE_OUT=sai.PATIENT_UID
					   ;



   
					UPDATE spr   
					   SET spr.PATIENT_RACE_BLACK_1 = sai.PATIENT_RACE_BLACK_1 ,
						   spr.PATIENT_RACE_BLACK_2 = sai.PATIENT_RACE_BLACK_2 ,
						   spr.PATIENT_RACE_BLACK_3 = sai.PATIENT_RACE_BLACK_3 ,
						   spr.PATIENT_RACE_BLACK_GT3_IND = sai.PATIENT_RACE_BLACK_GT3_IND ,
						   spr.PATIENT_RACE_BLACK_ALL = sai.PATIENT_RACE_BLACK_ALL 
					  FROM RDB.dbo.TMP_S_PERSON_RACE_OUT spr
						 JOIN RDB.dbo.TMP_S_PERSON_BLACK_RACE sai ON spr.PATIENT_UID_RACE_OUT=sai.PATIENT_UID
					   ;

   
					UPDATE spr   
					   SET spr.PATIENT_RACE_WHITE_1 = sai.PATIENT_RACE_WHITE_1 ,
						   spr.PATIENT_RACE_WHITE_2 = sai.PATIENT_RACE_WHITE_2 ,
						   spr.PATIENT_RACE_WHITE_3 = sai.PATIENT_RACE_WHITE_3 ,
						   spr.PATIENT_RACE_WHITE_GT3_IND = sai.PATIENT_RACE_WHITE_GT3_IND ,
						   spr.PATIENT_RACE_WHITE_ALL = sai.PATIENT_RACE_WHITE_ALL 
					  FROM RDB.dbo.TMP_S_PERSON_RACE_OUT spr
						 JOIN RDB.dbo.TMP_S_PERSON_WHITE_RACE sai ON spr.PATIENT_UID_RACE_OUT=sai.PATIENT_UID
					   ;

   
					UPDATE spr   
					   SET spr.PATIENT_RACE_ASIAN_1 = sai.PATIENT_RACE_ASIAN_1 ,
						   spr.PATIENT_RACE_ASIAN_2 = sai.PATIENT_RACE_ASIAN_2 ,
						   spr.PATIENT_RACE_ASIAN_3 = sai.PATIENT_RACE_ASIAN_3 ,
						   spr.PATIENT_RACE_ASIAN_GT3_IND = sai.PATIENT_RACE_ASIAN_GT3_IND ,
						   spr.PATIENT_RACE_ASIAN_ALL = sai.PATIENT_RACE_ASIAN_ALL 
					  FROM RDB.dbo.TMP_S_PERSON_RACE_OUT spr
						 JOIN RDB.dbo.TMP_S_PERSON_ASIAN_RACE sai ON spr.PATIENT_UID_RACE_OUT=sai.PATIENT_UID
					   ;




				--CREATE TABLE S_INITPAT_REV_W_RACE AS 

				 SELECT @ROWCOUNT_NO = @@ROWCOUNT;
				
				update TMP_S_PERSON_RACE_OUT set PATIENT_RACE_NAT_HI_ALL=RTRIM(LTRIM(PATIENT_RACE_NAT_HI_ALL));
				update TMP_S_PERSON_RACE_OUT set PATIENT_RACE_ASIAN_ALL=RTRIM(LTRIM(PATIENT_RACE_ASIAN_ALL));
				update TMP_S_PERSON_RACE_OUT set PATIENT_RACE_AMER_IND_ALL=RTRIM(LTRIM(PATIENT_RACE_AMER_IND_ALL));
				update TMP_S_PERSON_RACE_OUT set PATIENT_RACE_BLACK_ALL=RTRIM(LTRIM(PATIENT_RACE_BLACK_ALL));
				update TMP_S_PERSON_RACE_OUT set PATIENT_RACE_WHITE_ALL=RTRIM(LTRIM(PATIENT_RACE_WHITE_ALL));


							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO = 14;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_INITPAT_REV_W_RACE'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_INITPAT_REV_W_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPAT_REV_W_RACE;
 
				SELECT distinct sir.*, spro.* 
				into rdb.dbo.TMP_S_INITPAT_REV_W_RACE
				FROM rdb.dbo.TMP_S_INITPATIENT_REV  sir
					LEFT OUTER JOIN rdb.dbo.TMP_S_PERSON_RACE_OUT spro ON sir.PATIENT_UID= spro.PATIENT_UID_RACE_OUT
				;

				/*
				PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_INITPATIENT S_PERSON_ASIAN_RACE S_PERSON_AMER_INDIAN_RACE S_PERSON_BLACK_RACE
				S_PERSON_WHITE_RACE S_PERSON_HAWAIIAN_RACE S_INITPATIENT_REV S_PERSON_RACE_OUT S_PERSON_RACE; RUN;
				PROC SQL;
				*/


				--CREATE TABLE S_PERSON_NAME AS 
   							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =15;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_NAME'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_NAME', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_NAME ;


				 SELECT  distinct pn.PERSON_UID AS  PATIENT_UID ,PERSON_NAME_SEQ,  
						FIRST_NM,LAST_NM,MIDDLE_NM,NM_SUFFIX, NM_USE_CD ,
						cast( null as [bigint])   as PATIENT_TYPE_UID, --VS
					   cast( null as varchar(50)) as PATIENT_FIRST_NAME  ,
					   cast( null as varchar(50)) as PATIENT_LAST_NAME  ,
					   cast( null as varchar(50)) as PATIENT_MIDDLE_NAME  ,
					   cast( null as varchar(50)) as PATIENT_ALIAS_NICKNAME,   
					   cast( null as varchar(50)) as PATIENT_NAME_SUFFIX   
				   into rdb.dbo.TMP_S_PERSON_NAME
				   FROM  rdb.dbo.TMP_PATIENT_REV_UID pru
						INNER JOIN NBS_ODSE.dbo.PERSON_NAME pn with (nolock)  ON pru.PATIENT_UID=pn.PERSON_UID 
					WHERE NM_USE_CD IN ('L', 'AL') 
					--ORDER BY PATIENT_UID, PERSON_NAME_SEQ,  FIRST_NM, LAST_NM, MIDDLE_NM, NM_SUFFIX, NM_USE_CD DESC
					;

					/*

				DATA S_PERSON_NAME;
				SET S_PERSON_NAME;
				BY  PATIENT_UID PERSON_NAME_SEQ  FIRST_NM LAST_NM MIDDLE_NM NM_SUFFIX NM_USE_CD;
				RUN;
				DATA S_PERSON_NAME; 
				SET S_PERSON_NAME;

				RETAIN PATIENT_LAST_NAME;
				RETAIN PATIENT_FIRST_NAME;
				RETAIN PATIENT_MIDDLE_NAME;
				RETAIN PATIENT_ALIAS_NICKNAME;
				RETAIN PATIENT_TYPE_UID;
				SET S_PERSON_NAME; BY PATIENT_UID; 

				--VS IF FIRST.PATIENT_UID THEN PATIENT_LAST_NAME='';
				*/


				/*  --VS
				IF (PATIENT_TYPE_UID NE PATIENT_UID) THEN PATIENT_TYPE_UID = PATIENT_UID;
				IF (PATIENT_TYPE_UID NE PATIENT_UID) THEN PATIENT_LAST_NAME='';
				IF (PATIENT_TYPE_UID NE PATIENT_UID) THEN PATIENT_FIRST_NAME='';
				IF (PATIENT_TYPE_UID NE PATIENT_UID) THEN PATIENT_MIDDLE_NAME='';
				IF (PATIENT_TYPE_UID NE PATIENT_UID) THEN PATIENT_ALIAS_NICKNAME='';
				*/




				/*
					IF(NM_USE_CD= 'L')  THEN PATIENT_FIRST_NAME=FIRST_NM;
					IF(NM_USE_CD= 'L')  THEN PATIENT_LAST_NAME=LAST_NM;
					IF(NM_USE_CD= 'L')  THEN PATIENT_MIDDLE_NAME=MIDDLE_NM;
					IF(NM_USE_CD= 'L')  THEN PATIENT_NAME_SUFFIX=NM_SUFFIX;

					IF(NM_USE_CD= 'AL')  THEN PATIENT_ALIAS_NICKNAME=FIRST_NM;
				IF LAST.PATIENT_UID;
				*/

				/*

						update [RDB].[dbo].[TMP_S_PERSON_NAME] 
						  SET  
							  PATIENT_FIRST_NAME=FIRST_NM,
							  PATIENT_LAST_NAME=LAST_NM,
							  PATIENT_MIDDLE_NAME=MIDDLE_NM,
							  PATIENT_NAME_SUFFIX=NM_SUFFIX
						where NM_USE_CD= 'L'
						;

						update [RDB].[dbo].[TMP_S_PERSON_NAME] 
						  SET  PATIENT_ALIAS_NICKNAME = FIRST_NM
						where NM_USE_CD= 'AL'
						;
                */


				update [RDB].[dbo].[TMP_S_PERSON_NAME] 
						  SET  
							  PATIENT_FIRST_NAME= ltrim(rtrim(other_table.FIRST_NM)),
							  PATIENT_LAST_NAME=  other_table.LAST_NM,
							  PATIENT_MIDDLE_NAME=other_table.MIDDLE_NM,
							  PATIENT_NAME_SUFFIX=other_table.NM_SUFFIX
                  FROM     [TMP_S_PERSON_NAME]  
                   INNER JOIN    [TMP_S_PERSON_NAME] other_table   ON     [TMP_S_PERSON_NAME].patient_uid = other_table.patient_uid 
				                                                           and other_table.NM_USE_CD= 'L'
 

						;


						
						update [RDB].[dbo].[TMP_S_PERSON_NAME] 
						  SET  
							  PATIENT_ALIAS_NICKNAME = other_table.FIRST_NM
                           FROM  [TMP_S_PERSON_NAME]  
                            INNER JOIN [TMP_S_PERSON_NAME] other_table   ON     [TMP_S_PERSON_NAME].patient_uid = other_table.patient_uid 
							and other_table.NM_USE_CD= 'AL'
                           ;
 
 				-- PATIENT_NAME_SUFFIX=PUT(NM_SUFFIX, $DEM107F.);

				update rdb.dbo.[TMP_S_PERSON_NAME] 
				   set rdb.dbo.[TMP_S_PERSON_NAME].PATIENT_NAME_SUFFIX = SUBSTRING(cvg.[code_short_desc_txt], 1, 50)
				  FROM NBS_ODSE.[dbo].[NBS_question] nq with (nolock),
					   [NBS_SRTE].[dbo].[Codeset] cd with (nolock),
						[NBS_SRTE].[dbo].[Code_value_general] cvg with (nolock),
						 rdb.dbo.[TMP_S_PERSON_NAME] sir 
				  where nq.question_identifier = ( 'DEM107')
				  and   cd.code_set_group_id = nq.code_set_group_id
				  and   cvg.code_set_nm = cd.code_set_nm
				  and   sir.NM_SUFFIX = cvg.code
				  and   sir.NM_SUFFIX is not null
				 ;


 
							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =16;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_PERSON_NAME_FINAL'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_NAME_FINAL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_NAME_FINAL  ;

					SELECT   distinct * 
						into rdb.dbo.TMP_S_PERSON_NAME_FINAL
						FROM rdb.dbo.TMP_S_PERSON_NAME  t1
						WHERE person_name_seq = (SELECT max(person_name_seq) from rdb.dbo.TMP_S_PERSON_NAME WHERE [PATIENT_UID] = t1.[PATIENT_UID])
						;

				/*
				DROP 
				FIRST_NM LAST_NM MIDDLE_NM NM_SUFFIX NM_USE_CD;
				RUN;
				*/



				--CREATE TABLE S_PATIENT_REVISION AS 
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =17;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_PATIENT_REVISION'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PATIENT_REVISION', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PATIENT_REVISION ;

					SELECT distinct sir.* 
						  , spn.[PERSON_NAME_SEQ]
						  , spn.[FIRST_NM]
						  , spn.[LAST_NM]
						  , spn.[MIDDLE_NM]
						  , spn.[NM_SUFFIX]
						  , spn.[NM_USE_CD]
						  , spn.[PATIENT_FIRST_NAME]
						  , spn.[PATIENT_LAST_NAME]
						  , spn.[PATIENT_MIDDLE_NAME]
						  , spn.[PATIENT_ALIAS_NICKNAME]
						  , spn.[PATIENT_NAME_SUFFIX]
					into rdb.dbo.TMP_S_PATIENT_REVISION
					FROM rdb.dbo.TMP_S_INITPAT_REV_W_RACE sir
					 LEFT OUTER JOIN rdb.dbo.TMP_S_PERSON_NAME_FINAL spn ON sir.PATIENT_UID= spn.PATIENT_UID
					 ;
 
				--  PROC DATASETS LIBRARY = WORK NOLIST; DELETE S_INITPAT_REV_W_RACE S_PERSON_NAME; RUN; QUIT;



				--CREATE TABLE S_POSTAL_LOCATOR AS

						--CREATE TABLE S_POSTAL_LOCATOR AS
									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =18;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_POSTAL_LOCATOR'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_POSTAL_LOCATOR', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_POSTAL_LOCATOR;

		
				with lst as (
    
							SELECT distinct pl.add_time, 
								pl.CITY_DESC_TXT AS PATIENT_CITY ,             
								COALESCE(cc.CODE_SHORT_DESC_TXT,pl.CNTRY_CD)	AS PATIENT_COUNTRY ,            
								pl.CNTY_CD	AS PATIENT_COUNTY_CODE ,              
								pl.STATE_CD	AS PATIENT_STATE_CODE ,               
								pl.STREET_ADDR1 AS PATIENT_STREET_ADDRESS_1 ,
								pl.STREET_ADDR2	AS PATIENT_STREET_ADDRESS_2 ,
								pl.WITHIN_CITY_LIMITS_IND AS PATIENT_WITHIN_CITY_LIMITS ,
								pl.ZIP_CD AS PATIENT_ZIP ,
								pl.CENSUS_TRACT AS PATIENT_CENSUS_TRACT ,
								sc.CODE_DESC_TXT AS PATIENT_STATE_DESC ,
								scc.CODE_DESC_TXT AS PATIENT_COUNTY_DESC ,
								cc.CODE_SHORT_DESC_TXT AS PATIENT_COUNTRY_DESC ,
								elp.ENTITY_UID AS ENTITY_UID_POSTAL ,
								sc.CODE_DESC_TXT AS PATIENT_STATE ,
								scc.CODE_DESC_TXT AS PATIENT_COUNTY  
						,ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY pl.POSTAL_LOCATOR_UID DESC
         					   ) AS [ROWNO]
							FROM rdb.dbo.TMP_PATIENT_REV_UID pru
								LEFT OUTER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock)  ON pru.PATIENT_UID= elp.ENTITY_UID
								LEFT OUTER JOIN NBS_ODSE.dbo.POSTAL_LOCATOR pl with (nolock) ON elp.LOCATOR_UID=pl.POSTAL_LOCATOR_UID
								LEFT OUTER JOIN NBS_SRTE.dbo.STATE_CODE sc with (nolock) ON sc.STATE_CD=pl.STATE_CD
								LEFT OUTER JOIN NBS_SRTE.dbo.COUNTRY_CODE cc with (nolock) ON cc.CODE=pl.CNTRY_CD
								LEFT OUTER JOIN NBS_SRTE.dbo.STATE_COUNTY_CODE_VALUE scc with (nolock) ON scc.CODE=pl.CNTY_CD	
							WHERE elp.USE_CD='H'
							AND elp.CD='H'
							AND elp.CLASS_CD='PST'
							AND elp.RECORD_STATUS_CD='ACTIVE'
					--		and elp.locator_uid = (SELECT max(locator_uid) from NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION with (nolock) WHERE ENTITY_UID= elp.ENTITY_UID)
					--        and pl.POSTAL_LOCATOR_UID = (SELECT max(POSTAL_LOCATOR_UID) from NBS_ODSE.dbo.POSTAL_LOCATOR with (nolock) WHERE POSTAL_LOCATOR_UID = elp.LOCATOR_UID)
						)
						select 
								 add_time, 
								 PATIENT_CITY ,             
								 PATIENT_COUNTRY ,            
								 PATIENT_COUNTY_CODE ,              
								 PATIENT_STATE_CODE ,               
								 PATIENT_STREET_ADDRESS_1 ,
								 PATIENT_STREET_ADDRESS_2 ,
								 PATIENT_WITHIN_CITY_LIMITS ,
								 PATIENT_ZIP,
								 PATIENT_CENSUS_TRACT ,
								 PATIENT_STATE_DESC ,
								 PATIENT_COUNTY_DESC ,
								 PATIENT_COUNTRY_DESC ,
								 ENTITY_UID_POSTAL ,
								 PATIENT_STATE ,
								 PATIENT_COUNTY  
						INTO rdb.dbo.TMP_S_POSTAL_LOCATOR
						from lst
						where rowno = 1
					;
	
		 SELECT @ROWCOUNT_NO = @@ROWCOUNT;
			update TMP_S_POSTAL_LOCATOR set PATIENT_CITY=RTRIM(LTRIM(PATIENT_CITY));
			update TMP_S_POSTAL_LOCATOR set PATIENT_COUNTRY=RTRIM(LTRIM(PATIENT_COUNTRY));
			update TMP_S_POSTAL_LOCATOR set PATIENT_ZIP=RTRIM(LTRIM(PATIENT_ZIP));


	
	

				/*
				QUIT;
				DATA S_POSTAL_LOCATOR;
				SET S_POSTAL_LOCATOR;
				IF LENGTHN(TRIM(PATIENT_STATE_DESC))>1 THEN PATIENT_STATE=PATIENT_STATE_DESC;
				IF LENGTHN(TRIM(PATIENT_COUNTY_DESC))>1 THEN PATIENT_COUNTY=PATIENT_COUNTY_DESC;
				IF LENGTHN(TRIM(PATIENT_COUNTRY_DESC))>1 THEN PATIENT_COUNTRY=PATIENT_COUNTRY_DESC;
				RUN;
				*/


				/*
				PROC SORT DATA=S_POSTAL_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;
				PROC SQL;
				*/

				--CREATE TABLE S_BIRTH_LOCATOR AS

 
											

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =19;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_BIRTH_LOCATOR'; 

				IF OBJECT_ID('rdb.dbo.TMP_S_BIRTH_LOCATOR', 'U') IS NOT NULL  
					drop table RDB.dbo.TMP_S_BIRTH_LOCATOR ;
	
	
				with lst as (
        
					SELECT DISTINCT
						COUNTRY_CODE.CODE_SHORT_DESC_TXT AS PATIENT_BIRTH_COUNTRY ,
						elp.ENTITY_UID as ENTITY_UID_BIRTH
						,ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY pl.POSTAL_LOCATOR_UID DESC
         					   ) AS [ROWNO]

					FROM RDB.dbo.TMP_PATIENT_REV_UID 
						 LEFT OUTER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock) ON RDB.dbo.TMP_PATIENT_REV_UID.PATIENT_UID= elp.ENTITY_UID
						LEFT OUTER JOIN NBS_ODSE.dbo.POSTAL_LOCATOR pl with (nolock) ON elp.LOCATOR_UID=pl.POSTAL_LOCATOR_UID
						LEFT OUTER JOIN NBS_SRTE.dbo.CODE_VALUE_GENERAL COUNTRY_CODE ON COUNTRY_CODE.CODE=pl.CNTRY_CD
					WHERE 	CODE_SET_NM in( 'PHVS_BIRTHCOUNTRY_CDC', 'PHVS_TB_BIRTH_CNTRY') 
						AND elp.USE_CD='BIR'
						AND elp.CD='F'
						AND elp.CLASS_CD='PST'
						AND elp.RECORD_STATUS_CD='ACTIVE' 
					   --and elp.locator_uid = (SELECT max(locator_uid) from NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION  with (nolock) WHERE ENTITY_UID= elp.ENTITY_UID)
						--and POSTAL_LOCATOR_UID = (SELECT max(POSTAL_LOCATOR_UID) from NBS_ODSE.dbo.POSTAL_LOCATOR with (nolock) WHERE POSTAL_LOCATOR_UID= elp.LOCATOR_UID)
						)

						select ENTITY_UID_BIRTH,
							PATIENT_BIRTH_COUNTRY 
						 INTO RDB.dbo.TMP_S_BIRTH_LOCATOR
						from lst
						where rowno = 1
					;
	
		



				-- CREATE TABLE S_TELE_LOCATOR_HOME AS

									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =20;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_TELE_LOCATOR_HOME'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_HOME', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_HOME;


				with lst as (
						 SELECT 
							elp.ENTITY_UID AS ENTITY_UID_HOME,
							tl.EXTENSION_TXT AS PATIENT_PHONE_EXT_HOME ,        
							tl.PHONE_NBR_TXT AS PATIENT_PHONE_HOME,
							tl.TELE_LOCATOR_UID,
							ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY tl.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO]
						FROM rdb.dbo.TMP_PATIENT_REV_UID tpru
							INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock) ON tpru.PATIENT_UID= elp.ENTITY_UID
							INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR tl with (nolock) ON elp.LOCATOR_UID=tl.TELE_LOCATOR_UID 	and tl.TELE_LOCATOR_UID = (SELECT max(TELE_LOCATOR_UID) from NBS_ODSE.dbo.TELE_LOCATOR WHERE TELE_LOCATOR_UID= elp.LOCATOR_UID)

						WHERE elp.USE_CD='H'
						AND elp.CD='PH'
						AND elp.CLASS_CD='TELE'
						AND elp.RECORD_STATUS_CD='ACTIVE'
				--		and tl.TELE_LOCATOR_UID = (SELECT max(TELE_LOCATOR_UID) from NBS_ODSE.dbo.TELE_LOCATOR with (nolock) WHERE TELE_LOCATOR_UID= elp.LOCATOR_UID)
				--	    and elp.entity_uid = 15194813
						)

						select ENTITY_UID_HOME,
							PATIENT_PHONE_EXT_HOME ,        
							PATIENT_PHONE_HOME
						into RDB.dbo.TMP_S_TELE_LOCATOR_HOME
						from lst 
						where rowno = 1
					;
	







				/*QUIT;
				PROC SORT DATA=S_TELE_LOCATOR_HOME NODUPKEY; BY ENTITY_UID; RUN;
				PROC SQL;
				*/

				---CREATE TABLE S_TELE_LOCATOR_OFFICE 

    							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =21;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_TELE_LOCATOR_OFFICE'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE;




				with lst as (
						 SELECT  DISTINCT
						elp.ENTITY_UID AS ENTITY_UID_OFFICE,
						TELE_LOCATOR.EXTENSION_TXT AS PATIENT_PHONE_EXT_WORK ,        
						TELE_LOCATOR.PHONE_NBR_TXT AS PATIENT_PHONE_WORK
							, ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY TELE_LOCATOR.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO]    
					-- INTO  rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE
					FROM rdb.dbo.TMP_PATIENT_REV_UID 
					  INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock) ON rdb.dbo.TMP_PATIENT_REV_UID.PATIENT_UID= elp.ENTITY_UID
					  INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR with (nolock) ON elp.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
					WHERE elp.USE_CD='WP'
						AND elp.CD='PH'
						AND elp.CLASS_CD='TELE'
						AND elp.RECORD_STATUS_CD='ACTIVE'
					--	and elp.locator_uid = (SELECT max(locator_uid) from NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION with (nolock) WHERE ENTITY_UID= elp.ENTITY_UID)
					)

						select ENTITY_UID_OFFICE,
							PATIENT_PHONE_EXT_WORK ,        
							PATIENT_PHONE_WORK
						into RDB.dbo.TMP_S_TELE_LOCATOR_OFFICE
						from lst
						where rowno = 1
					;






				/*QUIT;
				PROC SORT DATA=S_TELE_LOCATOR_OFFICE NODUPKEY; BY ENTITY_UID; RUN;
				PROC SQL;

				*/

				-- CREATE TABLE 	S_TELE_LOCATOR_NET AS

         							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =22;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_TELE_LOCATOR_NET'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_NET', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_NET;
		

	
				with lst as (
						 SELECT  DISTINCT
	 						elp.ENTITY_UID AS ENTITY_UID_NET,
							TELE_LOCATOR.EMAIL_ADDRESS AS PATIENT_EMAIL
							 , ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY TELE_LOCATOR.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO] -- INTO rdb.dbo.TMP_S_TELE_LOCATOR_NET
						FROM rdb.dbo.TMP_PATIENT_REV_UID 
						  INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock) ON rdb.dbo.TMP_PATIENT_REV_UID.PATIENT_UID= elp.ENTITY_UID
						  INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR with (nolock) ON elp.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
						WHERE elp.CD='NET'
						AND elp.CLASS_CD='TELE'
						AND elp.RECORD_STATUS_CD='ACTIVE'
					--	and elp.locator_uid = (SELECT max(locator_uid) from NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION with (nolock) WHERE ENTITY_UID= elp.ENTITY_UID)
					)

						select ENTITY_UID_NET,
							PATIENT_EMAIL
						into RDB.dbo.TMP_S_TELE_LOCATOR_NET
						from lst
						where rowno = 1
					;








				/*
				QUIT;
				PROC SORT DATA=S_TELE_LOCATOR_NET NODUPKEY; BY ENTITY_UID; RUN;
				PROC SQL;

				*/

				--CREATE TABLE S_TELE_LOCATOR_CELL AS
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =23;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_TELE_LOCATOR_CELL'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_CELL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_CELL ;
    
	
	
				with lst as (
						 SELECT  DISTINCT
	 					elp.ENTITY_UID as ENTITY_UID_CELL,
						TELE_LOCATOR.PHONE_NBR_TXT AS PATIENT_PHONE_CELL 
					 , ROW_NUMBER() OVER (
									 PARTITION BY elp.ENTITY_UID 
									 ORDER BY TELE_LOCATOR.TELE_LOCATOR_UID DESC
         					   ) AS [ROWNO] 
							   -- INTO rdb.dbo.TMP_S_TELE_LOCATOR_CELL
							FROM rdb.dbo.TMP_PATIENT_REV_UID  pru
					   INNER JOIN NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION elp with (nolock) ON pru.PATIENT_UID= elp.ENTITY_UID
					   INNER JOIN NBS_ODSE.dbo.TELE_LOCATOR with (nolock) ON elp.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
					WHERE elp.CD='CP'
					 AND elp.CLASS_CD='TELE'
					 AND elp.RECORD_STATUS_CD='ACTIVE'
				--	and elp.locator_uid = (SELECT max(locator_uid) from NBS_ODSE.dbo.ENTITY_LOCATOR_PARTICIPATION with (nolock) WHERE ENTITY_UID= elp.ENTITY_UID)
					)

						select ENTITY_UID_CELL,
							PATIENT_PHONE_CELL
						into RDB.dbo.TMP_S_TELE_LOCATOR_CELL
						from lst
						where rowno = 1
					;












				/*QUIT;
				PROC SORT DATA=S_TELE_LOCATOR_CELL NODUPKEY; BY ENTITY_UID; RUN;
				PROC SQL;
				*/

				--CREATE TABLE S_LOCATOR AS 

									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =24;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_LOCATOR'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_LOCATOR', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_LOCATOR;

						SELECT distinct pl.*,lh.*, lo.*, 
						lc.*,
						ln.*,bl.*, pru.PATIENT_UID as PATIENT_UID_LOCATOR
						into rdb.dbo.TMP_S_LOCATOR
						FROM rdb.dbo.TMP_PATIENT_REV_UID pru 
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_TELE_LOCATOR_HOME lh   ON pru.PATIENT_UID=lh.ENTITY_UID_HOME
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE lo ON pru.PATIENT_UID=lo.ENTITY_UID_OFFICE
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_TELE_LOCATOR_NET ln    ON pru.PATIENT_UID=ln.ENTITY_UID_NET
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_TELE_LOCATOR_CELL lc   ON	pru.PATIENT_UID=lc.ENTITY_UID_CELL
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_POSTAL_LOCATOR pl      ON	pru.PATIENT_UID=pl.ENTITY_UID_POSTAL
						   LEFT OUTER JOIN  rdb.dbo.TMP_S_BIRTH_LOCATOR bl       ON pru.PATIENT_UID=bl.ENTITY_UID_BIRTH
								;





				/*QUIT; --vS
				PROC SORT DATA=S_LOCATOR NODUPKEY; BY PATIENT_UID; RUN;
				PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_POSTAL_LOCATOR S_TELE_LOCATOR_HOME S_TELE_LOCATOR_CELL S_TELE_LOCATOR_NET S_TELE_LOCATOR_OFFICE;RUN;QUIT;
				PROC SQL;
				*/


				--CREATE TABLE ENTITY_ID AS 

									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =25;
							SET @PROC_STEP_NAME = ' GENERATING TMP_ENTITY_ID'; 

						IF OBJECT_ID('rdb.dbo.TMP_ENTITY_ID', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_ENTITY_ID ;



						SELECT DISTINCT PATIENT_UID AS PATIENT_UID_ENTITY
							   , ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD  
						into rdb.dbo.TMP_ENTITY_ID
						FROM rdb.dbo.TMP_PATIENT_REV_UID  pru
						  LEFT OUTER JOIN NBS_ODSE.dbo.[ENTITY_ID] ei with (nolock) ON pru.PATIENT_UID=ei.ENTITY_UID AND ei.TYPE_CD = 'PN'
						where entity_id_seq = (SELECT max(entity_id_seq) from NBS_ODSE.dbo.[ENTITY_ID] with (nolock) WHERE ENTITY_UID = ei.ENTITY_UID and  TYPE_CD = 'PN')
	
						;






				/*QUIT;
				PROC SORT DATA=ENTITY_ID NODUPKEY; BY PATIENT_UID;  RUN;
				PROC SQL;
				*/

						-- CREATE TABLE SSN_ENTITY_ID AS 

									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =26;
							SET @PROC_STEP_NAME = ' GENERATING TMP_SSN_ENTITY_ID'; 

						IF OBJECT_ID('rdb.dbo.TMP_SSN_ENTITY_ID', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_SSN_ENTITY_ID;


						SELECT DISTINCT PATIENT_UID as PATIENT_UID_SSN,
							 ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD  
						into rdb.dbo.TMP_SSN_ENTITY_ID
						FROM rdb.dbo.TMP_PATIENT_REV_UID pru
						   LEFT OUTER JOIN NBS_ODSE.dbo.[ENTITY_ID] ei with (nolock) ON pru.PATIENT_UID=ei.ENTITY_UID AND ei.ASSIGNING_AUTHORITY_CD = 'SSA'
						  		                                  AND ei.entity_uid  = ( select max(entity_uid) from NBS_ODSE.dbo.[ENTITY_ID] ei2
														     where  ei2.ASSIGNING_AUTHORITY_CD = 'SSA'
															   and  ei2.entity_uid = ei.entity_uid)
				 ;





				/*QUIT;
				PROC SORT DATA=SSN_ENTITY_ID NODUPKEY; BY PATIENT_UID;  RUN;
				PROC SQL;
				*/


						-- CREATE TABLE S_PATIENT_REVISION_FINAL AS 

									 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =27;
							SET @PROC_STEP_NAME = ' GENERATING TMP_S_PATIENT_REVISION_FINAL'; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PATIENT_REVISION_FINAL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PATIENT_REVISION_FINAL;

						SELECT distinct sl.*, 
							   spr.* , 
							   ei.ROOT_EXTENSION_TXT AS PATIENT_NUMBER , ei.ASSIGNING_AUTHORITY_CD AS PATIENT_NUMBER_AUTH,
								sei.ROOT_EXTENSION_TXT AS PATIENT_SSN  
						into rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						FROM rdb.dbo.TMP_S_PATIENT_REVISION spr
						  LEFT OUTER JOIN rdb.dbo.TMP_S_LOCATOR sl ON spr.PATIENT_UID=sl.PATIENT_UID_LOCATOR
						  LEFT OUTER JOIN rdb.dbo.TMP_ENTITY_ID ei	ON spr.PATIENT_UID= ei.PATIENT_UID_ENTITY
						  LEFT OUTER JOIN rdb.dbo.TMP_SSN_ENTITY_ID sei ON spr.PATIENT_UID= sei.PATIENT_UID_SSN
						;

  						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  [PATIENT_PHONE_EXT_HOME]= null
						where rtrim(ltrim([PATIENT_PHONE_EXT_HOME])) = ''
						;

						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_MIDDLE_NAME = null
						where rtrim(ltrim(PATIENT_MIDDLE_NAME)) = ''
						;


						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_GENERAL_COMMENTS= null
						where rtrim(ltrim(PATIENT_GENERAL_COMMENTS)) = ''
						;

						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_STREET_ADDRESS_2= null
						where rtrim(ltrim(PATIENT_STREET_ADDRESS_2)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_STREET_ADDRESS_1= null
						where rtrim(ltrim(PATIENT_STREET_ADDRESS_1)) = ''
						;

						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_COUNTY_CODE= null
						where rtrim(ltrim(PATIENT_COUNTY_CODE)) = ''
						;
    
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_CITY= null
						where rtrim(ltrim(PATIENT_CITY)) = ''
						;

						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_PHONE_EXT_WORK= null
						where rtrim(ltrim(PATIENT_PHONE_EXT_WORK)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_WITHIN_CITY_LIMITS= null
						where rtrim(ltrim(PATIENT_WITHIN_CITY_LIMITS)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_ZIP= null
						where rtrim(ltrim(PATIENT_ZIP)) = ''
						;
						
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_STATE_CODE= null
						where rtrim(ltrim(PATIENT_STATE_CODE)) = ''
						
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_COUNTRY= null
						where rtrim(ltrim(PATIENT_COUNTRY)) = ''
						;
						
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_SSN= null
						where rtrim(ltrim(PATIENT_SSN)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_PHONE_HOME= null
						where rtrim(ltrim(PATIENT_PHONE_HOME)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_PHONE_WORK= null
						where rtrim(ltrim(PATIENT_PHONE_WORK)) = ''
						;
						update rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
						set  PATIENT_PHONE_EXT_WORK= null
						where rtrim(ltrim(PATIENT_PHONE_EXT_WORK)) = ''
						;




				/*QUIT;
				PROC SORT DATA=S_PATIENT_REVISION_FINAL NODUPKEY; BY PATIENT_UID;  RUN;
				%DBLOAD (S_PATIENT, S_PATIENT_REVISION_FINAL);
				PROC DATASETS LIBRARY = WORK NOLIST;DELETE ENTITY_ID S_PATIENT_REVISION S_LOCATOR PATIENT_REV_UID;RUN;QUIT;
				PROC SQL;
				*/


							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =28;
							SET @PROC_STEP_NAME = ' GENERATING S_PATIENT'; 

						IF OBJECT_ID('rdb.dbo.S_PATIENT', 'U') IS NOT NULL   
 							 drop table rdb.dbo.S_PATIENT;

							 /*
							select distinct *
							into  rdb.dbo.S_PATIENT
							from rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
							;
							*/

							SELECT [PATIENT_UID]
								  ,[PATIENT_MPR_UID]
								  ,[PATIENT_RECORD_STATUS]
								  ,[PATIENT_LOCAL_ID]
								  ,[PATIENT_GENERAL_COMMENTS]
								  ,[PATIENT_FIRST_NAME]
								  ,[PATIENT_MIDDLE_NAME]
								  ,[PATIENT_LAST_NAME]
								  ,[PATIENT_NAME_SUFFIX]
								  ,[PATIENT_ALIAS_NICKNAME]
								  ,[PATIENT_STREET_ADDRESS_1]
								  ,[PATIENT_STREET_ADDRESS_2]
								  ,[PATIENT_CITY]
								  ,[PATIENT_STATE_CODE]
								  ,[PATIENT_STATE]
								  ,[PATIENT_ZIP]
								  ,[PATIENT_COUNTY]
								  ,[PATIENT_COUNTY_CODE]
								  ,[PATIENT_COUNTRY]
								  ,[PATIENT_WITHIN_CITY_LIMITS]
								  ,[PATIENT_PHONE_HOME]
								  ,[PATIENT_PHONE_EXT_HOME]
								  ,[PATIENT_PHONE_WORK]
								  ,[PATIENT_PHONE_EXT_WORK]
								  ,[PATIENT_PHONE_CELL]
								  ,[PATIENT_EMAIL]
								  ,[PATIENT_DOB]
								  ,[PATIENT_AGE_REPORTED]
								  ,[PATIENT_AGE_REPORTED_UNIT]
								  ,[PATIENT_BIRTH_SEX]
								  ,[PATIENT_CURRENT_SEX]
								  ,[PATIENT_DECEASED_INDICATOR]
								  ,[PATIENT_DECEASED_DATE]
								  ,[PATIENT_MARITAL_STATUS]
								  ,[PATIENT_SSN]
								  ,[PATIENT_ETHNICITY]
								  ,[PATIENT_RACE_CALCULATED]
								  ,[PATIENT_RACE_CALC_DETAILS]
								  ,[PATIENT_RACE_AMER_IND_1]
								  ,[PATIENT_RACE_AMER_IND_2]
								  ,[PATIENT_RACE_AMER_IND_3]
								  ,[PATIENT_RACE_AMER_IND_GT3_IND]
								  ,[PATIENT_RACE_AMER_IND_ALL]
								  ,[PATIENT_RACE_ASIAN_1]
								  ,[PATIENT_RACE_ASIAN_2]
								  ,[PATIENT_RACE_ASIAN_3]
								  ,[PATIENT_RACE_ASIAN_GT3_IND]
								  ,[PATIENT_RACE_ASIAN_ALL]
								  ,[PATIENT_RACE_BLACK_1]
								  ,[PATIENT_RACE_BLACK_2]
								  ,[PATIENT_RACE_BLACK_3]
								  ,[PATIENT_RACE_BLACK_GT3_IND]
								  ,[PATIENT_RACE_BLACK_ALL]
								  ,[PATIENT_RACE_NAT_HI_1]
								  ,[PATIENT_RACE_NAT_HI_2]
								  ,[PATIENT_RACE_NAT_HI_3]
								  ,[PATIENT_RACE_NAT_HI_GT3_IND]
								  ,[PATIENT_RACE_NAT_HI_ALL]
								  ,[PATIENT_RACE_WHITE_1]
								  ,[PATIENT_RACE_WHITE_2]
								  ,[PATIENT_RACE_WHITE_3]
								  ,[PATIENT_RACE_WHITE_GT3_IND]
								  ,[PATIENT_RACE_WHITE_ALL]
								  ,[PATIENT_NUMBER]
								  ,[PATIENT_NUMBER_AUTH]
								  ,[PATIENT_ENTRY_METHOD]
								  ,[PATIENT_LAST_CHANGE_TIME]
								  ,[PATIENT_ADD_TIME]
								  ,[PATIENT_ADDED_BY]
								  ,[PATIENT_LAST_UPDATED_BY]
								  ,[PATIENT_SPEAKS_ENGLISH]
								  ,[PATIENT_UNK_ETHNIC_RSN]
								  ,[PATIENT_CURR_SEX_UNK_RSN]
								  ,[PATIENT_PREFERRED_GENDER]
								  ,[PATIENT_ADDL_GENDER_INFO]
								  ,[PATIENT_CENSUS_TRACT]
								  ,[PATIENT_RACE_ALL]
								  ,[PATIENT_BIRTH_COUNTRY]
								  ,[PATIENT_PRIMARY_OCCUPATION]
								  ,[PATIENT_PRIMARY_LANGUAGE]
							into  rdb.dbo.S_PATIENT
							from rdb.dbo.TMP_S_PATIENT_REVISION_FINAL
							;
				
							/*
							alter table rdb.dbo.S_PATIENT
							drop column  add_time, temp;
							*/




				-- CREATE TABLE L_PATIENT_N  AS 
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =29;
							SET @PROC_STEP_NAME = ' GENERATING TMP_L_PATIENT_N'; 

						IF OBJECT_ID('rdb.dbo.TMP_L_PATIENT_N', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_L_PATIENT_N ;

					CREATE TABLE rdb.[dbo].[TMP_L_PATIENT_N]
					(
					[PATIENT_id]  [int] IDENTITY(1,1) NOT NULL,
					[PATIENT_UID_N] [bigint] NOT NULL,  
					[PATIENT_KEY] [numeric](18, 0) NULL
					 ) ON [PRIMARY]
					 ;

					insert into rdb.[dbo].[TMP_L_PATIENT_N]([PATIENT_UID_N],[PATIENT_KEY])
					 SELECT DISTINCT PATIENT_UID AS PATIENT_UID_N ,null
					  FROM RDB.dbo.S_PATIENT
					 EXCEPT 
					 SELECT PATIENT_UID ,null
					   FROM Rdb.dbo.L_PATIENT
					 ;
	 			/* Added below code to Insert a null Row in L_PATIENT*/ 
					IF NOT EXISTS (SELECT * FROM rdb..L_PATIENT WHERE PATIENT_KEY=1) 
						BEGIN
						   INSERT INTO rdb..L_PATIENT (PATIENT_KEY,PATIENT_UID) VALUES (1,0);
						END
					IF NOT EXISTS (SELECT * FROM rdb..D_PATIENT WHERE PATIENT_KEY=1) 
						BEGIN
						   INSERT INTO rdb..D_PATIENT (PATIENT_KEY) VALUES (1);
						END

				--   %ASSIGN_ADDITIONAL_KEY (L_PATIENT_N, PATIENT_KEY);
				 UPDATE rdb.dbo.TMP_L_PATIENT_N 
				   SET PATIENT_KEY= PATIENT_ID + coalesce((SELECT MAX(PATIENT_KEY) FROM RDB.dbo.L_PATIENT),0)
				   ;

				 -- CREATE TABLE L_PATIENT_E AS 
 

    							 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =30;
							SET @PROC_STEP_NAME = ' GENERATING TMP_L_PATIENT_E'; 

						IF OBJECT_ID('rdb.dbo.TMP_L_PATIENT_E', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_L_PATIENT_E ;

  
					SELECT S_PATIENT.PATIENT_UID as PATIENT_UID_E ,
						   L_PATIENT.PATIENT_KEY
					into rdb.dbo.TMP_L_PATIENT_E
						FROM RDB.dbo.S_PATIENT,Rdb.dbo.L_PATIENT
						WHERE S_PATIENT.PATIENT_UID= L_PATIENT.PATIENT_UID
					;

  


				/*  ---VS


				PROC SORT DATA=L_PATIENT_N NODUPKEY; BY PATIENT_KEY; RUN;
				DATA L_PATIENT_N;
				SET L_PATIENT_N;
				IF PATIENT_KEY_MAX_VAL  ~=. THEN PATIENT_KEY= PATIENT_KEY+PATIENT_KEY_MAX_VAL;
				IF PATIENT_KEY_MAX_VAL  =. THEN PATIENT_KEY= PATIENT_KEY+1;
				DROP PATIENT_KEY_MAX_VAL;
				RUN;
				*/

				/* 
				%DBLOAD (L_PATIENT, L_PATIENT_N);
				PROC SQL;
				*/


				  insert into rdb.dbo.L_PATIENT
				   select [PATIENT_KEY] ,
						[PATIENT_UID_N]
					from rdb.dbo.TMP_L_PATIENT_N
				   ;



				/* ---VS

				  UPDATE rdb.dbo.ACTIVITY_LOG_DETAIL 
					SET SOURCE_ROW_COUNT=(SELECT COUNT(*) FROM  rdb.dbo.TMP_S_PATIENT_REVISION_FINAL),
					END_DATE=GETDATE(),
					DESTINATION_ROW_COUNT=(SELECT COUNT(*) FROM RDB.dbo.TMP_S_PATIENT),
					ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM RDB.dbo.ACTIVITY_LOG_DETAIL)+1),
					ROW_COUNT_INSERT=(SELECT COUNT(*) FROM  rdb.dbo.TMP_L_PATIENT_N),
					ROW_COUNT_UPDATE=(SELECT COUNT(*) FROM  rdb.dbo.TMP_L_PATIENT_E),
					PROCESS_UID= (SELECT PROCESS_UID FROM RDB.dbo.ETL_PROCESS WHERE PROCESS_NAME='S_PATIENT')
					;
				QUIT;
				DATA ACTIVITY_LOG_DETAIL;
				SET ACTIVITY_LOG_DETAIL;
				IF ACTIVITY_LOG_DETAIL_UID=. THEN ACTIVITY_LOG_DETAIL_UID=1;
				IF ROW_COUNT_UPDATE<0 THEN ROW_COUNT_UPDATE=0;
				ADMIN_COMMENT=COMPRESS(ROW_COUNT_INSERT) || ' RECORD(S) INSERTED AND ' ||COMPRESS(ROW_COUNT_UPDATE) || ' RECORD(S) UPDATED IN 
				ENT TABLE.'||
				' THERE IS(ARE) NOW '|| COMPRESS(DESTINATION_ROW_COUNT) || ' TOAL NUMBER OF RECORD(S) IN THE S_PATIENT TABLE.';
				RUN;
				%DBLOAD (ACTIVITY_LOG_DETAIL, ACTIVITY_LOG_DETAIL);
				*/

				   create index idx_PATIENT_UID on rdb.dbo.TMP_S_PATIENT_REVISION_FINAL(PATIENT_UID);
				   create index idx_PATIENT_UID on rdb.dbo.TMP_L_PATIENT_N(PATIENT_UID_N);
				   create index id_PATIENT_UID  on rdb.dbo.TMP_L_PATIENT_E(PATIENT_UID_E);


					--CREATE TABLE D_PATIENT_N AS 

								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;

							SET @PROC_STEP_NO =31;
							SET @PROC_STEP_NAME = ' GENERATING TMP_D_PATIENT_N'; 

						IF OBJECT_ID('rdb.dbo.TMP_D_PATIENT_N', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_D_PATIENT_N ;


					SELECT * 
					into rdb.dbo.TMP_D_PATIENT_N
					FROM RDB.dbo.S_PATIENT  sp,
					 rdb.dbo.TMP_L_PATIENT_N lpn
					WHERE sp.PATIENT_UID=lpn.PATIENT_UID_N
					;
	
  	
				--	CREATE TABLE D_PATIENT_E AS 

								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
		
							BEGIN TRANSACTION;
								  
							SET @PROC_STEP_NO =32;
							SET @PROC_STEP_NAME = ' REMOVE DUPLICATE RECORDS TMP_D_PATIENT_N'; 
							WITH CTE AS
								(
								SELECT *,ROW_NUMBER() OVER (PARTITION BY [PATIENT_KEY] ORDER BY [PATIENT_KEY]) AS RN
								FROM TMP_D_PATIENT_N
								)

								DELETE FROM CTE WHERE RN<>1
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
   		

			

							BEGIN TRANSACTION;
							  
							SET @PROC_STEP_NO =33;
							SET @PROC_STEP_NAME = ' GENERATING TMP_D_PATIENT_E'; 

						IF OBJECT_ID('rdb.dbo.TMP_D_PATIENT_E', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_D_PATIENT_E ;
 

						SELECT * 
						into  rdb.dbo.TMP_D_PATIENT_E
						   FROM RDB.dbo.S_PATIENT sp ,
								rdb.dbo.TMP_L_PATIENT_E lpe
						   WHERE sp.PATIENT_UID=lpe.PATIENT_UID_E;


								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

						COMMIT TRANSACTION;
						BEGIN TRANSACTION;
							SET @PROC_STEP_NO =34;
							SET @PROC_STEP_NAME = ' REMOVE DUPLICATE RECORDS TMP_D_PATIENT_E'; 
							WITH CTE AS
								(
								SELECT *,ROW_NUMBER() OVER (PARTITION BY [PATIENT_KEY] ORDER BY [PATIENT_KEY]) AS RN
								FROM TMP_D_PATIENT_E
								)

								DELETE FROM CTE WHERE RN<>1
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
   		
							BEGIN TRANSACTION;
							  
							SET @PROC_STEP_NO =35;
							SET @PROC_STEP_NAME = ' INSERTING D_PATIENT WITH TMP_D_PATIENT_N'; 

				--PROC SORT DATA=D_PATIENT_N NODUPKEY; BY PATIENT_KEY;RUN;

				--  %DBLOAD (D_PATIENT, D_PATIENT_N);

				 insert into [RDB].[dbo].[D_PATIENT_POC]
					([PATIENT_KEY]
						  ,[PATIENT_MPR_UID]
						  ,[PATIENT_RECORD_STATUS]
						  ,[PATIENT_LOCAL_ID]
						  ,[PATIENT_GENERAL_COMMENTS]
						  ,[PATIENT_FIRST_NAME]
						  ,[PATIENT_MIDDLE_NAME]
						  ,[PATIENT_LAST_NAME]
						  ,[PATIENT_NAME_SUFFIX]
						  ,[PATIENT_ALIAS_NICKNAME]
						  ,[PATIENT_STREET_ADDRESS_1]
						  ,[PATIENT_STREET_ADDRESS_2]
						  ,[PATIENT_CITY]
						  ,[PATIENT_STATE]
						  ,[PATIENT_STATE_CODE]
						  ,[PATIENT_ZIP]
						  ,[PATIENT_COUNTY]
						  ,[PATIENT_COUNTY_CODE]
						  ,[PATIENT_COUNTRY]
						  ,[PATIENT_WITHIN_CITY_LIMITS]
						  ,[PATIENT_PHONE_HOME]
						  ,[PATIENT_PHONE_EXT_HOME]
						  ,[PATIENT_PHONE_WORK]
						  ,[PATIENT_PHONE_EXT_WORK]
						  ,[PATIENT_PHONE_CELL]
						  ,[PATIENT_EMAIL]
						  ,[PATIENT_DOB]
						  ,[PATIENT_AGE_REPORTED] 
						  ,[PATIENT_AGE_REPORTED_UNIT] 
						  ,[PATIENT_BIRTH_SEX] 
						  ,[PATIENT_CURRENT_SEX] 
						  ,[PATIENT_DECEASED_INDICATOR] 
						  ,[PATIENT_DECEASED_DATE]
						  ,[PATIENT_MARITAL_STATUS] 
						  ,[PATIENT_SSN] 
						  ,[PATIENT_ETHNICITY] 
						  ,[PATIENT_RACE_CALCULATED] 
						  ,[PATIENT_RACE_CALC_DETAILS]
						  ,[PATIENT_RACE_AMER_IND_1]
						  ,[PATIENT_RACE_AMER_IND_2]
						  ,[PATIENT_RACE_AMER_IND_3]
						  ,[PATIENT_RACE_AMER_IND_GT3_IND]
						  ,[PATIENT_RACE_AMER_IND_ALL]
						  ,[PATIENT_RACE_ASIAN_1]
						  ,[PATIENT_RACE_ASIAN_2]
						  ,[PATIENT_RACE_ASIAN_3]
						  ,[PATIENT_RACE_ASIAN_GT3_IND]
						  ,[PATIENT_RACE_ASIAN_ALL]
						  ,[PATIENT_RACE_BLACK_1]
						  ,[PATIENT_RACE_BLACK_2]
						  ,[PATIENT_RACE_BLACK_3]
						  ,[PATIENT_RACE_BLACK_GT3_IND]
						  ,[PATIENT_RACE_BLACK_ALL]
						  ,[PATIENT_RACE_NAT_HI_1]
						  ,[PATIENT_RACE_NAT_HI_2]
						  ,[PATIENT_RACE_NAT_HI_3]
						  ,[PATIENT_RACE_NAT_HI_GT3_IND]
						  ,[PATIENT_RACE_NAT_HI_ALL]
						  ,[PATIENT_RACE_WHITE_1]
						  ,[PATIENT_RACE_WHITE_2]
						  ,[PATIENT_RACE_WHITE_3]
						  ,[PATIENT_RACE_WHITE_GT3_IND]
						  ,[PATIENT_RACE_WHITE_ALL]
						  ,[PATIENT_NUMBER] 
						  ,[PATIENT_NUMBER_AUTH]
						  ,[PATIENT_ENTRY_METHOD]
						  ,[PATIENT_LAST_CHANGE_TIME]
						  ,[PATIENT_UID]
						  ,[PATIENT_ADD_TIME]
						  ,[PATIENT_ADDED_BY]
						  ,[PATIENT_LAST_UPDATED_BY]
						  ,[PATIENT_SPEAKS_ENGLISH]
						  ,[PATIENT_UNK_ETHNIC_RSN]
						  ,[PATIENT_CURR_SEX_UNK_RSN]
						  ,[PATIENT_PREFERRED_GENDER]
						  ,[PATIENT_ADDL_GENDER_INFO]
						  ,[PATIENT_CENSUS_TRACT]
						  ,[PATIENT_RACE_ALL]
						  ,[PATIENT_BIRTH_COUNTRY] 
						  ,[PATIENT_PRIMARY_OCCUPATION] 
						  ,[PATIENT_PRIMARY_LANGUAGE] )
					  
					SELECT  distinct [PATIENT_KEY]
						  ,[PATIENT_MPR_UID]
						  ,[PATIENT_RECORD_STATUS]
						  ,[PATIENT_LOCAL_ID]
						  ,substring([PATIENT_GENERAL_COMMENTS] ,1,2000)
						  ,[PATIENT_FIRST_NAME]
						  ,[PATIENT_MIDDLE_NAME]
						  ,[PATIENT_LAST_NAME]
						  ,[PATIENT_NAME_SUFFIX]
						  ,[PATIENT_ALIAS_NICKNAME]
						  ,substring([PATIENT_STREET_ADDRESS_1],1,50)
						  ,substring([PATIENT_STREET_ADDRESS_2],1,50)
						  ,substring([PATIENT_CITY],1,50)
						  ,[PATIENT_STATE]
						  ,[PATIENT_STATE_CODE]
						  ,[PATIENT_ZIP]
						  ,substring([PATIENT_COUNTY],1,50)
						  ,[PATIENT_COUNTY_CODE]
						  ,[PATIENT_COUNTRY]
						  ,[PATIENT_WITHIN_CITY_LIMITS]
						  ,[PATIENT_PHONE_HOME]
						  ,[PATIENT_PHONE_EXT_HOME]
						  ,[PATIENT_PHONE_WORK]
						  ,[PATIENT_PHONE_EXT_WORK]
						  ,[PATIENT_PHONE_CELL]
						  ,[PATIENT_EMAIL]
						  ,[PATIENT_DOB]
						  ,substring([PATIENT_AGE_REPORTED] ,1,9)
						  ,substring([PATIENT_AGE_REPORTED_UNIT] ,1,20)
						  ,substring([PATIENT_BIRTH_SEX] ,1,50)
						  ,substring([PATIENT_CURRENT_SEX] ,1,50)
						  ,substring([PATIENT_DECEASED_INDICATOR] ,1,50)
						  ,[PATIENT_DECEASED_DATE]
						  ,substring([PATIENT_MARITAL_STATUS] ,1,50)
						  ,substring([PATIENT_SSN] ,1,50)
						  ,substring([PATIENT_ETHNICITY] ,1,50)
						  ,substring([PATIENT_RACE_CALCULATED] ,1,50)
						  ,[PATIENT_RACE_CALC_DETAILS]
						  ,[PATIENT_RACE_AMER_IND_1]
						  ,[PATIENT_RACE_AMER_IND_2]
						  ,[PATIENT_RACE_AMER_IND_3]
						  ,[PATIENT_RACE_AMER_IND_GT3_IND]
						  ,[PATIENT_RACE_AMER_IND_ALL]
						  ,[PATIENT_RACE_ASIAN_1]
						  ,[PATIENT_RACE_ASIAN_2]
						  ,[PATIENT_RACE_ASIAN_3]
						  ,[PATIENT_RACE_ASIAN_GT3_IND]
						  ,[PATIENT_RACE_ASIAN_ALL]
						  ,[PATIENT_RACE_BLACK_1]
						  ,[PATIENT_RACE_BLACK_2]
						  ,[PATIENT_RACE_BLACK_3]
						  ,[PATIENT_RACE_BLACK_GT3_IND]
						  ,[PATIENT_RACE_BLACK_ALL]
						  ,[PATIENT_RACE_NAT_HI_1]
						  ,[PATIENT_RACE_NAT_HI_2]
						  ,[PATIENT_RACE_NAT_HI_3]
						  ,[PATIENT_RACE_NAT_HI_GT3_IND]
						  ,[PATIENT_RACE_NAT_HI_ALL]
						  ,[PATIENT_RACE_WHITE_1]
						  ,[PATIENT_RACE_WHITE_2]
						  ,[PATIENT_RACE_WHITE_3]
						  ,[PATIENT_RACE_WHITE_GT3_IND]
						  ,[PATIENT_RACE_WHITE_ALL]
						  ,substring([PATIENT_NUMBER] ,1,50)
						  ,[PATIENT_NUMBER_AUTH]
						  ,[PATIENT_ENTRY_METHOD]
						  ,[PATIENT_LAST_CHANGE_TIME]
						  ,[PATIENT_UID]
						  ,[PATIENT_ADD_TIME]
						  ,[PATIENT_ADDED_BY]
						  ,[PATIENT_LAST_UPDATED_BY]
						  ,[PATIENT_SPEAKS_ENGLISH]
						  ,[PATIENT_UNK_ETHNIC_RSN]
						  ,[PATIENT_CURR_SEX_UNK_RSN]
						  ,[PATIENT_PREFERRED_GENDER]
						  ,[PATIENT_ADDL_GENDER_INFO]
						  ,[PATIENT_CENSUS_TRACT]
						  ,[PATIENT_RACE_ALL]
						  ,substring([PATIENT_BIRTH_COUNTRY] ,1,50)
						  ,substring([PATIENT_PRIMARY_OCCUPATION] ,1,50)
						  ,substring([PATIENT_PRIMARY_LANGUAGE] ,1,50)
					  FROM [RDB].[dbo].[TMP_D_PATIENT_N]
					  ;
					 
					 
								 SELECT @ROWCOUNT_NO = @@ROWCOUNT;

							 INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
								(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
								VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

							COMMIT TRANSACTION;
		
							BEGIN TRANSACTION;
							  
							SET @PROC_STEP_NO =36;
							SET @PROC_STEP_NAME = ' UPDATING D_PATIENT WITH TMP_D_PATIENT_E'; 

					--alter table  rdb.dbo.TMP_D_PATIENT_E   drop column PATIENT_UID_E;


				--	DELETE FROM RDB.dbo.D_PATIENT WHERE PATIENT_UID IN (SELECT PATIENT_UID_E FROM rdb.dbo.TMP_L_PATIENT_E);


					--%DBLOAD (D_PATIENT, D_PATIENT_E);
						
					         update rdb.dbo.D_PATIENT_POC
									set	[PATIENT_KEY]	=	tlpe.[PATIENT_KEY]	,
										[PATIENT_MPR_UID]	=	tlpe.[PATIENT_MPR_UID]	,
										[PATIENT_RECORD_STATUS]	=	tlpe.[PATIENT_RECORD_STATUS]	,
										[PATIENT_LOCAL_ID]	=	tlpe.[PATIENT_LOCAL_ID]	,
										[PATIENT_GENERAL_COMMENTS]	=	 substring(tlpe.[PATIENT_GENERAL_COMMENTS] ,1,2000)	,
										[PATIENT_FIRST_NAME]	=	tlpe.[PATIENT_FIRST_NAME]	,
										[PATIENT_MIDDLE_NAME]	=	tlpe.[PATIENT_MIDDLE_NAME]	,
										[PATIENT_LAST_NAME]	=	tlpe.[PATIENT_LAST_NAME]	,
										[PATIENT_NAME_SUFFIX]	=	tlpe.[PATIENT_NAME_SUFFIX]	,
										[PATIENT_ALIAS_NICKNAME]	=	tlpe.[PATIENT_ALIAS_NICKNAME]	,
										[PATIENT_STREET_ADDRESS_1]	=	substring(tlpe.[PATIENT_STREET_ADDRESS_1],1,50)	,
										[PATIENT_STREET_ADDRESS_2]	=	substring(tlpe.[PATIENT_STREET_ADDRESS_2],1,50)	,
										[PATIENT_CITY]	=	 substring(tlpe.[PATIENT_CITY] ,1,50)	,
										[PATIENT_STATE]	=	tlpe.[PATIENT_STATE]	,
										[PATIENT_STATE_CODE]	=	tlpe.[PATIENT_STATE_CODE]	,
										[PATIENT_ZIP]	=	tlpe.[PATIENT_ZIP]	,
										[PATIENT_COUNTY]	=		substring(tlpe.[PATIENT_COUNTY] ,1,50),
										[PATIENT_COUNTY_CODE]	=	tlpe.[PATIENT_COUNTY_CODE]	,
										[PATIENT_COUNTRY]	=	tlpe.[PATIENT_COUNTRY]	,
										[PATIENT_WITHIN_CITY_LIMITS]	=	tlpe.[PATIENT_WITHIN_CITY_LIMITS]	,
										[PATIENT_PHONE_HOME]	=	tlpe.[PATIENT_PHONE_HOME]	,
										[PATIENT_PHONE_EXT_HOME]	=	tlpe.[PATIENT_PHONE_EXT_HOME]	,
										[PATIENT_PHONE_WORK]	=	tlpe.[PATIENT_PHONE_WORK]	,
										[PATIENT_PHONE_EXT_WORK]	=	tlpe.[PATIENT_PHONE_EXT_WORK]	,
										[PATIENT_PHONE_CELL]	=	tlpe.[PATIENT_PHONE_CELL]	,
										[PATIENT_EMAIL]	=	tlpe.[PATIENT_EMAIL]	,
										[PATIENT_DOB]	=	tlpe.[PATIENT_DOB]	,
										[PATIENT_AGE_REPORTED]	=		substring(tlpe.[PATIENT_AGE_REPORTED] ,1,9),
										[PATIENT_AGE_REPORTED_UNIT]	=	 substring(tlpe.[PATIENT_AGE_REPORTED_UNIT] ,1,20)	,
										[PATIENT_BIRTH_SEX]	=	 substring(tlpe.[PATIENT_BIRTH_SEX] ,1,50)	,
										[PATIENT_CURRENT_SEX]	=		substring(tlpe.[PATIENT_CURRENT_SEX] ,1,50),
										[PATIENT_DECEASED_INDICATOR]	=		substring(tlpe.[PATIENT_DECEASED_INDICATOR] ,1,50),
										[PATIENT_DECEASED_DATE]	=	tlpe.[PATIENT_DECEASED_DATE]	,
										[PATIENT_MARITAL_STATUS]	=		substring(tlpe.[PATIENT_MARITAL_STATUS] ,1,50),
										[PATIENT_SSN]	=	substring(tlpe.[PATIENT_SSN] ,1,50)	,
										[PATIENT_ETHNICITY]	=		substring(tlpe.[PATIENT_ETHNICITY] ,1,50),
										[PATIENT_RACE_CALCULATED]	=		substring(tlpe.[PATIENT_RACE_CALCULATED] ,1,50),
										[PATIENT_RACE_CALC_DETAILS]	=	tlpe.[PATIENT_RACE_CALC_DETAILS]	,
										[PATIENT_RACE_AMER_IND_1]	=	tlpe.[PATIENT_RACE_AMER_IND_1]	,
										[PATIENT_RACE_AMER_IND_2]	=	tlpe.[PATIENT_RACE_AMER_IND_2]	,
										[PATIENT_RACE_AMER_IND_3]	=	tlpe.[PATIENT_RACE_AMER_IND_3]	,
										[PATIENT_RACE_AMER_IND_GT3_IND]	=	tlpe.[PATIENT_RACE_AMER_IND_GT3_IND]	,
										[PATIENT_RACE_AMER_IND_ALL]	=	tlpe.[PATIENT_RACE_AMER_IND_ALL]	,
										[PATIENT_RACE_ASIAN_1]	=	tlpe.[PATIENT_RACE_ASIAN_1]	,
										[PATIENT_RACE_ASIAN_2]	=	tlpe.[PATIENT_RACE_ASIAN_2]	,
										[PATIENT_RACE_ASIAN_3]	=	tlpe.[PATIENT_RACE_ASIAN_3]	,
										[PATIENT_RACE_ASIAN_GT3_IND]	=	tlpe.[PATIENT_RACE_ASIAN_GT3_IND]	,
										[PATIENT_RACE_ASIAN_ALL]	=	tlpe.[PATIENT_RACE_ASIAN_ALL]	,
										[PATIENT_RACE_BLACK_1]	=	tlpe.[PATIENT_RACE_BLACK_1]	,
										[PATIENT_RACE_BLACK_2]	=	tlpe.[PATIENT_RACE_BLACK_2]	,
										[PATIENT_RACE_BLACK_3]	=	tlpe.[PATIENT_RACE_BLACK_3]	,
										[PATIENT_RACE_BLACK_GT3_IND]	=	tlpe.[PATIENT_RACE_BLACK_GT3_IND]	,
										[PATIENT_RACE_BLACK_ALL]	=	tlpe.[PATIENT_RACE_BLACK_ALL]	,
										[PATIENT_RACE_NAT_HI_1]	=	tlpe.[PATIENT_RACE_NAT_HI_1]	,
										[PATIENT_RACE_NAT_HI_2]	=	tlpe.[PATIENT_RACE_NAT_HI_2]	,
										[PATIENT_RACE_NAT_HI_3]	=	tlpe.[PATIENT_RACE_NAT_HI_3]	,
										[PATIENT_RACE_NAT_HI_GT3_IND]	=	tlpe.[PATIENT_RACE_NAT_HI_GT3_IND]	,
										[PATIENT_RACE_NAT_HI_ALL]	=	tlpe.[PATIENT_RACE_NAT_HI_ALL]	,
										[PATIENT_RACE_WHITE_1]	=	tlpe.[PATIENT_RACE_WHITE_1]	,
										[PATIENT_RACE_WHITE_2]	=	tlpe.[PATIENT_RACE_WHITE_2]	,
										[PATIENT_RACE_WHITE_3]	=	tlpe.[PATIENT_RACE_WHITE_3]	,
										[PATIENT_RACE_WHITE_GT3_IND]	=	tlpe.[PATIENT_RACE_WHITE_GT3_IND]	,
										[PATIENT_RACE_WHITE_ALL]	=	tlpe.[PATIENT_RACE_WHITE_ALL]	,
										[PATIENT_NUMBER]	=		substring(tlpe.[PATIENT_NUMBER] ,1,50),
										[PATIENT_NUMBER_AUTH]	=	tlpe.[PATIENT_NUMBER_AUTH]	,
										[PATIENT_ENTRY_METHOD]	=	tlpe.[PATIENT_ENTRY_METHOD]	,
										[PATIENT_LAST_CHANGE_TIME]	=	tlpe.[PATIENT_LAST_CHANGE_TIME]	,
										--[PATIENT_UID]	=	tlpe.[PATIENT_UID_E]	,
										[PATIENT_ADD_TIME]	=	tlpe.[PATIENT_ADD_TIME]	,
										[PATIENT_ADDED_BY]	=	tlpe.[PATIENT_ADDED_BY]	,
										[PATIENT_LAST_UPDATED_BY]	=	tlpe.[PATIENT_LAST_UPDATED_BY]	,
										[PATIENT_SPEAKS_ENGLISH]	=	tlpe.[PATIENT_SPEAKS_ENGLISH]	,
										[PATIENT_UNK_ETHNIC_RSN]	=	tlpe.[PATIENT_UNK_ETHNIC_RSN]	,
										[PATIENT_CURR_SEX_UNK_RSN]	=	tlpe.[PATIENT_CURR_SEX_UNK_RSN]	,
										[PATIENT_PREFERRED_GENDER]	=	tlpe.[PATIENT_PREFERRED_GENDER]	,
										[PATIENT_ADDL_GENDER_INFO]	=	tlpe.[PATIENT_ADDL_GENDER_INFO]	,
										[PATIENT_CENSUS_TRACT]	=	tlpe.[PATIENT_CENSUS_TRACT]	,
										[PATIENT_RACE_ALL]	=	tlpe.[PATIENT_RACE_ALL]	,
										[PATIENT_BIRTH_COUNTRY]	=	 substring(tlpe.[PATIENT_BIRTH_COUNTRY] ,1,50)	,
										[PATIENT_PRIMARY_OCCUPATION]	=		substring(tlpe.[PATIENT_PRIMARY_OCCUPATION] ,1,50),
										[PATIENT_PRIMARY_LANGUAGE]	=		substring(tlpe.[PATIENT_PRIMARY_LANGUAGE] ,1,50)
							from rdb.dbo.TMP_D_PATIENT_E tlpe
	             			Where tlpe.PATIENT_UID_E  = D_PATIENT_POC.PATIENT_UID
							

				SELECT @ROWCOUNT_NO = @@ROWCOUNT;


					
		
		
					INSERT INTO RDB.[DBO].[JOB_FLOW_LOG] 
						(BATCH_ID,[DATAFLOW_NAME],[PACKAGE_NAME] ,[STATUS_TYPE],[STEP_NUMBER],[STEP_NAME],[ROW_COUNT])
						VALUES(@BATCH_ID,'D_PATIENT','RDB.D_PATIENT','START',@PROC_STEP_NO,@PROC_STEP_NAME,@ROWCOUNT_NO);  

					COMMIT TRANSACTION;
				
						-- Temporary solution for session table.
						UPDATE landing
						SET landing.cdc_process_status = 
							CASE 
								-- Set Processed to 1 if no new update to topic else 0
								WHEN d.cdc_id = landing.cdc_id AND d.__cdc_update_time=landing.__cdc_update_time THEN 1
								ELSE  0
							END,
						landing.cdc_processed_time = GETDATE()
						FROM NBS_ChangeData.dbo.[_Person] landing
							INNER JOIN NBS_ChangeData.dbo.TMP_CDC_Person d ON landing.person_uid = d.person_uid AND landing.cdc_id = d.cdc_id
				
						-- Delete TMP tables
						IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT_REV', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPATIENT_REV ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT_TEMP', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPATIENT_TEMP  ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPATIENT  ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_INITPATIENT_REV', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPATIENT_REV ; 

						IF OBJECT_ID('rdb.dbo.TMP_PATIENT_REV_UID', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_PATIENT_REV_UID ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_RACE  
						IF OBJECT_ID('rdb.dbo.TMP_PERSON_ROOT_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_PERSON_ROOT_RACE 
						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_AMER_INDIAN_RACE ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_BLACK_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_BLACK_RACE ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_WHITE_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_WHITE_RACE ; 

						IF OBJECT_ID('rdb.dbo.TEMP_AMER_IND_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_AMER_IND_RACE_ALL ; 

						IF OBJECT_ID('rdb.dbo.TEMP_BLACK_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_BLACK_RACE_ALL ; 

						IF OBJECT_ID('rdb.dbo.TEMP_WHITE_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_WHITE_RACE_ALL ; 

						IF OBJECT_ID('rdb.dbo.TEMP_ASIAN_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_ASIAN_RACE_ALL ; 

						IF OBJECT_ID('rdb.dbo.TEMP_HAWAIIAN_RACE_ALL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TEMP_HAWAIIAN_RACE_ALL ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_ASIAN_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_ASIAN_RACE ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_HAWAIIAN_RACE ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_ROOT_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_ROOT_RACE; 

						IF OBJECT_ID('RDB.DBO.TMP_S_PERSON_RACE_OUT', 'U') IS NOT NULL   
							 drop table rdb.dbo.TMP_S_PERSON_RACE_OUT; 

						
						IF OBJECT_ID('rdb.dbo.TMP_S_INITPAT_REV_W_RACE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_INITPAT_REV_W_RACE; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_NAME', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_NAME ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PERSON_NAME_FINAL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PERSON_NAME_FINAL  ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PATIENT_REVISION', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PATIENT_REVISION ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_POSTAL_LOCATOR', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_POSTAL_LOCATOR; 

				        IF OBJECT_ID('rdb.dbo.TMP_S_BIRTH_LOCATOR', 'U') IS NOT NULL  
			            		drop table RDB.dbo.TMP_S_BIRTH_LOCATOR ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_HOME', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_HOME; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_OFFICE; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_NET', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_NET; 

						IF OBJECT_ID('rdb.dbo.TMP_S_TELE_LOCATOR_CELL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_TELE_LOCATOR_CELL ; 

						IF OBJECT_ID('rdb.dbo.TMP_S_LOCATOR', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_LOCATOR; 

						IF OBJECT_ID('rdb.dbo.TMP_ENTITY_ID', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_ENTITY_ID ; 

						IF OBJECT_ID('rdb.dbo.TMP_SSN_ENTITY_ID', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_SSN_ENTITY_ID; 

						IF OBJECT_ID('rdb.dbo.TMP_S_PATIENT_REVISION_FINAL', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_S_PATIENT_REVISION_FINAL; 



						IF OBJECT_ID('rdb.dbo.TMP_L_PATIENT_N', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_L_PATIENT_N ; 

						IF OBJECT_ID('rdb.dbo.TMP_L_PATIENT_E', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_L_PATIENT_E ; 

						IF OBJECT_ID('rdb.dbo.TMP_D_PATIENT_N', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_D_PATIENT_N ; 

						IF OBJECT_ID('rdb.dbo.TMP_D_PATIENT_E', 'U') IS NOT NULL   
 							 drop table rdb.dbo.TMP_D_PATIENT_E ; 
						
		
					BEGIN TRANSACTION;

					SET @Proc_Step_no = 40;
					SET @Proc_Step_Name = 'SP_COMPLETE'; 


					INSERT INTO rdb.[dbo].[job_flow_log] (
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
						   'D_PATIENT'
						   ,'RDB.D_PATIENT'
						   ,'COMPLETE'
						   ,@Proc_Step_no
						   ,@Proc_Step_name
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
 
	
    INSERT INTO rdb.[dbo].[job_flow_log] (
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
           ,'D_PATIENT'
         ,'RDB.D_PATIENT'
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