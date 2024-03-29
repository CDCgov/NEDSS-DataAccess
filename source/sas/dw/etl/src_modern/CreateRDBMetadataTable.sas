%ETLLIB;
%MACRO DBLOAD (DBTABLE, DSNAME);
 PROC APPEND FORCE BASE=NBS_RDB.&DBTABLE DATA=&DSNAME;
 RUN;
 QUIT;
%MEND DBLOAD;

PROC SQL;
CREATE TABLE ACTIVITY_LOG_MASTER_LAST 
(ACTIVITY_LOG_MASTER_UID NUM,
START_DATE DATE, 
START_DATE2 DATE,
START_DATEINIT DATE,
COUNT NUM);
INSERT INTO ACTIVITY_LOG_MASTER_LAST( ACTIVITY_LOG_MASTER_UID, START_DATE,START_DATEINIT, START_DATE2) VALUES 
(1 , '01JUN1900'D,'01JUN1900'D, NULL);
UPDATE ACTIVITY_LOG_MASTER_LAST SET COUNT= (SELECT COUNT(*) FROM nbs_rdb.D_PATIENT);

UPDATE ACTIVITY_LOG_MASTER_LAST set ACTIVITY_LOG_MASTER_UID = (SELECT MAX(ACTIVITY_LOG_MASTER_UID)
FROM nbs_rdb.ACTIVITY_LOG_MASTER where (refresh_ind ='T' OR  refresh_ind is null) );
UPDATE ACTIVITY_LOG_MASTER_LAST SET START_DATE2= (SELECT START_DATE FROM nbs_rdb.ACTIVITY_LOG_MASTER 
WHERE ACTIVITY_LOG_MASTER_UID= (SELECT MAX(ACTIVITY_LOG_MASTER_UID)
FROM nbs_rdb.ACTIVITY_LOG_MASTER where (refresh_ind ='T' OR  refresh_ind is null) ));
CREATE TABLE ACTIVITY_LOG_MASTER  
(ACTIVITY_LOG_MASTER_UID NUM,
START_DATE DATE, END_DATE DATE);

CREATE TABLE ACTIVITY_LOG_MASTER
(ACTIVITY_LOG_MASTER_UID NUM,
START_DATE DATE, 
END_DATE DATE,
refresh_ind VARCHAR(20), 
refresh_description VARCHAR(100)
);
INSERT INTO ACTIVITY_LOG_MASTER( ACTIVITY_LOG_MASTER_UID, START_DATE, END_DATE) VALUES 
(1 , NULL, NULL);
UPDATE ACTIVITY_LOG_MASTER SET ACTIVITY_LOG_MASTER_UID= (SELECT MAX(ACTIVITY_LOG_MASTER_UID)
FROM nbs_rdb.ACTIVITY_LOG_MASTER),START_DATE =DATETIME();

CREATE TABLE ACTIVITY_LOG_DETAIL  (ACTIVITY_LOG_DETAIL_UID NUMERIC,	PROCESS_UID NUMERIC ,	
	SOURCE_ROW_COUNT NUMERIC , ROW_COUNT_INSERT NUMERIC, ROW_COUNT_UPDATE NUMERIC,
	SOURCE_ROW_COUNT_EXISTING NUMERIC ,	SOURCE_ROW_COUNT_NEW NUMERIC ,
	DESTINATION_ROW_COUNT NUMERIC ,
	START_DATE DATE, END_DATE DATE,
	START_DATE2 DATE,
	ADMIN_COMMENT VARCHAR(200), ACTIVITY_LOG_MASTER_UID NUMERIC);
INSERT INTO ACTIVITY_LOG_DETAIL( ACTIVITY_LOG_DETAIL_UID, PROCESS_UID,SOURCE_ROW_COUNT, DESTINATION_ROW_COUNT,
START_DATE,END_DATE, ACTIVITY_LOG_MASTER_UID) VALUES (1 , 1, NULL, NULL, NULL, NULL, NULL);
UPDATE ACTIVITY_LOG_DETAIL SET ACTIVITY_LOG_DETAIL_UID= (SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM nbs_rdb.ACTIVITY_LOG_DETAIL)+1 ;
UPDATE ACTIVITY_LOG_DETAIL SET START_DATE2= (
SELECT MAX(START_DATE)  FROM nbs_rdb.ACTIVITY_LOG_DETAIL);
QUIT;
DATA ACTIVITY_LOG_MASTER;
SET ACTIVITY_LOG_MASTER;
IF ACTIVITY_LOG_MASTER_UID > 0 THEN ACTIVITY_LOG_MASTER_UID= SUM(ACTIVITY_LOG_MASTER_UID+1);
ELSE ACTIVITY_LOG_MASTER_UID= 1;
RUN;
DATA ACTIVITY_LOG_MASTER_LAST;
SET ACTIVITY_LOG_MASTER_LAST;
IF COUNT<1 THEN START_DATE=START_DATEINIT;
ELSE IF START_DATE<START_DATE2 THEN START_DATE=START_DATE2;
RUN;
DATA rdbdata.ACTIVITY_LOG_MASTER_LAST;
SET ACTIVITY_LOG_MASTER_LAST;
RUN;

PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID)
FROM nbs_rdb.ACTIVITY_LOG_DETAIL)+1),
END_DATE =DATETIME(),
START_DATE =DATETIME(),
ACTIVITY_LOG_MASTER_UID= (SELECT MAX(ACTIVITY_LOG_MASTER_UID) FROM ACTIVITY_LOG_MASTER);
UPDATE ACTIVITY_LOG_MASTER SET refresh_ind= 'F', refresh_description='ETL process initiated.';
QUIT;
DATA ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
END_DATE =DATETIME();
START_DATE =DATETIME();
if START_DATE2=. then START_DATE2='01JUN1900'D;
RUN;
DATA rdbdata.ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
RUN;

%DBLOAD (ACTIVITY_LOG_MASTER, ACTIVITY_LOG_MASTER);

PROC SQL;
DROP TABLE nbs_rdb.RDB_TABLE_METADATA;

CREATE TABLE RDB_METADATAINIT AS
SELECT INVESTIGATION_FORM_CD, QUESTION_LABEL, CODE_SET_GROUP_ID, DATA_LOCATION, DATA_TYPE,PART_TYPE_CD, QUESTION_IDENTIFIER, BATCH_TABLE_APPEAR_IND_CD,
NBS_UI_COMPONENT.TYPE_CD_DESC, NBS_RDB_METADATA.RDB_TABLE_NM, NBS_RDB_METADATA.RDB_COLUMN_NM
 FROM nbs_ods.NBS_UI_METADATA
 LEFT JOIN nbs_cdc.NBS_UI_COMPONENT ON 
 NBS_UI_COMPONENT.NBS_UI_COMPONENT_UID = NBS_UI_METADATA.NBS_UI_COMPONENT_UID
 LEFT JOIN nbs_ods.NBS_RDB_METADATA ON 
 NBS_RDB_METADATA.NBS_UI_METADATA_UID = NBS_UI_METADATA.NBS_UI_METADATA_UID;
QUIT;
PROC SQL;
CREATE TABLE CODESETGROUPID AS
 
 SELECT DISTINCT  CODE_SET_GROUP_ID FROM nbs_ods.NBS_UI_METADATA;
QUIT;
PROC SQL;
CREATE TABLE CODESETNAME AS

 SELECT CODESET.CODE_SET_GROUP_ID , CODESET.CODE_SET_NM FROM NBS_SRT.CODESET 
WHERE CODE_SET_GROUP_ID IN 
(SELECT  DISTINCT CODE_SET_GROUP_ID FROM nbs_ods.NBS_UI_METADATA ) ORDER BY CODESET.CODE_SET_GROUP_ID;
QUIT;

PROC SQL;
	CREATE TABLE nbs_rdb.RDB_TABLE_METADATA AS
 SELECT A.*,  CODESETNAME.CODE_SET_NM FROM 
RDB_METADATAINIT A 
LEFT JOIN CODESETNAME B ON
A.CODE_SET_GROUP_ID  =  B.CODE_SET_GROUP_ID
ORDER BY A.QUESTION_IDENTIFIER;
QUIT;

PROC SQL;
DROP TABLE nbs_rdb.UPDATED_OBSERVATION_LIST;
DROP TABLE nbs_rdb.UPDATED_LAB_TEST_LIST;
QUIT;
%macro PATIENT_CHECKER;


PROC SQL;
CREATE TABLE 
	MISSING_LAB_CASES AS 
	SELECT LAB_RPT_LOCAL_ID as EVENT_LOCAL_ID  'EVENT_LOCAL_ID'   FROM nbs_rdb.lab100 
WHERE 
	PATIENT_KEY =1 
UNION 
	SELECT EVENT_LOCAL_ID as EVENT_LOCAL_ID 'EVENT_LOCAL_ID', ETL_MISSING_RECORD_UID  FROM nbs_rdb.ETL_MISSING_RECORD 
WHERE 
	PATIENT_UID =.
UNION 
SELECT OBSERVATION.LOCAL_ID FROM nbs_cdc.OBSERVATION 
INNER JOIN nbs_cdc.ACT_RELATIONSHIP ON
ACT_RELATIONSHIP.TARGET_ACT_UID = OBSERVATION.OBSERVATION_UID 
WHERE OBSERVATION.ELECTRONIC_IND='Y'
AND ACT_RELATIONSHIP.TYPE_CD='APND'
AND ACT_RELATIONSHIP.LAST_CHG_TIME > (SELECT START_DATE FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE START_DATE = (SELECT MAX(START_DATE) 
		FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE  REFRESH_IND='T' AND REFRESH_DESCRIPTION ='ETL process completed.'));
QUIT;

DATA MISSING_LAB_CASES;
SET MISSING_LAB_CASES;
LENGTH PROCESS_DESCRIPTION $50;
LENGTH PROCESS_UID 8;
LENGTH PROCESSED_INDICATOR 8;
RUN;


PROC SQL;
UPDATE MISSING_LAB_CASES SET PROCESS_UID=(SELECT PROCESS_UID FROM nbs_rdb.ETL_PROCESS WHERE PROCESS_NAME='LAB100');
UPDATE MISSING_LAB_CASES SET PROCESS_DESCRIPTION=(SELECT PROCESS_NAME FROM nbs_rdb.ETL_PROCESS WHERE PROCESS_NAME='LAB100');
RUN;


PROC SQL;
create table missing_patient as
  select distinct PERSON.PERSON_UID as PATIENT_UID, MISSING_LAB_CASES.PROCESS_UID, MISSING_LAB_CASES.PROCESS_DESCRIPTION,
  MISSING_LAB_CASES.ETL_MISSING_RECORD_UID, PROCESSED_INDICATOR, act_uid as EVENT_UID, TYPE_CD as TYPE_CODE, observation.last_chg_time as EVENT_LAST_CHG_TIME, 
	observation.local_id as EVENT_LOCAL_ID 'EVENT_LOCAL_ID', observation.CTRL_CD_DISPLAY_FORM, PERSON.LAST_CHG_TIME as PATIENT_LAST_CHG_TIME, 
	PROCESS_UID, PROCESS_DESCRIPTION
  from MISSING_LAB_CASES 
INNER JOIN nbs_cdc.observation
ON observation.local_id=MISSING_LAB_CASES.EVENT_LOCAL_ID
INNER JOIN  nbs_cdc.Participation 
ON observation.observation_uid=Participation.act_uid
INNER  JOIN nbs_cdc.PERSON 
ON PERSON.PERSON_UID= Participation.SUBJECT_ENTITY_UID
where Participation.type_cd in ('PATSBJ')
and observation.LAST_CHG_TIME<(SELECT MAX(ACTIVITY_LOG_DETAIL.START_DATE) FROM  nbs_rdb.ACTIVITY_LOG_DETAIL);
QUIT;


data missing_patient;
merge missing_patient MISSING_LAB_CASES;         
by EVENT_LOCAL_ID;
run;


DATA missing_patient;
SET missing_patient;
LENGTH ADMIN_COMMENT $200;
IF EVENT_LAST_CHG_TIME=. THEN EVENT_LAST_CHG_TIME = PATIENT_LAST_CHG_TIME;
IF PATIENT_UID =. then ADMIN_COMMENT='The Observation local id is not a valid local id. Please check.';
ELSE IF PATIENT_UID >0 and EVENT_UID> 0 then ADMIN_COMMENT='LAB100 record found with missing patient information. This will be fixed in the current run of ETL.';
IF PATIENT_UID =. then PROCESSED_INDICATOR=-1;
ELSE IF  missing(EVENT_LOCAL_ID) then PROCESSED_INDICATOR=0;
ELSE IF PATIENT_UID >0 and EVENT_UID> 0 then PROCESSED_INDICATOR=0;
RUN;

PROC SQL;
create table missing_patient_N 
as select * from missing_patient where ETL_MISSING_RECORD_UID is null;
QUIT;

PROC SQL;
create table missing_patient_E
as select * from missing_patient where ETL_MISSING_RECORD_UID is not null;
QUIT;

DATA missing_patient_N;
SET missing_patient_N;
DROP ETL_MISSING_RECORD_UID;
RUN;
PROC SQL;
INSERT INTO nbs_rdb.ETL_MISSING_RECORD
(PATIENT_UID, PROCESS_UID,EVENT_UID, PROCESS_DESCRIPTION, PROCESSED_INDICATOR, TYPE_CODE, EVENT_LAST_CHG_TIME, EVENT_LOCAL_ID, ctrl_cd_display_form, PATIENT_LAST_CHG_TIME, ADMIN_COMMENT)
SELECT 
PATIENT_UID, PROCESS_UID, EVENT_UID, PROCESS_DESCRIPTION, PROCESSED_INDICATOR, TYPE_CODE, EVENT_LAST_CHG_TIME, EVENT_LOCAL_ID, ctrl_cd_display_form, PATIENT_LAST_CHG_TIME, ADMIN_COMMENT
FROM missing_patient_N;
QUIT;

DATA nbs_rdb.ETL_MISSING_RECORD;
 MODIFY nbs_rdb.ETL_MISSING_RECORD missing_patient_E;
 BY ETL_MISSING_RECORD_UID;
RUN;
%mend PATIENT_CHECKER;
%MACRO ELR_COMMENT_CHECKER;
PROC SQL;
INSERT INTO nbs_rdb.ETL_MISSING_RECORD
(PATIENT_UID, PROCESS_UID,EVENT_UID, PROCESS_DESCRIPTION, PROCESSED_INDICATOR, TYPE_CODE, EVENT_LAST_CHG_TIME, EVENT_LOCAL_ID, ctrl_cd_display_form, PATIENT_LAST_CHG_TIME, ADMIN_COMMENT)
SELECT 
PATIENT_UID, PROCESS_UID, EVENT_UID, PROCESS_DESCRIPTION, PROCESSED_INDICATOR, TYPE_CODE, EVENT_LAST_CHG_TIME, EVENT_LOCAL_ID, ctrl_cd_display_form, PATIENT_LAST_CHG_TIME, ADMIN_COMMENT
FROM missing_patient_N;
QUIT;
%MEND ELR_COMMENT_CHECKER; 
PROC SQL;
CREATE TABLE MERGED_PATIENT AS 
SELECT PATIENT_KEY FROM nbs_cdc.PERSON 
INNER JOIN nbs_rdb.D_PATIENT ON D_PATIENT.PATIENT_MPR_UID = PERSON.PERSON_UID
WHERE RECORD_STATUS_CD='SUPERCEDED'  AND LAST_CHG_TIME > (SELECT START_DATE FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE START_DATE = (SELECT MAX(START_DATE) 
		FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE  REFRESH_IND='T' AND REFRESH_DESCRIPTION ='ETL process completed.'));
QUIT;

PROC SQL;
	UPDATE nbs_rdb.LAB100 SET PATIENT_KEY = 1 WHERE PATIENT_KEY IN (SELECT PATIENT_KEY FROM MERGED_PATIENT);
QUIT;
PROC SQL;
CREATE TABLE D_PATIENT_CHECKER 
(COUNT_MISSING_PAT NUM,COUNT_MISSING_LAB NUM, COUNT_MISSING_ELR_COMMENT NUM);
INSERT INTO D_PATIENT_CHECKER( COUNT_MISSING_PAT, COUNT_MISSING_LAB, COUNT_MISSING_ELR_COMMENT ) VALUES 
(NULL, NULL, NULL);
UPDATE D_PATIENT_CHECKER SET COUNT_MISSING_PAT = (
select count(*) from nbs_rdb.LAB100 WHERE PATIENT_KEY=1); 
UPDATE D_PATIENT_CHECKER SET COUNT_MISSING_LAB = (SELECT COUNT(*) from nbs_rdb.ETL_MISSING_RECORD 
												WHERE EVENT_LOCAL_ID is not null and PROCESSED_INDICATOR is null); 

UPDATE D_PATIENT_CHECKER SET COUNT_MISSING_ELR_COMMENT =(SELECT count(*) FROM nbs_cdc.OBSERVATION 
INNER JOIN nbs_cdc.ACT_RELATIONSHIP ON
ACT_RELATIONSHIP.TARGET_ACT_UID = OBSERVATION.OBSERVATION_UID
WHERE OBSERVATION.ELECTRONIC_IND='Y'
AND ACT_RELATIONSHIP.TYPE_CD='APND'
AND ACT_RELATIONSHIP.LAST_CHG_TIME > (SELECT START_DATE FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE START_DATE = (SELECT MAX(START_DATE) 
		FROM nbs_rdb.ACTIVITY_LOG_MASTER WHERE  REFRESH_IND='T' AND REFRESH_DESCRIPTION ='ETL process completed.')));

QUIT;
DATA _null_;
set D_PATIENT_CHECKER;
	IF COUNT_MISSING_PAT >0  then call execute('%PATIENT_CHECKER');
	ELSE IF COUNT_MISSING_LAB >0  then call execute('%PATIENT_CHECKER');
	IF COUNT_MISSING_ELR_COMMENT >0  then call execute('%PATIENT_CHECKER');
  
RUN;
PROC SQL;
DELETE * FROM nbs_rdb.S_UPDATED_LAB;
CREATE TABLE S_UPDATED_LAB AS SELECT CTRL_CD_DISPLAY_FORM,
	OBSERVATION_UID,LAST_CHG_TIME FROM nbs_cdc.OBSERVATION WHERE 
	CTRL_CD_DISPLAY_FORM IN ('LabReport','MorbReport') AND OBS_DOMAIN_CD_ST_1='Order'
	AND LAST_CHG_TIME >(SELECT MAX(ACTIVITY_LOG_DETAIL.START_DATE2) FROM  ACTIVITY_LOG_DETAIL)
	AND  LAST_CHG_TIME <(SELECT MAX(ACTIVITY_LOG_DETAIL.START_DATE) FROM  ACTIVITY_LOG_DETAIL)
	UNION 
	SELECT CTRL_CD_DISPLAY_FORM,
	EVENT_UID AS OBSERVATION_UID 'OBSERVATION_UID',EVENT_LAST_CHG_TIME AS  LAST_CHG_TIME 'LAST_CHG_TIME' FROM nbs_rdb.ETL_MISSING_RECORD WHERE PROCESSED_INDICATOR =0;
QUIT;
PROC SQL;
/*CREATE TABLE S_EDX_DOCUMENT AS SELECT EDX_DOCUMENT_UID, ACT_UID, ADD_TIME FROM nbs_cdc.EDX_DOCUMENT, S_UPDATED_LAB WHERE  EDX_DOCUMENT.ACT_UID=S_UPDATED_LAB.OBSERVATION_UID ORDER BY ADD_TIME DESC;*/

CREATE TABLE L_OBSERVATION_MAP AS SELECT OBSERVATION_UID, /*ACT1.TYPE_CD,ACT2.TYPE_CD,ACT3.TYPE_CD,*/
ACT1.SOURCE_ACT_UID AS SOURCE_ACT_UID1 'SOURCE_ACT_UID1' ,ACT2.SOURCE_ACT_UID AS SOURCE_ACT_UID2 'SOURCE_ACT_UID2',ACT3.SOURCE_ACT_UID AS SOURCE_ACT_UID3 'SOURCE_ACT_UID3',
ACT4.SOURCE_ACT_UID AS SOURCE_ACT_UID4 'SOURCE_ACT_UID4',
ACT1.TARGET_ACT_UID AS TARGET_ACT_UID1 'TARGET_ACT_UID1', ACT2.TARGET_ACT_UID AS TARGET_ACT_UID2 'TARGET_ACT_UID2', ACT3.TARGET_ACT_UID AS TARGET_ACT_UID3 'TARGET_ACT_UID3', 
ACT4.TARGET_ACT_UID AS TARGET_ACT_UID4 'TARGET_ACT_UID4'
FROM S_UPDATED_LAB 
LEFT OUTER JOIN nbs_cdc.ACT_RELATIONSHIP ACT1 ON  S_UPDATED_LAB.OBSERVATION_UID= ACT1.TARGET_ACT_UID 
LEFT OUTER JOIN nbs_cdc.ACT_RELATIONSHIP ACT2 ON ACT1.SOURCE_ACT_UID=ACT2.TARGET_ACT_UID  
LEFT OUTER JOIN nbs_cdc.ACT_RELATIONSHIP ACT3 ON ACT2.SOURCE_ACT_UID=ACT3.TARGET_ACT_UID
LEFT OUTER JOIN nbs_cdc.ACT_RELATIONSHIP ACT4 ON ACT3.SOURCE_ACT_UID=ACT4.TARGET_ACT_UID
ORDER BY OBSERVATION_UID;
QUIT;

%DBLOAD (L_OBSERVATION_MAP, L_OBSERVATION_MAP);
%DBLOAD (S_UPDATED_LAB, S_UPDATED_LAB);
PROC SQL;
CREATE TABLE UPDATED_OBSERVATION_MAP AS SELECT * FROM nbs_rdb.L_OBSERVATION_MAP WHERE 
OBSERVATION_UID IN (SELECT OBSERVATION_UID FROM L_OBSERVATION_MAP);
CREATE INDEX OBSERVATION_UID ON UPDATED_OBSERVATION_MAP(OBSERVATION_UID);
QUIT;
PROC SQL;
CREATE TABLE UPDATED_OBSERVATION_1 AS SELECT DISTINCT 
OBSERVATION_UID FROM UPDATED_OBSERVATION_MAP;

CREATE TABLE UPDATED_OBSERVATION_2 AS SELECT DISTINCT 
SOURCE_ACT_UID1 AS OBSERVATION_UID 'OBSERVATION_UID' FROM UPDATED_OBSERVATION_MAP;

CREATE TABLE UPDATED_OBSERVATION_3 AS SELECT DISTINCT 
SOURCE_ACT_UID2 AS OBSERVATION_UID 'OBSERVATION_UID' FROM UPDATED_OBSERVATION_MAP;

CREATE TABLE UPDATED_OBSERVATION_4 AS SELECT DISTINCT 
SOURCE_ACT_UID3 AS OBSERVATION_UID 'OBSERVATION_UID' FROM UPDATED_OBSERVATION_MAP;

CREATE TABLE UPDATED_OBSERVATION_5 AS SELECT DISTINCT 
SOURCE_ACT_UID4 AS OBSERVATION_UID 'OBSERVATION_UID' FROM UPDATED_OBSERVATION_MAP;

CREATE TABLE UPDATED_OBSERVATION_LIST AS 
SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_1
UNION 
SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_2
UNION 
SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_3
UNION 
SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_4
UNION 
SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_5;
CREATE INDEX OBSERVATION_UID ON UPDATED_OBSERVATION_LIST(OBSERVATION_UID);
CREATE TABLE nbs_rdb.UPDATED_LAB_TEST_LIST AS SELECT LAB_TEST_UID, LAB_TEST_KEY FROM nbs_rdb.LAB_TEST WHERE ROOT_ORDERED_TEST_PNTR IN (SELECT OBSERVATION_UID FROM UPDATED_OBSERVATION_LIST);
PROC SQL;
CREATE TABLE rdbdata.UPDATED_OBSERVATION_LIST AS SELECT * FROM UPDATED_OBSERVATION_LIST;
CREATE TABLE rdbdata.S_UPDATED_LAB AS SELECT * FROM S_UPDATED_LAB;
QUIT;
proc sql;
CREATE TABLE rdbdata.ACTIVITY_STATUS 
(
PROCESS VARCHAR,
UPDATE_COUNT NUM);

INSERT INTO rdbdata.ACTIVITY_STATUS ( UPDATE_COUNT, PROCESS) VALUES (0,'LAB100');
UPDATE rdbdata.ACTIVITY_STATUS set UPDATE_COUNT = 
(select count(*) FROM nbs_rdb.LAB100 WHERE RESULTED_LAB_TEST_KEY IN (SELECT LAB_TEST_KEY 
FROM nbs_rdb.UPDATED_LAB_TEST_LIST));
quit;
%DBLOAD (UPDATED_OBSERVATION_LIST, UPDATED_OBSERVATION_LIST);
