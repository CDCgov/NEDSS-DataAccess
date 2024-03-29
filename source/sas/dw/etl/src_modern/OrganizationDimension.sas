proc sql;
DROP table nbs_rdb.ACTIVITY_LOG_MASTER_LAST_SAS;
quit;
proc sql;
connect to odbc as sql (Datasrc=&datasource.  USER=&username.  PASSWORD=&password.);
EXECUTE (
SELECT * 
INTO ACTIVITY_LOG_MASTER_LAST_SAS
FROM RDB.DBO.ACTIVITY_LOG_MASTER 
WHERE 
ACTIVITY_LOG_MASTER_UID= (SELECT MAX(ACTIVITY_LOG_MASTER_UID) FROM RDB..ACTIVITY_LOG_MASTER );
) by sql;
disconnect from sql; 
Quit;


PROC SQL;
Create table rdbdata.ACTIVITY_LOG_MASTER_LAST as select * from nbs_rdb.ACTIVITY_LOG_MASTER_LAST_SAS;
quit;
PROC SQL;
UPDATE  rdbdata.ACTIVITY_LOG_MASTER_LAST SET 
ACTIVITY_LOG_MASTER_UID = (SELECT MAX(ACTIVITY_LOG_MASTER_UID) FROM nbs_rdb.ACTIVITY_LOG_MASTER where (refresh_ind ='T' OR  refresh_ind is null))
WHERE ACTIVITY_LOG_MASTER_UID>1;
UPDATE  rdbdata.ACTIVITY_LOG_MASTER_LAST SET 
START_DATE= (SELECT MAX(START_DATE) FROM nbs_rdb.ACTIVITY_LOG_MASTER where (refresh_ind ='T' OR  refresh_ind is null));
QUIT;
DATA rdbdata.ACTIVITY_LOG_MASTER_LAST;
SET rdbdata.ACTIVITY_LOG_MASTER_LAST;
IF START_DATE=. then START_DATE='01JUN1900'D;
RUN;

PROC SQL;
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
data ACTIVITY_LOG_DETAIL;
set ACTIVITY_LOG_DETAIL;
ACTIVITY_LOG_DETAIL_UID =MAX(ACTIVITY_LOG_DETAIL_UID,1);
run;

PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM nbs_rdb.ACTIVITY_LOG_DETAIL)+1),
END_DATE =DATETIME(),
START_DATE =DATETIME(),
ACTIVITY_LOG_MASTER_UID= (SELECT ACTIVITY_LOG_MASTER_UID FROM rdbdata.ACTIVITY_LOG_MASTER_LAST);
QUIT;

DATA ACTIVITY_LOG_MASTER_LAST;
SET rdbdata.ACTIVITY_LOG_MASTER_LAST; 
ODSE_COUNT=0;
RDB_COUNT=0;
RUN;
%MACRO ASSIGN_ADDITIONAL_KEY (DS, KEY);
 DATA &DS;
  IF &KEY=1 THEN OUTPUT;
  SET &DS;
	&KEY+1;
	OUTPUT;
 RUN;
%MEND;
DATA rdbdata.ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
RUN;
PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET 
START_DATE=DATETIME();
CREATE TABLE S_INITORGANIZATION_INIT AS 
SELECT 
ORGANIZATION.ORGANIZATION_UID AS ORGANIZATION_UID 'ORGANIZATION_UID',
ORGANIZATION.LOCAL_ID AS ORGANIZATION_LOCAL_ID 'ORGANIZATION_LOCAL_ID',              
ORGANIZATION.DESCRIPTION AS ORGANIZATION_GENERAL_COMMENTS 'ORGANIZATION_GENERAL_COMMENTS',      
ORGANIZATION.ELECTRONIC_IND AS ORGANIZATION_ENTRY_METHOD 'ORGANIZATION_ENTRY_METHOD',
ORGANIZATION.ADD_TIME AS ORGANIZATION_ADD_TIME 'ORGANIZATION_ADD_TIME',
ORGANIZATION.LAST_CHG_TIME AS ORGANIZATION_LAST_CHANGE_TIME 'ORGANIZATION_LAST_CHANGE_TIME',
NAICS_INDUSTRY_CODE.CODE_SHORT_DESC_TXT AS ORGANIZATION_STAND_IND_CLASS 'ORGANIZATION_STAND_IND_CLASS',
ORGANIZATION.RECORD_STATUS_CD AS ORGANIZATION_RECORD_STATUS 'ORGANIZATION_RECORD_STATUS',
ORGANIZATION.ADD_USER_ID,
ORGANIZATION.LAST_CHG_USER_ID
FROM nbs_cdc.ORGANIZATION 
LEFT OUTER JOIN NBS_SRT.NAICS_INDUSTRY_CODE NAICS
ON  NAICS_INDUSTRY_CODE.CODE=ORGANIZATION.STANDARD_INDUSTRY_CLASS_CD 
WHERE ORGANIZATION.LAST_CHG_TIME> (SELECT MAX(ACTIVITY_LOG_MASTER_LAST.START_DATE) FROM  ACTIVITY_LOG_MASTER_LAST);
CREATE TABLE ORGANIZATION_UID_COLL AS SELECT ORGANIZATION_UID  FROM S_INITORGANIZATION_INIT;

CREATE TABLE  S_INITORGANIZATION AS SELECT A.*, 
B.FIRST_NM AS ADD_USER_FIRST_NAME 'ADD_USER_FIRST_NAME', B.LAST_NM AS ADD_USER_LAST_NAME 'ADD_USER_LAST_NAME', 
C.FIRST_NM AS CHG_USER_FIRST_NAME 'CHG_USER_FIRST_NAME', C.LAST_NM AS CHG_USER_LAST_NAME 'CHG_USER_LAST_NAME' 
FROM
S_INITORGANIZATION_INIT A LEFT OUTER JOIN nbs_rdb.USER_PROFILE B
ON A.ADD_USER_ID=B.NEDSS_ENTRY_ID
LEFT OUTER JOIN nbs_rdb.USER_PROFILE C
ON A.ADD_USER_ID=C.NEDSS_ENTRY_ID;
QUIT;

DATA S_INITORGANIZATION;
SET S_INITORGANIZATION;
  	IF ORGANIZATION_RECORD_STATUS = '' THEN ORGANIZATION_RECORD_STATUS = 'ACTIVE';
  	IF ORGANIZATION_RECORD_STATUS = 'SUPERCEDED' THEN ORGANIZATION_RECORD_STATUS = 'INACTIVE' ;
  	IF ORGANIZATION_RECORD_STATUS = 'LOG_DEL' THEN ORGANIZATION_RECORD_STATUS = 'INACTIVE' ;
  	IF LENGTH(COMPRESS(ADD_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(ADD_USER_LAST_NAME))>0 THEN ORGANIZATION_ADDED_BY= TRIM(ADD_USER_LAST_NAME)|| ', ' ||TRIM(ADD_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(ADD_USER_FIRST_NAME))> 0 THEN ORGANIZATION_ADDED_BY= TRIM(ADD_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(ADD_USER_LAST_NAME))> 0 THEN ORGANIZATION_ADDED_BY= TRIM(ADD_USER_LAST_NAME);
	IF LENGTH(COMPRESS(CHG_USER_FIRST_NAME))> 0 AND LENGTHN(COMPRESS(CHG_USER_LAST_NAME))>0 THEN ORGANIZATION_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME)|| ', ' ||TRIM(CHG_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(CHG_USER_FIRST_NAME))> 0 THEN ORGANIZATION_LAST_UPDATED_BY= TRIM(CHG_USER_FIRST_NAME);
	ELSE IF LENGTHN(COMPRESS(CHG_USER_LAST_NAME))> 0 THEN ORGANIZATION_LAST_UPDATED_BY= TRIM(CHG_USER_LAST_NAME);

RUN;

PROC SQL;
CREATE TABLE S_ORGANIZATION_NAME AS SELECT ORGANIZATION_NAME.NM_TXT AS ORGANIZATION_NAME 'ORGANIZATION_NAME',ORGANIZATION_NAME.ORGANIZATION_UID AS  ORGANIZATION_UID ' ORGANIZATION_UID' FROM  ORGANIZATION_UID_COLL INNER JOIN nbs_cdc.ORGANIZATION_NAME
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID=ORGANIZATION_NAME.ORGANIZATION_UID
ORDER BY ORGANIZATION_NAME.ORGANIZATION_UID;
QUIT;
PROC SQL;
CREATE TABLE S_ORGANIZATION_WITH_NM AS SELECT S_INITORGANIZATION.*, S_ORGANIZATION_NAME.*
FROM S_INITORGANIZATION LEFT OUTER JOIN S_ORGANIZATION_NAME
ON S_INITORGANIZATION.ORGANIZATION_UID= S_ORGANIZATION_NAME.ORGANIZATION_UID;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST; DELETE S_INITORGANIZATION_INIT S_INITORGANIZATION S_ORGANIZATION_NAME; RUN; QUIT;
PROC SQL;
CREATE TABLE S_POSTAL_LOCATOR AS
SELECT 
POSTAL_LOCATOR.CITY_DESC_TXT AS ORGANIZATION_CITY 'ORGANIZATION_CITY',             
POSTAL_LOCATOR.CNTRY_CD	AS ORGANIZATION_COUNTRY 'ORGANIZATION_COUNTRY',            
POSTAL_LOCATOR.CNTY_CD	AS ORGANIZATION_COUNTY_CODE 'ORGANIZATION_COUNTY_CODE',              
POSTAL_LOCATOR.STATE_CD	AS ORGANIZATION_STATE_CODE 'ORGANIZATION_STATE_CODE',               
POSTAL_LOCATOR.STREET_ADDR1 AS ORGANIZATION_STREET_ADDRESS_1 'ORGANIZATION_STREET_ADDRESS_1',
POSTAL_LOCATOR.STREET_ADDR2	AS ORGANIZATION_STREET_ADDRESS_2 'ORGANIZATION_STREET_ADDRESS_2',
POSTAL_LOCATOR.ZIP_CD AS ORGANIZATION_ZIP 'ORGANIZATION_ZIP',
STATE_CODE.CODE_DESC_TXT AS ORGANIZATION_STATE_DESC 'ORGANIZATION_STATE_DESC',
STATE_COUNTY_CODE_VALUE.CODE_DESC_TXT AS ORGANIZATION_COUNTY_DESC 'ORGANIZATION_COUNTY_DESC',
COUNTRY_CODE.CODE_SHORT_DESC_TXT AS ORGANIZATION_COUNTRY_DESC 'ORGANIZATION_COUNTRY_DESC',
ENTITY_LOCATOR_PARTICIPATION.LOCATOR_DESC_TXT AS ORGANIZATION_ADDRESS_COMMENTS 'ORGANIZATION_ADDRESS_COMMENTS',
ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
FROM ORGANIZATION_UID_COLL LEFT OUTER JOIN nbs_cdc.ENTITY_LOCATOR_PARTICIPATION
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
LEFT OUTER JOIN nbs_cdc.POSTAL_LOCATOR 
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=POSTAL_LOCATOR.POSTAL_LOCATOR_UID
LEFT OUTER JOIN NBS_SRT.STATE_CODE
ON STATE_CODE.STATE_CD=POSTAL_LOCATOR.STATE_CD
LEFT OUTER JOIN NBS_SRT.COUNTRY_CODE
ON COUNTRY_CODE.CODE=POSTAL_LOCATOR.CNTRY_CD
LEFT OUTER JOIN NBS_SRT.STATE_COUNTY_CODE_VALUE
ON STATE_COUNTY_CODE_VALUE.CODE=POSTAL_LOCATOR.CNTY_CD	
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='WP'
AND ENTITY_LOCATOR_PARTICIPATION.CD='O'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='PST';
QUIT;
DATA S_POSTAL_LOCATOR;
SET S_POSTAL_LOCATOR;
IF LENGTHN(TRIM(ORGANIZATION_STATE_DESC))>1 THEN ORGANIZATION_STATE=ORGANIZATION_STATE_DESC;
IF LENGTHN(TRIM(ORGANIZATION_COUNTY_DESC))>1 THEN ORGANIZATION_COUNTY=ORGANIZATION_COUNTY_DESC;
IF LENGTHN(TRIM(ORGANIZATION_COUNTRY_DESC))>1 THEN ORGANIZATION_COUNTRY=ORGANIZATION_COUNTRY_DESC;
RUN;
PROC SORT DATA=S_POSTAL_LOCATOR NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_TELE_LOCATOR_FAX AS
SELECT DISTINCT
ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
TELE_LOCATOR.PHONE_NBR_TXT AS ORGANIZATION_FAX 'ORGANIZATION_FAX'
FROM ORGANIZATION_UID_COLL INNER JOIN nbs_cdc.ENTITY_LOCATOR_PARTICIPATION
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN nbs_cdc.TELE_LOCATOR 
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='WP'
AND ENTITY_LOCATOR_PARTICIPATION.CD='FAX'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_FAX NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_TELE_LOCATOR_OFFICE AS
SELECT DISTINCT
ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID,
TELE_LOCATOR.EXTENSION_TXT AS ORGANIZATION_PHONE_EXT_WORK 'ORGANIZATION_PHONE_EXT_WORK',        
TELE_LOCATOR.PHONE_NBR_TXT AS ORGANIZATION_PHONE_WORK 'ORGANIZATION_PHONE_WORK', 
TELE_LOCATOR.EMAIL_ADDRESS AS ORGANIZATION_EMAIL 'ORGANIZATION_EMAIL',
ENTITY_LOCATOR_PARTICIPATION.LOCATOR_DESC_TXT AS ORGANIZATION_PHONE_COMMENTS 'ORGANIZATION_PHONE_COMMENTS'
FROM ORGANIZATION_UID_COLL INNER JOIN nbs_cdc.ENTITY_LOCATOR_PARTICIPATION
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID= ENTITY_LOCATOR_PARTICIPATION.ENTITY_UID
INNER JOIN nbs_cdc.TELE_LOCATOR 
ON ENTITY_LOCATOR_PARTICIPATION.LOCATOR_UID=TELE_LOCATOR.TELE_LOCATOR_UID
WHERE ENTITY_LOCATOR_PARTICIPATION.USE_CD='WP'
AND ENTITY_LOCATOR_PARTICIPATION.CD='PH'
AND ENTITY_LOCATOR_PARTICIPATION.CLASS_CD='TELE';
QUIT;
PROC SORT DATA=S_TELE_LOCATOR_OFFICE NODUPKEY; BY ENTITY_UID; RUN;
PROC SQL;
CREATE TABLE S_LOCATOR AS SELECT S_POSTAL_LOCATOR.*,S_TELE_LOCATOR_OFFICE.*, S_TELE_LOCATOR_FAX.*, ORGANIZATION_UID_COLL.ORGANIZATION_UID
FROM ORGANIZATION_UID_COLL LEFT OUTER JOIN  S_TELE_LOCATOR_OFFICE ON
ORGANIZATION_UID_COLL.ORGANIZATION_UID=S_TELE_LOCATOR_OFFICE.ENTITY_UID
LEFT OUTER JOIN  S_POSTAL_LOCATOR ON
ORGANIZATION_UID_COLL.ORGANIZATION_UID=S_POSTAL_LOCATOR.ENTITY_UID
LEFT OUTER JOIN  S_TELE_LOCATOR_FAX ON
ORGANIZATION_UID_COLL.ORGANIZATION_UID=S_TELE_LOCATOR_FAX.ENTITY_UID;
QUIT;
PROC SORT DATA=S_LOCATOR NODUPKEY; BY ORGANIZATION_UID; RUN;
PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_POSTAL_LOCATOR S_TELE_LOCATOR_OFFICE;RUN;QUIT;
PROC SQL;
CREATE TABLE QEC_ENTITY_ID AS SELECT DISTINCT ORGANIZATION_UID, ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD  
FROM ORGANIZATION_UID_COLL LEFT OUTER JOIN nbs_cdc.ENTITY_ID
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID=ENTITY_ID.ENTITY_UID
AND ENTITY_ID.TYPE_CD = 'QEC';
QUIT;
PROC SORT DATA=QEC_ENTITY_ID NODUPKEY; BY ORGANIZATION_UID; RUN;
PROC SQL;
CREATE TABLE FI_ENTITY_ID AS SELECT DISTINCT ORGANIZATION_UID, ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD  
FROM ORGANIZATION_UID_COLL LEFT OUTER JOIN nbs_cdc.ENTITY_ID
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID=ENTITY_ID.ENTITY_UID
AND ENTITY_ID.TYPE_CD = 'FI';
QUIT;
PROC SORT DATA=FI_ENTITY_ID NODUPKEY; BY ORGANIZATION_UID; RUN;
PROC SQL;
CREATE TABLE CLIA_ENTITY_ID AS SELECT DISTINCT ORGANIZATION_UID, ROOT_EXTENSION_TXT, ASSIGNING_AUTHORITY_CD  
FROM ORGANIZATION_UID_COLL LEFT OUTER JOIN nbs_cdc.ENTITY_ID
ON ORGANIZATION_UID_COLL.ORGANIZATION_UID=ENTITY_ID.ENTITY_UID
AND ENTITY_ID.TYPE_CD = 'CLIA';
QUIT;
PROC SORT DATA=CLIA_ENTITY_ID NODUPKEY; BY ORGANIZATION_UID; RUN;
PROC SQL;
CREATE TABLE S_ORGANIZATION_FINAL AS SELECT S_ORGANIZATION_WITH_NM.*, S_LOCATOR.*, QEC_ENTITY_ID.ROOT_EXTENSION_TXT AS ORGANIZATION_QUICK_CODE 'ORGANIZATION_QUICK_CODE', 
FI_ENTITY_ID.ROOT_EXTENSION_TXT AS ORGANIZATION_FACILITY_ID 'ORGANIZATION_FACILITY_ID', FI_ENTITY_ID.ASSIGNING_AUTHORITY_CD AS FI_ENTITY_ID_CD 'FI_ENTITY_ID_CD'
FROM S_ORGANIZATION_WITH_NM LEFT OUTER JOIN  S_LOCATOR 
ON S_ORGANIZATION_WITH_NM.ORGANIZATION_UID=S_LOCATOR.ORGANIZATION_UID
LEFT OUTER JOIN QEC_ENTITY_ID
ON  S_ORGANIZATION_WITH_NM.ORGANIZATION_UID= QEC_ENTITY_ID.ORGANIZATION_UID
LEFT OUTER JOIN FI_ENTITY_ID
ON  S_ORGANIZATION_WITH_NM.ORGANIZATION_UID= FI_ENTITY_ID.ORGANIZATION_UID;
QUIT;
DATA S_ORGANIZATION_FINAL;
SET S_ORGANIZATION_FINAL;
ORGANIZATION_FACILITY_ID_AUTH=PUT(FI_ENTITY_ID_CD,$ORD107F.);
DROP CLIA_ENTITY_ID_CD FI_ENTITY_ID_CD;
RUN;
PROC SORT DATA=S_ORGANIZATION_FINAL NODUPKEY; BY ORGANIZATION_UID; RUN;
%DBLOAD (S_ORGANIZATION, S_ORGANIZATION_FINAL);
PROC DATASETS LIBRARY = WORK NOLIST;DELETE CLIA_ENTITY_ID FI_ENTITY_ID S_ORGANIZATION_WITH_NM S_LOCATOR QEC_ENTITY_ID ORGANIZATION_UID_COLL PRN_ENTITY_ID;RUN;QUIT;
PROC SQL;
CREATE TABLE L_ORGANIZATION_N AS
	SELECT DISTINCT S_ORGANIZATION.ORGANIZATION_UID  FROM nbs_rdb.S_ORGANIZATION
	EXCEPT SELECT L_ORGANIZATION.ORGANIZATION_UID FROM  nbs_rdb.L_ORGANIZATION;
CREATE TABLE L_ORGANIZATION_E AS
	SELECT S_ORGANIZATION.ORGANIZATION_UID, L_ORGANIZATION.ORGANIZATION_KEY
		FROM nbs_rdb.S_ORGANIZATION, nbs_rdb.L_ORGANIZATION
WHERE S_ORGANIZATION.ORGANIZATION_UID= L_ORGANIZATION.ORGANIZATION_UID;
ALTER TABLE L_ORGANIZATION_N ADD ORGANIZATION_KEY_MAX_VAL NUMERIC;
UPDATE L_ORGANIZATION_N SET ORGANIZATION_KEY_MAX_VAL=(SELECT MAX(ORGANIZATION_KEY) FROM nbs_rdb.L_ORGANIZATION);
QUIT;
%ASSIGN_ADDITIONAL_KEY (L_ORGANIZATION_N, ORGANIZATION_KEY);
PROC SORT DATA=L_ORGANIZATION_N NODUPKEY; BY ORGANIZATION_KEY; RUN;
DATA L_ORGANIZATION_N;
SET L_ORGANIZATION_N;
IF ORGANIZATION_KEY_MAX_VAL  ~=. THEN ORGANIZATION_KEY= ORGANIZATION_KEY+ORGANIZATION_KEY_MAX_VAL;
IF ORGANIZATION_KEY_MAX_VAL  =. THEN ORGANIZATION_KEY= ORGANIZATION_KEY+1;
DROP ORGANIZATION_KEY_MAX_VAL;
RUN;
%DBLOAD (L_ORGANIZATION, L_ORGANIZATION_N);
PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET SOURCE_ROW_COUNT=(SELECT COUNT(*) FROM S_ORGANIZATION_FINAL),
END_DATE=DATETIME(),
DESTINATION_ROW_COUNT=(SELECT COUNT(*) FROM nbs_rdb.S_ORGANIZATION),
ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM nbs_rdb.ACTIVITY_LOG_DETAIL)+1),
ROW_COUNT_INSERT=(SELECT COUNT(*) FROM L_ORGANIZATION_N),
ROW_COUNT_UPDATE=(SELECT COUNT(*) FROM L_ORGANIZATION_E),
PROCESS_UID= (SELECT PROCESS_UID FROM nbs_rdb.ETL_PROCESS WHERE PROCESS_NAME='S_ORGANIZATION');
QUIT;
DATA ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
IF ROW_COUNT_UPDATE<0 THEN ROW_COUNT_UPDATE=0;
ADMIN_COMMENT=COMPRESS(ROW_COUNT_INSERT) || ' RECORD(S) INSERTED AND ' ||COMPRESS(ROW_COUNT_UPDATE) || ' RECORD(S) UPDATED IN S_ORGANIZATION TABLE.'||
' THERE IS(ARE) NOW '|| COMPRESS(DESTINATION_ROW_COUNT) || ' TOTAL NUMBER OF RECORD(S) IN THE S_ORGANIZATION TABLE.';
RUN;
%DBLOAD (ACTIVITY_LOG_DETAIL, ACTIVITY_LOG_DETAIL);

PROC SQL;
CREATE TABLE D_ORGANIZATION_N AS 
	SELECT * FROM nbs_rdb.S_ORGANIZATION , L_ORGANIZATION_N
WHERE S_ORGANIZATION.ORGANIZATION_UID=L_ORGANIZATION_N.ORGANIZATION_UID;
CREATE TABLE D_ORGANIZATION_E AS 
	SELECT * FROM nbs_rdb.S_ORGANIZATION , L_ORGANIZATION_E
WHERE S_ORGANIZATION.ORGANIZATION_UID=L_ORGANIZATION_E.ORGANIZATION_UID;
QUIT;
PROC SORT DATA=D_ORGANIZATION_N NODUPKEY; BY ORGANIZATION_KEY;RUN;
DATA nbs_rdb.D_ORGANIZATION;
 MODIFY nbs_rdb.D_ORGANIZATION D_ORGANIZATION_E;
 BY ORGANIZATION_KEY;
RUN;
%DBLOAD (D_ORGANIZATION, D_ORGANIZATION_N);
PROC SQL;
UPDATE ACTIVITY_LOG_DETAIL SET SOURCE_ROW_COUNT=(SELECT COUNT(*) FROM nbs_rdb.S_ORGANIZATION),
END_DATE=DATETIME(),
DESTINATION_ROW_COUNT=(SELECT COUNT(*) FROM nbs_rdb.D_ORGANIZATION),
ACTIVITY_LOG_DETAIL_UID= ((SELECT MAX(ACTIVITY_LOG_DETAIL_UID) FROM nbs_rdb.ACTIVITY_LOG_DETAIL)+1),
ROW_COUNT_INSERT=(SELECT COUNT(*) FROM D_ORGANIZATION_N),
ROW_COUNT_UPDATE=(SELECT COUNT(*) FROM D_ORGANIZATION_E),
PROCESS_UID= (SELECT PROCESS_UID FROM nbs_rdb.ETL_PROCESS WHERE PROCESS_NAME='D_ORGANIZATION');
QUIT;
DATA ACTIVITY_LOG_DETAIL;
SET ACTIVITY_LOG_DETAIL;
ACTIVITY_LOG_DETAIL_UID= ACTIVITY_LOG_DETAIL_UID +1;
ADMIN_COMMENT=COMPRESS(ROW_COUNT_INSERT) || ' RECORD(S) INSERTED AND ' ||COMPRESS(ROW_COUNT_UPDATE) || ' RECORD(S) UPDATED IN D_ORGANIZATION TABLE.'||
' THERE ARE NOW '|| COMPRESS(DESTINATION_ROW_COUNT) || ' TOTAL NUMBER OF RECORD(S) IN THE D_ORGANIZATION TABLE.';
RUN;
PROC DATASETS LIBRARY = WORK NOLIST;DELETE S_ORGANIZATION_FINAL L_ORGANIZATION_E L_ORGANIZATION_N D_ORGANIZATION_E D_ORGANIZATION_N ;RUN;QUIT;
%DBLOAD (ACTIVITY_LOG_DETAIL, ACTIVITY_LOG_DETAIL);

