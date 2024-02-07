PROC SQL;
DROP TABLE NBS_RDB.LDF_TETANUS;

CREATE TABLE BASE_TETANUS AS 
SELECT * FROM LDF_DIMENSIONAL_DATA WHERE  PHC_CD IN(SELECT CONDITION_CD FROM 
		NBS_RDB.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_TETANUS');

	CREATE TABLE LINKED_TETANUS AS 
	SELECT GEN_LDF.*, 
		INV.INVESTIGATION_KEY, 
		INV.INV_LOCAL_ID AS INVESTIGATION_LOCAL_ID 'INVESTIGATION_LOCAL_ID', 
		INV.CASE_OID AS PROGRAM_JURISDICTION_OID 'PROGRAM_JURISDICTION_OID',
		GEN.PATIENT_KEY,
		PATIENT.PATIENT_LOCAL_ID AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
		CONDITION.CONDITION_SHORT_NM AS DISEASE_NAME 'DISEASE_NAME',
		CONDITION.CONDITION_CD,
		GEN_LDF.PHC_CD 
	FROM
		BASE_TETANUS GEN_LDF
		INNER JOIN  NBS_RDB.INVESTIGATION INV
	ON  
		GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
	INNER JOIN NBS_RDB.GENERIC_CASE GEN
	ON 
		GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
	INNER JOIN NBS_RDB.CONDITION
	ON 
		CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
	INNER JOIN NBS_RDB.D_PATIENT PATIENT 
	ON 
		PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
	ORDER BY 
		INVESTIGATION_UID;

CREATE TABLE ALL_TETANUS AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	NBS_RDB.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_TETANUS A 
	ON A.LDF_UID= B.LDF_UID WHERE
	(B.LDF_PAGE_SET ='OTHER'
	OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
		NBS_RDB.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_TETANUS') 
	)
	ORDER BY 
		INVESTIGATION_UID;
QUIT;
DATA ALL_TETANUS;
SET ALL_TETANUS;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_TETANUS NODUPKEY OUT=ALL_TETANUS; BY INVESTIGATION_KEY DATAMART_COLUMN_NM; RUN;
PROC SQL;
CREATE TABLE ALL_TETANUS_SHORT_COL AS
SELECT * FROM ALL_TETANUS WHERE data_type IN ('CV', 'ST');

CREATE TABLE ALL_TETANUS_TA AS
SELECT * FROM ALL_TETANUS WHERE data_type IN ('LIST_ST');

QUIT;
DATA ALL_TETANUS_TA;
SET ALL_TETANUS_TA;
LENGTH ANSWERCOL $2000;
ANSWERCOL=COL1;
DROP COL1;
RUN;
DATA ALL_TETANUS_SHORT_COL;
SET ALL_TETANUS_SHORT_COL;
LENGTH ANSWERCOL $200;
ANSWERCOL=COL1;
DROP COL1;
RUN;
PROC TRANSPOSE DATA=  ALL_TETANUS_TA  OUT=  TETANUS_TA;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC TRANSPOSE DATA=  ALL_TETANUS_SHORT_COL  OUT=  TETANUS_SHORT_COL;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC SQL;
DELETE * FROM TETANUS_SHORT_COL WHERE _NAME_ IS NULL;
DELETE * FROM TETANUS_TA WHERE _NAME_ IS NULL;
QUIT;
DATA TETANUS;
   MERGE  TETANUS_SHORT_COL TETANUS_TA;
   BY   INVESTIGATION_KEY;
RUN;
DATA TETANUS;
SET TETANUS;
DROP _NAME_;
RUN;
PROC SQL;
DELETE * FROM TETANUS WHERE INVESTIGATION_KEY IS NULL;
QUIT;
PROC SQL;
CREATE TABLE  NBS_RDB.LDF_TETANUS AS SELECT * FROM TETANUS;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_TETANUS
LINKED_TETANUS
ALL_TETANUS
TETANUS;
RUN;
QUIT;
