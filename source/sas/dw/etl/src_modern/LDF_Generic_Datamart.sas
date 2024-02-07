PROC SQL;
DROP TABLE nbs_rdb.LDF_GENERIC;

CREATE TABLE BASE_GENERIC AS 
SELECT * FROM LDF_DIMENSIONAL_DATA WHERE  PHC_CD IN(SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC');

	CREATE TABLE LINKED_GENERIC AS 
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
		BASE_GENERIC GEN_LDF
		INNER JOIN  nbs_rdb.INVESTIGATION INV
	ON  
		GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
	INNER JOIN nbs_rdb.GENERIC_CASE GEN
	ON 
		GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
	INNER JOIN nbs_rdb.CONDITION
	ON 
		CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
	INNER JOIN nbs_rdb.D_PATIENT PATIENT
	ON 
		PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
	ORDER BY 
		INVESTIGATION_UID;

CREATE TABLE ALL_GENERIC AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	nbs_rdb.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_GENERIC A 
	ON A.LDF_UID= B.LDF_UID WHERE
	(B.LDF_PAGE_SET ='OTHER'
	OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC') 
	)
	ORDER BY 
		INVESTIGATION_UID;
QUIT;
DATA ALL_GENERIC;
SET ALL_GENERIC;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_GENERIC NODUPKEY OUT=ALL_GENERIC; BY INVESTIGATION_KEY DATAMART_COLUMN_NM; RUN;
PROC SQL;
CREATE TABLE ALL_GENERIC_SHORT_COL AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('CV', 'ST');

CREATE TABLE ALL_GENERIC_TA AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('LIST_ST');

QUIT;
DATA ALL_GENERIC_TA;
SET ALL_GENERIC_TA;
LENGTH ANSWERCOL $2000;
ANSWERCOL=COL1;
DROP COL1;
RUN;
DATA ALL_GENERIC_SHORT_COL;
SET ALL_GENERIC_SHORT_COL;
LENGTH ANSWERCOL $200;
ANSWERCOL=COL1;
DROP COL1;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_TA  OUT=  GENERIC_TA;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_SHORT_COL  OUT=  GENERIC_SHORT_COL;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC SQL;
DELETE * FROM GENERIC_SHORT_COL WHERE _NAME_ IS NULL;
DELETE * FROM GENERIC_TA WHERE _NAME_ IS NULL;
QUIT;
DATA GENERIC;
   MERGE  GENERIC_SHORT_COL GENERIC_TA;
   BY   INVESTIGATION_KEY;
RUN;
DATA GENERIC;
SET GENERIC;
DROP _NAME_;
RUN;
PROC SQL;
DELETE * FROM GENERIC WHERE INVESTIGATION_KEY IS NULL;
QUIT;
PROC SQL;
CREATE TABLE  nbs_rdb.LDF_GENERIC AS SELECT * FROM GENERIC;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_GENERIC
LINKED_GENERIC
ALL_GENERIC
GENERIC;
RUN;
QUIT;
PROC SQL;
DROP TABLE nbs_rdb.LDF_GENERIC1;

CREATE TABLE BASE_GENERIC AS 
SELECT * FROM LDF_DIMENSIONAL_DATA WHERE  PHC_CD IN(SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC1');

	CREATE TABLE LINKED_GENERIC AS 
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
		BASE_GENERIC GEN_LDF
		INNER JOIN  nbs_rdb.INVESTIGATION INV
	ON  
		GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
	INNER JOIN nbs_rdb.GENERIC_CASE GEN
	ON 
		GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
	INNER JOIN nbs_rdb.CONDITION
	ON 
		CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
	INNER JOIN nbs_rdb.D_PATIENT PATIENT
	ON 
		PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
	ORDER BY 
		INVESTIGATION_UID;

CREATE TABLE ALL_GENERIC AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	nbs_rdb.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_GENERIC A 
	ON A.LDF_UID= B.LDF_UID WHERE
	(B.LDF_PAGE_SET ='OTHER'
	OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC1') 
	)
	ORDER BY 
		INVESTIGATION_UID;
QUIT;
DATA ALL_GENERIC;
SET ALL_GENERIC;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_GENERIC NODUPKEY OUT=ALL_GENERIC; BY INVESTIGATION_KEY DATAMART_COLUMN_NM; RUN;
PROC SQL;
CREATE TABLE ALL_GENERIC_SHORT_COL AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('CV', 'ST');

CREATE TABLE ALL_GENERIC_TA AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('LIST_ST');

QUIT;
DATA ALL_GENERIC_TA;
SET ALL_GENERIC_TA;
LENGTH ANSWERCOL $2000;
ANSWERCOL=COL1;
DROP COL1;
RUN;
DATA ALL_GENERIC_SHORT_COL;
SET ALL_GENERIC_SHORT_COL;
LENGTH ANSWERCOL $200;
ANSWERCOL=COL1;
DROP COL1;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_TA  OUT=  GENERIC_TA;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_SHORT_COL  OUT=  GENERIC_SHORT_COL;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC SQL;
DELETE * FROM GENERIC_SHORT_COL WHERE _NAME_ IS NULL;
DELETE * FROM GENERIC_TA WHERE _NAME_ IS NULL;
QUIT;
DATA GENERIC;
   MERGE  GENERIC_SHORT_COL GENERIC_TA;
   BY   INVESTIGATION_KEY;
RUN;
DATA GENERIC;
SET GENERIC;
DROP _NAME_;
RUN;
PROC SQL;
DELETE * FROM GENERIC WHERE INVESTIGATION_KEY IS NULL;
QUIT;
PROC SQL;
CREATE TABLE  nbs_rdb.LDF_GENERIC1 AS SELECT * FROM GENERIC;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_GENERIC
LINKED_GENERIC
ALL_GENERIC
GENERIC;
RUN;
QUIT;
PROC SQL;
DROP TABLE nbs_rdb.LDF_GENERIC2;

CREATE TABLE BASE_GENERIC AS 
SELECT * FROM LDF_DIMENSIONAL_DATA WHERE  PHC_CD IN(SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC2');

	CREATE TABLE LINKED_GENERIC AS 
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
		BASE_GENERIC GEN_LDF
		INNER JOIN  nbs_rdb.INVESTIGATION INV
	ON  
		GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
	INNER JOIN nbs_rdb.GENERIC_CASE GEN
	ON 
		GEN.INVESTIGATION_KEY=INV.INVESTIGATION_KEY
	INNER JOIN nbs_rdb.CONDITION
	ON 
		CONDITION.CONDITION_KEY= GEN.CONDITION_KEY
	INNER JOIN nbs_rdb.D_PATIENT PATIENT
	ON 
		PATIENT.PATIENT_KEY=GEN.PATIENT_KEY
	ORDER BY 
		INVESTIGATION_UID;

CREATE TABLE ALL_GENERIC AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	nbs_rdb.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_GENERIC A 
	ON A.LDF_UID= B.LDF_UID WHERE
	(B.LDF_PAGE_SET ='OTHER'
	OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_GENERIC2') 
	)
	ORDER BY 
		INVESTIGATION_UID;
QUIT;
DATA ALL_GENERIC;
SET ALL_GENERIC;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_GENERIC NODUPKEY OUT=ALL_GENERIC; BY INVESTIGATION_KEY DATAMART_COLUMN_NM; RUN;
PROC SQL;
CREATE TABLE ALL_GENERIC_SHORT_COL AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('CV', 'ST');

CREATE TABLE ALL_GENERIC_TA AS
SELECT * FROM ALL_GENERIC WHERE data_type IN ('LIST_ST');

QUIT;
DATA ALL_GENERIC_TA;
SET ALL_GENERIC_TA;
LENGTH ANSWERCOL $2000;
ANSWERCOL=COL1;
DROP COL1;
RUN;
DATA ALL_GENERIC_SHORT_COL;
SET ALL_GENERIC_SHORT_COL;
LENGTH ANSWERCOL $200;
ANSWERCOL=COL1;
DROP COL1;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_TA  OUT=  GENERIC_TA;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC TRANSPOSE DATA=  ALL_GENERIC_SHORT_COL  OUT=  GENERIC_SHORT_COL;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC SQL;
DELETE * FROM GENERIC_SHORT_COL WHERE _NAME_ IS NULL;
DELETE * FROM GENERIC_TA WHERE _NAME_ IS NULL;
QUIT;
DATA GENERIC;
   MERGE  GENERIC_SHORT_COL GENERIC_TA;
   BY   INVESTIGATION_KEY;
RUN;
DATA GENERIC;
SET GENERIC;
DROP _NAME_;
RUN;
PROC SQL;
DELETE * FROM GENERIC WHERE INVESTIGATION_KEY IS NULL;
QUIT;
PROC SQL;
CREATE TABLE  nbs_rdb.LDF_GENERIC2 AS SELECT * FROM GENERIC;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_GENERIC
LINKED_GENERIC
ALL_GENERIC
GENERIC;
RUN;
QUIT;

