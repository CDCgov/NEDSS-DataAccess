PROC SQL;
DROP TABLE nbs_rdb.LDF_BMIRD;

CREATE TABLE BASE_BMIRD AS 
SELECT * FROM LDF_DIMENSIONAL_DATA WHERE  PHC_CD IN(SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_BMIRD' );

	CREATE TABLE LINKED_BMIRD AS 
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
		BASE_BMIRD GEN_LDF
		INNER JOIN  nbs_rdb.INVESTIGATION INV
	ON  
		GEN_LDF.INVESTIGATION_UID=INV.CASE_UID 
	INNER JOIN nbs_rdb.BMIRD_CASE GEN
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

CREATE TABLE ALL_BMIRD AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	nbs_rdb.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_BMIRD A 
	ON A.LDF_UID= B.LDF_UID WHERE
	(B.LDF_PAGE_SET ='BMIRD'
	OR B.CONDITION_CD IN (SELECT CONDITION_CD FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_BMIRD') 
	)
	ORDER BY 
		INVESTIGATION_UID;
QUIT;
DATA ALL_BMIRD;
SET ALL_BMIRD;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_BMIRD NODUPKEY OUT=ALL_BMIRD; BY INVESTIGATION_UID DATAMART_COLUMN_NM; RUN;
PROC SQL;
CREATE TABLE ALL_BMIRD_SHORT_COL AS
SELECT * FROM ALL_BMIRD WHERE data_type IN ('CV', 'ST') order by INVESTIGATION_KEY ;

CREATE TABLE ALL_BMIRD_TA AS
SELECT * FROM ALL_BMIRD WHERE data_type IN ('LIST_ST') order by INVESTIGATION_KEY ;

QUIT;
DATA ALL_BMIRD_TA;
SET ALL_BMIRD_TA;
LENGTH ANSWERCOL $2000;
ANSWERCOL=COL1;
DROP COL1;
RUN;
DATA ALL_BMIRD_SHORT_COL;
SET ALL_BMIRD_SHORT_COL;
LENGTH ANSWERCOL $200;
ANSWERCOL=COL1;
DROP COL1;
RUN;
PROC TRANSPOSE DATA=  ALL_BMIRD_TA  OUT=  BMIRD_TA;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC TRANSPOSE DATA=  ALL_BMIRD_SHORT_COL  OUT=  BMIRD_SHORT_COL;
    BY   INVESTIGATION_KEY;
   	COPY INVESTIGATION_KEY INVESTIGATION_LOCAL_ID PROGRAM_JURISDICTION_OID PATIENT_KEY PATIENT_LOCAL_ID DISEASE_NAME DISEASE_CD;
    ID DATAMART_COLUMN_NM;
	VAR ANSWERCOL;
RUN;
PROC SQL;
DELETE * FROM BMIRD_SHORT_COL WHERE _NAME_ IS NULL;
DELETE * FROM BMIRD_TA WHERE _NAME_ IS NULL;
QUIT;
DATA BMIRD;
   MERGE  BMIRD_SHORT_COL BMIRD_TA;
   BY   INVESTIGATION_KEY;
RUN;
DATA BMIRD;
SET BMIRD;
DROP _NAME_;
RUN;
PROC SQL;
DELETE * FROM BMIRD WHERE INVESTIGATION_KEY IS NULL;
QUIT;
PROC SQL;
CREATE TABLE  nbs_rdb.LDF_BMIRD AS SELECT * FROM BMIRD;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_BMIRD
LINKED_BMIRD
ALL_BMIRD
BMIRD;
RUN;
QUIT;
