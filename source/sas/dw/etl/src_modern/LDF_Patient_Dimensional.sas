PROC SQL;
CREATE TABLE LDF_PAT_DATA  AS 
	SELECT     
		A.LDF_UID, 
		A.ACTIVE_IND, 
		A.BUSINESS_OBJECT_NM, 
		A.CDC_NATIONAL_ID, 
		A.CLASS_CD, 
		A.CODE_SET_NM, 
		A.CONDITION_CD, 
		A.LABEL_TXT, 
		A.STATE_CD, 
		A.CUSTOM_SUBFORM_METADATA_UID, 
		B.LDF_UID,
		B.BUSINESS_OBJECT_UID, 
		B.LDF_VALUE,
		PAGE_SET.CODE_SHORT_DESC_TXT AS PAGE_SET 'PAGE_SET',
		PERSON.CD AS PHC_CD 'PHC_CD'
	FROM          
		nbs_cdc.STATE_DEFINED_FIELD_METADATA AS A 
	INNER JOIN
		nbs_cdc.STATE_DEFINED_FIELD_DATA AS B 
	ON 
		A.LDF_UID = B.LDF_UID
	INNER JOIN
		NBS_SRT.LDF_PAGE_SET AS PAGE_SET 
	ON 
		PAGE_SET.LDF_PAGE_ID =A.LDF_PAGE_ID
	INNER JOIN
		nbs_cdc.PERSON AS PERSON
	ON 
		PERSON.PERSON_UID=B.BUSINESS_OBJECT_UID	
   /*CODE TO GET PERSON REVISION FOR INV. ONLY*/
   	INNER JOIN
   		nbs_cdc.PARTICIPATION AS PART
    ON
		PART.SUBJECT_ENTITY_UID=PERSON.PERSON_UID
	WHERE 
		A.BUSINESS_OBJECT_NM IN ('PAT')
	AND 
		A.DATA_TYPE 
	IN ('ST', 'CV','LIST_ST')
	AND 
		PART.TYPE_CD='SubjOfPHC';

CREATE TABLE
		LDF_PAT_DATA_WITH_SOURCE AS 
	SELECT 
		CODESET.CLASS_CD AS DATA_SOURCE,* 
	FROM 
		LDF_PAT_DATA 
	LEFT OUTER JOIN  
		NBS_SRT.CODESET CODESET 
	ON 
		LDF_PAT_DATA.CODE_SET_NM=CODESET.CODE_SET_NM
ORDER BY CONDITION_CD;
QUIT;
DATA LDF_PAT_DATA_TRANSLATED;
SET LDF_PAT_DATA_WITH_SOURCE;
	ARRAY LDF_DATA(100) $200 LDF_DATA1-LDF_DATA100;
	DO I=1 TO 100;
		LDF_DATA{I}=SCAN(LDF_VALUE,I,'|');
    END;    
RUN;    
 
PROC SORT 
	DATA=LDF_PAT_DATA_TRANSLATED; 
	BY BUSINESS_OBJECT_UID LDF_UID CODE_SET_NM LABEL_TXT DATA_SOURCE CLASS_CD; 
RUN;
DATA LDF_PAT_DATA_TRANSLATED;
SET LDF_PAT_DATA_TRANSLATED( DROP= LDF_VALUE);
RUN;    
PROC TRANSPOSE DATA=  LDF_PAT_DATA_TRANSLATED OUT=  LDF_PAT_DATA_TRANSLATED_ROWS;
    BY   BUSINESS_OBJECT_UID LDF_UID CODE_SET_NM LABEL_TXT DATA_SOURCE;
    COPY CDC_NATIONAL_ID CODE_SET_NM LABEL_TXT LDF_UID BUSINESS_OBJECT_NM   CONDITION_CD  
                       CUSTOM_SUBFORM_METADATA_UID PAGE_SET  PHC_CD
                       LDF_UID  CLASS_CD;    
		VAR LDF_DATA1 LDF_DATA2 LDF_DATA3 LDF_DATA4 LDF_DATA5 LDF_DATA6 LDF_DATA7 LDF_DATA8 LDF_DATA9 
		LDF_DATA10 LDF_DATA11 LDF_DATA12 LDF_DATA13 LDF_DATA14 LDF_DATA15 LDF_DATA16 LDF_DATA17 LDF_DATA18 
		LDF_DATA19 LDF_DATA20 LDF_DATA21 LDF_DATA22 LDF_DATA23 LDF_DATA24 LDF_DATA25 LDF_DATA26 LDF_DATA27 
		LDF_DATA28 LDF_DATA29 LDF_DATA30 LDF_DATA31 LDF_DATA32 LDF_DATA33 LDF_DATA34 LDF_DATA35 LDF_DATA36 
		LDF_DATA37 LDF_DATA38 LDF_DATA39 LDF_DATA40 LDF_DATA41 LDF_DATA42 LDF_DATA43 LDF_DATA44 LDF_DATA45 
		LDF_DATA46 LDF_DATA47 LDF_DATA48 LDF_DATA49 LDF_DATA50 LDF_DATA51 LDF_DATA52 LDF_DATA53 LDF_DATA54 
		LDF_DATA55 LDF_DATA56 LDF_DATA57 LDF_DATA58 LDF_DATA59 LDF_DATA60 LDF_DATA61 LDF_DATA62 LDF_DATA63 
		LDF_DATA64 LDF_DATA65 LDF_DATA66 LDF_DATA67 LDF_DATA68 LDF_DATA69 LDF_DATA70 LDF_DATA71 LDF_DATA72 
		LDF_DATA73 LDF_DATA74 LDF_DATA75 LDF_DATA76 LDF_DATA77 LDF_DATA78 LDF_DATA79 LDF_DATA80 LDF_DATA81 
		LDF_DATA82 LDF_DATA83 LDF_DATA84 LDF_DATA85 LDF_DATA86 LDF_DATA87 LDF_DATA88 LDF_DATA89 LDF_DATA90 
		LDF_DATA91 LDF_DATA92 LDF_DATA93 LDF_DATA94 LDF_DATA95 LDF_DATA96 LDF_DATA97 LDF_DATA98 LDF_DATA99 
		/*LDF_DATA100 LDF_DATA101 LDF_DATA102 LDF_DATA103 LDF_DATA104 LDF_DATA105 LDF_DATA106 LDF_DATA107 
		LDF_DATA108 LDF_DATA109 LDF_DATA110 LDF_DATA111 LDF_DATA112 LDF_DATA113 LDF_DATA114 LDF_DATA115 
		LDF_DATA116 LDF_DATA117 LDF_DATA118 LDF_DATA119 LDF_DATA120 LDF_DATA121 LDF_DATA122 LDF_DATA123 
		LDF_DATA124 LDF_DATA125 LDF_DATA126 LDF_DATA127 LDF_DATA128 LDF_DATA129 LDF_DATA130 LDF_DATA131 
		LDF_DATA132 LDF_DATA133 LDF_DATA134 LDF_DATA135 LDF_DATA136 LDF_DATA137 LDF_DATA138 LDF_DATA139 
		LDF_DATA140 LDF_DATA141 LDF_DATA142 LDF_DATA143 LDF_DATA144 LDF_DATA145 LDF_DATA146 LDF_DATA147 
		LDF_DATA148 LDF_DATA149 LDF_DATA150 LDF_DATA151 LDF_DATA152 LDF_DATA153 LDF_DATA154 LDF_DATA155 
		LDF_DATA156 LDF_DATA157 LDF_DATA158 LDF_DATA159 LDF_DATA160 LDF_DATA161 LDF_DATA162 LDF_DATA163 
		LDF_DATA164 LDF_DATA165 LDF_DATA166 LDF_DATA167 LDF_DATA168 LDF_DATA169 LDF_DATA170 LDF_DATA171 
		LDF_DATA172 LDF_DATA173 LDF_DATA174 LDF_DATA175 LDF_DATA176 LDF_DATA177 LDF_DATA178 LDF_DATA179 
		LDF_DATA180 LDF_DATA181 LDF_DATA182 LDF_DATA183 LDF_DATA184 LDF_DATA185 LDF_DATA186 LDF_DATA187 
		LDF_DATA188 LDF_DATA189 LDF_DATA190 LDF_DATA191 LDF_DATA192 LDF_DATA193 LDF_DATA194 LDF_DATA195 
		LDF_DATA196 LDF_DATA197 LDF_DATA198 LDF_DATA199 LDF_DATA200 LDF_DATA201 LDF_DATA202 LDF_DATA203 
		LDF_DATA204 LDF_DATA205 LDF_DATA206 LDF_DATA207 LDF_DATA208 LDF_DATA209 LDF_DATA210 LDF_DATA211 
		LDF_DATA212 LDF_DATA213 LDF_DATA214 LDF_DATA215 LDF_DATA216 LDF_DATA217 LDF_DATA218 LDF_DATA219 
		LDF_DATA220 LDF_DATA221 LDF_DATA222 LDF_DATA223 LDF_DATA224 LDF_DATA225 LDF_DATA226 LDF_DATA227 
		LDF_DATA228 LDF_DATA229 LDF_DATA230 LDF_DATA231 LDF_DATA232 LDF_DATA233 LDF_DATA234 LDF_DATA235 
		LDF_DATA236 LDF_DATA237 LDF_DATA238 LDF_DATA239 LDF_DATA240 LDF_DATA241 LDF_DATA242 LDF_DATA243 
		LDF_DATA244 LDF_DATA245 LDF_DATA246 LDF_DATA247 LDF_DATA248 LDF_DATA249 LDF_DATA250*/;
RUN;
PROC SQL;
	CREATE TABLE 
		LDF_PAT_DATA_TRANSLATED_ROWS_NE AS   /*NON EMPTY DATA*/
	SELECT 
		* 
	FROM 
		LDF_PAT_DATA_TRANSLATED_ROWS 
	WHERE 
		PAGE_SET 
	IS NOT NULL;
QUIT;

PROC SQL;
CREATE TABLE 
	LDF_PAT_BASE_CODED_TRANSLATED AS 
	SELECT 	
		COL1,   
		CVG.CODE_DESC_TXT AS CODE_SHORT_DESC_TXT, 
		LDF.CODE_SET_NM,
		BUSINESS_OBJECT_UID, 
		LDF_UID,
		CLASS_CD,
		LABEL_TXT,
		CDC_NATIONAL_ID,
		BUSINESS_OBJECT_NM,
		CONDITION_CD,
		DATA_SOURCE,
		CUSTOM_SUBFORM_METADATA_UID,
		PAGE_SET,
		PHC_CD
	FROM
		LDF_PAT_DATA_TRANSLATED_ROWS_NE LDF
	LEFT JOIN 
		NBS_SRT.CODE_VALUE_GENERAL CVG
	ON
		CVG.CODE_SET_NM=LDF_PAT_DATA_TRANSLATED_ROWS_NE.CODE_SET_NM
	AND 
		CVG.CODE=LDF_PAT_DATA_TRANSLATED_ROWS_NE.COL1
	AND 
		LDF_PAT_DATA_TRANSLATED_ROWS_NE.DATA_SOURCE='code_value_general'
	ORDER BY 
		BUSINESS_OBJECT_UID, LDF_UID, COL1;
QUIT;

DATA LDF_PAT_BASE_CODED_TRANSLATED;
	SET LDF_PAT_BASE_CODED_TRANSLATED;
	IF 
		LENGTHN(CODE_SHORT_DESC_TXT)>0 
	THEN 
		COL1= CODE_SHORT_DESC_TXT;
	ELSE
		COL1= COL1;
RUN;
PROC SQL;
CREATE TABLE LDF_PAT_BASE_CLINICAL_TRANSLATED AS 
	SELECT 	
		COL1, 
		CVG.CODE_DESC_TXT AS CODE_SHORT_DESC_TXT, 
		LDF.CODE_SET_NM,
		BUSINESS_OBJECT_UID, 
		LDF_UID,
		CLASS_CD,
		LDF.CODE_SET_NM,
		LABEL_TXT,
		DATA_SOURCE,
		CDC_NATIONAL_ID,
		BUSINESS_OBJECT_NM,
		CONDITION_CD,
		CUSTOM_SUBFORM_METADATA_UID,
		PAGE_SET,
		PHC_CD
	FROM	
		LDF_PAT_BASE_CODED_TRANSLATED LDF
		LEFT JOIN NBS_SRT.CODE_VALUE_CLINICAL CVG
	ON 
		CVG.CODE_SET_NM=LDF.CODE_SET_NM
	AND
		CVG.CODE=LDF.COL1
	AND 
		LDF.DATA_SOURCE='code_value_clinical'
	ORDER BY BUSINESS_OBJECT_UID, COL1;
QUIT;
DATA LDF_PAT_BASE_CLINICAL_TRANSLATED;
SET LDF_PAT_BASE_CLINICAL_TRANSLATED;
	IF LENGTHN(CODE_SHORT_DESC_TXT)>0 THEN COL1= CODE_SHORT_DESC_TXT;
	ELSE COL1= COL1;
RUN;
PROC SQL;
CREATE TABLE LDF_PAT_BASE_STATE_TRANSLATED AS 
	SELECT 	
		COL1,  
		BUSINESS_OBJECT_UID, 
		CVG.CODE_DESC_TXT AS CODE_SHORT_DESC_TXT, 
		LDF_UID,
		CLASS_CD,
		LDF.CODE_SET_NM,
		LABEL_TXT,
		DATA_SOURCE,
		CDC_NATIONAL_ID,
		BUSINESS_OBJECT_NM,
		CONDITION_CD,
		CUSTOM_SUBFORM_METADATA_UID,
		PAGE_SET,
		PHC_CD
	FROM	
		LDF_PAT_BASE_CLINICAL_TRANSLATED LDF
		LEFT OUTER JOIN NBS_SRT.V_STATE_CODE CVG
	ON 
		CVG.CODE_SET_NM=LDF.CODE_SET_NM
	AND 
		CVG.STATE_CD=LDF.COL1
	AND 
		LDF.DATA_SOURCE 
	IN 
		('STATE_CCD', 'V_STATE_CODE')
	ORDER BY 
		BUSINESS_OBJECT_UID, COL1;
QUIT;
DATA  LDF_PAT_BASE_STATE_TRANSLATED;
SET  LDF_PAT_BASE_STATE_TRANSLATED;
	IF LENGTHN(CODE_SHORT_DESC_TXT)>0 THEN COL1= CODE_SHORT_DESC_TXT;
	ELSE COL1= COL1;
RUN;
PROC SQL;
CREATE TABLE LDF_PAT_BASE_COUNTRY_TRANSLATED AS 
SELECT 
		COL1,  
		BUSINESS_OBJECT_UID, 
		CVG.CODE_DESC_TXT AS CODE_SHORT_DESC_TXT, 
		LDF_UID,
		CLASS_CD,
		LDF.CODE_SET_NM,
		LABEL_TXT,
		DATA_SOURCE,
		CDC_NATIONAL_ID,
		BUSINESS_OBJECT_NM,
		CONDITION_CD,
		CUSTOM_SUBFORM_METADATA_UID,
		PAGE_SET,
		PHC_CD
	FROM	
			LDF_PAT_BASE_STATE_TRANSLATED LDF
	    	LEFT OUTER JOIN NBS_SRT.COUNTRY_CODE CVG
	ON 
			CVG.CODE_SET_NM=LDF.CODE_SET_NM
	AND 
			CVG.CODE=LDF.COL1
	AND 
			LDF.DATA_SOURCE IN ('COUNTRY_CODE')
	ORDER BY 
			BUSINESS_OBJECT_UID, COL1;
QUIT;
DATA LDF_PAT_BASE_COUNTRY_TRANSLATED;
SET LDF_PAT_BASE_COUNTRY_TRANSLATED;
	IF LENGTHN(CODE_SHORT_DESC_TXT)>0 THEN COL1= CODE_SHORT_DESC_TXT;
	ELSE COL1= COL1;
RUN;
PROC SORT DATA= LDF_PAT_BASE_COUNTRY_TRANSLATED; BY BUSINESS_OBJECT_UID LABEL_TXT; 
RUN;
DATA  LDF_PAT_BASE_COUNTRY_TRANSLATED; 
LENGTH X $4000; 
LENGTH COL1 $4000; 
	SET  LDF_PAT_BASE_COUNTRY_TRANSLATED; BY BUSINESS_OBJECT_UID LABEL_TXT; 
	RETAIN X; 
	IF  FIRST.LABEL_TXT THEN X=' '; X=CATX(' | ',X,COL1); 
	IF LAST.LABEL_TXT; 
	IF LENGTHN(X)>0 THEN COL1=X;
RUN; 
PROC SORT DATA= LDF_PAT_BASE_COUNTRY_TRANSLATED; BY LABEL_TXT  CDC_NATIONAL_ID LDF_UID CLASS_CD; 
RUN;

PROC SQL;
	CREATE TABLE LDF AS
		SELECT * FROM LDF_PAT_BASE_COUNTRY_TRANSLATED A,
		nbs_rdb.LDF_DATAMART_COLUMN_REF B
		WHERE A.LDF_UID= B.LDF_UID;
QUIT;
PROC SQL;
	CREATE TABLE LDF_PAT_TRANSLATED_DATA AS
	SELECT 
		COL1,  
		A.BUSINESS_OBJECT_UID, 
		A.CODE_SHORT_DESC_TXT, 
		A.CODE_SET_NM,
		A.DATA_SOURCE,
		A.PHC_CD,
		B.LDF_UID,
		B.CDC_NATIONAL_ID,
		B.BUSINESS_OBJECT_NM,
		B.CONDITION_CD,
		B.CUSTOM_SUBFORM_METADATA_UID,
		B.PAGE_SET,
		B.LDF_UID,
		B.LABEL_TXT,
		LDF_META_DATA.PAGE_SET AS LDF_PAGE_SET 'LDF_PAGE_SET'
	FROM 
		LDF_META_DATA B LEFT OUTER JOIN LDF_PAT_BASE_COUNTRY_TRANSLATED A  
	ON 
		A.LDF_UID= B.LDF_UID
	ORDER BY 
		LABEL_TXT,PAGE_SET, BUSINESS_OBJECT_UID, LDF_UID, CODE_SET_NM,  DATA_SOURCE;

	DELETE * FROM 
		LDF_PAT_TRANSLATED_DATA 
	WHERE 
		LDF_UID IS NULL;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
	LDF_PAT_BASE_CLINICAL_TRANSLATED
	LDF_PAT_BASE_CODED_TRANSLATED
	LDF_PAT_BASE_STATE_TRANSLATED
	LDF_PAT_DATA
	LDF_PAT_DATA_TRANSLATED
	LDF_PAT_DATA_TRANSLATED_ROWS
	LDF_PAT_DATA_TRANSLATED_ROWS_NE
	LDF_PAT_DATA_WITH_SOURCE
	LDF_METADATA_N
	LDF
	LDF_PAT_BASE_COUNTRY_TRANSLATED
	LDF_METADATA
	LDF_META_DATA;
RUN;
QUIT;
PROC SQL;
CREATE TABLE LDF_PAT_DIMENSIONAL_DATA AS
SELECT 
		COL1,  
		BUSINESS_OBJECT_UID AS PERSON_UID 'PERSON_UID', 
		DIM.CODE_SHORT_DESC_TXT, 
		DIM.LDF_UID,
		DIM.CODE_SET_NM,
		DIM.LABEL_TXT,
		DIM.DATA_SOURCE,
		DIM.CDC_NATIONAL_ID,
		DIM.BUSINESS_OBJECT_NM,
		DIM.CONDITION_CD,
		DIM.CUSTOM_SUBFORM_METADATA_UID,
		DIM.PAGE_SET,
		REF.DATAMART_COLUMN_NM AS DATAMART_COLUMN_NM1 'DATAMART_COLUMN_NM1',
		REF2.DATAMART_COLUMN_NM AS DATAMART_COLUMN_NM2 'DATAMART_COLUMN_NM2',
		DIM.PHC_CD
	FROM 
		LDF_PAT_TRANSLATED_DATA DIM 
	LEFT OUTER JOIN
		nbs_rdb.LDF_DATAMART_COLUMN_REF  REF 
	ON 
		DIM.LDF_UID  =REF.LDF_UID
	LEFT OUTER JOIN 
		nbs_rdb.LDF_DATAMART_COLUMN_REF  REF2 
	ON
		DIM.CDC_NATIONAL_ID  =REF2.CDC_NATIONAL_ID
	AND 
		REF2.LDF_UID IS NULL
	AND 
		REF.CDC_NATIONAL_ID IS NULL
	AND 
		REF.BUSINESS_OBJECT_NM='PAT'
ORDER BY BUSINESS_OBJECT_UID;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
	LDF_PAT_TRANSLATED_DATA;
RUN;
QUIT;
DATA LDF_PAT_DIMENSIONAL_DATA;
SET LDF_PAT_DIMENSIONAL_DATA;
  IF LENGTH(DATAMART_COLUMN_NM1)<2 THEN DATAMART_COLUMN_NM=DATAMART_COLUMN_NM2;
  ELSE DATAMART_COLUMN_NM= DATAMART_COLUMN_NM1;
RUN;
PROC SQL;
 DROP TABLE nbs_rdb.LDF_PATIENT;

CREATE TABLE BASE_PATIENT AS 
SELECT * FROM LDF_PAT_DIMENSIONAL_DATA WHERE  PHC_CD=(SELECT ENTITY_DESC FROM 
		nbs_rdb.LDF_DATAMART_TABLE_REF WHERE DATAMART_NAME = 'LDF_Patient');

	CREATE TABLE LINKED_PATIENT AS 
	SELECT PAT_LDF.*, 
		PERSON.PERSON_KEY AS PATIENT_KEY 'PATIENT_KEY' ,
		PERSON.PERSON_LOCAL_ID AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
		PAT_LDF.PHC_CD 
	FROM
		BASE_PATIENT PAT_LDF
		INNER JOIN  nbs_rdb.PERSON PERSON
	ON 
		PERSON.PERSON_UID=PAT_LDF.PERSON_UID
	ORDER BY 
		PERSON_UID;

CREATE TABLE ALL_PATIENT AS 
	SELECT A.*, 
	B.DATAMART_COLUMN_NM AS DM 'DM'
	FROM 	nbs_rdb.LDF_DATAMART_COLUMN_REF  B 
	FULL OUTER JOIN LINKED_PATIENT A 
	ON A.LDF_UID= B.LDF_UID WHERE
	B.BUSINESS_OBJECT_NM='PAT'
ORDER BY 
		PERSON_UID;
QUIT;
DATA ALL_PATIENT;
SET ALL_PATIENT;
	IF  LENGTH(COMPRESS(CONDITION_CD))>1 
		THEN DISEASE_CD= CONDITION_CD;
	ELSE DISEASE_CD= PHC_CD;
	IF  LENGTH(DISEASE_NM)<2 
		THEN DISEASE_NM= PAGE_SET;
	IF  LENGTH(DM)>2 
		THEN DATAMART_COLUMN_NM=DM;
RUN;
PROC SORT DATA=ALL_PATIENT NODUPKEY OUT=ALL_PATIENT; BY PERSON_UID DATAMART_COLUMN_NM; RUN;
PROC TRANSPOSE DATA=  ALL_PATIENT  OUT=  PATIENT;
    BY   PERSON_UID;;
   	COPY  PATIENT_KEY PATIENT_LOCAL_ID ;
    ID DATAMART_COLUMN_NM;
	VAR COL1;
RUN;
PROC SQL;
DELETE * FROM PATIENT WHERE _NAME_ IS NULL;
DELETE * FROM PATIENT WHERE PATIENT_KEY IS NULL;
QUIT;
DATA PATIENT;
SET PATIENT;
DROP _NAME_;
RUN;
PROC SQL;
CREATE TABLE  nbs_rdb.LDF_PATIENT AS SELECT * FROM PATIENT;
QUIT;
PROC DATASETS LIBRARY = WORK NOLIST;
DELETE 
BASE_PATIENT
LINKED_PATIENT
ALL_PATIENT
PATIENT;
RUN;
QUIT;
