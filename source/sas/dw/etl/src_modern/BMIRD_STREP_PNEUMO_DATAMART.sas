OPTIONS SORTPGM=BEST;
options compress=yes;
proc sql NOPRINT;
/* CREATE A TABLE WITH DISTINCT INVESTIGATION_KEY AND 11717 & 11720 CONDITION(S) */
CREATE TABLE INVKEYS AS
	SELECT BC.INVESTIGATION_KEY, C.CONDITION_CD
	FROM nbs_rdb.BMIRD_CASE BC INNER JOIN nbs_rdb.CONDITION C ON BC.CONDITION_KEY = C.CONDITION_KEY 
	INNER JOIN nbs_rdb.INVESTIGATION I ON BC.INVESTIGATION_KEY = I.INVESTIGATION_KEY
	WHERE (BC.INVESTIGATION_KEY <> 1) AND (C.CONDITION_CD in ('11723','11717' ,'11720'))
	AND (I.RECORD_STATUS_CD = 'ACTIVE')
	ORDER BY BC.INVESTIGATION_KEY;	
QUIT;

proc sql NOPRINT;
create table BMIRD_PATIENT1 as 
	select  BC.PATIENT_KEY, BC.INVESTIGATION_KEY,
			BC.TYPES_OF_OTHER_INFECTION	AS TYPE_INFECTION_OTHER_SPECIFY,
			BC.BACTERIAL_SPECIES_ISOLATED AS BACTERIAL_SPECIES_ISOLATED,
			BC.BACTERIAL_OTHER_ISOLATED	AS	BACTERIAL_SPECIES_ISOLATED_OTH,
			BC.FIRST_POSITIVE_CULTURE_DT AS FIRST_POSITIVE_CULTURE_DT,
			BC.STERILE_SITE_OTHER AS STERILE_SITE_OTHER,
			BC.INTBODYSITE AS INTERNAL_BODY_SITE,
			BC.OTHNONSTER AS NON_STERILE_SITE_OTHER,
			BC.UNDERLYING_CONDITION_IND	AS	UNDERLYING_CONDITION_IND,
			BC.OTHER_MALIGNANCY	AS OTHER_MALIGNANCY,
			BC.ORGAN_TRANSPLANT	AS ORGAN_TRANSPLANT,
			BC.UNDERLYING_CONDITIONS_OTHER	AS	OTHER_PRIOR_ILLNESS_1,
			BC.OTHILL2 AS OTHER_PRIOR_ILLNESS_2,
			BC.OTHILL3 AS OTHER_PRIOR_ILLNESS_3,
			BC.SAME_PATHOGEN_RECURRENT_IND	AS	SAME_PATHOGEN_RECURRENT,
			BC.CASE_REPORT_STATUS AS CASE_REPORT_STATUS,
			BC.OXACILLIN_INTERPRETATION	AS OXACILLIN_INTERPRETATION,
			BC.PERSISTENT_DISEASE_IND AS PERSISTENT_DISEASE_IND,
			BC.FIRST_ADDITIONAL_SPECIMEN_DT	AS ADD_CULTURE_1_DATE,
			BC.OTH_STREP_PNEUMO1_CULT_SITES	AS ADD_CULTURE_1_OTHER_SITE,
			BC.SECOND_ADDITIONAL_SPECIMEN_DT AS ADD_CULTURE_2_DATE,
			BC.OTH_STREP_PNEUMO2_CULT_SITES	AS	ADD_CULTURE_2_OTHER_SITE,
			BC.PNEUVACC_RECEIVED_IND AS VACCINE_POLYSACCHARIDE,
			BC.PNEUCONJ_RECEIVED_IND AS	VACCINE_CONJUGATE,
			BC.OXACILLIN_ZONE_SIZE AS OXACILLIN_ZONE_SIZE,
			BC.CULTURE_SEROTYPE AS CULTURE_SEROTYPE,                                                                                                                                          
            BC.OTHSEROTYPE AS OTHSEROTYPE,
			C.CONDITION_CD AS DISEASE_CD 'DISEASE_CD',
			C.CONDITION_SHORT_NM AS DISEASE 'DISEASE',
			p.PATIENT_local_id AS PATIENT_LOCAL_ID 'PATIENT_LOCAL_ID',
			P.PATIENT_FIRST_NAME AS PATIENT_FIRST_NAME 'PATIENT_FIRST_NAME',
			P.PATIENT_LAST_NAME AS PATIENT_LAST_NAME 'PATIENT_LAST_NAME',         
		 	P.PATIENT_DOB AS PATIENT_DOB 'PATIENT_DOB',   
			P.PATIENT_CURRENT_SEX AS PATIENT_CURRENT_SEX 'PATIENT_CURRENT_SEX',   
			P.PATIENT_AGE_REPORTED AS AGE_REPORTED 'AGE_REPORTED',
			P.PATIENT_AGE_REPORTED_UNIT AS AGE_REPORTED_UNIT 'AGE_REPORTED_UNIT', 
			P.PATIENT_ETHNICITY AS PATIENT_ETHNICITY  'PATIENT_ETHNICITY',
			P.PATIENT_STREET_ADDRESS_1 AS PATIENT_STREET_ADDRESS_1 'PATIENT_STREET_ADDRESS_1',
		    P.PATIENT_STREET_ADDRESS_2 AS PATIENT_STREET_ADDRESS_2 'PATIENT_STREET_ADDRESS_2',
		    P.PATIENT_CITY AS PATIENT_CITY 'PATIENT_CITY',
		    P.PATIENT_STATE AS PATIENT_STATE 'PATIENT_STATE',
		    P.PATIENT_ZIP AS PATIENT_ZIP 'PATIENT_ZIP',
		    P.PATIENT_COUNTY AS PATIENT_COUNTY 'PATIENT_COUNTY',
			P.PATIENT_RACE_CALCULATED AS RACE_CALCULATED 'RACE_CALCULATED',
			P.PATIENT_RACE_CALC_DETAILS AS  RACE_CALC_DETAILS 'RACE_CALC_DETAILS'
from nbs_rdb.BMIRD_CASE as BC
	left join nbs_rdb.D_PATIENT as P
	on BC.PATIENT_KEY = P.PATIENT_key
	left join nbs_rdb.CONDITION as C
	on C.CONDITION_KEY = BC.CONDITION_KEY
	AND P.PATIENT_KEY ~= 1
	WHERE C.CONDITION_CD in ('11717' , '11723','11720');
QUIT;
data BMIRD_PATIENT1;
SET BMIRD_PATIENT1; 
LENGTH PATIENT_ADDRESS  $100;
IF lengthn(TRIM(PATIENT_STREET_ADDRESS_2))>0 then PATIENT_ADDRESS=TRIM(PATIENT_ADDRESS) ||',' ||TRIM(PATIENT_STREET_ADDRESS_2);
IF lengthn(TRIM(PATIENT_CITY))>0 then PATIENT_ADDRESS=TRIM(PATIENT_ADDRESS) ||',' ||TRIM(PATIENT_CITY);
IF lengthn(TRIM(PATIENT_COUNTY))>0 then PATIENT_ADDRESS=TRIM(PATIENT_ADDRESS) ||',' ||TRIM(PATIENT_COUNTY);
IF lengthn(TRIM(PATIENT_ZIP))>0 then PATIENT_ADDRESS=TRIM(PATIENT_ADDRESS) ||',' ||TRIM(PATIENT_ZIP);
IF lengthn(TRIM(PATIENT_STATE))>0 then PATIENT_ADDRESS=TRIM(PATIENT_ADDRESS) ||',' ||TRIM(PATIENT_STATE);run;
RUN;
proc sql;
create table BMIRD_PAT_ADD_INV as
	select BPA.*,
			i.INV_LOCAL_ID AS INVESTIGATION_LOCAL_ID 'INVESTIGATION_LOCAL_ID',	
			i.EARLIEST_RPT_TO_CNTY_DT 'EARLIEST_RPT_TO_CNTY_DT',	
			i.HSPTLIZD_IND AS HOSPITALIZED 'HOSPITALIZED',
			i.HSPTL_ADMISSION_DT AS HOSPITALIZED_ADMISSION_DATE 'HOSPITALIZED_ADMISSION_DATE',
			i.HSPTL_DISCHARGE_DT AS HOSPITALIZED_DISCHARGE_DATE 'HOSPITALIZED_DISCHARGE_DATE',
			i.HSPTL_DURATION_DAYS AS HOSPITALIZED_DURATION_DAYS 'HOSPITALIZED_DURATION_DAYS',
			i.ILLNESS_ONSET_DT AS ILLNESS_ONSET_DATE 'ILLNESS_ONSET_DATE',
			i.ILLNESS_END_DT AS ILLNESS_END_DATE 'ILLNESS_END_DATE',
			i.DIE_FRM_THIS_ILLNESS_IND AS DIE_FRM_THIS_ILLNESS_IND 'DIE_FRM_THIS_ILLNESS_IND',
			i.INV_CASE_STATUS AS CASE_STATUS 'CASE_STATUS',	
			i.CASE_RPT_MMWR_WK AS MMWR_WEEK 'MMWR_WEEK',	
			i.CASE_RPT_MMWR_YR AS MMWR_YEAR 'MMWR_YEAR',	
			i.CASE_OID AS PROGRAM_JURISDICTION_OID 'PROGRAM_JURISDICTION_OID',
			i.INV_COMMENTS AS GENERAL_COMMENTS 'GENERAL_COMMENTS',
			em.ADD_TIME AS PHC_ADD_TIME 'PHC_ADD_TIME',
			em.LAST_CHG_TIME AS PHC_LAST_CHG_TIME 'PHC_LAST_CHG_TIME',
			i.EARLIEST_RPT_TO_STATE_DT 'EARLIEST_RPT_TO_STATE_DT'
			from work.BMIRD_PATIENT1 as BPA
	left join nbs_rdb.investigation as i
	on BPA.investigation_key=i.investigation_key
	left join nbs_rdb.EVENT_METRIC as em
	on em.event_uid = i.case_uid
	and i.investigation_key ~= 1
	WHERE     (I.RECORD_STATUS_CD NE 'INACTIVE') AND (I.CASE_TYPE NE 'S')
order by BPA.investigation_key;
QUIT; 
/* Retrieve Hospital Name (INV129) */
PROC SQL NOPRINT;
	CREATE TABLE INV129 AS SELECT I.*, O.ORGANIZATION_NAME  AS HOSPITAL_NAME 'HOSPITAL_NAME'
	FROM INVKEYS I INNER JOIN rdbdata.PHC_KEYS P ON I.INVESTIGATION_KEY = P.INVESTIGATION_KEY
	INNER JOIN nbs_rdb.D_ORGANIZATION O ON P.ADT_HSPTL_KEY = O.ORGANIZATION_KEY
	WHERE P.ADT_HSPTL_KEY ~= 1 ORDER BY I.INVESTIGATION_KEY;
QUIT;
DATA BMIRD_PAT_ADD_INV;
	MERGE BMIRD_PAT_ADD_INV INV129;
	BY INVESTIGATION_KEY;
RUN;
proc datasets memtype=DATA;
   DELETE BMIRD_PATIENT1; DELETE LOCATION_PL; DELETE PATIENT_LOCATION;  DELETE INV129; 
run;

/* SHALL calculate Event Date in the data mart by taking the earliest date from INV137,BMD124,INV121,PHC_ADD_TIME*/
data EVENTDATE_CALC(drop = i); 
	set BMIRD_PAT_ADD_INV; 
	array dateTypes(4) PHC_ADD_TIME EARLIEST_RPT_TO_STATE_DT FIRST_POSITIVE_CULTURE_DT ILLNESS_ONSET_DATE;	
	EVENT_DATE=min(of PHC_ADD_TIME EARLIEST_RPT_TO_STATE_DT FIRST_POSITIVE_CULTURE_DT ILLNESS_ONSET_DATE); 
	do i=1 TO DIM(dateTypes);		
		if dateTypes(i) = event_date then EVENT_TYP=VNAME(dateTypes(i)) ;	
	end ;
format EVENT_DATE datetime22.3;
run; 

DATA BMIRD_WITH_EVENT_DATE(DROP=EVENT_TYP);
	SET EVENTDATE_CALC;
	FORMAT EVENT_DATE_TYPE $50.;
	IF EVENT_TYP='EARLIEST_RPT_TO_STATE_DT' THEN EVENT_DATE_TYPE='Earliest Date Reported to State';
	IF EVENT_TYP='FIRST_POSITIVE_CULTURE_DT' THEN EVENT_DATE_TYPE='Date First Positive Culture Obtained';
	IF EVENT_TYP='ILLNESS_ONSET_DATE' THEN EVENT_DATE_TYPE='Illness Onset Date';
	IF EVENT_TYP='PHC_ADD_TIME' THEN EVENT_DATE_TYPE='Investigation Add Date';
RUN;

proc datasets memtype=DATA;
   delete BMIRD_PR_RACE_CONCAT; DELETE BMIRD_PAT_RACE;
run;

DATA BMIRD_WITH_EVENT_DATE;
	SET BMIRD_WITH_EVENT_DATE;
	DROP PATIENT_HISPANIC_IND RACE_KEY PERSON_RACE_AS_OF_DT RACE_DESC RACE_CAT_CD race1 race2 race3 race4 race5 i j;
RUN;

/* step 3
(1) Retrive the BatchEntry Answers from ANTIMICROBIAL table and pivoting to 8 (Max) Columns

Create 2 Tables ANTIMICRO1A (with Only Pencillin) and ANTIMICRO1B (everything except Pencillin) and merge them together 
to Make Pencillin as the first columns (if any) */
proc sql noprint;
CREATE TABLE ANTIMICRO1A AS 
		SELECT 	i.INVESTIGATION_KEY,
				a.ANTIMICROBIAL_AGENT_TESTED_IND AS ANTIMICROBIAL_AGENT_TESTED_ 'ANTIMICROBIAL_AGENT_TESTED_',
				a.SUSCEPTABILITY_METHOD AS SUSCEPTABILITY_METHOD_ 'SUSCEPTABILITY_METHOD_',
				a.S_I_R_U_RESULT AS S_I_R_U_RESULT_ 'S_I_R_U_RESULT_',
				a.MIC_SIGN AS MIC_SIGN_ 'MIC_SIGN_',
				a.MIC_VALUE AS MIC_VALUE_ 'MIC_VALUE_',
				1 as SORT_ORDER
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.ANTIMICROBIAL a ON bc.ANTIMICROBIAL_GRP_KEY = a.ANTIMICROBIAL_GRP_KEY
	WHERE a.ANTIMICROBIAL_GRP_KEY ~= 1 AND a.ANTIMICROBIAL_AGENT_TESTED_IND='PENICILLIN'
	ORDER BY INVESTIGATION_KEY, SORT_ORDER;

CREATE TABLE ANTIMICRO1B AS 
		SELECT 	i.INVESTIGATION_KEY,
				a.ANTIMICROBIAL_AGENT_TESTED_IND AS ANTIMICROBIAL_AGENT_TESTED_ 'ANTIMICROBIAL_AGENT_TESTED_',
				a.SUSCEPTABILITY_METHOD AS SUSCEPTABILITY_METHOD_ 'SUSCEPTABILITY_METHOD_',
				a.S_I_R_U_RESULT AS S_I_R_U_RESULT_ 'S_I_R_U_RESULT_',
				a.MIC_SIGN AS MIC_SIGN_ 'MIC_SIGN_',
				a.MIC_VALUE AS MIC_VALUE_ 'MIC_VALUE_',
				9 AS SORT_ORDER
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.ANTIMICROBIAL a ON bc.ANTIMICROBIAL_GRP_KEY = a.ANTIMICROBIAL_GRP_KEY
	WHERE a.ANTIMICROBIAL_GRP_KEY ~= 1 AND a.ANTIMICROBIAL_AGENT_TESTED_IND <>'PENICILLIN'
	ORDER BY INVESTIGATION_KEY, SORT_ORDER;

quit;
DATA ANTIMICRO1(DROP=SORT_ORDER);
	MERGE ANTIMICRO1B ANTIMICRO1A;
	BY INVESTIGATION_KEY SORT_ORDER;
RUN;

PROC SQL NOPRINT;
	INSERT INTO ANTIMICRO1 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0) 
	VALUES(9999999,'','','','',0);
QUIT;

proc sort data=ANTIMICRO1;
	BY INVESTIGATION_KEY;
RUN;

data ANTIMICRO2;                                     
   set ANTIMICRO1; 
	by INVESTIGATION_KEY;
   if first.INVESTIGATION_KEY then counter=1 ; 
       else counter + 1; 
run;  

proc sort data=ANTIMICRO2;
	BY INVESTIGATION_KEY COUNTER;
RUN;
data ANTIMICRO2_A;
	set ANTIMICRO2;
	if COUNTER > 8 THEN OUTPUT;
run;
data ANTIMICRO2;
	set ANTIMICRO2;
	if COUNTER <= 8 THEN OUTPUT;
run;

PROC TRANSPOSE DATA=ANTIMICRO2 OUT= ANTIMICRO3;
    BY INVESTIGATION_KEY COUNTER;
	VAR ANTIMICROBIAL_AGENT_TESTED_ SUSCEPTABILITY_METHOD_ S_I_R_U_RESULT_ MIC_SIGN_ MIC_VALUE_;
RUN;

data ANTIMICRO3;                                     
   set ANTIMICRO3;                                   
   idvar=cats(_name_,counter);
run;        

proc transpose data=ANTIMICRO3                       
                out=ANTIMICRO4(drop=_name_);         
   var col1;                                   
   id idvar;        
   BY  INVESTIGATION_KEY;
run; 

DATA BMIRD_ANTIMICRO;
	MERGE BMIRD_WITH_EVENT_DATE ANTIMICRO4;
	BY INVESTIGATION_KEY;
RUN;
/* New Requirement:  
Batch Entry Results for Antimicrobial Agent & S/I/R/U Result need to be displayed in a concatinated string when there are greater than 8 batch entries.
*/
DATA ANTIMICRO2_A;
	SET ANTIMICRO2_A;
	DROP COUNTER ANTIMICROBIAL_AGENT_TESTED_ SUSCEPTABILITY_METHOD_ S_I_R_U_RESULT_ MIC_SIGN_ MIC_VALUE_;
	RETAIN INVESTIGATION_KEY CONCAT_COL;
	CONCAT_COL = trim(ANTIMICROBIAL_AGENT_TESTED_) || ': ' || trim(S_I_R_U_RESULT_);
RUN;
proc sort data=ANTIMICRO2_A;
	BY INVESTIGATION_KEY DESCENDING CONCAT_COL;
RUN;

DATA ANTIMICRO2_B(rename=(
			concatCol14=ANTIMIC_GT_8_AGENT_AND_RESULT)
			);
	set ANTIMICRO2_A;
	by	INVESTIGATION_KEY;
	format concatCol1-concatCol13 $50. concatCol14 $500.;
	array concatCol(13) concatCol1-concatCol13;
	retain concatCol1-concatCol14 ' ' i 0;

	if first.INVESTIGATION_KEY then do;
		do j=1 to 13; concatCol(j) = ' ';	end;
		i = 0; concatCol14 = ''; 
		end;
	i+1;
	if i <= 13 then do;
		concatCol(i) = CONCAT_COL;
		concatCol14 =left(trim(CONCAT_COL))|| ', ' || left(trim(concatCol14)) ;
	end;
	if last.INVESTIGATION_KEY then output;
run;

DATA ANTIMICRO2_C(KEEP=INVESTIGATION_KEY ANTIMIC_GT_8_AGENT_AND_RESULT);
	SET ANTIMICRO2_B;
	if ANTIMIC_GT_8_AGENT_AND_RESULT ~=''
		then ANTIMIC_GT_8_AGENT_AND_RESULT  = SUBSTR(ANTIMIC_GT_8_AGENT_AND_RESULT,1,length(trim(ANTIMIC_GT_8_AGENT_AND_RESULT))-1 );
RUN;
DATA BMIRD_ANTIMICRO;
	MERGE BMIRD_ANTIMICRO ANTIMICRO2_C;
	BY INVESTIGATION_KEY;
RUN;
PROC DATASETS MEMTYPE=DATA;
   DELETE ANTIMICRO1;DELETE ANTIMICRO1A;DELETE ANTIMICRO1B;DELETE ANTIMICRO2;DELETE ANTIMICRO3;DELETE ANTIMICRO4;DELETE BMIRD_WITH_EVENT_DATE;DELETE ANTIMICRO2_A;DELETE ANTIMICRO2_B;DELETE ANTIMICRO2_C;
RUN;
/* Step 3
(2) Retrieve Underlying Conditions from BMIRD_MULTI_VALUE_FIELD Table to 8 (Max) Columns
*/
proc sql noprint;
CREATE TABLE BMD127 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.UNDERLYING_CONDITION_NM AS UNDERLYING_CONDITION_ Label = 'UNDERLYING_CONDITION_'
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	WHERE A.UNDERLYING_CONDITION_NM IS NOT NULL
	ORDER BY i.INVESTIGATION_KEY, a.UNDERLYING_CONDITION_NM;
quit;

PROC SQL NOPRINT;
	INSERT INTO BMD127 VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'')  VALUES(9999999,'');
QUIT;

proc sort data=BMD127;
	BY INVESTIGATION_KEY;
RUN;

data BMD127_2;                                     
   set BMD127; 
	by INVESTIGATION_KEY;
   if first.INVESTIGATION_KEY then counter=1 ; 
       else counter + 1; 
run;  

proc sort data=BMD127_2;
	BY INVESTIGATION_KEY COUNTER;
RUN;

data BMD127_2;
	set BMD127_2;
	if COUNTER <= 8 THEN OUTPUT;
run;

PROC TRANSPOSE DATA=BMD127_2 OUT= BMD127_3;
    BY INVESTIGATION_KEY COUNTER;
	VAR UNDERLYING_CONDITION_;
RUN;

data BMD127_3;                                     
   set BMD127_3;                                   
   idvar=cats(_name_,counter);
run;        

proc transpose data=BMD127_3                       
                out=BMD127_4(drop=_name_);         
   var col1;                                   
   id idvar;        
   BY  INVESTIGATION_KEY;
run; 
DATA BMIRD_ANTIMICRO;
	MERGE BMIRD_ANTIMICRO BMD127_4;
	BY INVESTIGATION_KEY;
RUN;

PROC DATASETS MEMTYPE=DATA;
   DELETE BMD127;DELETE BMD127_2;DELETE BMD127_3;DELETE BMD127_4;
RUN;


/* Step 4 */
/* Retrieve BMD125, BMD142 and BMD144 from BMIRD_MULTI_VALUE_FIELD and pivot into 3 columns */
proc sql noprint;
CREATE TABLE DM_BMD125 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.NON_STERILE_SITE AS NON_STERILE_SITE_ Label = 'NON_STERILE_SITE_'
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	WHERE A.NON_STERILE_SITE IS NOT NULL
	ORDER BY i.INVESTIGATION_KEY, 	a.NON_STERILE_SITE;
CREATE TABLE DM_BMD142 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.STREP_PNEUMO_1_CULTURE_SITES AS ADD_CULTURE_1_SITE_ Label="ADD_CULTURE_1_SITE_"
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	WHERE A.STREP_PNEUMO_1_CULTURE_SITES IS NOT NULL
	ORDER BY i.INVESTIGATION_KEY, 	a.STREP_PNEUMO_1_CULTURE_SITES;
CREATE TABLE DM_BMD144 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.STREP_PNEUMO_2_CULTURE_SITES  AS ADD_CULTURE_2_SITE_ Label="ADD_CULTURE_2_SITE_"
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	WHERE A.STREP_PNEUMO_2_CULTURE_SITES IS NOT NULL
	ORDER BY i.INVESTIGATION_KEY, 	a.STREP_PNEUMO_2_CULTURE_SITES;
quit;
DATA DM_BR7;
	MERGE DM_BMD125 DM_BMD142 DM_BMD144;
	BY INVESTIGATION_KEY;
RUN;
PROC SQL NOPRINT;
	INSERT INTO DM_BR7 VALUES(9999999,'','','') VALUES(9999999,'','','') VALUES(9999999,'','','');
QUIT;

data DM_BR7;
	set DM_BR7;
	by INVESTIGATION_KEY;
   if first.INVESTIGATION_KEY then counter=1 ; 
       else counter + 1; 
run;
data DM_BR7;
	set DM_BR7;
	if COUNTER <= 3 THEN OUTPUT;
run;

PROC TRANSPOSE DATA=DM_BR7 OUT= DM_BR7_T;
    BY INVESTIGATION_KEY COUNTER;
	VAR NON_STERILE_SITE_ ADD_CULTURE_1_SITE_ ADD_CULTURE_2_SITE_;
RUN;

data DM_BR7_T;                                     
   set DM_BR7_T;                                   
   idvar=cats(_name_,counter);
run;        

proc transpose data=DM_BR7_T                       
                out=DM_BR7_T(drop=_name_);         
   var col1;                                   
   id idvar;        
   BY  INVESTIGATION_KEY;
run; 

DATA BMIRD_BR7P1;
	MERGE Bmird_antimicro DM_BR7_T;
	BY INVESTIGATION_KEY;
RUN;
PROC DATASETS MEMTYPE=DATA;
   DELETE Bmird_antimicro;DELETE DM_BMD125;DELETE DM_BMD142;DELETE DM_BMD144;DELETE DM_BR7;DELETE DM_BR7_T;
RUN;
/* Step 5 */
/* BMD118 'Types of Infection' pivot to 10 columns*/
proc sql noprint;
CREATE TABLE DM_BMD118 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.TYPES_OF_INFECTIONS AS TYPES_OF_INFECTIONS_ Label = 'TYPES_OF_INFECTIONS_'
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	ORDER BY i.INVESTIGATION_KEY, a.TYPES_OF_INFECTIONS;
quit;
DATA STEP5_1;
	set DM_BMD118;
  	by INVESTIGATION_KEY;
  	IF TYPES_OF_INFECTIONS_='Bacteremia without focus' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_BACTEREMIA'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Pneumonia' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_PNEUMONIA'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Meningitis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_MENINGITIS'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Empyema' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_EMPYEMA'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Cellulitis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_CELLULITIS'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Peritonitis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_PERITONITIS'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Pericarditis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_PERICARDITIS'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Puerperal sepsis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_PUERPERAL_SEP'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='Septic arthritis' then do;TYPES_OF_INFECTIONS_='TYPE_INFECTION_SEP_ARTHRITIS'; _mark_=1;end;
	if TYPES_OF_INFECTIONS_='' then _mark_=1;
run;
DATA STEP5_2(drop=_mark_);
	SET STEP5_1;
	IF _mark_=1 THEN OUTPUT;
RUN;
proc sql noprint;       
	INSERT INTO STEP5_2 VALUES(9999999,'TYPE_INFECTION_BACTEREMIA')  
	VALUES(9999999,'TYPE_INFECTION_PNEUMONIA')  
	VALUES(9999999,'TYPE_INFECTION_MENINGITIS')  
	VALUES(9999999,'TYPE_INFECTION_EMPYEMA')  
	VALUES(9999999,'TYPE_INFECTION_CELLULITIS')  
	VALUES(9999999,'TYPE_INFECTION_PERITONITIS')  
	VALUES(9999999,'TYPE_INFECTION_PERICARDITIS') 
	VALUES(9999999,'TYPE_INFECTION_PUERPERAL_SEP')  
	VALUES(9999999,'TYPE_INFECTION_SEP_ARTHRITIS');
quit;
proc sql noprint;       
	SELECT DISTINCT TYPES_OF_INFECTIONS_ INTO :COLUMNS SEPARATED BY ' ' FROM STEP5_2;
quit;

data STEP5_3(drop=i TYPES_OF_INFECTIONS_ numfound);  
	set STEP5_2;  
	by INVESTIGATION_KEY;                          

	array cols{*} $3 &columns;  
	retain numfound 0 &columns;  
	** reinitialize array to 'No' for first of group;  
	if first.INVESTIGATION_KEY then do;     
		do i = 1 to dim(cols) by 1;        
			cols(i) = 'No';     
		end;     
		numfound = 0;  
	end;                                               
	** check every obs against list of var names in array;  
	do i=1 to dim(cols);     
		if TYPES_OF_INFECTIONS_ = vname(cols(i)) then do;        
			cols(i) = 'Yes';        
			numfound + 1;     
		end;  
	end;                                                        
	** if last of group, then output;  
	if last.INVESTIGATION_KEY then output;
run;                                
DATA STEP5_4(DROP=_MARK_);
	SET STEP5_1;
	IF _MARK_ ~=1 THEN OUTPUT;
RUN;
PROC SORT DATA=STEP5_4;
	BY INVESTIGATION_KEY DESCENDING TYPES_OF_INFECTIONS_;
RUN;

data STEP5_5(rename=(orgnsm14=TYPE_INFECTION_OTHERS_CONCAT));
	set STEP5_4;
	by	INVESTIGATION_KEY;
	format orgnsm1-orgnsm13 $50. orgnsm14 $500.;
	array orgnsm(13) orgnsm1-orgnsm13;
	retain orgnsm1-orgnsm14 ' ' i 0;

	if first.INVESTIGATION_KEY then do;
		do j=1 to 13; orgnsm(j) = ' ';	end;
		i = 0; orgnsm14 = ''; 
		end;
	i+1;
	if i <= 13 then do;
		orgnsm(i) = TYPES_OF_INFECTIONS_;
		orgnsm14 =left(trim(TYPES_OF_INFECTIONS_))||','|| left(trim(orgnsm14)) ;
	end;
	if last.INVESTIGATION_KEY then output;
run;

DATA STEP5_5(KEEP=INVESTIGATION_KEY TYPE_INFECTION_OTHERS_CONCAT);
	SET STEP5_5;
	if TYPE_INFECTION_OTHERS_CONCAT ~=''
		then TYPE_INFECTION_OTHERS_CONCAT  = SUBSTR(TYPE_INFECTION_OTHERS_CONCAT,1,length(trim(TYPE_INFECTION_OTHERS_CONCAT))-1 );
RUN;

DATA STEP5_6;
	MERGE STEP5_3 STEP5_5;
	BY INVESTIGATION_KEY;
RUN;

DATA BMIRD_BR7P2;
	MERGE BMIRD_BR7P1 STEP5_6;
	BY INVESTIGATION_KEY;
RUN;
PROC DATASETS MEMTYPE=DATA;
   DELETE BMIRD_BR7P1;DELETE DM_BMD118;DELETE STEP5_1;DELETE STEP5_2;DELETE STEP5_3;DELETE STEP5_4;DELETE STEP5_5;DELETE STEP5_6;
RUN;
/* Step 6 */
/* BMD122 'Sterile Sites from which Organism Isolated' pivot to 7 columns*/
proc sql noprint;
CREATE TABLE DM_BMD122 AS 
	SELECT 	distinct i.INVESTIGATION_KEY,
	a.STERILE_SITE AS STERILE_SITE_ Label = 'STERILE_SITE_'
	FROM nbs_rdb.BMIRD_CASE bc INNER JOIN
	InvKeys i ON bc.INVESTIGATION_KEY = i.INVESTIGATION_KEY INNER JOIN
	nbs_rdb.BMIRD_MULTI_VALUE_FIELD a ON bc.BMIRD_MULTI_VAL_GRP_KEY = a.BMIRD_MULTI_VAL_GRP_KEY
	ORDER BY i.INVESTIGATION_KEY, a.STERILE_SITE;
quit;
DATA STEP6_1;
	set DM_BMD122;
  	by INVESTIGATION_KEY;
  	IF STERILE_SITE_='Blood' then do;STERILE_SITE_='STERILE_SITE_BLOOD'; _mark_=1;end;
	if STERILE_SITE_='Cerebral Spinal Fluid' then do;STERILE_SITE_='STERILE_SITE_CEREBRAL_SF'; _mark_=1;end;
	if STERILE_SITE_='Pleural Fluid' then do;STERILE_SITE_='STERILE_SITE_PLEURAL_FLUID'; _mark_=1;end;
	if STERILE_SITE_='Peritoneal fluid' then do;STERILE_SITE_='STERILE_SITE_PERITONEAL_FLUID'; _mark_=1;end;
	if STERILE_SITE_='Pericardial Fluid' then do;STERILE_SITE_='STERILE_SITE_PERICARDIAL_FLUID'; _mark_=1;end;
	if STERILE_SITE_='Joint' then do;STERILE_SITE_='STERILE_SITE_JOINT_FLUID'; _mark_=1;end;
	if STERILE_SITE_='' then _mark_=1;
run;
DATA STEP6_2(drop=_mark_);
	SET STEP6_1;
	IF _mark_=1 THEN OUTPUT;
RUN;
proc sql noprint;       
	INSERT INTO STEP6_2 VALUES(9999999,'STERILE_SITE_BLOOD')  
	VALUES(9999999,'STERILE_SITE_CEREBRAL_SF')  
	VALUES(9999999,'STERILE_SITE_PLEURAL_FLUID')  
	VALUES(9999999,'STERILE_SITE_PERITONEAL_FLUID')  
	VALUES(9999999,'STERILE_SITE_PERICARDIAL_FLUID')  
	VALUES(9999999,'STERILE_SITE_JOINT_FLUID');
quit;
proc sql noprint;       
	SELECT DISTINCT STERILE_SITE_ INTO :COLUMNS SEPARATED BY ' ' FROM STEP6_2;
quit;

data STEP6_3(drop=i STERILE_SITE_ numfound);  
	set STEP6_2;  
	by INVESTIGATION_KEY;                          

	array cols{*} $3 &columns;  
	retain numfound 0 &columns;  
	** reinitialize array to 'No' for first of group;  
	if first.INVESTIGATION_KEY then do;     
		do i = 1 to dim(cols) by 1;        
			cols(i) = 'No';     
		end;     
		numfound = 0;  
	end;                                               
	** check every obs against list of var names in array;  
	do i=1 to dim(cols);     
		if STERILE_SITE_ = vname(cols(i)) then do;        
			cols(i) = 'Yes';        
			numfound + 1;     
		end;  
	end;                                                        
	** if last of group, then output;  
	if last.INVESTIGATION_KEY then output;
run;                                
DATA STEP6_4(DROP=_MARK_);
	SET STEP6_1;
	IF _MARK_ ~=1 THEN OUTPUT;
RUN;
PROC SORT DATA=STEP6_4;
	BY INVESTIGATION_KEY DESCENDING STERILE_SITE_;
RUN;

data STEP6_5(rename=(orgnsm14=STERILE_SITE_OTHERS_CONCAT));
	set STEP6_4;
	by	INVESTIGATION_KEY;
	format orgnsm1-orgnsm13 $50. orgnsm14 $500.;
	array orgnsm(13) orgnsm1-orgnsm13;
	retain orgnsm1-orgnsm14 ' ' i 0;

	if first.INVESTIGATION_KEY then do;
		do j=1 to 13; orgnsm(j) = ' ';	end;
		i = 0; orgnsm14 = ''; 
		end;
	i+1;
	if i <= 13 then do;
		orgnsm(i) = STERILE_SITE_;
		orgnsm14 =left(trim(STERILE_SITE_))||','|| left(trim(orgnsm14)) ;
	end;
	if last.INVESTIGATION_KEY then output;
run;

DATA STEP6_5(KEEP=INVESTIGATION_KEY STERILE_SITE_OTHERS_CONCAT);
	SET STEP6_5;
	if STERILE_SITE_OTHERS_CONCAT ~=''
		then STERILE_SITE_OTHERS_CONCAT  = SUBSTR(STERILE_SITE_OTHERS_CONCAT,1,length(trim(STERILE_SITE_OTHERS_CONCAT))-1 );
RUN;

DATA STEP6_6;
	MERGE STEP6_3 STEP6_5;
	BY INVESTIGATION_KEY;
RUN;

DATA BMIRD_STREP_PNEUMO;
	MERGE BMIRD_BR7P2 STEP6_6;
	BY INVESTIGATION_KEY;
	IF INVESTIGATION_KEY ~=9999999 THEN OUTPUT;
RUN;
PROC DATASETS MEMTYPE=DATA;
   DELETE INVKEYS; DELETE BMIRD_BR7P2;DELETE DM_BMD122;DELETE STEP6_1;DELETE STEP6_2;DELETE STEP6_3;DELETE STEP6_4;DELETE STEP6_5;DELETE STEP6_6;
RUN;

PROC SQL;
CREATE TABLE rdbdata.BMIRD_STREP_PNEUMO_DATAMART AS
		SELECT INVESTIGATION_KEY
		      ,PATIENT_LOCAL_ID
		      ,INVESTIGATION_LOCAL_ID
		      ,DISEASE
		      ,DISEASE_CD
		      ,PATIENT_FIRST_NAME
		      ,PATIENT_LAST_NAME
		      ,PATIENT_DOB
		      ,PATIENT_CURRENT_SEX
		      ,AGE_REPORTED
		      ,AGE_REPORTED_UNIT
		      ,PATIENT_STREET_ADDRESS_1
		      ,PATIENT_STREET_ADDRESS_2
		      ,PATIENT_CITY
		      ,PATIENT_STATE
		      ,PATIENT_ZIP
		      ,PATIENT_COUNTY
		      ,PATIENT_ETHNICITY
		      ,RACE_CALCULATED
		      ,RACE_CALC_DETAILS
		      ,EARLIEST_RPT_TO_CNTY_DT
			  ,EARLIEST_RPT_TO_STATE_DT 
		      ,HOSPITALIZED
		      ,HOSPITALIZED_ADMISSION_DATE
		      ,HOSPITALIZED_DISCHARGE_DATE
		      ,HOSPITALIZED_DURATION_DAYS
		      ,HOSPITAL_NAME
		      ,ILLNESS_ONSET_DATE
		      ,ILLNESS_END_DATE
		      ,DIE_FRM_THIS_ILLNESS_IND
		      ,TYPE_INFECTION_BACTEREMIA
		      ,TYPE_INFECTION_PNEUMONIA
		      ,TYPE_INFECTION_MENINGITIS
		      ,TYPE_INFECTION_EMPYEMA
		      ,TYPE_INFECTION_CELLULITIS
		      ,TYPE_INFECTION_PERITONITIS
		      ,TYPE_INFECTION_PERICARDITIS
		      ,TYPE_INFECTION_PUERPERAL_SEP
		      ,TYPE_INFECTION_SEP_ARTHRITIS
		      ,TYPE_INFECTION_OTHERS_CONCAT
		      ,TYPE_INFECTION_OTHER_SPECIFY
		      ,BACTERIAL_SPECIES_ISOLATED
		      ,BACTERIAL_SPECIES_ISOLATED_OTH
		      ,FIRST_POSITIVE_CULTURE_DT
		      ,STERILE_SITE_BLOOD
		      ,STERILE_SITE_CEREBRAL_SF
		      ,STERILE_SITE_PLEURAL_FLUID
		      ,STERILE_SITE_PERITONEAL_FLUID
		      ,STERILE_SITE_PERICARDIAL_FLUID
		      ,STERILE_SITE_JOINT_FLUID
		      ,STERILE_SITE_OTHERS_CONCAT
		      ,STERILE_SITE_OTHER
		      ,INTERNAL_BODY_SITE
		      ,NON_STERILE_SITE_1
		      ,NON_STERILE_SITE_2
		      ,NON_STERILE_SITE_3
		      ,NON_STERILE_SITE_OTHER
		      ,UNDERLYING_CONDITION_IND
		      ,UNDERLYING_CONDITION_1
		      ,UNDERLYING_CONDITION_2
		      ,UNDERLYING_CONDITION_3
		      ,UNDERLYING_CONDITION_4
		      ,UNDERLYING_CONDITION_5
		      ,UNDERLYING_CONDITION_6
		      ,UNDERLYING_CONDITION_7
		      ,UNDERLYING_CONDITION_8
		      ,OTHER_MALIGNANCY
		      ,ORGAN_TRANSPLANT
		      ,OTHER_PRIOR_ILLNESS_1
		      ,OTHER_PRIOR_ILLNESS_2
		      ,OTHER_PRIOR_ILLNESS_3
		      ,CASE_STATUS
		      ,MMWR_WEEK
		      ,MMWR_YEAR
		      ,SAME_PATHOGEN_RECURRENT
		      ,CASE_REPORT_STATUS
		      ,OXACILLIN_INTERPRETATION
			  ,OXACILLIN_ZONE_SIZE
		      ,ANTIMICROBIAL_AGENT_TESTED_1
		      ,SUSCEPTABILITY_METHOD_1
		      ,S_I_R_U_RESULT_1
		      ,MIC_SIGN_1
		      ,MIC_VALUE_1
		      ,ANTIMICROBIAL_AGENT_TESTED_2
		      ,SUSCEPTABILITY_METHOD_2
		      ,S_I_R_U_RESULT_2
		      ,MIC_SIGN_2
		      ,MIC_VALUE_2
		      ,ANTIMICROBIAL_AGENT_TESTED_3
		      ,SUSCEPTABILITY_METHOD_3
		      ,S_I_R_U_RESULT_3
		      ,MIC_SIGN_3
		      ,MIC_VALUE_3
		      ,ANTIMICROBIAL_AGENT_TESTED_4
		      ,SUSCEPTABILITY_METHOD_4
		      ,S_I_R_U_RESULT_4
		      ,MIC_SIGN_4
		      ,MIC_VALUE_4
		      ,ANTIMICROBIAL_AGENT_TESTED_5
		      ,SUSCEPTABILITY_METHOD_5
		      ,S_I_R_U_RESULT_5
		      ,MIC_SIGN_5
		      ,MIC_VALUE_5
		      ,ANTIMICROBIAL_AGENT_TESTED_6
		      ,SUSCEPTABILITY_METHOD_6
		      ,S_I_R_U_RESULT_6
		      ,MIC_SIGN_6
		      ,MIC_VALUE_6
		      ,ANTIMICROBIAL_AGENT_TESTED_7
		      ,SUSCEPTABILITY_METHOD_7
		      ,S_I_R_U_RESULT_7
		      ,MIC_SIGN_7
		      ,MIC_VALUE_7
		      ,ANTIMICROBIAL_AGENT_TESTED_8
		      ,SUSCEPTABILITY_METHOD_8
		      ,S_I_R_U_RESULT_8
		      ,MIC_SIGN_8
		      ,MIC_VALUE_8
			  ,ANTIMIC_GT_8_AGENT_AND_RESULT
		      ,PERSISTENT_DISEASE_IND
		      ,ADD_CULTURE_1_DATE
		      ,ADD_CULTURE_1_SITE_1
		      ,ADD_CULTURE_1_SITE_2
		      ,ADD_CULTURE_1_SITE_3
		      ,ADD_CULTURE_1_OTHER_SITE
		      ,ADD_CULTURE_2_DATE
		      ,ADD_CULTURE_2_SITE_1
		      ,ADD_CULTURE_2_SITE_2
		      ,ADD_CULTURE_2_SITE_3
		      ,ADD_CULTURE_2_OTHER_SITE
		      ,VACCINE_POLYSACCHARIDE
		      ,VACCINE_CONJUGATE
		      ,PROGRAM_JURISDICTION_OID
		      ,GENERAL_COMMENTS
		      ,PHC_ADD_TIME
		      ,PHC_LAST_CHG_TIME
		      ,EVENT_DATE
		      ,EVENT_DATE_TYPE
		      ,CULTURE_SEROTYPE                                                                                                                                                                   
              ,OTHSEROTYPE
		      			  			  
	FROM BMIRD_STREP_PNEUMO
	ORDER BY EVENT_DATE;
QUIT;


%dbload (BMIRD_STREP_PNEUMO_DATAMART, rdbdata.BMIRD_STREP_PNEUMO_DATAMART);

QUIT;

/* DELETES ALL FILES FROM THE WORK FOLDER */
PROC DATASETS LIB=WORK MEMTYPE=DATA
		KILL;
RUN;

QUIT;
