 	libname nbs_ods ODBC DSN=nedss1 UID=nbs_ods PASSWORD=ods ACCESS=READONLY;
	libname nbs_rdb ODBC DSN=nbs_rdb UID=nbs_rdb PASSWORD=rdb ACCESS=READONLY;
	libname nbs_srt ODBC DSN=nbs_srt UID=nbs_ods PASSWORD=ods ACCESS=READONLY;
PROC SQL;
CREATE TABLE INVESTIGATION_METRICS AS
SELECT     
			INVESTIGATION.INV_LOCAL_ID, 
			INVESTIGATION.DISEASE_IMPORTED_IND,
			INVESTIGATION.CASE_RPT_MMWR_WK, 
			INVESTIGATION.CASE_RPT_MMWR_YR, 
			INVESTIGATION.OUTBREAK_IND, 
			INVESTIGATION.CASE_TYPE, 
			INVESTIGATION.INV_CASE_STATUS, 
			INVESTIGATION.INVESTIGATION_KEY, 
			INVESTIGATION.CASE_UID, 
			EVENT_METRIC.CONDITION_CD, 
			EVENT_METRIC.CONDITION_DESC_TXT, 
			EVENT_METRIC.ADD_TIME, 
			EVENT_METRIC.LAST_CHG_TIME, 
			EVENT_METRIC.LOCAL_PATIENT_ID, 
			EVENT_METRIC.LOCAL_ID, 
			/*EVENT_METRIC.EVENT_TYPE,*/
			EVENT_METRIC.EVENT_UID, 
			PERSON.AGE_REPORTED, 
			PERSON.AGE_REPORT_UNIT_CD, 
			PERSON.PERSON_DOB, 
			PERSON.PATIENT_HISPANIC_IND, 
			PERSON.PERSON_CURR_GENDER, 
			PERSON.PERSON_KEY,
			CASE_COUNT.CASE_COUNT,
			LOCATION.STATE_FIPS,
			LOCATION.CNTY_FIPS,
			LOCATION.CNTY_SHORT_DESC
FROM        nbs_rdb.INVESTIGATION INNER JOIN
			nbs_rdb.EVENT_METRIC ON INVESTIGATION.INV_LOCAL_ID = EVENT_METRIC.LOCAL_ID INNER JOIN
			nbs_rdb.CASE_COUNT ON INVESTIGATION.INVESTIGATION_KEY = CASE_COUNT.INVESTIGATION_KEY INNER JOIN
			nbs_rdb.PERSON ON CASE_COUNT.PATIENT_KEY = PERSON.PERSON_KEY  INNER JOIN
            nbs_rdb.LOCATION ON CASE_COUNT.CASE_LOCATION_KEY = LOCATION.LOCATION_KEY left outer JOIN
			nbs_rdb.PERSON_RACE ON PERSON.PERSON_KEY = PERSON_RACE.PERSON_KEY left outer JOIN
            nbs_rdb.RACE ON PERSON_RACE.RACE_KEY = RACE.RACE_KEY;
QUIT;
PROC SQL;
CREATE TABLE INV_PERSON_RACE  AS
SELECT     INVESTIGATION_METRICS.*,
			RACE.RACE_CD
FROM        INVESTIGATION_METRICS INVESTIGATION_METRICS LEFT OUTER JOIN
            nbs_rdb.PERSON_RACE ON INVESTIGATION_METRICS.PERSON_KEY = PERSON_RACE.PERSON_KEY LEFT OUTER JOIN
            nbs_rdb.RACE ON PERSON_RACE.RACE_KEY = RACE.RACE_KEY;
QUIT;
PROC SQL;
CREATE TABLE INV_PERSON_COUNTY  AS
SELECT     INV_PERSON_RACE.*,
			STATE_CODE.STATE_NM
FROM        INV_PERSON_RACE INV_PERSON_RACE LEFT OUTER JOIN
            NBS_SRT.STATE_CODE ON INV_PERSON_RACE.STATE_FIPS= STATE_CODE.STATE_CD;
QUIT;


DATA INV_PERSON_COUNTY;
SET INV_PERSON_COUNTY;

/*AGE UNIT CONVERSION*/
IF AGE_REPORT_UNIT_CD = 'Y'      THEN AGE_REPORT_UNIT_CD=0;
ELSE IF AGE_REPORT_UNIT_CD = 'M' THEN AGE_REPORT_UNIT_CD=1;
ELSE IF AGE_REPORT_UNIT_CD = 'W' THEN AGE_REPORT_UNIT_CD = 2;
ELSE IF AGE_REPORT_UNIT_CD= 'D'  THEN AGE_REPORT_UNIT_CD=3;
ELSE IF AGE_REPORT_UNIT_CD= 'H'  THEN AGE_REPORT_UNIT_CD=9;
ELSE IF AGE_REPORT_UNIT_CD = 'U' THEN AGE_REPORT_UNIT_CD = 9;
ELSE IF AGE_REPORT_UNIT_CD = 'Y' THEN AGE_REPORT_UNIT_CD = '';

/*CASE_TYPE CONVERSION */
IF CASE_TYPE = 'I'      THEN CASE_TYPE='M';

/*ETHNICITY CONVERSION */
IF PATIENT_HISPANIC_IND = '2135-2' THEN PATIENT_HISPANIC_IND=1;
ELSE IF PATIENT_HISPANIC_IND = '2186-5' THEN PATIENT_HISPANIC_IND=2;
ELSE PATIENT_HISPANIC_IND =3;

/* IMPORTED IND CONVERSION */
IF DISEASE_IMPORTED_IND = 'Out of country' THEN DISEASE_IMPORTED_IND = 2;
IF DISEASE_IMPORTED_IND = 'Out of state' THEN DISEASE_IMPORTED_IND = 3;
IF DISEASE_IMPORTED_IND = 'Out of jurisdiction' THEN DISEASE_IMPORTED_IND = 9;
IF DISEASE_IMPORTED_IND = 'Unknown' THEN DISEASE_IMPORTED_IND = 9;
IF DISEASE_IMPORTED_IND = 'Indigenous' THEN DISEASE_IMPORTED_IND = 1;

/*RACE_CD CONVERSION */
IF RACE_CD ='1002-5'  THEN RACE_CD = 1;
IF RACE_CD ='2028-9'  THEN RACE_CD = 2;
IF RACE_CD ='2054-5'  THEN RACE_CD = 3;
IF RACE_CD ='2106-3'  THEN RACE_CD = 5;
IF RACE_CD ='2076-8'  THEN RACE_CD = 2;
IF RACE_CD ='U'       THEN RACE_CD = 9;

/* PERSON_CURR_GENDER CONVERSION */
IF PERSON_CURR_GENDER = 'M' THEN PERSON_CURR_GENDER = 1;
IF PERSON_CURR_GENDER = 'F' THEN PERSON_CURR_GENDER = 2;
IF PERSON_CURR_GENDER = 'U' THEN PERSON_CURR_GENDER = 9;

substr(CNTY_FIPS, 1,2)='';
put cnty_fips;
RUN;

PROC SQL;
CREATE TABLE NEDSSSDAT AS
SELECT INV_LOCAL_ID AS CASEID 'CASEID',
DISEASE_IMPORTED_IND AS IMPORTED 'IMPORTED',
CASE_RPT_MMWR_WK AS MMWRWEEK 'MMWRWEEK',
CASE_RPT_MMWR_YR as CASE_RPT_MMWR_YR 'CASE_RPT_MMWR_YR',
OUTBREAK_IND as OUTBRKID 'OUTBRKID',
CASE_TYPE,       
INV_CASE_STATUS AS STATUS 'STATUS',
INVESTIGATION_KEY, 
CASE_UID,
CONDITION_CD AS EVNTCODE 'EVNTCODE',
CONDITION_DESC_TXT AS EVNTNAME 'EVNTNAME',
ADD_TIME AS INITDATE 'INITDATE',
LAST_CHG_TIME AS UPDATED 'UPDATED',
LOCAL_PATIENT_ID, 
LOCAL_ID, 
/*EVENT_TYPE, */
EVENT_UID, 
AGE_REPORTED as AGE 'AGE',        
AGE_REPORT_UNIT_CD as AGETYPE 'AGETYPE',
PERSON_DOB AS BRTHDATE 'BRTHDATE',
PATIENT_HISPANIC_IND AS ETHNCITY 'ETHNCITY',
PERSON_CURR_GENDER AS SEX 'SEX',
PERSON_KEY, 
CASE_COUNT AS CASCOUNT 'CASCOUNT',
STATE_FIPS AS STATEFIP 'STATEFIP',
CNTY_FIPS AS CNTYCODE 'CNTYCODE',
CNTY_SHORT_DESC AS CNTYNAME 'CNTYNAME',
RACE_CD AS RACE 'RACE'
FROM INV_PERSON_COUNTY;
QUIT;	

 DATA SRCDATA (DROP = CASEID SEX RACE AGETYPE YR HOLD NDATE MMWR_WK CDATE OTHER
                      EXT_DATE PROG_DAT UPDATE PROG_DT2) ;
  SET NEDSSSDAT ;                                    
/*LENGTH CASEID1 $6 SEX1 RACE1 AGETYPE1 $1 ;
  
  CASEID1 = CASEID ;
  SEX1 = SEX ;
  RACE1 = RACE ;
*/

  LENGTH CDATE $6 MMWR_WK NDATE MONTH 8 CNTYNAME $25 EVNTNAME $20 YEARGRP 8 ;


* CALCULATE MMWR WEEK NUMBER, MONTH AND YEAR ;

  /*MMWR_WK = ((YEAR - 2000) * 100) + MMWR_WK ;
  CDATE = PUT(MMWR_WK,MMTOWK.) ;
  CDATE=input(MMWR_WK,weeku11.);*/
  NDATE = INPUT(MMWR_WK,YYMMDD6.) ;
  MONTH = MONTH(NDATE) ;
  YR = YEAR(NDATE) ;

  IF YR LT YEAR THEN MONTH = 1 ;
  ELSE IF YR GT YEAR THEN MONTH = 12 ;

* CALCULATE AGE IN YEARS ;

  IF AGETYPE = 1 THEN DO ;        * AGE IS IN MONTH UNITS ;
     AGETYPE = 0 ;
     AGE = INT(AGE / 12) ;
  END;
  ELSE IF AGETYPE = 2 THEN DO ;   * AGE IS IN WEEK UNITS ;
     AGETYPE = 0 ;
     AGE = INT(AGE / 52) ;
  END ;
  ELSE IF AGETYPE = 3 THEN DO ;   * AGE IS IN DAY UNITS ;
     AGETYPE = 0 ;
     AGE = INT(AGE / 365.25) ;
  END ;

  IF RECTYPE = 'S' THEN DO ;      * SUMMARY RECORD AGE VALUE SUBSTITUTION ;
     AGETYPE = 9 ;
     AGE = 999 ;
  END ;

* CREATE YEARGRP VARIABLE ;

  IF AGETYPE = 0 AND AGE = 0 THEN YEARGRP = 1 ;
  ELSE IF AGETYPE = 0 AND  1 GT AGE THEN YEARGRP = 1 ;
  ELSE IF AGETYPE = 0 AND  1 LE AGE LT   5 THEN YEARGRP = 2 ;
  ELSE IF AGETYPE = 0 AND  5 LE AGE LT  10 THEN YEARGRP = 3 ;
  ELSE IF AGETYPE = 0 AND 10 LE AGE LT  15 THEN YEARGRP = 4 ;
  ELSE IF AGETYPE = 0 AND 15 LE AGE LT  20 THEN YEARGRP = 5 ;
  ELSE IF AGETYPE = 0 AND 20 LE AGE LT  25 THEN YEARGRP = 6 ;
  ELSE IF AGETYPE = 0 AND 25 LE AGE LT  30 THEN YEARGRP = 7 ;
  ELSE IF AGETYPE = 0 AND 30 LE AGE LT  40 THEN YEARGRP = 8 ;
  ELSE IF AGETYPE = 0 AND 40 LE AGE LT  50 THEN YEARGRP = 9 ;
  ELSE IF AGETYPE = 0 AND 50 LE AGE LT  60 THEN YEARGRP = 10 ;
  ELSE IF AGETYPE = 0 AND 60 LE AGE LT 999 THEN YEARGRP = 11 ;

IF AGETYPE = 9 THEN YEARGRP = 99 ;

  IF AGE = . THEN DO ;
     AGETYPE = . ;
     YEARGRP = . ;
  END ;

  AGETYPE1 = AGETYPE ;
 RUN ;

* CONVERT TEMPORARY CALCULATION VARIABLES INTO FINAL FORM VARIABLES ;

 DATA TEMP;
  LENGTH RECTYPE   $1
         /*MMWRYEAR  
         MMWRMNTH
         MMWRWEEK*/
         STATEFIP   
         STATABRV  $2
         CNTYCODE   
         CNTYNAME $25
         SITECODE  $3
         CASEID    $6
         EVNTCODE  $5
         EVNTNAME $20
         CASCOUNT   8
         /*BRTHDATE*/ 
         AGE       
         AGETYPE
         SEX
         RACE      
         ETHNCITY  
         OUTBRKID   
         IMPORTED  
         STATUS    $1
         EVNTDATE $10
         DATETYPE  $1
         INITDATE  8
         UPDATED  
         YRGRP      8 ;
  SET SRCDATA ;
/*
  MMWRYEAR = YEAR ;
  MMWRMNTH = MONTH ;
  MMWRWEEK = WEEK ;
  STATEFIP = STATE ;
  STATABRV = STCODE ;
  CNTYCODE = COUNTY ;
  SITECODE = SITE ;
  CASEID   = CASEID1 ;
  EVNTCODE = EVENT ;
  CASCOUNT = COUNT ;
  BRTHDATE = PUT(BIRTHD,MMDDYY10.) ;
  AGETYPE  = AGETYPE1 ;
  SEX      = SEX1 ;
  RACE     = RACE1 ;
  ETHNCITY = HISPANIC ;
  OUTBRKID = OUTBR ;
  IMPORTED = IMPORT ;
  STATUS   = CASSTAT ;
  EVNTDATE = PUT(EVENTD,MMDDYY10.) ;
  DATETYPE = DATET ;
  INITDATE = PUT(INT_DATE,MMDDYY10.) ;
  UPDATED  = PUT(CDCDATE,MMDDYY10.) ;
  YRGRP    = YEARGRP ;
*/
* CONVERT NUMERIC MISSING VALUE TO CHARACTER MISSING VALUE ;

  IF LEFT(SEX)      = '.' THEN SEX      = ' ' ;
  IF LEFT(RACE)     = '.' THEN RACE     = ' ' ;
  IF LEFT(ETHNCITY) = '.' THEN ETHNCITY = ' ' ;
  IF LEFT(OUTBRKID) = '.' THEN OUTBRKID = ' ' ;
  IF LEFT(IMPORTED) = '.' THEN IMPORTED = ' ' ;
  IF LEFT(STATUS)   = '.' THEN STATUS   = ' ' ;
  IF LEFT(BRTHDATE) = '.' THEN BRTHDATE = ' ' ;
  IF LEFT(EVNTDATE) = '.' THEN EVNTDATE = ' ' ;
  IF LEFT(INITDATE) = '.' THEN INITDATE = ' ' ;
  IF LEFT(UPDATED)  = '.' THEN UPDATED  = ' ' ;
 RUN ;

 /************************************************
  * FURTHER PROCESS DATA INTO NNDSS LINK FORM    *
  ************************************************/

 DATA CURRENT (DROP = X Y) ;
  SET TEMP ;

* CREATE FIVE-YEAR AGE GROUP VARIABLE ;

  IF 0 LE AGE LT 1 THEN AGE5GRP = 1 ;
  ELSE IF  1 LE AGE LE  4 THEN AGE5GRP =  2 ;
  ELSE IF  5 LE AGE LE  9 THEN AGE5GRP =  3 ;
  ELSE IF 10 LE AGE LE 14 THEN AGE5GRP =  4 ;
  ELSE IF 15 LE AGE LE 19 THEN AGE5GRP =  5 ;
  ELSE IF 20 LE AGE LE 24 THEN AGE5GRP =  6 ;
  ELSE IF 25 LE AGE LE 29 THEN AGE5GRP =  7 ;
  ELSE IF 30 LE AGE LE 34 THEN AGE5GRP =  8 ;
  ELSE IF 35 LE AGE LE 39 THEN AGE5GRP =  9 ;
  ELSE IF 40 LE AGE LE 44 THEN AGE5GRP = 10 ;
  ELSE IF 45 LE AGE LE 49 THEN AGE5GRP = 11 ;
  ELSE IF 50 LE AGE LE 54 THEN AGE5GRP = 12 ;
  ELSE IF 55 LE AGE LE 59 THEN AGE5GRP = 13 ;
  ELSE IF 60 LE AGE LE 64 THEN AGE5GRP = 14 ;
  ELSE IF 65 LE AGE THEN AGE5GRP = 15 ;
  ELSE AGE5GRP = . ;

* CREATE TEN-YEAR AGE GROUP VARIABLE ;

  IF 0 LE AGE LT 1 THEN AGE10GRP = 1 ;
  ELSE IF  1 LE AGE LE  9 THEN AGE10GRP = 2 ;
  ELSE IF 10 LE AGE LE 19 THEN AGE10GRP = 3 ;
  ELSE IF 20 LE AGE LE 29 THEN AGE10GRP = 4 ;
  ELSE IF 30 LE AGE LE 39 THEN AGE10GRP = 5 ;
  ELSE IF 40 LE AGE LE 49 THEN AGE10GRP = 6 ;
  ELSE IF 50 LE AGE LE 59 THEN AGE10GRP = 7 ;
  ELSE IF 60 LE AGE LE 69 THEN AGE10GRP = 8 ;
  ELSE IF 70 LE AGE THEN AGE10GRP = 9 ;
  ELSE AGE10GRP = . ;

* CONVERT SUMMARY RECORDS TO INDIVIDUAL CASE RECORDS ;

* IF THERE IS A SUMMARY RECORD FOR 10 CASES, THIS PORTION OF THE PROGRAM ;
* WILL CREATE 10 INDIVIDUAL CASE RECORDS, AND DELETE THE SUMMARY RECORD. ;

  IF CASCOUNT GT 1 THEN DO ;
     Y = CASCOUNT ;

     DO X = 1 TO Y ;
        CASCOUNT = 1 ;
        RECTYPE  = 'M' ;
        OUTPUT ;
     END ;
  END ;
  ELSE OUTPUT ;

* CREATE VARIABLE LABELS ;

  LABEL RECTYPE  = 'RECORD TYPE'
        MMWRYEAR = 'MMWR YEAR'
        MMWRMNTH = 'MMWR MONTH'
        MMWRWEEK = 'MMWR WEEK'
        STATEFIP = 'STATE FIPS'
        STATABRV = 'STATE NAME ABBREV.'
        CNTYCODE = 'COUNTY CODE'
        CNTYNAME = 'COUNTY NAME'
        SITECODE = 'SITE CODE'
        CASEID   = 'CASE ID'
        EVNTCODE = 'EVENT CODE'
        EVNTNAME = 'EVENT NAME'
        CASCOUNT = 'CASE COUNT'
        BRTHDATE = 'BIRTH DATE'
        AGE      = 'AGE'
        AGETYPE  = 'AGE TYPE'
        SEX      = 'SEX'
        RACE     = 'RACE'
        ETHNCITY = 'ETHNICITY'
        OUTBRKID = 'OUTBREAK ID'
        IMPORTED = 'IMPORTED'
        STATUS   = 'STATUS'
        EVNTDATE = 'EVENT DATE'
        DATETYPE = 'EVENT DATE TYPE'
        INITDATE = 'INIT. DATE'
        UPDATED  = 'UPDATED'
        YRGRP    = 'FSI AGE GROUP (YRS)'
        AGE5GRP  = '5 YR AGE GROUP'
        AGE10GRP = '10 YR AGE GROUP' ;
 RUN ;

* STORE FINALIZED DATA (OUTPUT DATA) ;
TITLE 'TEMP';
PROC PRINT DATA =TEMP;       
ODS HTML FILE='C:\TEMP.html';
quit;
ODS HTML CLOSE;
ODS LISTING;
