%macro PLACE_INVESTIGATION;
PROC SQL;
CREATE TABLE PLACE_TYPE AS SELECT  DISTINCT NBS_QUESTION_UID, PART_TYPE_CD,QUESTION_IDENTIFIER FROM NBS_ODS.NBS_UI_METADATA 
		WHERE PART_TYPE_CD IN ('PlaceAsHangoutOfPHC','PlaceAsSexOfPHC');	
QUIT;
/*DATA PLACE_TYPE;
SET PLACE_TYPE;
IF PART_TYPE_CD ='PlaceAsHangoutOfPHC' THEN PART_TYPE_CD ='PLACE_HANGOUT_OF_PHC';
IF PART_TYPE_CD ='PlaceAsSexOfPHC' THEN PART_TYPE_CD ='PLACE_AS_SEX_OF_PHC';
RUN;
*/
PROC SQL;
CREATE TABLE PLACE_INIT AS
SELECT ANSWER_TXT,ACT_UID AS PAGE_CASE_UID 'PAGE_CASE_UID',ANSWER_GROUP_SEQ_NBR, PLACE_TYPE.* FROM NBS_ODS.NBS_CASE_ANSWER INNER JOIN PLACE_TYPE ON 
PLACE_TYPE.NBS_QUESTION_UID= NBS_CASE_ANSWER.NBS_QUESTION_UID ORDER BY ACT_UID, ANSWER_GROUP_SEQ_NBR;
QUIT;
DATA PLACE_INIT;
SET PLACE_INIT;
a = countc(ANSWER_TXT, '^');

if a <2 then ANSWER_TXT =  COMPRESS(ANSWER_TXT || '^');

RUN;
PROC TRANSPOSE DATA=PLACE_INIT OUT=PLACE_INIT_OUT (drop=_name_ _label_ );
    BY PAGE_CASE_UID ANSWER_GROUP_SEQ_NBR;
	ID PART_TYPE_CD;
	VAR ANSWER_TXT;
RUN;

DATA PLACE_INIT_OUT;
SET PLACE_INIT_OUT;
LENGTH PLACE_HANGOUT_OF_PHC $2000;
LENGTH PLACE_AS_SEX_OF_PHC $2000;
IF missing(PlaceAsHangoutOfPHC) then do; PLACE_HANGOUT_OF_PHC=''; end;
else do; 
PLACE_HANGOUT_OF_PHC=trim(PlaceAsHangoutOfPHC); end;
IF missing(PlaceAsSexOfPHC) then do; PLACE_AS_SEX_OF_PHC=''; end;
else do; 
PLACE_AS_SEX_OF_PHC=trim(PlaceAsSexOfPHC); end;
RUN;

PROC SQL;
CREATE TABLE S_INV_PLACE_REPEATA AS 
SELECT 
PLACE_INIT_OUT.* , D_PLACE.*
FROM PLACE_INIT_OUT INNER JOIN NBS_RDB.D_PLACE
ON D_PLACE.PLACE_LOCATOR_UID= PLACE_INIT_OUT.PLACE_HANGOUT_OF_PHC;
QUIT;
DATA S_INV_PLACE_REPEATA;
SET S_INV_PLACE_REPEATA;
PLACE_AS_SEX_OF_PHC =.;
RUN;
PROC SQL;
CREATE TABLE S_INV_PLACE_REPEATB AS 
SELECT 
PLACE_INIT_OUT.*, D_PLACE.*
FROM PLACE_INIT_OUT INNER JOIN NBS_RDB.D_PLACE
ON D_PLACE.PLACE_LOCATOR_UID= PLACE_INIT_OUT.PLACE_AS_SEX_OF_PHC;
QUIT;
DATA S_INV_PLACE_REPEATB;
SET S_INV_PLACE_REPEATB;
PLACE_HANGOUT_OF_PHC=.;
RUN;
PROC SQL;
CREATE TABLE S_INV_PLACE_REPEAT AS SELECT * FROM S_INV_PLACE_REPEATA
UNION SELECT * FROM S_INV_PLACE_REPEATB;
QUIT;
%DBLOAD (S_INV_PLACE_REPEAT, S_INV_PLACE_REPEAT);
PROC SQL;
DROP TABLE NBS_RDB.L_INV_PLACE_REPEAT;
QUIT;
PROC SQL;
CREATE TABLE L_INV_PLACE_REPEAT AS SELECT DISTINCT PAGE_CASE_UID FROM S_INV_PLACE_REPEAT;
QUIT;
%ASSIGN_KEY (L_INV_PLACE_REPEAT, D_INV_PLACE_REPEAT_KEY);

%DBLOAD (L_INV_PLACE_REPEAT, L_INV_PLACE_REPEAT);
PROC SQL;
CREATE TABLE D_INV_PLACE_REPEAT AS
SELECT A.*, B.D_INV_PLACE_REPEAT_KEY FROM L_INV_PLACE_REPEAT B LEFT OUTER JOIN S_INV_PLACE_REPEAT A
ON A.PAGE_CASE_UID=B.PAGE_CASE_UID;
QUIT;
PROC SQL;
DROP TABLE NBS_RDB.D_INV_PLACE_REPEAT;
QUIT;
%DBLOAD (D_INV_PLACE_REPEAT, D_INV_PLACE_REPEAT);
%MEND PLACE_INVESTIGATION;


%MACRO NO_PLACE_INVESTIGATION; 
PROC SQL;

CREATE TABLE L_INV_PLACE_REPEAT (PAGE_CASE_UID NUM,D_INV_PLACE_REPEAT_KEY NUM );
INSERT INTO L_INV_PLACE_REPEAT( PAGE_CASE_UID, D_INV_PLACE_REPEAT_KEY) VALUES 
(NULL, 1);
PROC SQL;
DROP TABLE NBS_RDB.L_INV_PLACE_REPEAT;
QUIT;

proc sql;
create table nbs_rdb.L_INV_PLACE_REPEAT as select * from L_INV_PLACE_REPEAT;
quit;

proc sql;
DROP TABLE NBS_RDB.D_INV_PLACE_REPEAT;
CREATE TABLE D_INV_PLACE_REPEAT(
	PAGE_CASE_UID NUM ,
	answer_group_seq_nbr NUM ,
	PLACE_HANGOUT_OF_PHC char(2000) ,
	PLACE_AS_SEX_OF_PHC char(2000) ,
	PLACE_KEY NUM ,
	PLACE_ADD_TIME date ,
	PLACE_ADD_USER_ID NUM ,
	PLACE_ADDED_BY char(102) ,
	PLACE_ADDRESS_COMMENTS char(2000) ,
	PLACE_CITY char(100) ,
	PLACE_COUNTRY char(20) ,
	PLACE_COUNTRY_DESC char(50) ,
	PLACE_COUNTY_CODE char(20) ,
	PLACE_COUNTY_DESC char(255) ,
	PLACE_EMAIL char(100) ,
	PLACE_GENERAL_COMMENTS char(1000) ,
	PLACE_LAST_CHANGE_TIME date ,
	PLACE_LAST_CHG_USER_ID NUM ,
	PLACE_LAST_UPDATED_BY char(102) ,
	PLACE_LOCAL_ID char(50) ,
	PLACE_LOCATOR_UID char(30) ,
	PLACE_NAME char(50) ,
	PLACE_PHONE char(20) ,
	PLACE_PHONE_COMMENTS char(2000) ,
	PLACE_PHONE_EXT char(20) ,
	PLACE_POSTAL_UID NUM ,
	PLACE_QUICK_CODE char(100) ,
	PLACE_RECORD_STATUS char(20) ,
	PLACE_RECORD_STATUS_TIME date,
	PLACE_STATE_CODE char(20) ,
	PLACE_STATE_DESC char(50) ,
	PLACE_STATUS_CD char(1) ,
	PLACE_STATUS_TIME date,
	PLACE_STREET_ADDRESS_1 char(100) ,
	PLACE_STREET_ADDRESS_2 char(100) ,
	PLACE_TELE_LOCATOR_UID NUM ,
	PLACE_TELE_TYPE char(14) ,
	PLACE_TELE_USE char(10) ,
	PLACE_TYPE_DESCRIPTION char(25) ,
	PLACE_UID NUM ,
	PLACE_ZIP char(20) ,
	D_INV_PLACE_REPEAT_KEY NUM 
) ;

INSERT INTO D_INV_PLACE_REPEAT(  D_INV_PLACE_REPEAT_KEY) VALUES 
(1);
QUIT;
proc sql;
create table nbs_rdb.D_INV_PLACE_REPEAT as select * from D_INV_PLACE_REPEAT;
quit;

%MEND NO_PLACE_INVESTIGATION;

PROC SQL;
CREATE TABLE DATA_CHECKER_MASTER_LAST 
(COUNT NUM);
INSERT INTO DATA_CHECKER_MASTER_LAST( COUNT) VALUES 
(NULL);
UPDATE DATA_CHECKER_MASTER_LAST SET COUNT= (SELECT COUNT(*)  FROM NBS_ODS.NBS_CASE_ANSWER WHERE
NBS_QUESTION_UID IN (
SELECT NBS_QUESTION_UID  FROM NBS_ODS.NBS_UI_METADATA 
		WHERE PART_TYPE_CD IN ('PlaceAsHangoutOfPHC','PlaceAsSexOfPHC')));
QUIT;
data _null_;
  set DATA_CHECKER_MASTER_LAST;
  	if count>0 then call execute('%PLACE_INVESTIGATION');
	if count=0 then call execute('%NO_PLACE_INVESTIGATION');
run;