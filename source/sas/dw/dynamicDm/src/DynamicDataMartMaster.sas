/**
Author: Pradeep Kumar Sharma
Company : CSRA
Year : 2018
NBS Version: 5.4
Description: This is the root sas class to create the dynamic datamart for page builder pages.
This process not only create dynamic datamarts, but also drop orphan datamarts 

*/

/*calling the existing macro etllib*/
%etllib;
/*formatting*/
OPTIONS COMPRESS=YES;
options fmtsearch=(nbsfmt);
/*Creates a global variable assigned to 0*/
%global etlerr;
%let etlerr=0; 
/*Include this file in order to be able to call the macros. We need to run the code from Autoexec*/
%include dyndmpgm(DynamicDataMacro.sas);
OPTIONS NOCARDIMAGE;
/***TBD start*/
/*Only shows if key is not null*/
%MACRO ASSIGN_KEY (DS, KEY);
 DATA &DS;
  IF &KEY=1 THEN OUTPUT;
  SET &DS;  
	&KEY+1;
	OUTPUT;     
 RUN; 
 PROC SORT DATA=&DS NODUPKEY; BY &KEY;RUN;
%MEND ASSIGN_KEY;
/*loading the data in the database*/
PROC DATASETS LIB=WORK MEMTYPE=DATA 
		KILL; 
RUN; 
QUIT; 

PROC SQL;
  DROP TABLE NBS_RDB.INIT;
QUIT;

PROC SQL;
/*It creates a table with 2 columns: form_cd and the datamart_nm associated to the investigation page, where there's a datamart name defined and a condition associated to the investigation.*/
CREATE TABLE NBS_RDB.INIT AS
	SELECT  NBS_PAGE.FORM_CD, NBS_PAGE.DATAMART_NM FROM NBS_ODS.PAGE_COND_MAPPING INNER JOIN NBS_ODS.NBS_PAGE
	ON PAGE_COND_MAPPING.WA_TEMPLATE_UID = NBS_PAGE.WA_TEMPLATE_UID
WHERE DATAMART_NM IS NOT NULL AND CONDITION_CD IS NOT NULL;
QUIT;

PROC SQL;
  DROP TABLE NBS_RDB.NBS_PAGE;
QUIT;

PROC SQL;
/*It creates a table with 2 columns: form_cd and the datamart_nm associated to the investigation page, where there's a datamart name defined and a condition associated to the investigation.*/
CREATE TABLE NBS_RDB.NBS_PAGE AS
	SELECT  DISTINCT NBS_PAGE.FORM_CD, NBS_PAGE.DATAMART_NM FROM NBS_ODS.PAGE_COND_MAPPING INNER JOIN NBS_ODS.NBS_PAGE
	ON PAGE_COND_MAPPING.WA_TEMPLATE_UID = NBS_PAGE.WA_TEMPLATE_UID
WHERE DATAMART_NM IS NOT NULL AND CONDITION_CD IS NOT NULL;
QUIT;


PROC SQL;
/*it creates a table with 2 columns: investigation_form_cd and the datamart_nm associated to the investigation page where the rdb_table_nm is investigation*/
CREATE TABLE INIT_FORM_SET AS
SELECT INIT.FORM_CD  AS INVESTIGATION_FORM_CD 'INVESTIGATION_FORM_CD', INIT.DATAMART_NM  FROM NBS_RDB.INIT INNER JOIN NBS_ODS.NBS_UI_METADATA
ON NBS_UI_METADATA.INVESTIGATION_FORM_CD = INIT.FORM_CD
INNER JOIN NBS_ODS.NBS_RDB_METADATA
ON NBS_UI_METADATA.NBS_UI_METADATA_UID = NBS_RDB_METADATA.NBS_UI_METADATA_UID
WHERE RDB_TABLE_NM='INVESTIGATION' ORDER BY INIT.FORM_CD,  NBS_RDB_METADATA.RDB_COLUMN_NM;
QUIT;
/*Sorting the data with no duplicates and order by investigation_form_cd*/
PROC SORT DATA= INIT_FORM_SET NODUPKEY; BY INVESTIGATION_FORM_CD;RUN;
/*SAS needs to assign keys this way*/
%ASSIGN_KEY (INIT_FORM_SET, SORT_KEY);
/*DATA _NULL_;
set INIT_FORM_SET;
   	RDB_TABLE_NM='D_INV_ADMINISTRATIVE';
   	DIMENSION= TRIM("'")||TRIM('D_INV_ADMINISTRATIVE')||TRIM("'") ;
  	INVESTIGATION_FORM_CODE = TRIM("'")||TRIM(FORM_CD)||TRIM("'") ;
 		

  DIM_KEY = 	TRIM('D_INV_ADMINISTRATIVE')||TRIM('_KEY');
  IF COUNTSTD>0 then call execute('%MANAGE_D_INV('||RDB_TABLE_NM||','|| DIMENSION||','|| DIM_KEY||','|| 'F_STD_PAGE_CASE'||','|| INVESTIGATION_FORM_CODE||')');
  IF COUNTSTD=0 then call execute('%MANAGE_D_INV('||RDB_TABLE_NM||','|| DIMENSION||','|| DIM_KEY||','|| 'F_PAGE_CASE'||','|| INVESTIGATION_FORM_CODE||')');
  RUN;
*/

DATA INIT_FORM_SET;
  SET INIT_FORM_SET;
	BY SORT_KEY;
	LENGTH RDB_COLUMN_NAME $30;
	LENGTH INVESTIGATION_FORM_CODE $30; 
	IF FIRST.SORT_KEY THEN/*this is the way SAS iterates through each of the records*/

		DATAMART_NAME = COMPRESS( "'"||DATAMART_NM||"'") ;
		INVESTIGATION_FORM_CODE = TRIM("'")||TRIM(FORM_CD)||TRIM("'") ;
 		call symputx('DATAMART_NAME', COMPRESS("'"||DATAMART_NM||"'"));/*set the value into the first parameter (variable) and trims*/
 		DATAMART_TABLE_NAME = COMPRESS('DM_INV_'|| DATAMART_NM);
		
		/*Compiling and organizing and formatting metadata tables for investigation, patients, etc.*/
		call execute('%INVEST_FORM_PROC()');
		
		/*Compiling and organizing and formatting metadata tables for case management*/
		call execute('%MANAGE_CASE_MANAGEMENT()');
	
		/*MANAGE_D_INV ('D_INV_ADMINISTRATIVE','D_INV_ADMINISTRATIVE','D_INV_ADMINISTRATIVE_KEY')       */
		/*MANAGE_D_INV: creates all the columns necessary like OTH, UNIT, plus the regular ones. etc*/
		DIM1='D_INV_ADMINISTRATIVE';
		DIM1_KEY=COMPRESS(DIM1|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM1||','|| "'"||DIM1||"'"||','|| DIM1_KEY ||')');

		DIM2='D_INV_CLINICAL';
		DIM2_KEY=COMPRESS(DIM2|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM2||','|| "'"||DIM2||"'"||','|| DIM2_KEY ||')');

		DIM3='D_INV_COMPLICATION';
		DIM3_KEY=COMPRESS(DIM3|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM3||','|| "'"||DIM3||"'"||','|| DIM3_KEY ||')');

		DIM4='D_INV_CONTACT';
		DIM4_KEY=COMPRESS(DIM4|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM4||','|| "'"||DIM4||"'"||','|| DIM4_KEY ||')');
		 
		DIM5='D_INV_DEATH';
		DIM5_KEY=COMPRESS(DIM5|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM5||','|| "'"||DIM5||"'"||','|| DIM5_KEY ||')');

 		DIM6='D_INV_EPIDEMIOLOGY';
		DIM6_KEY=COMPRESS(DIM6|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM6||','|| "'"||DIM6||"'"||','|| DIM6_KEY ||')');

		DIM7='D_INV_HIV';
		DIM7_KEY=COMPRESS(DIM7|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM7||','|| "'"||DIM7||"'"||','|| DIM7_KEY ||')');

		DIM8='D_INV_PATIENT_OBS';
		DIM8_KEY=COMPRESS(DIM8|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM8||','|| "'"||DIM8||"'"||','|| DIM8_KEY ||')');

		DIM9='D_INV_ISOLATE_TRACKING ';
		DIM9_KEY=COMPRESS(DIM9|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM9||','|| "'"||DIM9||"'"||','|| DIM9_KEY ||')');

		DIM10='D_INV_LAB_FINDING';
		DIM10_KEY=COMPRESS(DIM10|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM10||','|| "'"||DIM10||"'"||','|| DIM10_KEY ||')');

		DIM11='D_INV_MEDICAL_HISTORY';
		DIM11_KEY=COMPRESS(DIM11|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM11||','|| "'"||DIM11||"'"||','|| DIM11_KEY ||')');

		DIM12='D_INV_MOTHER';
		DIM12_KEY=COMPRESS(DIM12|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM12||','|| "'"||DIM12||"'"||','|| DIM12_KEY ||')');

		DIM13='D_INV_OTHER';
		DIM13_KEY=COMPRESS(DIM13|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM13||','|| "'"||DIM13||"'"||','|| DIM13_KEY ||')');

		DIM14='D_INV_PREGNANCY_BIRTH';
		DIM14_KEY=COMPRESS(DIM14|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM14||','|| "'"||DIM14||"'"||','|| DIM14_KEY ||')');

		DIM15='D_INV_RESIDENCY';
		DIM15_KEY=COMPRESS(DIM15|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM15||','|| "'"||DIM15||"'"||','|| DIM15_KEY ||')');

		DIM16='D_INV_RISK_FACTOR';
		DIM16_KEY=COMPRESS(DIM16|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM16||','|| "'"||DIM16||"'"||','|| DIM16_KEY ||')');

 		DIM17='D_INV_SOCIAL_HISTORY';
		DIM17_KEY=COMPRESS(DIM17|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM17||','|| "'"||DIM17||"'"||','|| DIM17_KEY ||')');

		DIM18='D_INV_SYMPTOM';
		DIM18_KEY=COMPRESS(DIM18|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM18||','|| "'"||DIM18||"'"||','|| DIM18_KEY ||')');

		DIM19='D_INV_TREATMENT';
		DIM19_KEY=COMPRESS(DIM19|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM19||','|| "'"||DIM19||"'"||','|| DIM19_KEY ||')');

		DIM20='D_INV_TRAVEL';
		DIM20_KEY=COMPRESS(DIM20|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM20||','|| "'"||DIM20||"'"||','|| DIM20_KEY ||')');

		DIM21='D_INV_UNDER_CONDITION';
		DIM21_KEY=COMPRESS(DIM21|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM21||','|| "'"||DIM21||"'"||','|| DIM21_KEY ||')');

		DIM22='D_INV_VACCINATION';
		DIM22_KEY=COMPRESS(DIM22|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM22||','|| "'"||DIM22||"'"||','|| DIM22_KEY ||')');
		
		DIM23='D_INV_STD';
		DIM23_KEY=COMPRESS(DIM23|| '_KEY');
		call execute('%MANAGE_D_INV ('||DIM23||','|| "'"||DIM23||"'"||','|| DIM23_KEY ||')');

		/*
		It creates a table with rdb_column_nm, user_defined_column_nm, part_type_cd, which relates to the elements coming from D_Organization 
		and it creates the key, detail and qec columns for each of them removing the uid from the user_defined_column_nm, and then appending
		_key, _detail, _qec.
		*/
		call execute('%ORGDATA ()');
		
		/*Same than previous one but the data comes from rdb_table_nm = D_PROVIDER*/
		call execute('%PROVDATA ()');
		
		/*This macro handles repeating blocks of varchar questions*/
		call execute('%REPEATVARCHARDATA()');
		
		/*This macro handles repeating blocks of date questions*/
		call execute('%REPEATDATEDATA()');
		
		/*This macro handles repeating blocks of numeric questions*/
		
		call execute('%REPEATNUMERICDATA()');

		/*it creates the actual datamart_table_name. First it removed the table if already exists, and then it is created.*/
		call execute('%CREATEDM('||DATAMART_TABLE_NAME||')');



	OUTPUT;

RUN;

/*The following code was created */

/*A table with the investigations with data mart name associated to it but not condition*/

PROC SQL;
CREATE TABLE INIT_BLANK AS
	SELECT  NBS_PAGE.FORM_CD, NBS_PAGE.DATAMART_NM FROM NBS_ODS.NBS_PAGE LEFT JOIN NBS_ODS.PAGE_COND_MAPPING 
	ON PAGE_COND_MAPPING.WA_TEMPLATE_UID = NBS_PAGE.WA_TEMPLATE_UID
WHERE DATAMART_NM IS NOT NULL AND CONDITION_CD IS NULL;
QUIT;

/*table for those investigations without condition associated to it, where the rdb_table_nm is investigation */
PROC SQL;
CREATE TABLE INIT_FORM_BLANK_SET AS
SELECT INIT_BLANK.FORM_CD, INIT_BLANK.DATAMART_NM  FROM INIT_BLANK INNER JOIN NBS_ODS.NBS_UI_METADATA
ON NBS_UI_METADATA.INVESTIGATION_FORM_CD = INIT_BLANK.FORM_CD
INNER JOIN NBS_ODS.NBS_RDB_METADATA
ON NBS_UI_METADATA.NBS_UI_METADATA_UID = NBS_RDB_METADATA.NBS_UI_METADATA_UID
WHERE RDB_TABLE_NM='INVESTIGATION' ORDER BY INIT_BLANK.FORM_CD,  NBS_RDB_METADATA.RDB_COLUMN_NM;
QUIT;
PROC SORT DATA= INIT_FORM_BLANK_SET NODUPKEY; BY FORM_CD;RUN;

%ASSIGN_KEY (INIT_FORM_BLANK_SET, SORT_KEY);

/*dropping the table*/

DATA INIT_FORM_BLANK_SET;
  SET INIT_FORM_BLANK_SET;
	BY SORT_KEY;
	LENGTH RDB_COLUMN_NAME $30; 
	LENGTH INVESTIGATION_FORM_CODE $30; 
	IF FIRST.SORT_KEY THEN
		DATAMART_NAME = 'DM_INV_'|| DATAMART_NM;
		call execute('%INVEST_FORM_CLEAN_PROC('|| DATAMART_NAME||')');


	OUTPUT;
	
	
RUN; 


 
