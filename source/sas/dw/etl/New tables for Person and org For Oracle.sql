
CREATE TABLE S_PATIENT(
PATIENT_UID 			NUMBER(20) 		NOT NULL PRIMARY KEY, 
PATIENT_MPR_UID 		NUMBER(20) 		NULL, 
PATIENT_RECORD_STATUS       	VARCHAR2(50)     NULL,
PATIENT_LOCAL_ID		VARCHAR2(50) 	NULL,     
PATIENT_GENERAL_COMMENTS	VARCHAR2 (2000) 	NULL,     
PATIENT_FIRST_NAME		VARCHAR2 (50) 	NULL,      
PATIENT_MIDDLE_NAME		VARCHAR2 (50) 	NULL,      
PATIENT_LAST_NAME  		VARCHAR2 (50) 	NULL,     
PATIENT_NAME_SUFFIX		VARCHAR2 (50) 	NULL,      
PATIENT_ALIAS_NICKNAME		VARCHAR2 (50) 	NULL,  
PATIENT_STREET_ADDRESS_1	VARCHAR2 (50) 	NULL,  
PATIENT_STREET_ADDRESS_2	VARCHAR2 (50) 	NULL, 
PATIENT_CITY			VARCHAR2 (50) 	NULL, 
PATIENT_STATE			VARCHAR2 (50) 	NULL, 
PATIENT_ZIP			VARCHAR2 (50) 	NULL,
PATIENT_COUNTY			VARCHAR2 (50) 	NULL,
PATIENT_COUNTRY			VARCHAR2 (50) 	NULL,
PATIENT_WITHIN_CITY_LIMITS	VARCHAR2 (10) 	NULL,
PATIENT_PHONE_HOME		VARCHAR2 (50) 	NULL, 		
PATIENT_PHONE_EXT_HOME		VARCHAR2 (50) 	NULL,       
PATIENT_PHONE_WORK    		VARCHAR2 (50) 	NULL, 
PATIENT_PHONE_EXT_WORK		VARCHAR2 (50) 	NULL,       
PATIENT_PHONE_CELL    		VARCHAR2 (50) 	NULL, 
PATIENT_EMAIL           	VARCHAR2 (100)	NULL, 
PATIENT_DOB             	DATE 	NULL,      
PATIENT_AGE_REPORTED       	NUMERIC 	NULL,           
PATIENT_AGE_REPORTED_UNIT  	VARCHAR2 (20)	NULL,         
PATIENT_BIRTH_SEX           VARCHAR2 (50) 	NULL,
PATIENT_CURRENT_SEX         VARCHAR2 (50) 	NULL,
PATIENT_DECEASED_INDICATOR  VARCHAR2 (50) 	NULL,
PATIENT_DECEASED_DATE       DATE 	NULL, 
PATIENT_MARITAL_STATUS      VARCHAR2 (50) 	NULL, 
PATIENT_SSN               	VARCHAR2 (50) 	NULL,     
PATIENT_ETHNICITY         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_CALCULATED     VARCHAR2 (50) 	NULL,
PATIENT_RACE_CALC_DETAILS   VARCHAR2 (4000) 	NULL,          
PATIENT_RACE_AMER_IND_1	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_2	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_3	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_GT3_IND	VARCHAR2 (50) 	NULL,        
PATIENT_RACE_AMER_IND_ALL    	VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_ASIAN_1         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_2         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_3         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_GT3_IND   VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_ALL       VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_BLACK_1         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_2         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_3         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_GT3_IND   VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_ALL       VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_NAT_HI_1        VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_2        VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_3        VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_GT3_IND  VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_ALL      VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_WHITE_1         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_2         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_3         VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_GT3_IND   VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_ALL       VARCHAR2 (2000) 	NULL, 
PATIENT_NUMBER 	     	     VARCHAR2 (50) 	NULL, 
PATIENT_NUMBER_AUTH          VARCHAR2 (50) 	NULL, 
PATIENT_ENTRY_METHOD         VARCHAR2 (50) 	NULL,
PATIENT_LAST_CHANGE_TIME     DATE	NULL ,
PATIENT_ADD_TIME		DATE NULL,
PATIENT_ADDED_BY		VARCHAR2 (50) 	NULL,
PATIENT_LAST_UPDATED_BY		VARCHAR2 (50) 	NULL
);

CREATE TABLE L_PATIENT (
	PATIENT_KEY NUMERIC NOT NULL PRIMARY KEY,
	PATIENT_UID NUMBER(20) NOT NULL 
	
); 

CREATE TABLE D_PATIENT (
PATIENT_KEY             	NUMERIC(20) 	NOT NULL PRIMARY KEY,
PATIENT_MPR_UID 		NUMBER(20) 		NULL, 
PATIENT_UID 			NUMBER(20) 		NULL, 
PATIENT_RECORD_STATUS       	VARCHAR2(50)     NULL,
PATIENT_LOCAL_ID		VARCHAR2(50) 	NULL,     
PATIENT_GENERAL_COMMENTS	VARCHAR2 (2000) 	NULL,     
PATIENT_FIRST_NAME		VARCHAR2 (50) 	NULL,      
PATIENT_MIDDLE_NAME		VARCHAR2 (50) 	NULL,      
PATIENT_LAST_NAME  		VARCHAR2 (50) 	NULL,     
PATIENT_NAME_SUFFIX		VARCHAR2 (50) 	NULL,      
PATIENT_ALIAS_NICKNAME		VARCHAR2 (50) 	NULL,  
PATIENT_STREET_ADDRESS_1	VARCHAR2 (50) 	NULL,  
PATIENT_STREET_ADDRESS_2	VARCHAR2 (50) 	NULL, 
PATIENT_CITY			VARCHAR2 (50) 	NULL, 
PATIENT_STATE			VARCHAR2 (50) 	NULL, 
PATIENT_ZIP			VARCHAR2 (50) 	NULL,
PATIENT_COUNTY			VARCHAR2 (50) 	NULL,
PATIENT_COUNTRY			VARCHAR2 (50) 	NULL,
PATIENT_WITHIN_CITY_LIMITS	VARCHAR2 (10) 	NULL,
PATIENT_PHONE_HOME		VARCHAR2 (50) 	NULL, 		
PATIENT_PHONE_EXT_HOME		VARCHAR2 (50) 	NULL,       
PATIENT_PHONE_WORK    		VARCHAR2 (50) 	NULL, 
PATIENT_PHONE_EXT_WORK		VARCHAR2 (50) 	NULL,       
PATIENT_PHONE_CELL    		VARCHAR2 (50) 	NULL, 
PATIENT_EMAIL           	VARCHAR2 (100)	NULL, 
PATIENT_DOB             	DATE 	NULL,      
PATIENT_AGE_REPORTED       	NUMERIC 	NULL,           
PATIENT_AGE_REPORTED_UNIT  	VARCHAR2 (20)	NULL,         
PATIENT_BIRTH_SEX           	VARCHAR2 (50) 	NULL,
PATIENT_CURRENT_SEX         	VARCHAR2 (50) 	NULL,
PATIENT_DECEASED_INDICATOR  	VARCHAR2 (50) 	NULL,
PATIENT_DECEASED_DATE       	DATE 	NULL, 
PATIENT_MARITAL_STATUS      	VARCHAR2 (50) 	NULL, 
PATIENT_SSN               	VARCHAR2 (50) 	NULL,     
PATIENT_ETHNICITY         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_CALCULATED     	VARCHAR2 (50) 	NULL,
PATIENT_RACE_CALC_DETAILS   	VARCHAR2 (4000) 	NULL,          
PATIENT_RACE_AMER_IND_1		VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_2		VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_3		VARCHAR2 (50) 	NULL, 
PATIENT_RACE_AMER_IND_GT3_IND	VARCHAR2 (50) 	NULL,        
PATIENT_RACE_AMER_IND_ALL    	VARCHAR2 (2000) NULL, 
PATIENT_RACE_ASIAN_1         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_2         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_3         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_GT3_IND   	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_ASIAN_ALL       	VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_BLACK_1         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_2         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_3         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_GT3_IND   	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_BLACK_ALL       	VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_NAT_HI_1        	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_2        	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_3        	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_GT3_IND  	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_NAT_HI_ALL      	VARCHAR2 (2000) 	NULL, 
PATIENT_RACE_WHITE_1         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_2         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_3         	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_GT3_IND   	VARCHAR2 (50) 	NULL, 
PATIENT_RACE_WHITE_ALL       	VARCHAR2 (2000) 	NULL, 
PATIENT_NUMBER 	     	     	VARCHAR2 (50) 	NULL, 
PATIENT_NUMBER_AUTH          	VARCHAR2 (50) 	NULL, 
PATIENT_ENTRY_METHOD         	VARCHAR2 (50) 	NULL,
PATIENT_LAST_CHANGE_TIME     	DATE		NULL, 
PATIENT_ADD_TIME		DATE NULL,
PATIENT_ADDED_BY		VARCHAR2 (50) 	NULL,
PATIENT_LAST_UPDATED_BY		VARCHAR2 (50) 	NULL
);

CREATE TABLE  L_PROVIDER (
     PROVIDER_KEY   		INTEGER NOT NULL PRIMARY KEY,
     PROVIDER_UID   		NUMBER(20)  NOT NULL
);

CREATE TABLE S_PROVIDER (
     PROVIDER_UID   		NUMBER(20)  NOT NULL PRIMARY KEY,
     PROVIDER_LOCAL_ID   	VARCHAR2 (50) NULL,
     PROVIDER_RECORD_STATUS   	VARCHAR2 (50) NULL,
     PROVIDER_NAME_PREFIX   	VARCHAR2 (50) NULL,
     PROVIDER_FIRST_NAME   	VARCHAR2 (50) NULL,
     PROVIDER_MIDDLE_NAME   	VARCHAR2 (50) NULL,
     PROVIDER_LAST_NAME   	VARCHAR2 (50) NULL,
     PROVIDER_NAME_SUFFIX   	VARCHAR2 (50) NULL,
     PROVIDER_NAME_DEGREE   	VARCHAR2 (50) NULL,
     PROVIDER_GENERAL_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_QUICK_CODE   	VARCHAR2 (50) NULL,
     PROVIDER_REGISTRATION_NUM  VARCHAR2 (50) NULL,
     PROVIDER_REGISRATION_NUM_AUTH   VARCHAR2 (50) NULL,
     PROVIDER_STREET_ADDRESS_1   	VARCHAR2 (50) NULL,
     PROVIDER_STREET_ADDRESS_2   	VARCHAR2 (50) NULL,
     PROVIDER_CITY   		VARCHAR2 (50) NULL,
     PROVIDER_STATE   VARCHAR2 (50) NULL,
     PROVIDER_ZIP   VARCHAR2 (50) NULL,
     PROVIDER_COUNTY   VARCHAR2 (50) NULL,
     PROVIDER_COUNTRY   VARCHAR2 (50) NULL,
     PROVIDER_ADDRESS_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_PHONE_WORK   VARCHAR2 (50) NULL,
     PROVIDER_PHONE_EXT_WORK   VARCHAR2 (50) NULL,
     PROVIDER_EMAIL_WORK   VARCHAR2 (50) NULL,
     PROVIDER_PHONE_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_PHONE_CELL   VARCHAR2 (50) NULL,
     PROVIDER_ENTRY_METHOD   VARCHAR2 (50) NULL,
     PROVIDER_LAST_CHANGE_TIME   DATE  NULL,
     PROVIDER_ADD_TIME	DATE NULL,
     PROVIDER_ADDED_BY		VARCHAR2 (50) 	NULL,
     PROVIDER_LAST_UPDATED_BY	VARCHAR2 (50) 	NULL
); 

CREATE TABLE D_PROVIDER (
     PROVIDER_UID   NUMBER(20)  NULL,
     PROVIDER_KEY   NUMBER  NOT NULL PRIMARY KEY,
     PROVIDER_LOCAL_ID   VARCHAR2 (50) NULL,
     PROVIDER_RECORD_STATUS   VARCHAR2 (50) NULL,
     PROVIDER_NAME_PREFIX   VARCHAR2 (50) NULL,
     PROVIDER_FIRST_NAME   VARCHAR2 (50) NULL,
     PROVIDER_MIDDLE_NAME   VARCHAR2 (50) NULL,
     PROVIDER_LAST_NAME   VARCHAR2 (50) NULL,
     PROVIDER_NAME_SUFFIX   VARCHAR2 (50) NULL,
     PROVIDER_NAME_DEGREE   VARCHAR2 (50) NULL,
     PROVIDER_GENERAL_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_QUICK_CODE   VARCHAR2 (50) NULL,
     PROVIDER_REGISTRATION_NUM   VARCHAR2 (50) NULL,
     PROVIDER_REGISRATION_NUM_AUTH   VARCHAR2 (50) NULL,
     PROVIDER_STREET_ADDRESS_1   VARCHAR2 (50) NULL,
     PROVIDER_STREET_ADDRESS_2   VARCHAR2 (50) NULL,
     PROVIDER_CITY   VARCHAR2 (50) NULL,
     PROVIDER_STATE   VARCHAR2 (50) NULL,
     PROVIDER_ZIP   VARCHAR2 (50) NULL,
     PROVIDER_COUNTY   VARCHAR2 (50) NULL,
     PROVIDER_COUNTRY   VARCHAR2 (50) NULL,
     PROVIDER_ADDRESS_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_PHONE_WORK   VARCHAR2 (50) NULL,
     PROVIDER_PHONE_EXT_WORK   VARCHAR2 (50) NULL,
     PROVIDER_EMAIL_WORK   VARCHAR2 (50) NULL,
     PROVIDER_PHONE_COMMENTS   VARCHAR2 (2000) NULL,
     PROVIDER_PHONE_CELL   VARCHAR2 (50) NULL,
     PROVIDER_ENTRY_METHOD   VARCHAR2 (50) NULL,
     PROVIDER_LAST_CHANGE_TIME   DATE  NULL,
     PROVIDER_ADD_TIME	DATE NULL,
     PROVIDER_ADDED_BY		VARCHAR2 (50) 	NULL,
     PROVIDER_LAST_UPDATED_BY	VARCHAR2 (50) 	NULL
); 

CREATE TABLE S_ORGANIZATION (
ORGANIZATION_UID		NUMBER(20) PRIMARY KEY,
ORGANIZATION_LOCAL_ID	 	VARCHAR2(50) NULL,
ORGANIZATION_RECORD_STATUS	 VARCHAR2(50) NULL,
ORGANIZATION_NAME		 VARCHAR2(50) NULL,
ORGANIZATION_GENERAL_COMMENTS	 VARCHAR2(2000) NULL,	
ORGANIZATION_QUICK_CODE		 VARCHAR2(50) NULL,	
ORGANIZATION_STAND_IND_CLASS	 VARCHAR2(50) NULL,
ORGANIZATION_FACILITY_ID	 VARCHAR2(50) NULL,
ORGANIZATION_FACILITY_ID_AUTH	 VARCHAR2(50) NULL,	
ORGANIZATION_STREET_ADDRESS_1    VARCHAR2(50) NULL,	   
ORGANIZATION_STREET_ADDRESS_2    VARCHAR2(50) NULL,	   
ORGANIZATION_CITY                VARCHAR2(50) NULL,   
ORGANIZATION_STATE               VARCHAR2(50) NULL,   
ORGANIZATION_ZIP                 VARCHAR2(10) NULL,   
ORGANIZATION_COUNTY              VARCHAR2(50) NULL,   
ORGANIZATION_COUNTRY             VARCHAR2(50) NULL,   
ORGANIZATION_ADDRESS_COMMENTS	 VARCHAR2(2000) NULL,
ORGANIZATION_PHONE_WORK		 VARCHAR2(50) NULL,
ORGANIZATION_PHONE_EXT_WORK	 VARCHAR2(50) NULL,
ORGANIZATION_EMAIL		 VARCHAR2(50) NULL,
ORGANIZATION_PHONE_COMMENTS	 VARCHAR2(2000) NULL,
ORGANIZATION_ENTRY_METHOD	 VARCHAR2(50) NULL,
ORGANIZATION_LAST_CHANGE_TIME  DATE NULL,
ORGANIZATION_ADD_TIME	       DATE NULL,
ORGANIZATION_ADDED_BY		VARCHAR2 (50) 	NULL,
ORGANIZATION_LAST_UPDATED_BY	VARCHAR2 (50) 	NULL
);

CREATE TABLE L_ORGANIZATION (
	ORGANIZATION_KEY NUMERIC NOT NULL PRIMARY KEY,
    ORGANIZATION_UID NUMBER(20) NOT NULL 
    
); 


CREATE TABLE D_ORGANIZATION (
ORGANIZATION_KEY        NUMBER PRIMARY KEY,
ORGANIZATION_UID        NUMBER(20),
ORGANIZATION_LOCAL_ID         VARCHAR2(50) NULL,
ORGANIZATION_RECORD_STATUS     VARCHAR2(50) NULL,
ORGANIZATION_NAME         VARCHAR2(50) NULL,
ORGANIZATION_GENERAL_COMMENTS     VARCHAR2(2000) NULL,    
ORGANIZATION_QUICK_CODE         VARCHAR2(50) NULL,    
ORGANIZATION_STAND_IND_CLASS     VARCHAR2(50) NULL,
ORGANIZATION_FACILITY_ID     VARCHAR2(50) NULL,
ORGANIZATION_FACILITY_ID_AUTH     VARCHAR2(50) NULL,
ORGANIZATION_STREET_ADDRESS_1    VARCHAR2(50) NULL,       
ORGANIZATION_STREET_ADDRESS_2    VARCHAR2(50) NULL,       
ORGANIZATION_CITY                VARCHAR2(50) NULL,   
ORGANIZATION_STATE               VARCHAR2(50) NULL,   
ORGANIZATION_ZIP                 VARCHAR2(10) NULL,   
ORGANIZATION_COUNTY              VARCHAR2(50) NULL,   
ORGANIZATION_COUNTRY             VARCHAR2(50) NULL,   
ORGANIZATION_ADDRESS_COMMENTS     VARCHAR2(2000) NULL,
ORGANIZATION_PHONE_WORK         VARCHAR2(50) NULL,
ORGANIZATION_PHONE_EXT_WORK     VARCHAR2(50) NULL,
ORGANIZATION_EMAIL         VARCHAR2(50) NULL,
ORGANIZATION_PHONE_COMMENTS     VARCHAR2(2000) NULL,
ORGANIZATION_ENTRY_METHOD     VARCHAR2(50) NULL,
ORGANIZATION_LAST_CHANGE_TIME  DATE NULL,
ORGANIZATION_ADD_TIME	       DATE NULL,
ORGANIZATION_ADDED_BY		VARCHAR2 (50) 	NULL,
ORGANIZATION_LAST_UPDATED_BY	VARCHAR2 (50) 	NULL
);

DELETE FROM ACTIVITY_LOG_DETAIL;

DELETE FROM ACTIVITY_LOG_MASTER;


CREATE   TABLE nbs_rdb.HEP100  (
    PATIENT_LOCAL_ID varchar2  ( 50 )   NOT   NULL,
    PATIENT_FIRST_NM varchar2  ( 50 )   NULL,
    PATIENT_MIDDLE_NM varchar2  ( 50 )   NULL,
    PATIENT_LAST_NM varchar2  ( 50 )   NULL,
    PATIENT_DOB date  NULL,
    PATIENT_REPORTEDAGE number  ( 8 ,  0 )   NULL,
    PATIENT_REPORTED_AGE_UNITS varchar2  ( 50 )   NULL,
    ADDR_USE_CD_DESC varchar2  ( 300 )   NULL,
    ADDR_CD_DESC varchar2  ( 300 )   NULL,
    PATIENT_ADDRESS varchar2  ( 725 )   NULL,
    PATIENT_CITY varchar2  ( 100 )   NULL,
    PATIENT_COUNTY varchar2  ( 300 )   NULL,
    PATIENT_ZIP_CODE varchar2  ( 20 )   NULL,
    PATIENT_CURR_GENDER varchar2  ( 50 )   NULL,
    PATIENT_ELECTRONIC_IND varchar2  ( 50 )   NULL,
    RACE varchar2  ( 500 )   NULL,
    CONDITION_CD varchar2  ( 50 )   NULL,
    CONDITION varchar2  ( 50 )   NULL,
    PROGRAM_JURISDICTION_OID number  ( 20 ,  0 )   NULL,
    INV_LOCAL_ID varchar2  ( 50 )   NOT   NULL,
    INVESTIGATION_STATUS varchar2  ( 50 )   NULL,
    INV_CASE_STATUS varchar2  ( 50 )   NULL,
    INV_JURISDICTION_NM varchar2  ( 100 )   NULL,
    RPT_SRC_CD_DESC varchar2  ( 100 )   NULL,
    REPORTING_SOURCE varchar2  ( 100 )   NULL,
    REPORTING_SOURCE_COUNTY varchar2  ( 255 )   NULL,
    REPORTING_SOURCE_CITY varchar2  ( 100 )   NULL,
    REPORTING_SOURCE_STATE varchar2  ( 100 )   NULL,
    REPORTING_SOURCE_ADDRESS_USE varchar2  ( 300 )   NULL,
    REPORTING_SOURCE_ADDRESS_TYPE varchar2  ( 300 )   NULL,
    PHYSICIAN_NAME varchar2  ( 152 )   NULL,
    PHYSICIAN_COUNTY varchar2  ( 255 )   NULL,
    PHYSICIAN_CITY varchar2  ( 100 )   NULL,
    PHYSICIAN_STATE varchar2  ( 100 )   NULL,
    PHYSICIAN_ADDRESS_USE_DESC varchar2  ( 300 )   NULL,
    PHYSICIAN_ADDRESS_TYPE_DESC varchar2  ( 300 )   NULL,
    INVESTIGATOR_NAME varchar2  ( 152 )   NULL,
    HEP_A_TOTAL_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_A_IGM_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_B_SURFACE_ANTIGEN varchar2  ( 50 )   NULL,
    HEP_B_TOTAL_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_B_IGM_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_C_TOTAL_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_D_TOTAL_ANTIBODY varchar2  ( 50 )   NULL,
    HEP_E_TOTAL_ANTIBODY varchar2  ( 50 )   NULL,
    ANTIHCV_SIGNAL_TO_CUTOFF_RATIO varchar2  ( 2000 )   NULL,
    ANTIHCV_SUPPLEMENTAL_ASSAY varchar2  ( 50 )   NULL,
    HCV_RNA varchar2  ( 50 )   NULL,
    ALT_SGPT_RESULT number  ( 11 ,  0 )   NULL,
    ALT_SGPT_RESULT_UPPER_LIMIT number  ( 11 ,  0 )   NULL,
    AST_SGOT_RESULT number  ( 11 ,  0 )   NULL,
    AST_SGOT_RESULT_UPPER_LIMIT number  ( 11 ,  0 )   NULL,
    ALT_RESULT_DT date  NULL,
    AST_RESULT_DT date  NULL,
    INV_START_DT date  NULL,
    INV_RPT_DT date  NULL,
    EARLIEST_RPT_TO_CNTY_DT date  NULL,
    EARLIEST_RPT_TO_STATE_DT date  NULL,
    DIE_FRM_THIS_ILLNESS_IND varchar2  ( 50 )   NULL,
    ILLNESS_ONSET_DT date  NULL,
    DIAGNOSIS_DT date  NULL,
    HSPTLIZD_IND varchar2  ( 50 )   NULL,
    HSPTL_ADMISSION_DT date  NULL,
    HSPTL_DISCHARGE_DT date  NULL,
    HSPTL_DURATION_DAYS number  ( 20 ,  0 )   NULL,
    OUTBREAK_IND varchar2  ( 50 )   NULL,
    TRANSMISSION_MODE varchar2  ( 50 )   NULL,
    DISEASE_IMPORTED_IND varchar2  ( 50 )   NULL,
    IMPORT_FROM_COUNTRY varchar2  ( 50 )   NULL,
    IMPORT_FROM_STATE varchar2  ( 50 )   NULL,
    IMPORT_FROM_COUNTY varchar2  ( 50 )   NULL,
    IMPORT_FROM_CITY varchar2  ( 2000 )   NULL,
    INV_COMMENTS varchar2  ( 2000 )   NULL,
    CASE_RPT_MMWR_WEEK number  ( 8 ,  0 )   NULL,
    CASE_RPT_MMWR_YEAR number  ( 8 ,  0 )   NULL,
    PATIENT_SYMPTOMATIC_IND varchar2  ( 50 )   NULL,
    PATIENT_JUNDICED_IND varchar2  ( 50 )   NULL,
    PATIENT_PREGNANT_IND varchar2  ( 50 )   NULL,
    PATIENT_PREGNANCY_DUE_DT date  NULL,
    HEP_A_EPLINK_IND varchar2  ( 50 )   NULL,
    HEP_A_CONTACTED_IND varchar2  ( 50 )   NULL,
    D_N_P_EMPLOYEE_IND varchar2  ( 50 )   NULL,
    D_N_P_HOUSEHOLD_CONTACT_IND varchar2  ( 50 )   NULL,
    HEP_A_KEYENT_IN_CHILDCARE_IND varchar2  ( 50 )   NULL,
    HEPA_MALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    HEPA_FEMALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    STREET_DRUG_INJECTED_IN_2_6_WK varchar2  ( 50 )   NULL,
    STREET_DRUG_USED_IN_2_6_WK varchar2  ( 50 )   NULL,
    TRAVEL_OUT_USA_CAN_IND varchar2  ( 50 )   NULL,
    HOUSEHOLD_NPP_OUT_USA_CAN varchar2  ( 50 )   NULL,
    PART_OF_AN_OUTBRK_IND varchar2  ( 50 )   NULL,
    ASSOCIATED_OUTBRK_TYPE varchar2  ( 50 )   NULL,
    FOODBORNE_OUTBRK_FOOD_ITEM varchar2  ( 2000 )   NULL,
    FOODHANDLER_2_WK_PRIOR_ONSET varchar2  ( 50 )   NULL,
    HEP_A_VACC_RECEIVED_IND varchar2  ( 50 )   NULL,
    HEP_A_VACC_RECEIVED_DOSE varchar2  ( 50 )   NULL,
    HEP_A_VACC_LAST_RECEIVED_YR number  ( 11 ,  0 )   NULL,
    IMMUNE_GLOBULIN_RECEIVED_IND varchar2  ( 50 )   NULL,
    GLOBULIN_LAST_RECEIVED_YR date  NULL,
    HEP_B_CONTACTED_IND varchar2  ( 50 )   NULL,
    HEPB_STD_TREATED_IND varchar2  ( 50 )   NULL,
    HEPB_STD_LAST_TREATMENT_YR number  ( 11 ,  0 )   NULL,
    STREET_DRUG_INJECTED_IN6WKMON varchar2  ( 50 )   NULL,
    STREET_DRUG_USED_IN6WKMON varchar2  ( 50 )   NULL,
    HEPB_FEMALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    HEPB_MALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    HEMODIALYSIS_IN_LAST_6WKMON varchar2  ( 50 )   NULL,
    BLOOD_CONTAMINATION_IN6WKMON varchar2  ( 50 )   NULL,
    HEPB_BLOOD_RECEIVED_IN6WKMON varchar2  ( 50 )   NULL,
    HEPB_BLOOD_RECEIVED_DT date  NULL,
    OUTPATIENT_IV_INFUSION_IN6WKMO varchar2  ( 50 )   NULL,
    BLOOD_EXPOSURE_IN_LAST6WKMON varchar2  ( 50 )   NULL,
    BLOOD_EXPOSURE_IN6WKMON_OTHER varchar2  ( 2000 )   NULL,
    HEPB_MED_DEN_EMPLOYEE_IN6WKMON varchar2  ( 50 )   NULL,
    HEPB_MED_DEN_BLOOD_CONTACT_FRQ varchar2  ( 50 )   NULL,
    HEPB_PUB_SAFETY_WORKER_IN6WKMO varchar2  ( 50 )   NULL,
    HEPB_PUBSAFETY_BLOODCONTACTFRQ varchar2  ( 50 )   NULL,
    TATTOOED_IN6WKMON_BEFORE_ONSET varchar2  ( 50 )   NULL,
    PIERCING_IN6WKMON_BEFORE_ONSET varchar2  ( 50 )   NULL,
    DEN_WORK_OR_SURGERY_IN6WKMON varchar2  ( 50 )   NULL,
    NON_ORAL_SURGERY_IN6WKMON varchar2  ( 50 )   NULL,
    HSPTLIZD_IN6WKMON_BEFORE_ONSET varchar2  ( 50 )   NULL,
    LONGTERMCARE_RESIDENT_IN6WKMON varchar2  ( 50 )   NULL,
    B_INCARCERATED24PLUSHRSIN6WKMO varchar2  ( 50 )   NULL,
    B_INCARCERATED_6PLUS_MON_IND varchar2  ( 50 )   NULL,
    B_LAST6PLUSMON_INCARCERATE_YR number  ( 8 ,  0 )   NULL,
    BLAST6PLUSMO_INCARCERATEPERIOD number  ( 8 ,  0 )   NULL,
    B_LAST_INCARCERATE_PERIOD_UNIT varchar2  ( 50 )   NULL,
    HEP_B_VACC_RECEIVED_IND varchar2  ( 50 )   NULL,
    HEP_B_VACC_SHOT_RECEIVED_NBR varchar2  ( 50 )   NULL,
    HEP_B_VACC_LAST_RECEIVED_YR number  ( 8 ,  0 )   NULL,
    ANTI_HBSAG_TESTED_IND varchar2  ( 50 )   NULL,
    ANTI_HBS_POSITIVE_REACTIVE_IND varchar2  ( 50 )   NULL,
    HEP_C_CONTACTED_IND varchar2  ( 50 )   NULL,
    MED_DEN_EMPLOYEE_IN_2WK6MO varchar2  ( 50 )   NULL,
    HEPC_MED_DEN_BLOOD_CONTACT_FRQ varchar2  ( 50 )   NULL,
    PUBLIC_SAFETY_WORKER_IN_2WK6MO varchar2  ( 50 )   NULL,
    HEPC_PUBSAFETY_BLOODCONTACTFRQ varchar2  ( 50 )   NULL,
    TATTOOED_IN2WK6MO_BEFORE_ONSET varchar2  ( 50 )   NULL,
    TATTOOED_IN2WK6MO_LOCATION varchar2  ( 50 )   NULL,
    PIERCING_IN2WK6MO_BEFORE_ONSET varchar2  ( 50 )   NULL,
    PIERCING_IN2WK6MO_LOCATION varchar2  ( 50 )   NULL,
    STREET_DRUG_INJECTED_IN_2WK6MO varchar2  ( 50 )   NULL,
    STREET_DRUG_USED_IN_2WK6MO varchar2  ( 50 )   NULL,
    HEMODIALYSIS_IN_LAST_2WK6MO varchar2  ( 50 )   NULL,
    BLOOD_CONTAMINATION_IN_2WK6MO varchar2  ( 50 )   NULL,
    HEPC_BLOOD_RECEIVED_IN_2WK6MO varchar2  ( 50 )   NULL,
    HEPC_BLOOD_RECEIVED_DT date  NULL,
    BLOOD_EXPOSURE_IN_LAST2WK6MO varchar2  ( 50 )   NULL,
    BLOOD_EXPOSURE_IN2WK6MO_OTHER varchar2  ( 2000 )   NULL,
    DEN_WORK_OR_SURGERY_IN2WK6MO varchar2  ( 50 )   NULL,
    NON_ORAL_SURGERY_IN2WK6MO varchar2  ( 50 )   NULL,
    HSPTLIZD_IN2WK6MO_BEFORE_ONSET varchar2  ( 50 )   NULL,
    LONGTERMCARE_RESIDENT_IN2WK6MO varchar2  ( 50 )   NULL,
    INCARCERATED_24PLUSHRSIN2WK6MO varchar2  ( 50 )   NULL,
    HEPC_FEMALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    HEPC_MALE_SEX_PARTNER_NBR varchar2  ( 50 )   NULL,
    C_INCARCERATED_6PLUS_MON_IND varchar2  ( 50 )   NULL,
    C_LAST6PLUSMON_INCARCERATE_YR number  ( 11 ,  0 )   NULL,
    CLAST6PLUSMO_INCARCERATEPERIOD number  ( 11 ,  0 )   NULL,
    C_LAST_INCARCERATE_PERIOD_UNIT varchar2  ( 50 )   NULL,
    HEPC_STD_TREATED_IND varchar2  ( 50 )   NULL,
    HEPC_STD_LAST_TREATMENT_YR number  ( 11 ,  0 )   NULL,
    BLOOD_TRANSFUSION_BEFORE_1992 varchar2  ( 50 )   NULL,
    ORGAN_TRANSPLANT_BEFORE_1992 varchar2  ( 50 )   NULL,
    CLOT_FACTOR_CONCERN_BEFORE1987 varchar2  ( 50 )   NULL,
    LONGTERM_HEMODIALYSIS_IND varchar2  ( 50 )   NULL,
    EVER_INJECT_NONPRESCRIBED_DRUG varchar2  ( 50 )   NULL,
    LIFETIME_SEX_PARTNER_NBR number  ( 15 ,  5 )   NULL,
    EVER_INCARCERATED_IND varchar2  ( 50 )   NULL,
    HEPATITIS_CONTACTED_IND varchar2  ( 50 )   NULL,
    HEPATITIS_CONTACT_TYPE varchar2  ( 50 )   NULL,
    HEPATITIS_OTHER_CONTACT_TYPE varchar2  ( 2000 )   NULL,
    HEPC_MED_DEN_EMPLOYEE_IND varchar2  ( 50 )   NULL,
    OUTPATIENT_IV_INFUSIONIN2WK6MO varchar2  ( 50 )   NULL,
    EVENT_DATE date  NULL,
    HEP_MULTI_VAL_GRP_KEY NUMBER(20)  NULL,
    INVESTIGATION_KEY NUMBER(20)  NOT   NULL,
    HEP_B_E_ANTIGEN varchar2  ( 50 )   NULL,
    HEP_B_DNA varchar2  ( 50 )   NULL,
    INVESTIGATOR_UID NUMBER(20) NULL,
    PHYSICIAN_UID NUMBER(20) NULL,
    PATIENT_UID NUMBER(20)  NOT   NULL,
    REFRESH_DATETIME date  NULL,
    CASE_UID  NUMBER(20) NOT NULL,
    REPORTING_SOURCE_UID NUMBER(20) NULL
); 

ALTER TABLE HEP100
        ADD PRIMARY KEY (INVESTIGATION_KEY);

