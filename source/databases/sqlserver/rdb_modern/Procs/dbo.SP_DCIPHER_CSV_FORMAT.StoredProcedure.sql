USE [rdb_modern]
GO
/****** Object:  StoredProcedure [dbo].[SP_DCIPHER_CSV_FORMAT]    Script Date: 1/17/2024 8:40:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SP_DCIPHER_CSV_FORMAT]
AS
	
	BEGIN
	
	BEGIN TRY
	
	 BEGIN TRANSACTION;
	--If the temporal table already exists, drop it.
			IF OBJECT_ID('dbo.DCIPHER_CSV_FORMAT','U') IS NOT NULL 
			BEGIN
				DROP TABLE dbo.DCIPHER_CSV_FORMAT 
			END
			create table dbo.DCIPHER_CSV_FORMAT (DCIPHER varchar(8000));
				--Inserting the header
				INSERT INTO dbo.DCIPHER_CSV_FORMAT VALUES ('cdc_ncov2019_id,state,local_id,healthdept,nndss_id,contact_id,interviewer_ln,interviewer_fn,interviewer_org,interviewer_tele,interviewer_email,current_status,pui_cdcreport_dt,case_cdcreport_dt,res_county,res_state,ethnicity,sex,race_asian,race_aian,race_black,race_nhpi,race_white,race_unk,race_other,race_spec,dob,age,ageunit,pos_spec_dt,pos_spec_unk,pos_spec_na,pna_yn,acuterespdistress_yn,diagother,abxchest_yn,sympstatus,onset_dt,onset_unk,symp_res_dt,symp_res_yn,hosp_yn,adm1_dt,dis1_dt,icu_yn,mechvent_yn,mechvent_dur,ecmo_yn,death_yn,death_dt,death_unk,hc_work_yn,hc_work_china_yn,exp_wuhan,exp_hubei,exp_china,exp_othcountry,exp_othcountry_spec,exp_house,exp_community,exp_health,exp_health_pt,exp_health_vis,exp_health_hcw,exp_animal,exp_cluster,exp_other,exp_other_spec,exp_unk,cont_lab_us,cdc_ncov2019_sourceid_2,process_pui,process_cont,process_surv,process_epix,process_dgmqid,process_unk,process_other,process_other_spec,collect_ptinterview,collect_medchart,fever_yn,sfever_yn,chills_yn,myalgia_yn,runnose_yn,sthroat_yn,cough_yn,sob_yn,nauseavomit_yn,headache_yn,abdom_yn,diarrhea_yn,othsym1_yn,othsym1_spec,othsym2_yn,othsym2_spec,othsym3_yn,othsym3_spec,medcond_yn,cld_yn,diabetes_yn,cvd_yn,renaldis_yn,liverdis_yn,immsupp_yn,neuro_yn,neuro_spec,otherdis_yn,otherdis_spec,pregnant_yn,smoke_curr_yn,smoke_former_yn,resp_flua_ag,resp_flub_ag,resp_flua_pcr,resp_flub_pcr,resp_rsv,resp_hm,resp_pi,resp_adv,resp_rhino,resp_cov,resp_mp,resp_rcp,othrp,othrp_spec,spec_npswab1id,spec_npswab1_dt,spec_npswab1cdc,spec_npswab1state,spec_npswab2id,spec_npswab2_dt,spec_npswab2cdc,spec_npswab2state,spec_npswab3id,spec_npswab3_dt,spec_npswab3cdc,spec_npswab3state,spec_opswab1id,spec_opswab1_dt,spec_opswab1cdc,spec_opswab1state,spec_opswab2id,spec_opswab2_dt,spec_opswab2cdc,spec_opswab2state,spec_opswab3id,spec_opswab3_dt,spec_opswab3cdc,spec_opswab3state,spec_sputum1id,spec_sputum1_dt,spec_sputum1cdc,spec_sputum1state,spec_sputum2id,spec_sputum2_dt,spec_sputum2cdc,spec_sputum2state,spec_sputum3id,spec_sputum3_dt,spec_sputum3cdc,spec_sputum3state,spec_otherspecimen1_yn,spec_otherspecimen1_spec,spec_otherspecimen1id,spec_otherspecimen1_dt,spec_otherspecimen1cdc,spec_otherspecimen1state,spec_otherspecimen2_yn,spec_otherspecimen2_spec,spec_otherspecimen2id,spec_otherspecimen2_dt,spec_otherspecimen2cdc,spec_otherspecimen2state,spec_otherspecimen3_yn,spec_otherspecimen3_spec,spec_otherspecimen3id,spec_otherspecimen3_dt,spec_otherspecimen3cdc,spec_otherspecimen3state,lab_local_id1,final_notes,spec_npswab1Stateresult,spec_npswab1CDCresult,spec_npswab2Stateresult,spec_npswab2CDCresult,spec_npswab3Stateresult,spec_npswab3CDCresult,spec_opswab1Stateresult,spec_opswab1CDCresult,spec_opswab2Stateresult,spec_opswab2CDCresult,spec_opswab3Stateresult,spec_opswab3CDCresult,spec_sputum1Stateresult,spec_sputum1CDCresult,spec_sputum2Stateresult,spec_sputum2CDCresult,spec_sputum3Stateresult,spec_sputum3CDCresult,spec_otherspecimen1Stateresult,spec_otherspecimen1CDCresult,spec_otherspecimen2Stateresult,spec_otherspecimen2CDCresult,spec_otherspecimen3Stateresult,spec_otherspecimen3CDCresult,
				probable,translator_yn,translator_spec,icu_adm1_dt,icu_dis1_dt,tribe,tribe_name,tribe_member,housing,housing_spec,hc_job,hc_job_spec,hc_setting,hc_setting_spec,exp_othstate,exp_othstate_spec,exp_ship,exp_ship_spec,exp_work,exp_work_critical,exp_work_critical_spec,exp_airport,exp_adultfacility,exp_school,exp_correctional,exp_gathering,exp_animal_spec,exp_contact,cdc_ncov2019_sourceid_3,cdc_ncov2019_sourceid_4,outbreak_associated,outbreak_name,abxekg_yn,rigors_yn,taste_yn,fatigue_yn,wheezing_yn,diffbreathing_yn,chestpain_yn,othsym1_spec2,othsym1_spec3,hypertension_yn,obesity_yn,othercond_yn,othercond_spec,autoimm_yn,substance_yn,psych_yn,psych_spec,test_PCR,test_serologic,test_other,test_other_spec');

				INSERT INTO dbo.DCIPHER_CSV_FORMAT 
				SELECT DISTINCT
				CONCAT(CONCAT(
				
				'"',REPLACE(CDC_ASSIGNED_ID,'"','""'),'"',',',
				'"',REPLACE((SELECT DISTINCT TOP 1 state_nm from nbs_srte.dbo.state_code where state_cd in (
						  select config_value from nbs_odse.dbo.NBS_configuration where config_key ='NBS_STATE_CODE')),'"','""'),'"',',',		  
				'"',REPLACE(INV_LOCAL_ID,'"','""'),'"',',',
				'"',REPLACE((SELECT config_value FROM  nbs_odse.dbo.NBS_configuration WHERE config_key='NND_SENDING_FACILITY_NM'),'"','""'),'"',',',	
				'"',REPLACE(INV_LEGACY_CASE_ID,'"','""'),'"',',',
				'"',REPLACE(LINKED_TO_CASE_ID,'"','""'),'"',',',
				'"',REPLACE((SELECT SUBSTRING(config_value,0,CHARINDEX(',',config_value)) FROM  nbs_odse.dbo.NBS_configuration WHERE config_key='MSG_REPORTING_PERSON_NAME'),'"','""'),'"',',',
				'',',',--interviewer_fn
				'',',',--interviewer_org
				
				'"',REPLACE((SELECT config_value FROM  nbs_odse.dbo.NBS_configuration WHERE config_key='MSG_REPORTING_PERSON_PHONE'),'"','""'),'"',',',
				'',',',--interviewer_email

				CASE PAT_PROCESS_STATUS 
					WHEN 'PUI, testing pending' THEN 1
					WHEN 'PUI, tested negative' THEN 2
					WHEN 'Presumptive case (positive local test), confirmatory testing pending' THEN 3
					WHEN 'Presumptive case (positive local test), confirmatory tested negative' THEN 4
					WHEN 'Laboratory-confirmed case' THEN 5
					WHEN 'Probable case' THEN 6
				END,',',

				PUI_REPORT_TO_CDC_DT,',',
				CASE_REPORT_TO_CDC_DT,',',
				'"',REPLACE(REPLACE((SELECT code_Desc_txt FROM nbs_srte.dbo.State_county_code_value WHERE code = PATIENT_COUNTY),' County',''),'"','""'),'"',',',
				--'"',REPLACE((SELECT SUBSTRING((SELECT code_Desc_txt FROM nbs_srte.dbo.State_county_code_value WHERE code = PATIENT_COUNTY),0,CHARINDEX('County',(SELECT code_Desc_txt FROM nbs_srte.dbo.State_county_code_value WHERE code = PATIENT_COUNTY))-1)),'"','""'),'"',',',
				'"',REPLACE((SELECT DISTINCT TOP 1 state_nm from nbs_srte.dbo.state_code where state_cd =PATIENT_STATE),'"','""'),'"',',',	
				CASE PATIENT_ETHNICITY 
					WHEN '2135-2' THEN 1
					WHEN '2186-5' THEN 0
					WHEN 'UNK' THEN 9
				END,',',
				CASE PATIENT_CURRENT_SEX 
					WHEN 'M' THEN 1
					WHEN 'F' THEN 2
					WHEN 'U' THEN 9
				END,',',

				CASE WHEN PATIENT_RACE_CALC LIKE '%Asian%' THEN '1' ELSE '' END,',',--race_asian
				CASE WHEN PATIENT_RACE_CALC LIKE '%American Indian or Alaska Native%' THEN '1' ELSE '' END,',',--race_aian
				CASE WHEN PATIENT_RACE_CALC LIKE '%Black%' THEN '1' ELSE '' END,',',--race_black
				CASE WHEN PATIENT_RACE_CALC LIKE '%Native Hawaiian or Other Pacific Islander%' THEN '1' ELSE '' END,',',--race_nhpi
				CASE WHEN PATIENT_RACE_CALC LIKE '%White%' THEN '1' ELSE '' END,',',--race_white
				CASE WHEN PATIENT_RACE_CALC LIKE '%Unknown%' THEN '1' ELSE '' END,',',--race_unk
				CASE WHEN PATIENT_RACE_CALC LIKE '%Other%' THEN '1' ELSE '' END,','--race_other
				),CONCAT(
				'',',',--race_spec
				convert(varchar, PATIENT_DOB, 101),',',
				PATIENT_AGE_REPORTED,',',
				CASE PATIENT_AGE_RPTD_UNIT 
					WHEN 'Y' THEN 1
					WHEN 'M' THEN 2
					WHEN 'D' THEN 3
				END,',',

				FRST_POS_SPEC_CLCT_DT,',',
				'',',',--pos_spec_unk
				'',',',--pos_spec_na
				CASE PNEUMONIA 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE ARDS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE OTH_DIAGNOSIS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE ABN_CHEST_XRAY_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
					WHEN 'Not Applicable' THEN 5
				END,',',
				CASE Symptomatic 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				convert(varchar, ILLNESS_ONSET_DT, 101),',',
				'',',',--onset_unk
				convert(varchar, ILLNESS_END_DT, 101),',',
				CASE SYMPTOM_STATUS 
					WHEN 'Still symptomatic' THEN 1
					WHEN 'Symptoms resolved, unknown date' THEN 0
					WHEN 'Unknown symptom status' THEN 9
				END,',',
				CASE HSPTLIZD_IND
					WHEN 'Y' THEN 1
					WHEN 'N' THEN 0
					WHEN 'U' THEN 9
				END,',',
				convert(varchar, HSPTL_ADMISSION_DT, 101),',',
				convert(varchar, HSPTL_DISCHARGE_DT, 101),',',
				CASE HOSPITAL_ICU_STAY 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE RECEIVED_MV_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				TOTAL_MV_DAYS,',',
				CASE RECEIVED_ECMO_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE DIE_FROM_ILLNESS_IND 
					WHEN 'Y' THEN 1
					WHEN 'N' THEN 0
					WHEN 'U' THEN 9
				END,',',

				convert(varchar, INV_DEATH_DT, 101),',',
				CASE KNOWN_DEATH_DT 
					WHEN 'Yes' THEN 1
				END,',',

				CASE US_HC_WORKER_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CHINA_HC_HISTORY_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				
				CASE WHEN HIGH_RISK_TRAVEL_LOC LIKE '%Wuhan%' THEN '1' ELSE '' END,',',--exp_wuhan
				CASE WHEN HIGH_RISK_TRAVEL_LOC LIKE '%Hubei%' THEN '1' ELSE '' END,',',--exp_hubei
				CASE WHEN HIGH_RISK_TRAVEL_LOC LIKE '%China%' THEN '1' ELSE '' END,',',--exp_china
				CASE WHEN HIGH_RISK_TRAVEL_LOC LIKE '%OTH^%' THEN '1' ELSE '' END,',',--exp_othcountry
				
				--'"',REPLACE(INTL_DESTINATIONS,'"','""'),'"',',',--to avoid splitting the value in different columns in case it contains ,
				CASE CTT_CONF_CASE_HSHLD 
					WHEN 'Yes' THEN 1
				END,',',

				CASE CTT_CONF_CASE_COMM 
					WHEN 'Yes' THEN 1
				END,',',

				CASE CTT_CONF_CASE_HLTHCR 
					WHEN 'Yes' THEN 1
				END,',',

				CASE WHEN HC_CONTACT_TYPE LIKE '%patient%' THEN '1' ELSE '' END,',',--Healthcare contact with another lab-confirmed COVID-19 case-patient -- patient
				CASE WHEN HC_CONTACT_TYPE LIKE '%visitor%' THEN '1' ELSE '' END,',',--Healthcare contact with another lab-confirmed COVID-19 case-patient -- visitor
				CASE WHEN HC_CONTACT_TYPE LIKE '%healthcare worker%' THEN '1' ELSE '' END,',',--Healthcare contact with another lab-confirmed COVID-19 case-patient -- healthcare worker


				CASE ANIMAL_EXPOSURE_IND 
					WHEN 'Yes' THEN 1
				END,',',

				CASE SEVERE_ARD_EXP_IND 
					WHEN 'Yes' THEN 1
				END,',',

				CASE OTH_EXPOSURE_IND 
					WHEN 'Yes' THEN 1
				END,',',

				'"',REPLACE(OTH_EXPOSURE_SPECIFY,'"','""'),'"',',',
				CASE UNK_EXPOSURE_SOURCE 
					WHEN 'Yes' THEN 1
				END,',',
				CASE US_COVID_CASE_EXP_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
					WHEN 'NA' THEN 5

				END,',',

				'"',REPLACE(SOURCE_CASE_ID,'"','""'),'"',',',
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%evaluation%' THEN '1' ELSE '' END,',',----Clinical evaluation leading to PUI determination   
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%tracing%' THEN '1' ELSE '' END,',',--Contact tracing of case patient
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%surveillance%' THEN '1' ELSE '' END,',',--Routine surveillance
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%notification%' THEN '1' ELSE '' END,',',--EpiX notification of travelers
			
				'"',REPLACE(DGMQ_ID,'"','""'),'"',',',
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%Unknown%' THEN '1' ELSE '' END,',',--Unknown
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%OTH%' THEN '1' ELSE '' END,',',--Other
				CASE WHEN CASE_IDENTIFY_PROCESS LIKE '%OTH%' THEN (CONCAT('"',
							SUBSTRING(
							SUBSTRING(CASE_IDENTIFY_PROCESS,CHARINDEX('^',CASE_IDENTIFY_PROCESS)+1,LEN(CASE_IDENTIFY_PROCESS))
							,0,CHARINDEX(';',SUBSTRING(CASE_IDENTIFY_PROCESS,CHARINDEX('^',CASE_IDENTIFY_PROCESS)+1,LEN(CASE_IDENTIFY_PROCESS))+';'))
							,'"')
							) ELSE '' END,',',--If other, specify
			
			
			CASE WHEN INFO_SOURCE LIKE '%Patient%' THEN '1' ELSE '' END,',',--Patient interview
			CASE WHEN INFO_SOURCE LIKE '%Medical%' THEN '1' ELSE '' END,',',--Medical record review
			
				CASE FEVER 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE FEVERISH_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CHILLS_RIGORS 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE MYALGIA 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CORYZA_RUNNY_NOSE_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE SORE_THROAT_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE COUGH_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE DYSPNEA_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE NAUSEA 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE HEADACHE 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE ABDOMINAL_PAIN 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE DIARRHEA 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				CASE OTH_SYMPTOM_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				'"',REPLACE(OTHER_SYM_SPEC,'"','""'),'"',',',
				'',',',--othsym2_yn
				'',',',--othsym2_spec
				'',',',--othsym3_yn
				'',',',--othsym3_spec
				CASE PREEXISTING_COND_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CHRONIC_LUNG_DIS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE DIABETES_MELLITUS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CV_DISEASE_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CHRONIC_RENAL_DIS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE CHRONIC_LIVER_DIS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE IMMUNO_CONDITION_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE NEURO_DISABLITY_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				'"',REPLACE(NEURO_DISABLITY_INFO,'"','""'),'"',',',
				CASE OTH_CHRONIC_DIS_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				'"',REPLACE(OTH_CHRONIC_DIS_TXT,'"','""'),'"',',',
				CASE PATIENT_PREGNANT_IND 
					WHEN 'Y' THEN 1
					WHEN 'N' THEN 0
					WHEN 'U' THEN 9
				END,','
				),concat(
				CASE CURRENT_SMOKER_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE FORMER_SMOKER_IND 
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				CASE FLU_A_RAPID_AG_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',
				CASE FLU_B_RAPID_AG_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE FLU_A_PCR_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE FLU_B_PCR_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE RSV_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE H_METAPNEUMOVRS_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE PARAINFLUENZA1_4_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE ADENOVIRUS_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE RHINO_ENTERO_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE CORONAVIRUS_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE M_PNEUMONIAE_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				CASE C_PNEUMONIAE_RSLT 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',

				/*CASE OTH_PATHOGEN_TEST_IND 
					WHEN 'Positive' THEN 1
					WHEN 'Negative' THEN 2
					WHEN 'Pending' THEN 3
					WHEN 'Not Done' THEN 4
				END,',',*/
				'',',',--othrp
				'',',',--othrp_spec
				'',',',--spec_npswab1id
				'',',',--spec_npswab1_dt
				'',',',--spec_npswab1cdc
				'',',',--spec_npswab1state
				'',',',--spec_npswab2id
				'',',',--spec_npswab2_dt
				'',',',--spec_npswab2cdc
				'',',',--spec_npswab2state
				'',',',--spec_npswab3id
				'',',',--spec_npswab3_dt
				'',',',--spec_npswab3cdc
				'',',',--spec_npswab3state
				'',',',--spec_opswab1id
				'',',',--spec_opswab1_dt
				'',',',--spec_opswab1cdc
				'',',',--spec_opswab1state
				'',',',--spec_opswab2id
				'',',',--spec_opswab2_dt
				'',',',--spec_opswab2cdc
				'',',',--spec_opswab2state
				'',',',--spec_opswab3id
				'',',',--spec_opswab3_dt
				'',',',--spec_opswab3cdc
				'',',',--spec_opswab3state
				'',',',--spec_sputum1id
				'',',',--spec_sputum1_dt
				'',',',--spec_sputum1cdc
				'',',',--spec_sputum1state
				'',',',--spec_sputum2id
				'',',',--spec_sputum2_dt
				'',',',--spec_sputum2cdc
				'',',',--spec_sputum2state
				'',',',--spec_sputum3id
				'',',',--spec_sputum3_dt
				'',',',--spec_sputum3cdc
				'',',',--spec_sputum3state
				'',',',--spec_otherspecimen1_yn
				'',',',--spec_otherspecimen1_spec
				'',',',--spec_otherspecimen1id
				'',',',--spec_otherspecimen1_dt
				'',',',--spec_otherspecimen1cdc
				'',',',--spec_otherspecimen1state
				'',',',--spec_otherspecimen2_yn
				'',',',--spec_otherspecimen2_spec
				'',',',--spec_otherspecimen2id
				'',',',--spec_otherspecimen2_dt
				'',',',--spec_otherspecimen2cdc
				'',',',--spec_otherspecimen2state
				'',',',--spec_otherspecimen3_yn
				'',',',--spec_otherspecimen3_spec
				'',',',--spec_otherspecimen3id
				'',',',--spec_otherspecimen3_dt
				'',',',--spec_otherspecimen3cdc
				'',',',--spec_otherspecimen3state
				'',',',--lab_local_id1
				'"',REPLACE(NOTIF_COMMENT,'"','""'),'"',',',
				'',',',--spec_npswab1Stateresult
				'',',',--spec_npswab1CDCresult
				'',',',--spec_npswab2Stateresult
				'',',',--spec_npswab2CDCresult
				'',',',--spec_npswab3Stateresult
				'',',',--spec_npswab3CDCresult
				'',',',--spec_opswab1Stateresult
				'',',',--spec_opswab1CDCresult
				'',',',--spec_opswab2Stateresult
				'',',',--spec_opswab2CDCresult
				'',',',--spec_opswab3Stateresult
				'',',',--spec_opswab3CDCresult
				'',',',--spec_sputum1Stateresult
				'',',',--spec_sputum1CDCresult
				'',',',--spec_sputum2Stateresult
				'',',',--spec_sputum2CDCresult
				'',',',--spec_sputum3Stateresult
				'',',',--spec_sputum3CDCresult
				'',',',--spec_otherspecimen1Stateresult
				'',',',--spec_otherspecimen1CDCresult
				'',',',--spec_otherspecimen2Stateresult
				'',',',--spec_otherspecimen2CDCresult
				'',',',--spec_otherspecimen3Stateresult
				'',','--spec_otherspecimen3CDCresult

				--New columns added on 05/08/2020
				
				),CONCAT(
				CASE CASE_STATUS_REASON 
				WHEN 'Meets Clinical/Epi, No Lab Conf' THEN 1
				WHEN 'Meets Presump Lab and Clinical or Epi' THEN 2
				WHEN 'Meets Vital Records, No Lab Confirm' THEN 3
				END,',',
				
				CASE TRANSLATOR_REQ_IND
				WHEN 'Yes' THEN 1
				WHEN 'No' THEN 0
				WHEN 'Unknown' THEN 9
				END,',',

				'"',REPLACE(PREFERRED_LANGUAGE,'"','""'),'"',',',
				ICU_ADMIT_DT,',',
				ICU_DISCHARGE_DT,',',
				
				CASE TRIBAL_AFFIL_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',

				
				'"',REPLACE(TRIBE_NAME,'"','""'),'"',',',
				
				CASE TRIBAL_ENROLLED_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END,',',
				
				CASE WHEN TYPE_OF_RESIDENCE LIKE '%OTH^%' THEN 13
				ELSE			
					CASE TYPE_OF_RESIDENCE
						WHEN 'House/Single Family Home' THEN 1
						WHEN 'Apartment' THEN 2
						WHEN 'Hotel/Motel' THEN 3
						WHEN 'Long Term Care Facility' THEN 4				
						WHEN 'Nursing Home/Assisted Living Facility' THEN 5
						WHEN 'Acute Care Inpatient Facility' THEN 6
						WHEN 'Rehabilitation Facility' THEN 7
						WHEN 'Correctional Facility' THEN 8
						WHEN 'Mobile Home' THEN 9
						WHEN 'Group Home' THEN 10
						WHEN 'Homeless Shelter' THEN 11
						WHEN 'Outside, Car, Other Location' THEN 12
						WHEN '%OTH^%' THEN 13
						WHEN 'Unknown' THEN 14
					END 
				END,',',
				
				CASE WHEN TYPE_OF_RESIDENCE LIKE '%OTH^%' THEN (SELECT (SUBSTRING (TYPE_OF_RESIDENCE,CHARINDEX('^',TYPE_OF_RESIDENCE)+1,LEN(TYPE_OF_RESIDENCE))))
				ELSE '' END ,',',
				
				CASE WHEN HCW_OCCUPATION LIKE '%OTH^%' THEN 5
			ELSE
				CASE HCW_OCCUPATION
					WHEN 'Physician' THEN 1
					WHEN 'Nurse' THEN 2
					WHEN 'Respiratory Therapist' THEN 3
					WHEN 'Environmental Services' THEN 4
					WHEN 'Unknown' THEN 9
				END
			END,',',
				
				CASE WHEN  HCW_OCCUPATION LIKE '%OTH^%'THEN (SELECT (SUBSTRING (HCW_OCCUPATION,CHARINDEX('^',HCW_OCCUPATION)+1,LEN(HCW_OCCUPATION))))
				ELSE '' END ,',',
							
				CASE HCW_SETTING
					WHEN 'Hospital' THEN 1
					WHEN 'Long Term Care Facility' THEN 2
					WHEN 'Rehabilitation Facility' THEN 3
					WHEN 'Nursing Home/Assisted Living Facility' THEN 4				
					WHEN '%OTH^%' THEN 5
					WHEN 'Unknown' THEN 9
				END,',',
			
				CASE WHEN HCW_SETTING LIKE '%OTH^%'THEN (SELECT (SUBSTRING (HCW_SETTING,CHARINDEX('^',HCW_SETTING)+1,LEN(HCW_SETTING))))
				ELSE '' END ,',',
				
				CASE TRAVEL_DOMESTICALLY
					WHEN 'Yes' THEN 1
				END ,',',
				
				--'"',REPLACE(TRAVEL_STATE,'"','""'),'"',',',

				CASE CRUISE_TRAVEL_EXP
					WHEN 'Yes' THEN 1
				END ,',',

				'"',REPLACE(SHIP_NAME,'"','""'),'"',',',
				
				CASE WORKPLACE_EXP
					WHEN 'Yes' THEN 1
				END ,',',
				
				CASE WKPLC_CRITICAL_INFRA
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				'"',REPLACE(WKPLC_SETTING,'"','""'),'"',',',
				
				CASE AIR_TRAVEL_EXP
					WHEN 'Yes' THEN 1
				END ,',',
							
				CASE ADULT_CONG_LIVING_EXP
					WHEN 'Yes' THEN 1
				END ,',',
							
				CASE EDUCATIONAL_EXP
					WHEN 'Yes' THEN 1
				END ,',',
				
				CASE CORRECTIONAL_EXP
					WHEN 'Yes' THEN 1
				END ,',',
				
				CASE ATTEND_EVENTS
					WHEN 'Yes' THEN 1
				END ,',',
				
				'"',REPLACE(ANIMAL_TYPE_TXT,'"','""'),'"',',',
				
				CASE CTT_CONF_CASE_PAT_IND
					WHEN 'Yes' THEN 1
				END ,',',
				
				'"',REPLACE(EPI_LINKED_CASE_ID2,'"','""'),'"',',',
				
				'"',REPLACE(EPI_LINKED_CASE_ID3,'"','""'),'"',',',
				
				CASE OUTBREAK_IND
					WHEN 'Y' THEN 1
					WHEN 'N' THEN 0
					WHEN 'U' THEN 9
				END ,',',
				
				'"',REPLACE(OUTBREAK_NAME,'"','""'),'"',',',
				
				CASE EKG_ABNORMAL
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
					WHEN 'Not Applicable' THEN 5
				END ,',',
				
				CASE RIGORS_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE LOSS_TASTE_SMELL
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE FATIGUE_MALAISE
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE WHEEZING_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE DIFFICULT_BREATH_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE CHEST_PAIN_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				'' ,',',
				'' ,',',
				
				
				CASE HYPERTENSION
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE OBESITY_IND
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE UNDERLYING_COND_OTH
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				'"',REPLACE(UNDRLYNG_COND_SPECIFY,'"','""'),'"',',',
				
				
				CASE MDH_AUTOIMMUNE_DISEASE
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE SUBSTANCE_ABUSE
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				CASE PSYCHIATRIC_CONDITION
					WHEN 'Yes' THEN 1
					WHEN 'No' THEN 0
					WHEN 'Unknown' THEN 9
				END ,',',
				
				'"',REPLACE(PSYCH_CONDITION_SPEC,'"','""'),'"',',',
				'' ,',',
				'' ,',',
				'' ,',',
				'' ,','
		
				
				

				)) AS DCIPHER
				FROM
				dbo.COVID_CASE_DATAMART--, nbs_odse.dbo.NBS_configuration;
				WHERE COVID_CASE_DATAMART.INV_CASE_STATUS IN ('P', 'C');-- Only include cases with a case status of Confirmed or Probable.
	

		--SELECT * from dbo.DCIPHER_CSV_FORMAT;

		--DROP TABLE dbo.DCIPHER_CSV_FORMAT;-- Dropping the temporal table
		COMMIT TRANSACTION;
		END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            DECLARE @ErrorNumber INT= ERROR_NUMBER();
            DECLARE @ErrorLine INT= ERROR_LINE();
            DECLARE @ErrorMessage NVARCHAR(4000)= ERROR_MESSAGE();
            DECLARE @ErrorSeverity INT= ERROR_SEVERITY();
            DECLARE @ErrorState INT= ERROR_STATE();
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_SEVERITY() AS ErrorSeverity, ERROR_STATE() AS ErrorState, ERROR_PROCEDURE() AS ErrorProcedure, ERROR_LINE() AS ErrorLine, ERROR_MESSAGE() AS ErrorMessage;
            RETURN -1;
        END CATCH;
    END;
GO
