USE [rdb_modern]
GO
/****** Object:  View [dbo].[VACCINATION]    Script Date: 1/17/2024 8:39:44 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

 
CREATE VIEW [dbo].[VACCINATION] AS SELECT DISTINCT
D_VACCINATION.D_VACCINATION_KEY AS D_VACCINATION_KEY,
D_VACCINATION.VACCINATION_ADMINISTERED_NM AS  VACCINATION_ADMINISTERED_NM,
D_VACCINATION.LOCAL_ID AS LOCAL_ID,
D_VACCINATION.VACCINE_ADMINISTERED_DATE AS VACCINE_ADMINISTERED_DATE,
D_VACCINATION.VACCINATION_ANATOMICAL_SITE AS	 VACCINATION_ANATOMICAL_SITE,
D_VACCINATION.AGE_AT_VACCINATION AS AGE_AT_VACCINATION,
D_VACCINATION.AGE_AT_VACCINATION_UNIT AS AGE_AT_VACCINATION_UNIT,
D_VACCINATION.VACCINE_MANUFACTURER_NM AS VACCINE_MANUFACTURER_NM,
D_VACCINATION.VACCINE_LOT_NUMBER_TXT AS	VACCINE_LOT_NUMBER_TXT,
D_VACCINATION.VACCINE_EXPIRATION_DT AS VACCINE_EXPIRATION_DT,
D_VACCINATION.VACCINE_DOSE_NBR AS VACCINE_DOSE_NBR,
D_VACCINATION.VACCINE_INFO_SOURCE AS VACCINE_INFO_SOURCE,
D_VACCINATION.RECORD_STATUS_CD AS RECORD_STATUS_CD,
D_VACCINATION.ELECTRONIC_IND AS ELECTRONIC_IND,
D_PATIENT.PATIENT_LOCAL_ID AS PATIENT_LOCAL_ID,
D_PATIENT.PATIENT_LAST_NAME AS PATIENT_LAST_NAME,
D_PATIENT.PATIENT_FIRST_NAME AS PATIENT_FIRST_NAME,
D_PATIENT.PATIENT_MIDDLE_NAME AS PATIENT_MIDDLE_NAME,
D_PATIENT.PATIENT_CURRENT_SEX AS PATIENT_CURRENT_SEX,
D_PATIENT.PATIENT_BIRTH_SEX AS PATIENT_BIRTH_SEX,
D_PATIENT.PATIENT_DOB AS PATIENT_DOB,
D_PATIENT.PATIENT_AGE_REPORTED AS PATIENT_AGE_REPORTED,
D_PATIENT.PATIENT_AGE_REPORTED_UNIT AS PATIENT_AGE_REPORTED_UNIT,
D_PATIENT.PATIENT_STREET_ADDRESS_1 AS PATIENT_STREET_ADDRESS_1,
D_PATIENT.PATIENT_STREET_ADDRESS_2 AS PATIENT_STREET_ADDRESS_2,
D_PATIENT.PATIENT_CITY AS PATIENT_CITY,
D_PATIENT.PATIENT_STATE_CODE AS	PATIENT_STATE_CODE,
D_PATIENT.PATIENT_ZIP AS PATIENT_ZIP,
D_PATIENT.PATIENT_COUNTY AS PATIENT_COUNTY,
D_PATIENT.PATIENT_COUNTRY AS PATIENT_COUNTRY,
D_PATIENT.PATIENT_SSN AS PATIENT_SSN,
D_PATIENT.PATIENT_PRIMARY_OCCUPATION AS OCCUPATION,
D_PATIENT.PATIENT_MARITAL_STATUS AS PATIENT_MARITAL_STATUS,
D_PATIENT.PATIENT_RACE_CALC_DETAILS AS PATIENT_RACE_CALC_DETAILS,
D_PATIENT.PATIENT_ETHNICITY AS PATIENT_ETHNICITY,
D_PATIENT.PATIENT_BIRTH_COUNTRY AS PATIENT_BIRTH_COUNTRY,
D_PROVIDER.PROVIDER_FIRST_NAME AS PROVIDER_FIRST_NAME,
D_PROVIDER.PROVIDER_LAST_NAME AS PROVIDER_LAST_NAME,
D_PROVIDER.PROVIDER_NAME_DEGREE AS PROVIDER_NAME_DEGREE,
D_PROVIDER.PROVIDER_STREET_ADDRESS_1 AS PROVIDER_STREET_ADDRESS_1,
D_PROVIDER.PROVIDER_STREET_ADDRESS_2 AS PROVIDER_STREET_ADDRESS_2,
D_PROVIDER.PROVIDER_CITY AS PROVIDER_CITY,
D_PROVIDER.PROVIDER_STATE_CODE AS PROVIDER_STATE_CODE,
D_PROVIDER.PROVIDER_ZIP AS PROVIDER_ZIP,
D_PROVIDER.PROVIDER_COUNTY AS PROVIDER_COUNTY,
D_PROVIDER.PROVIDER_COUNTRY AS PROVIDER_COUNTRY,
D_ORGANIZATION.ORGANIZATION_NAME AS ORGANIZATION_NAME,
D_ORGANIZATION.ORGANIZATION_STREET_ADDRESS_1 AS ORGANIZATION_STREET_ADDRESS_1,
D_ORGANIZATION.ORGANIZATION_STREET_ADDRESS_2 AS ORGANIZATION_STREET_ADDRESS_2,
D_ORGANIZATION.ORGANIZATION_CITY AS ORGANIZATION_CITY,
D_ORGANIZATION.ORGANIZATION_STATE_CODE AS ORGANIZATION_STATE_CODE,
D_ORGANIZATION.ORGANIZATION_ZIP AS ORGANIZATION_ZIP,
D_ORGANIZATION.ORGANIZATION_COUNTY AS ORGANIZATION_COUNTY,
D_ORGANIZATION.ORGANIZATION_COUNTRY AS ORGANIZATION_COUNTRY,
D_VACCINATION.ADD_TIME AS ADD_TIME,
D_VACCINATION.ADD_USER_ID AS ADD_USER_ID,
D_VACCINATION.LAST_CHG_TIME AS LAST_CHG_TIME,
D_VACCINATION.LAST_CHG_USER_ID AS LAST_CHG_USER_ID
FROM RDB..D_VACCINATION
LEFT JOIN RDB..F_VACCINATION ON F_VACCINATION.D_VACCINATION_KEY=D_VACCINATION.D_VACCINATION_KEY
LEFT JOIN RDB..D_PATIENT ON F_VACCINATION.PATIENT_KEY=D_PATIENT.PATIENT_KEY
LEFT JOIN RDB..D_ORGANIZATION ON F_VACCINATION.VACCINE_GIVEN_BY_ORG_KEY=D_ORGANIZATION.ORGANIZATION_KEY
LEFT JOIN RDB..D_PROVIDER ON F_VACCINATION.VACCINE_GIVEN_BY_KEY =D_PROVIDER.PROVIDER_KEY;
GO
