PROC SQL;
CREATE TABLE D_STD_LAB_INV_INIT AS SELECT 
OBSERVATION.OBSERVATION_UID, OBSERVATION.LOCAL_ID AS LAB_LOCAL_ID 'LAB_LOCAL_ID', PUBLIC_HEALTH_CASE_UID AS CASE_UID 'CASE_UID', 
PUBLIC_HEALTH_CASE.LOCAL_ID AS PHC_LOCAL_ID 'PHC_LOCAL_ID'
FROM nbs_cdc.OBSERVATION
INNER JOIN nbs_cdc.ACT_RELATIONSHIP
ON ACT_RELATIONSHIP.SOURCE_ACT_UID=OBSERVATION.OBSERVATION_UID
INNER JOIN nbs_cdc.PUBLIC_HEALTH_CASE
ON PUBLIC_HEALTH_CASE.PUBLIC_HEALTH_CASE_UID=ACT_RELATIONSHIP.TARGET_ACT_UID;
QUIT;
PROC SQL;
DROP TABLE nbs_rdb.D_STD_LAB_INV;
CREATE TABLE nbs_rdb.D_STD_LAB_INV AS SELECT 
D_STD_LAB_INV_INIT.*, INVESTIGATION.INVESTIGATION_KEY
FROM D_STD_LAB_INV_INIT
INNER JOIN nbs_rdb.INVESTIGATION
ON INVESTIGATION.CASE_UID = D_STD_LAB_INV_INIT.CASE_UID;
QUIT;
