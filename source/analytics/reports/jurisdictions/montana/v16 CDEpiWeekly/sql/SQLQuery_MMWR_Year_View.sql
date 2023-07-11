DECLARE @date_value date = {{date}}

SELECT
    I.JURISDICTION_NM AS Jurisdiction,
    CON.DISEASE_GRP_CD AS Disease,
    COALESCE(SUM(CASE WHEN YEAR(D.DATE_MM_DD_YYYY) = YEAR(@date_value) THEN C.CASE_COUNT ELSE 0 END), 0) AS 'Given Year',
    COALESCE(SUM(CASE WHEN YEAR(D.DATE_MM_DD_YYYY) = YEAR(@date_value)-1 THEN C.CASE_COUNT ELSE 0 END), 0) AS 'Given Year -1',
    COALESCE(SUM(CASE WHEN YEAR(D.DATE_MM_DD_YYYY) = YEAR(@date_value)-2 THEN C.CASE_COUNT ELSE 0 END), 0) AS 'Given Year -2',
    COALESCE(SUM(CASE WHEN YEAR(D.DATE_MM_DD_YYYY) = YEAR(@date_value)-3 THEN C.CASE_COUNT ELSE 0 END), 0) AS 'Given Year -3',
    SUM(COALESCE(C.CASE_COUNT, 0)) AS 'Total'
FROM
    INVESTIGATION I
    LEFT JOIN CASE_COUNT C ON C.INVESTIGATION_KEY = I.INVESTIGATION_KEY
    LEFT JOIN CONDITION CON ON C.CONDITION_KEY = CON.CONDITION_KEY
    LEFT JOIN RDB_DATE D ON C.INV_ASSIGNED_DT_KEY = D.DATE_KEY
WHERE 
    YEAR(D.DATE_MM_DD_YYYY) BETWEEN (YEAR(@date_value) - 3) AND YEAR(@date_value) 
    AND DATEPART(WK, D.DATE_MM_DD_YYYY) = DATEPART(WK, @date_value) 
    AND CON.PROGRAM_AREA_CD IN ('COV', 'STD', 'HEP', 'HEBAB', 'HEPC', 'VARI', 'TB', 'RAB')
    AND I.INV_CASE_STATUS = 'Confirmed'
    AND I.DIE_FRM_THIS_ILLNESS_IND = 'Yes'

GROUP BY 
    I.JURISDICTION_NM,
    CON.DISEASE_GRP_CD, 
    C.CASE_COUNT
    
ORDER BY 
Jurisdiction ASC,
Disease ASC