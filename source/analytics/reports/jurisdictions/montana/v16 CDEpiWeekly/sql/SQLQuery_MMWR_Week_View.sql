DECLARE @date_value date = {{date_value}};


DECLARE @ColumnName AS NVARCHAR(MAX);
select  @ColumnName = '[Generic_Case],[INV_FORM_HEPGEN],[INV_FORM_RVCT],[PG_Hepatitis_C,_acute],[PG_Hepatitis_C,_chronic],[PG_Hepatitis_E,_acute],[PG_Latent_TB_Infection],[PG_Novel_Coronavirus_(COVID-19)],[PG_PrEP],[PG_Rabies,_Animal],[PG_STD_Investigation],[PG_Syphilis,_congenital_Investigation],[PG_Syphilis]';

DECLARE @mmwr NVARCHAR(MAX) = N'
SELECT * FROM (
SELECT
    I.JURISDICTION_NM AS Jurisdiction,
    CON.DISEASE_GRP_CD AS Disease,
    SUM(COALESCE(C.CASE_COUNT, 0)) AS count_value
FROM
    INVESTIGATION I
    LEFT JOIN CASE_COUNT C ON C.INVESTIGATION_KEY = I.INVESTIGATION_KEY
    LEFT JOIN CONDITION CON ON C.CONDITION_KEY = CON.CONDITION_KEY
    LEFT JOIN RDB_DATE D ON C.INV_ASSIGNED_DT_KEY = D.DATE_KEY
WHERE 
    CON.PROGRAM_AREA_CD IN (''COV'', ''STD'', ''HEP'', ''HEBAB'', ''HEPC'', ''VARI'', ''TB'', ''RAB'')
    AND YEAR(D.DATE_MM_DD_YYYY) = YEAR(@date_value) 
    -- AND DATEPART(WK, D.DATE_MM_DD_YYYY) = DATEPART(WK, @date_value) 
    AND I.INV_CASE_STATUS = ''Confirmed''
    -- AND I.DIE_FRM_THIS_ILLNESS_IND = ''Yes''

GROUP BY 
    I.JURISDICTION_NM,
    CON.DISEASE_GRP_CD, 
    C.CASE_COUNT
) t
PIVOT 
(
    SUM(count_value)
    FOR Disease IN (' + @ColumnName + ')
) AS p;';

;

EXEC sp_executesql @mmwr, N'@date_value DATE', @date_value = @date_value;
