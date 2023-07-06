SELECT state, REPLACE(jurisdiction, ' County', '') as jurisdiction, PHC_code_desc, COUNT(DISTINCT public_health_case_uid) AS CaseCount
FROM PUBLICHEALTHCASEFACT
WHERE jurisdiction IS NOT NULL AND state IS NOT NULL and {{Disease_value}}
and {{State_value}} and {{Date_range}}
GROUP BY state, jurisdiction, PHC_code_desc
ORDER BY CaseCount DESC;