SELECT state, PHC_code_desc AS Disease, MONTH(Event_Date) AS Month, COUNT(DISTINCT public_health_case_uid) AS CaseCount
FROM PUBLICHEALTHCASEFACT
WHERE state IS NOT NULL AND {{Disease_value}}
AND {{state_value}} AND {{Date_Range}}
GROUP BY state, PHC_code_desc, Event_Date
ORDER BY CaseCount DESC;