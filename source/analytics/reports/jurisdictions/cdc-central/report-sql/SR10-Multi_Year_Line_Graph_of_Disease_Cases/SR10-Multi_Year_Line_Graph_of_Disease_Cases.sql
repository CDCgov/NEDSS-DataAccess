SELECT state, PHC_code_desc AS Disease, YEAR(event_date) AS PHC_Year, COUNT(DISTINCT public_health_case_uid) AS CaseCount
FROM PUBLICHEALTHCASEFACT
WHERE jurisdiction IS NOT NULL AND state IS NOT NULL AND {{Disease_value}} AND {{state_value}} AND {{Date_Range}}
GROUP BY state, event_date, PHC_code_desc;