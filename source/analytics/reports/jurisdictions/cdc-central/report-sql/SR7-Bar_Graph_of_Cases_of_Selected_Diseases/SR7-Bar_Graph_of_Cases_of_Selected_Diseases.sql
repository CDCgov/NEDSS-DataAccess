WITH disease_by_date AS  
(
SELECT Disease, event_year, event_month, monthly_count, disease_year_total_month
, ROW_NUMBER() OVER(PARTITION BY disease ORDER BY monthly_count, event_year, event_month) disease_median_order
, disease_total = COUNT(*) OVER (PARTITION BY disease ORDER BY disease)
, ROW_NUMBER() OVER(PARTITION BY disease,event_year ORDER BY monthly_count, event_month) disease_year_median_order
, SUM(monthly_count) OVER (PARTITION BY Disease, event_year 
		ORDER BY Disease, event_year, event_month ROWS BETWEEN unbounded preceding AND current row) AS cummulative_todate_count
, AVG(monthly_count) OVER (PARTITION BY Disease, event_year 
		ORDER BY Disease, event_year, event_month ROWS BETWEEN unbounded preceding AND current row) AS avg_todate_count
 FROM (
 	SELECT data1.disease, data1.event_year, data1.event_month, data1.monthly_count, data1.disease_year_total_month
 	FROM (
			SELECT PHC_code_desc AS Disease, year(event_Date) AS event_year, month(event_date) AS event_month
			, count(*) monthly_count	
			, count((month(event_date))) OVER (PARTITION BY PHC_code_desc, year(event_date)) AS disease_year_total_month
			FROM dbo.PublicHealthCaseFact 
			WHERE year(event_date) BETWEEN year(GetDate()) - 7 AND year(GetDate()) - 2 
			AND {{disease_value}}
			AND {{state_value}}
			GROUP BY PHC_code_desc, year(event_Date) , month(event_date)
		) data1
		) data2
)
SELECT disease, COALESCE(string_agg([curr_cumm],''),0) AS "Current YTD",
COALESCE(string_agg(median_overall,''),0) AS "Five Year Median YTD"
FROM 
(
SELECT disease
, CASE WHEN event_year = year(GetDate()) - 2 AND event_month=month(getdate()) then cummulative_todate_count  END AS "curr_cumm"
, CASE WHEN disease_median_order_position = disease_median_order THEN monthly_count END AS median_overall
FROM ( 
SELECT Disease, event_year, event_month, monthly_count, disease_median_order, disease_total
, CASE WHEN (disease_total%2=1) THEN (disease_total +1)/2 ELSE (disease_total/2) END AS disease_median_order_position
, disease_year_median_order, disease_year_total_month
, CASE 
		WHEN (disease_year_total_month % 2)=1 THEN (disease_year_total_month+1)/2 
		ELSE (disease_year_total_month/2)
	END
 AS disease_year_median_order_position
 , cummulative_todate_count, avg_todate_count
FROM disease_by_date
 ) all_year  
WHERE event_year BETWEEN year(GetDate()) - 7 AND year(GetDate()) - 2 
) data_final
GROUP BY disease 
ORDER BY disease 
;