with disease_by_date as  
(
select Disease,event_year,event_month,monthly_count,disease_year_total_month
,ROW_NUMBER() over(partition by disease order by monthly_count,event_year,event_month) disease_median_order
, disease_total = count(*) over (partition by disease order by disease)
,ROW_NUMBER() over(partition by disease,event_year order by monthly_count,event_month) disease_year_median_order
,sum(monthly_count) over (partition by Disease,event_year 
		order by Disease,event_year,event_month  rows between unbounded preceding and current row) as cummulative_todate_count
,avg(monthly_count) over (partition by Disease,event_year 
		order by Disease,event_year,event_month  rows between unbounded preceding and current row) as avg_todate_count
 from (
 	select data1.disease,data1.event_year,data1.event_month,data1.monthly_count,data1.disease_year_total_month
 	from (
			select PHC_code_desc as Disease,year(event_Date) as event_year,month(event_date) as event_month
			,count(*) monthly_count	
			,count((month(event_date))) over (PARTITION by PHC_code_desc, year(event_date)) as disease_year_total_month
			from dbo.PublicHealthCaseFact 
			where year(event_date) between year(GetDate()) - 7 and year(GetDate()) - 2 
			and {{disease_value}}
			and {{state_value}}
			group by PHC_code_desc, year(event_Date) , month(event_date)
		) data1
		) data2
)
select disease,COALESCE(string_agg([curr_cnt],''),0) as "current_month", COALESCE(string_agg([curr_cumm],''),0) as "current_year_todate_cummumative" 
,COALESCE(string_agg([prev_cumm],''),0)  as "previous_year_todate_cummulative",COALESCE(string_agg(median_overall,''),0) as median_overall
,COALESCE(string_agg(median_current_year,''),0) as medain_current_year
,case 
	when COALESCE(string_agg(median_overall,''),0) > 0 
		then Round((100.00*(COALESCE(string_agg(median_current_year,''),0) - COALESCE(string_agg(median_overall,''),0))/COALESCE(string_agg(median_overall,''),0)),2)
	else 0	
end as percentage_median_change
from 
(
select disease
, case  when event_year = year(GetDate()) - 2 and event_month=month(getdate()) then monthly_count end as "curr_cnt"
, case  when event_year = year(GetDate()) - 2 and event_month=month(getdate()) then cummulative_todate_count  end as "curr_cumm"
, case  when event_year = year(GetDate()) - 3 and event_month=month(getdate()) then cummulative_todate_count  end as "prev_cumm"
,case when disease_median_order_position = disease_median_order then monthly_count end as median_overall
, case when (event_year = year(getdate()) - 2 and disease_year_median_order = disease_year_median_order_position) then monthly_count end as median_current_year
from ( 
select Disease,event_year,event_month,monthly_count,disease_median_order,disease_total
,case when (disease_total%2=1) then (disease_total +1)/2 else (disease_total/2) end as disease_median_order_position
,disease_year_median_order,  disease_year_total_month
,case 
		when (disease_year_total_month % 2)=1 then (disease_year_total_month+1)/2 
		else (disease_year_total_month/2) -- concat((disease_year_total_month/2),'-',(disease_year_total_month/2)+1)
	end
 as disease_year_median_order_position
 ,cummulative_todate_count, avg_todate_count
from disease_by_date
 ) all_year  
where event_year between year(GetDate()) - 7 and year(GetDate()) - 2 
) data_final
group by disease 
order by disease 
;