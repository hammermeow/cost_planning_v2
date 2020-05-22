select
'RAMP' as ADS_process_type,
cast(actual_month as int) as proc_month,
cast(actual_year as int) as proc_year,
country,
'5140' as cost_center,
sum(processed_count) proc,
sum(production_hours) hours,
sum(cast(processed_count as double precision)) proc_double_recision,
sum(cast(production_hours as double precision)) hours_double_precision

From nvads.RAMP_edx a
left Join nvdev.date_dim b
On date(a.week_start_date)=b.calendar_date

Where cost_center = '5140'
and cast(actual_year as int) = 2020
--and cast(actual_month as int) = 3

group by 1,2,3,4,5

---------------------April JPN Production hour is 7.5 not 4, round up issue----------------------------------
SELECT * FROM nvads.RAMP_edx
Where Country = 'JPN'
and cast(actual_year as int) = 2020
and cast(actual_month as int) = 4
