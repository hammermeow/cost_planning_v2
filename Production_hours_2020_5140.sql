SELECT 
 proc_year, proc_month,  cost_center, country, ads_process_type,
  sum(hours) hours
  
 FROM
  (select
'ATAT' as ads_process_type,
date_part('month',calendar_date)::int proc_month,
date_part('year',calendar_date)::int proc_year,
CASE WHEN calendar_date - first_proc_date >= 28 THEN '>= 4 Weeks'
	WHEN calendar_date - first_proc_date < 28 THEN '< 4 Weeks'
	ELSE 'problem' END new_da_flag,
country,
building site,
cost_center,
queue_type_tag,
task_data_language,
transformation,
vertical,
device,
sum(processed) proc,
sum(hours) hours

from
	(
	select
	fqat.analyst,
	deh.department_id cost_center,
	deh.building,
	deh.country,
	fqat.task_queue_id,
	dtq.queue_type_tag,
	dtq.task_data_language,
	dtq.transformation,
	dtq.vertical_or_sub_initiative vertical,
	dtq.device_or_initiative device,
	fqat.calendar_date,
	MIN(fqat.calendar_date) over (partition by fqat.analyst) first_proc_date,
	SUM(fqat.processed_count) processed,
	SUM(fqat.production_hrs) hours
	    
	from nvrpt.daily_queue_analyst_metrics fqat

	left join nvads.dim_employee_history deh

	on fqat.analyst = deh.login_name
	and fqat.calendar_date >= deh.effective_start_day
	and fqat.calendar_date < deh.effective_end_day
	and (fqat.calendar_date < deh.termination_day
	or deh.termination_day is null)

	left join nvads.dim_task_queue dtq

	on fqat.task_queue_id = dtq.task_queue_id

	where date_part('year',fqat.calendar_date) >= 2017
	and date_trunc('week',fqat.calendar_date) <> date_trunc('week',SYSDATE)

	group by 
	deh.department_id,
	fqat.calendar_date,
	deh.country,
	deh.building,
	dtq.task_data_language,
	fqat.analyst,
	dtq.queue_type_tag,
	dtq.transformation,
	dtq.vertical_or_sub_initiative,
	dtq.device_or_initiative,
	fqat.task_queue_id
	)

where cost_center = '5140'
and date_part('year',calendar_date) = 2020
--and date_part('month',calendar_date) = 3

group by
cost_center,
CASE WHEN calendar_date - first_proc_date >= 28 THEN '>= 4 Weeks'
	WHEN calendar_date - first_proc_date < 28 THEN '< 4 Weeks'
	ELSE 'problem' END,
date_part('year',calendar_date),
date_part('month',calendar_date),
country,
building,
task_data_language,
transformation,
queue_type_tag,
vertical,
device,
ads_process_type


union all


select
'RVD' as ADS_process_type,
calendar_mth as proc_month,
calendar_year as proc_year,
'null' as new_da_flag,
country,
site,
cost_center,
'null' as queue_type_tag,
REPLACE(lang_code, '-', '_')  as task_data_language, 
transformation,
cycle_customer as vertical,
'null' as device,
sum(testcase_count) proc,
sum(processed_hrs) hours


from
(select rvd.*, deh.country, deh.building as site, deh.department_id as cost_center,
cycle_type || ' ~ work state = ' || cycle_state as transformation

from
(SELECT "Test Case Counts"."cycle_id" AS "cycle_id",
       "Test Case Counts"."user_id" AS "user_id",
        "Test Case Counts"."user_login" AS "user_login",
       "Test Case Counts"."processed_date" AS "processed_date",
       "Test Case Counts"."status" AS "status",
       "Test Case Counts"."testcase_count" AS "testcase_count",
       "Test Case Counts"."processed_hrs" AS "processed_hrs",
       "Test Case Counts"."processed_hrs_rr" AS "processed_hrs_rr",
       "date_dim"."date_key" AS "date_key",
       "date_dim"."calendar_date" AS "calendar_date",
       "date_dim"."calendar_half" AS "calendar_half",
       "date_dim"."calendar_mth" AS "calendar_mth",
       "date_dim"."calendar_qtr" AS "calendar_qtr",
       "date_dim"."calendar_week" AS "calendar_week",
       "date_dim"."calendar_year" AS "calendar_year",
       "date_dim"."day_of_mth" AS "day_of_mth",
       "date_dim"."day_of_week" AS "day_of_week",
       "date_dim"."day_of_year" AS "day_of_year",
       "date_dim"."days_in_mth" AS "days_in_mth",
       "date_dim"."days_in_qtr" AS "days_in_qtr",
       "date_dim"."fiscal_half" AS "fiscal_half",
       "date_dim"."fiscal_mth" AS "fiscal_mth",
       "date_dim"."fiscal_qtr" AS "fiscal_qtr",
       "date_dim"."fiscal_week" AS "fiscal_week",
       "date_dim"."fiscal_year" AS "fiscal_year",
       "date_dim"."julian_day_num" AS "julian_day_num",
       "date_dim"."julian_mth_num" AS "julian_mth_num",
       "date_dim"."julian_qtr_num" AS "julian_qtr_num",
       "date_dim"."julian_week_num" AS "julian_week_num",
       "date_dim"."period_name_half" AS "period_name_half",
       "date_dim"."period_name_mth" AS "period_name_mth",
       "date_dim"."period_name_qtr" AS "period_name_qtr",
       "date_dim"."period_name_week" AS "period_name_week",
       "date_dim"."period_name_year" AS "period_name_year",
       "date_dim"."reporting_week" AS "reporting_week",
       "date_dim"."reporting_year" AS "reporting_year",
       "date_dim"."rpt_period_name_week" AS "rpt_period_name_week",
       "date_dim"."rpt_period_name_year" AS "rpt_period_name_year",
       "date_dim"."rolling_day" AS "rolling_day",
       "date_dim"."rolling_week" AS "rolling_week",
       "date_dim"."rolling_month" AS "rolling_month",
       "date_dim"."rolling_qtr" AS "rolling_qtr",
       "date_dim"."rolling_year" AS "rolling_year",
       "tap_cycle"."cycle_id" AS "cycle_id _tap_cycle_",
       "tap_cycle"."created_date" AS "created_date",
       "tap_cycle"."modified_date" AS "modified_date",
       "tap_cycle"."cycle_name" AS "cycle_name",
       "tap_cycle"."cycle_type" AS "cycle_type",
       "tap_cycle"."lang_code" AS "lang_code",
       "tap_cycle"."target_submissions" AS "target_submissions",
       "tap_cycle"."cycle_state" AS "cycle_state",
       "tap_cycle"."cycle_owner" AS "cycle_owner",
       "tap_cycle"."cycle_priority" AS "cycle_priority",
       "tap_cycle"."cycle_customer" AS "cycle_customer",
       "tap_cycle"."closing_date" AS "closing_date",
       "tap_cycle"."review_required" AS "review_required",
       "tap_cycle"."sim_issue" AS "sim_issue",
       "tap_cycle"."close" AS "close"
FROM (SELECT cycle_id,
             user_id,
             user_login,
             processed_date,
             resulting_testcase_completeness_status AS status,
             COUNT(DISTINCT testcase_id) AS testcase_count,
             SUM(total_duration_seconds) /(60*60::FLOAT) AS processed_hrs,
             SUM(total_duration_seconds_lex) /(60*60::FLOAT) AS processed_hrs_rr
      --to get most recent submission at cycle,testcase,user,date and status level(duration is added)       
             FROM (SELECT *,
                          CASE
                            WHEN resulting_testcase_completeness_status IN ('FINISHED') THEN SUM(
                              CASE
                                WHEN resulting_testcase_completeness_status IN ('FINISHED','NEED_REVIEW') THEN total_duration_seconds
                                ELSE 0
                              END ) OVER (PARTITION BY cycle_id,testcase_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following)
                            ELSE 0
                          END AS total_duration_seconds_lex
                   -- to get the total time taken for review req workflows(lexicon)
                          FROM (SELECT DISTINCT cycle_id,
                                       testcase_id,
                                       user_id,
                                       user_login,
                                       DATE (created_date) AS processed_date,
                                       resulting_testcase_completeness_status,
                                       -- ROW_NUMBER() OVER (PARTITION BY cycle_id,DATE (created_date),user_id,testcase_id,resulting_testcase_completeness_status ORDER BY created_date DESC) AS rn,
                                       FIRST_VALUE(submission_id IGNORE NULLS) OVER (PARTITION BY cycle_id,DATE (created_date),user_id,testcase_id,resulting_testcase_completeness_status ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS latest_submission_id,
                                       -- FIRST_VALUE(created_date IGNORE NULLS) OVER (PARTITION BY cycle_id,DATE (created_date),user_id,testcase_id,resulting_testcase_completeness_status ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS processed_datetime,
                                       SUM(submission_duration_seconds) OVER (PARTITION BY cycle_id,testcase_id,user_id,resulting_testcase_completeness_status ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS total_duration_seconds
                                FROM nvrvd.tap_submission))

      where processed_date>='2019-01-01'

      GROUP BY cycle_id,
               user_id,
               user_login,
               processed_date,
               resulting_testcase_completeness_status) "Test Case Counts"
  INNER JOIN "nvdev"."date_dim" "date_dim" ON ("Test Case Counts"."processed_date" = "date_dim"."calendar_date")
  LEFT JOIN "nvrvd"."tap_cycle" "tap_cycle" ON ("Test Case Counts"."cycle_id" = "tap_cycle"."cycle_id"))rvd

	LEFT JOIN nvads.dim_employee_history deh

	on rvd.user_login = deh.login_name
	and rvd.processed_date >= deh.effective_start_day
	and rvd.processed_date < deh.effective_end_day
	and (rvd.processed_date < deh.termination_day
	or deh.termination_day is null))
	
where cost_center = '5140'
and calendar_year = 2020
--and calendar_mth = 3

group by 1,2,3,4,5,6,7,8,9,10,11,12

	
union all


select
'RAMP' as ADS_process_type,
cast(actual_month as int) as proc_month,
cast(actual_year as int) as proc_year,
'null' as new_da_flag,
country,
site,
'5140' as cost_center,
'null' as queue_type_tag,
task_data_language, 
transformation,
customer_grp_value as vertical,
'null' as device,
sum(processed_count) proc,
sum(production_hours) hours

From nvads.RAMP_edx a
left Join nvdev.date_dim b
On date(a.week_start_date)=b.calendar_date

Where cost_center = '5140'
and cast(actual_year as int) = 2020
--and cast(actual_month as int) = 3

group by 1,2,3,4,5,6,7,8,9,10,11,12

)
Group by proc_year, proc_month, cost_center, country,ads_process_type
