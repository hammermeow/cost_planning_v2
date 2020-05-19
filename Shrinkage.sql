-- removes business_days calc for weekly_staff_hours
-- adds end_datetime_utc

SELECT
	a.*,
	TRUNC(a.start_datetime) AS start_date,
	TRUNC(DATE_TRUNC('WEEK', a.start_datetime+1)-1) AS week_start_date,
	dat.rpt_period_name_week AS report_week_name,
	dat.reporting_week
	
FROM
(
	SELECT DISTINCT 
		ana.employee_id,
		ana.supervisor_login_name,
		ab.login_name,
		CASE WHEN (ana.building ILIKE '%%VIR%%' OR ana.building ILIKE '%%VCC%%' OR ana.building ILIKE '%%VPA%%' OR ana.building ILIKE '%%VNY%%' OR ana.building ILIKE '%%VCA%%' OR ana.building ILIKE '%%VIL%%') THEN 'Y'
			WHEN ana.amzn_loc_desc ILIKE '%%vir%%' THEN 'Y'
			ELSE 'N'
			END AS is_vir,
		ana.building,
		ana.country,
		LEFT(ana.rollup_site,3) AS rollup_site,
		ana.role,
		ana.business_title,
		ana.department_id,
		ana.weekly_staff_hrs,
		CASE WHEN map.level1 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level1,25,100)
			WHEN map.level1 ILIKE 'ads_central_user%%' AND map.level1 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level1,18,100)
			END AS status_l1,
		CASE WHEN map.level2 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level2,25,100)
			WHEN map.level2 ILIKE 'ads_central_user%%' AND map.level2 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level2,18,100)
			END AS status_l2,
		CASE WHEN map.level3 = 'ads_central_user_status_l3_daily_hudle' THEN 'l3_daily_huddle'
			WHEN map.level3 = 'ads_central_user_status_l3_poject_analysis' THEN 'l3_project_analysis'
			WHEN map.level3 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level3,25,100)
			WHEN map.level3 ILIKE 'ads_central_user%%' AND map.level3 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level3,18,100)
			END AS status_l3,
		ava.start_datetime AS start_datetime_utc,
		CASE WHEN ana.country = 'JPN' THEN CONVERT_TIMEZONE('Japan', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'IND' THEN CONVERT_TIMEZONE('Asia/Calcutta', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'USA' THEN CONVERT_TIMEZONE('US/Eastern', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'CAN' THEN CONVERT_TIMEZONE('Canada/Eastern', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'POL' THEN CONVERT_TIMEZONE('Poland', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'ROU' THEN CONVERT_TIMEZONE('Europe/Bucharest', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'DEU' THEN CONVERT_TIMEZONE('Europe/Berlin', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'CRI' THEN CONVERT_TIMEZONE('America/Costa_Rica', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'GBR' THEN CONVERT_TIMEZONE('Europe/London', ava.start_datetime::TIMESTAMP)
			WHEN ana.country = 'NLD' THEN CONVERT_TIMEZONE('Europe/Amsterdam', ava.start_datetime::TIMESTAMP)
			ELSE ava.start_datetime::TIMESTAMP
			END AS start_datetime,
		CASE WHEN ana.country = 'JPN' THEN CONVERT_TIMEZONE('Japan', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'IND' THEN CONVERT_TIMEZONE('Asia/Calcutta', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'USA' THEN CONVERT_TIMEZONE('US/Eastern', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'CAN' THEN CONVERT_TIMEZONE('Canada/Eastern', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'POL' THEN CONVERT_TIMEZONE('Poland', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'ROU' THEN CONVERT_TIMEZONE('Europe/Bucharest', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'DEU' THEN CONVERT_TIMEZONE('Europe/Berlin', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'CRI' THEN CONVERT_TIMEZONE('America/Costa_Rica', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'GBR' THEN CONVERT_TIMEZONE('Europe/London', ava.end_datetime::TIMESTAMP)
			WHEN ana.country = 'NLD' THEN CONVERT_TIMEZONE('Europe/Amsterdam', ava.end_datetime::TIMESTAMP)
			ELSE ava.end_datetime::TIMESTAMP
			END AS end_datetime,
		ava.end_datetime AS end_datetime_utc,
		-- cap duration_min at 12 hours
		CASE WHEN ava.duration/60 :: float > 720 THEN 720.0 ELSE ava.duration/60 :: float END AS duration_min,
	  LTRIM(ava.npt_comment) AS comment,
            ROW_NUMBER() OVER (PARTITION BY ava.start_datetime ORDER BY ava.user_id) as row_num,
            ab.Team_Manager,
            ab.tm_business_title,
            ab.Ops_Manager,
            ab.ops_business_title,
            ab.Sr_Ops_Manager,
            ab.Sr_ops_business_title
	FROM nvads.fact_analyst_availability_14days ava
	
	LEFT JOIN nvads.da_mapping_table map
	ON ava.status_i18n_id = map.level3
	
	LEFT JOIN ( 
		SELECT DISTINCT
			ana.employee_id,
			ana.login_name,
			ana.supervisor_login_name,
			ana.amzn_loc_desc,
			ana.building,
			ana.country,
			ana.rollup_site,
			ana.department_id,
			wud.work_status AS role,
			wud.business_title,
			wud.file_date,
			wud.weekly_staff_hrs,
			ROW_NUMBER() OVER (PARTITION BY wud.user_login, wud.file_date ORDER BY wud.load_time DESC) AS rank_no
		FROM nvdev.wbr_analyst_dim ana
		
		LEFT JOIN nvdev.wbr_user_details_hist wud
		ON wud.user_login = ana.login_name
                
	) ana
	ON ava.user_id = ana.employee_id
	AND (CASE WHEN ava.start_datetime::DATE=GETDATE()::DATE THEN DATEADD('day',-1,GETDATE()::DATE) ELSE ava.start_datetime::DATE END) = ana.file_date
	AND ana.rank_no = 1
	
	LEFT JOIN
	( 
		Select DISTINCT 
                a.employee_id as employee_id,
				a.login_name as login_name,
				b.login_name as Team_Manager,
                b.business_title as tm_business_title,
                c.login_name as Ops_Manager,
                c.business_title as ops_business_title,
                d.login_name as Sr_Ops_Manager,
                d.business_title as Sr_ops_business_title,
                a.is_active_record
     from   nvdev.analyst_dim a 
     left join nvdev.analyst_dim b on a.supervisor_login_name= b.login_name
     left join nvdev.analyst_dim c on b.supervisor_login_name = c.login_name
     left join nvdev.analyst_dim d on c.supervisor_login_name = d.login_name
        where a.is_active_record='Y'
    ) ab 
	ON ab.employee_id= ava.user_id
      	
	WHERE ava.end_datetime IS NOT NULL
) a

INNER JOIN nvdev.date_dim dat
ON a.start_datetime::date = dat.calendar_date
where 1=1 
--AND rolling_week>='-2'
AND a.row_num= 1
