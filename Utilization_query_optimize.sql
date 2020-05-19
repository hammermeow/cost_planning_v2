SELECT "wbr_analyst_dim"."employee_id" AS "employee_id",
       "wbr_analyst_dim"."login_name" AS "login_name",
       "wbr_analyst_dim"."supervisor_id" AS "supervisor_id",
       "wbr_analyst_dim"."supervisor_login_name" AS "supervisor_login_name",
       "wbr_analyst_dim"."amzn_loc_desc" AS "amzn_loc_desc",
       "wbr_analyst_dim"."business_title" AS "business_title",
       "wbr_analyst_dim"."job_code" AS "job_code",
       "wbr_analyst_dim"."job_title" AS "job_title",
       "wbr_analyst_dim"."job_family" AS "job_family",
       "wbr_analyst_dim"."location" AS "location",
       "wbr_analyst_dim"."country" AS "country",
       "wbr_analyst_dim"."city" AS "city",
       "wbr_analyst_dim"."building" AS "building",
       "wbr_analyst_dim"."hire_date" AS "hire_date",
       analyst_hist."alexa_start_date" AS "alexa_start_date",
       "wbr_analyst_dim"."termination_day" AS "termination_day",
       "wbr_analyst_dim"."is_active_record" AS "is_active_record",
       "wbr_analyst_dim"."is_employed" AS "is_employed",
       "wbr_analyst_dim"."skill_map_site" AS "skill_map_site",
       Left("wbr_analyst_dim"."rollup_site", 3) AS "rollup_site",
       "wbr_analyst_dim".department_id,
       aai."Status",
       aai."status_l2",
       aai."status_l3",
       aai."current_status",
       aai."level3",
       aai."level2",
       aai."level1",
       CASE WHEN aai.duration_hours :: FLOAT > 12.0 THEN 12.0 ELSE aai.duration_hours :: FLOAT  END AS duration_hours_capped,
       aai.duration_hours,
       a.role,
       a.weekly_staff_hrs,
       a.manager_flag,
       a.hc_role,
       a.job_level,
       a.is_active,
       NVL(aai.week_start_day,a.week_start_date) AS "Week_Start_Date",
       "date_dim"."calendar_date" AS "calendar_Date",
       "date_dim"."calendar_year" AS "calendar_year",
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
       "date_dim"."rolling_year" AS "rolling_year"
FROM (SELECT CAST(user_id AS TEXT) "employee_id",
             current_status AS "Status",
             status_l2,
             status_l3,
             current_status,
             level3,
             level2,
             level1,
             TRUNC(start_datetime) AS Work_date,
             cl.start_date AS week_start_day,
             ad."login_name" AS "login_name",
             SUM(duration / 3600::FLOAT) AS duration_hours
      FROM (SELECT user_id,
                   COMMENT,
                   start_datetime,
                   duration,
                   CASE
                     WHEN level1 ilike '%atat_production%' THEN 'ATAT Production'
                     WHEN level1 ilike '%production_non_atat%' THEN 'Production-Non ATAT'
                     WHEN level1 ilike '%idle_no_data%' THEN 'Idle - No Data'
                     ELSE INITCAP(REPLACE(
                       CASE
                         WHEN map.level1 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level1,25,100)
                         WHEN map.level1 ILIKE 'ads_central_user%%' AND map.level1 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level1,18,100)
                       END ,'_',' '))
                   END AS current_status,
                   -- new handling current status column to reflect the right data for historic metrics as per changes in https://sim.amazon.com/issues/TPROG-4551
                   CASE
                     WHEN map.level2 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level2,25,100)
                     WHEN map.level2 ILIKE 'ads_central_user%%' AND map.level2 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level2,18,100)
                   END AS status_l2,
                   CASE
                     WHEN map.level3 = 'ads_central_user_status_l3_daily_hudle' THEN 'l3_daily_huddle'
                     WHEN map.level3 ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level3,25,100)
                     WHEN map.level3 ILIKE 'ads_central_user%%' AND map.level3 NOT ILIKE 'ads_central_user_status%%' THEN SUBSTRING(map.level3,18,100)
                   END AS status_l3,
                   map.*
            FROM nvrpt.fact_da_availability_daily ava
              LEFT JOIN nvads.da_mapping_table MAP ON ava.status_i18n_id = map.level3
            --where TRUNC(start_datetime) > '2018-11-01'
            ) fa,
           Nvdev.wbr_analyst_dim ad,
           nvdev.wbr_week_cal cl
      WHERE TRUNC(fa.start_datetime) BETWEEN cl.start_date AND cl.end_date
      AND   ad."employee_id" = fa.user_id
      GROUP BY CAST(user_id AS TEXT),
               current_status,
               status_l2,
               status_l3,
               current_status,
               level3,
               level2,
               level1,
               TRUNC(start_datetime),
               cl.start_date,
               ad."login_name") aai
  FULL OUTER JOIN (SELECT *
                   FROM (SELECT user_id,
                                user_login AS analyst,
                                role AS hc_role,
                                is_active,
                                job_level,
                                work_status AS role,
                                file_date AS calendar_date,
                                wwc.start_date AS week_start_date,
                                wud.weekly_staff_hrs*business_days / 5::FLOAT AS weekly_staff_hrs,
                                CASE
                                  WHEN UPPER(wud.business_title) LIKE '%MANAGER%' THEN 'M'
                                  ELSE 'NM'
                                END Manager_Flag,
                                ROW_NUMBER() OVER (PARTITION BY user_login,file_date ORDER BY load_time DESC) AS RANK_no
                         FROM nvdev.wbr_user_details_hist wud
                           JOIN nvdev.wbr_week_cal wwc
                             ON wud.file_date BETWEEN wwc.start_date
                            AND wwc.end_date
                         -- WHERE wud.file_date >= '2018-10-01'
                         )
                   WHERE RANK_no = 1) a
               ON (a.analyst = aai.login_name
              AND aai.Work_date = a.calendar_date)
  JOIN "nvdev"."date_dim" "date_dim"
    ON (NVL (aai.Work_date,a.calendar_date) = "date_dim"."calendar_date"
   AND "date_dim".rolling_month >= -13)
  JOIN Nvdev.wbr_analyst_dim "wbr_analyst_dim" ON (NVL (aai.login_name,a.analyst) = "wbr_analyst_dim".login_name)
  LEFT OUTER JOIN (SELECT login_name,
                          MIN(effective_start_day) AS alexa_start_date
                   FROM nvads.dim_employee_history a
                   WHERE department_id IN ('5140','7327')
                   GROUP BY 1) analyst_hist ON NVL (aai.login_name,a.analyst) = analyst_hist.login_name


