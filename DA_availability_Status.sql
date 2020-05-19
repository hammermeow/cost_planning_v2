SELECT TOP 50 user_id,
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
              
----
SELECT Top 50 CAST(user_id AS TEXT) "employee_id",
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
      AND TRUNC(fa.start_datetime) between '2019-03-28' and '2019-03-28'
      AND CAST(user_id AS TEXT) = '102593458'
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
               ad."login_name"
