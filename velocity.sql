SELECT qam.data_type,
       qam.task_queue_id,
       qam.analyst,
       qam.analyst_role,
       qam.manager_flag,
       qam.sla_planned_flg,
       qam.owner_id,
       qam.week_month_start_date,
       qam.sla_volume,
       qam.op2_volume,
       qam.op2_head_count,
       qam.ingested_count,
       qam.processed_count,
       qam.discard_count,
       qam.production_hrs,
       qam.processed_hrs,
       qam.verified_count,
       qam.overturned_count,
       qam.auto_skipped_count,
       qam.expected_hrs,
       qam.weekly_staff_hrs,
       qam.target_volume,
       qam.committed_volume,
       qam.demand_volume,
       qam."load_date _daily_queue_analyst_metrics_",
       qam."analyst_site _daily_queue_analyst_metrics_",
       qam."sla_site _daily_queue_analyst_metrics_",
       qam.target_velocity,
       qam.is_active,
       qam.job_level,
       qam.hc_role,
       qam."task_queue_id _weekly_sla_dim__",
       qam."week_month_start_date _weekly_sla_dim__",
       qam.sla_derived,
       qam."device_environment _weekly sla dimension_",
       qam.priority,
       qam.aim,
       qam.reason_code,
       qam.feature,
       qam.rc_owner,
       qam.ads_pm,
       qam.team_manager,
       qam."table name",
       qam.date_key,
       qam.calendar_date,
       qam.calendar_half,
       qam.calendar_mth,
       qam.calendar_qtr,
       qam.calendar_week,
       qam.calendar_year,
       qam.day_of_mth,
       qam.day_of_week,
       qam.day_of_year,
       qam.days_in_mth,
       qam.days_in_qtr,
       qam.fiscal_half,
       qam.fiscal_mth,
       qam.fiscal_qtr,
       qam.fiscal_week,
       qam.fiscal_year,
       qam.julian_day_num,
       qam.julian_mth_num,
       qam.julian_qtr_num,
       qam.julian_week_num,
       qam.period_name_half,
       qam.period_name_mth,
       qam.period_name_qtr,
       qam.period_name_week,
       qam.period_name_year,
       qam.reporting_week,
       qam.reporting_year,
       qam.rpt_period_name_week,
       qam.rpt_period_name_year,
       qam.rolling_day,
       qam.rolling_week,
       qam.rolling_month,
       qam.rolling_qtr,
       qam.rolling_year,
       qam."task_queue_id _wbr_task_queue_dim_",
       qam.task_queue_name,
       qam.action_type,
       qam.device_or_initiative,
       qam.vertical_or_sub_initiative,
       qam.new_vertical,
       qam.task_data_language,
       qam.queue_type,
       qam.device_environment,
       qam.sla_frequency,
       qam.skill_mapping,
       qam.workflow_locale,
       qam.ambiguous_flag,
       qam.gsr_categorization,
       qam.initiative,
       qam.deliverable,
       qam.LANGUAGE,
       qam.user_group,
       qam.dteam,
       qam.accuracy,
       qam.training,
       qam.transformation_type,
       qam.new_transformation_type,
       qam.task_queue_id_key,
       qam.marketplace,
       qam.workflow_type,
       qam.queue_created_time,
       qam.last_queue_modified_time,
       qam.queue_deleted_time,
       qam.load_date,
       qam.convention,
       qam.created_by_user,
       qam.data_region,
       qam.description,
       qam.last_modified_by_user,
       qam.primary_analyst_timezone,
       qam.skip_completed,
       qam.task_order,
       qam.task_queue_type,
       qam.atat_transformation,
       qam.skill_type,
       qam."employee_id _custom sql query_",
       qam."login_name _custom sql query_",
       qam."employee_name _custom sql query_",
       qam."department_id _custom sql query_",
       qam."department_name _custom sql query_",
       qam."supervisor_id _custom sql query_",
       qam."supervisor_login_name _custom sql query_",
       qam."supervisor_name _custom sql query_",
       qam."amzn_loc_desc _custom sql query_",
       qam."business_title _custom sql query_",
       qam."job_code _custom sql query_",
       qam."job_title _custom sql query_",
       qam."job_family _custom sql query_",
       qam."location _custom sql query_",
       qam."country _custom sql query_",
       qam."city _custom sql query_",
       qam."building _custom sql query_",
       qam."hire_date _custom sql query_",
       qam."termination_day _custom sql query_",
       qam."is_active_record _custom sql query_",
       qam."is_employed _custom sql query_",
       qam."dw_creation_date _custom sql query_",
       qam."dw_last_updated _custom sql query_",
       qam."company _custom sql query_",
       qam."last_insert_date _custom sql query_",
       qam."skill_map_site _custom sql query_",
       qam."rollup_site _custom sql query_",
       qam."actual_site _custom sql query_",
       qam.rn,
       qam.OWNER,
       dtqc.root_task_queue_id,
       dtqc.queue_chain_id,
       dtqc.task_queue_is_verification,
       dtqc.position_in_chain,
       dtqc.all_queues_in_chain,
       qam.op2_target_velocity,
       qam.vertical_op2_target_velocity,
       qam.manual_verified_count AS manual_verified_count_temp,
       qam.auto_verified_count AS auto_verified_count_temp,
       (qam.verified_count - nvl(ttp.auto_verified_count,0)) AS manual_verified_count,
       --verified_count_ttp,
       ttp.auto_verified_count,
       -- ttp_prc.processed_count AS processed_count_ttp,
       nvl(ttp_prc.auto_processed_count,0) AS auto_processed_count,
       qam.processed_count - nvl(ttp_prc.auto_processed_count,0) AS manual_processed_count,
       qam.volume_per_week
FROM nvrpt.daily_queue_analyst_metrics_vw qam
  LEFT JOIN (SELECT ttp.processed_date,
                    ttp.task_queue_id,
                    ttp.analyst,
                    -- SUM(ttp.verified_cnt) AS verified_count_ttp,
                    -- SUM(ttp.manual_verified_cnt) AS manual_verified_cnt,
                    SUM(ttp.verif_auto_saved_cnt) AS auto_verified_count
             --,SUM(ttp.verified_cnt) - SUM(ttp.verif_auto_saved_cnt) AS manual_verified_count
                    FROM nvads.fact_ttop_audit_summary ttp
             GROUP BY 1,
                      2,
                      3) ttp
         ON qam.task_queue_id = ttp.task_queue_id
        AND qam.analyst = ttp.analyst
        AND qam.calendar_date = ttp.processed_date
        AND qam.data_type = 'Actuals'
  LEFT JOIN (SELECT verif_analyst AS analyst,
                    verif_date AS processed_date,
                    verif_task_queue_id AS task_queue_id,
                    COUNT(DISTINCT verif_utterance_id) AS processed_count,
                    COUNT(DISTINCT CASE WHEN verif_auto_save = 1 THEN verif_utterance_id END) AS auto_processed_count
             FROM nvads.fact_ttop_utterance
             WHERE verif_utterance_Id IS NOT NULL
             -- AND   dedup_rn = 1
             GROUP BY verif_analyst,
                      verif_date,
                      verif_task_queue_id) ttp_prc
         ON qam.task_queue_id = ttp_prc.task_queue_id
        AND qam.analyst = ttp_prc.analyst
        AND qam.calendar_date = ttp_prc.processed_date
        AND qam.data_type = 'Actuals'
  LEFT JOIN (SELECT DISTINCT a.*,
                    COUNT(task_queue_id) OVER (PARTITION BY root_task_queue_id) all_queues_in_chain
             FROM nvads.dim_task_queue_chain a) dtqc ON qam.task_queue_id = dtqc.task_queue_id
