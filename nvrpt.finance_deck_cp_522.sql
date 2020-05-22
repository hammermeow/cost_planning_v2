DROP VIEW nvrpt.finance_deck_cp;

CREATE OR REPLACE VIEW nvrpt.finance_deck_cp
(
ads_process_type,
analyst,
badge_color,
supervisor_id,
supervisor_login_name,
week,
month,
quarter,
year,
reporting_year,
reporting_week,
country,
site,
cost_center,
priority,
queue_type,
task_data_language,
transformation,
root_transformation,
transformation_type,
task_queue_type,
device_environment,
ads_service,
order_name,
service_order_id,
customer_grp_value,
program,
vertical_or_sub_initiative,
device_or_initiative,
task_queue_id,
task_queue_name,
sla_planned_flg,
owner_id,
rolling_month,
rolling_week,
rolling_qtr,
rolling_year,
fiscal_mth,
fiscal_qtr,
fiscal_week,
fiscal_year,
period_name_mth,
period_name_qtr,
period_name_week,
period_name_year,
rpt_period_name_week,
rpt_period_name_year,
auto_verified_volume,
manual_verified_volume,
auto_processed_volume,
verified_volume,
weekly_staff_hrs,
weekly_allocated_hrs,
total_working_hrs,
op2_volume,
target_volume,
committed_volume,
ingested_count,
discard_count,
auto_skipped_count,
overturned_count,
processed_volume,
production_hours,
sla_volume,
actual_site, 
rollup_site, 
amzn_loc_desc,
customer,
parent_work_type,
work_type,
workflow_app,
base_verify_workflow_app,
core_non_core_workflow_app,
average_da_hourly_wage,
opex_total_cph_atat_hours_denominator,
opex_total_cph_all_hours_denominator,
opex_labor_cph_atat_hours_denominator,
opex_labor_cph_all_hours_denominator,
op2_total_cph_atat_hours_denominator,
op2_total_cph_all_hours_denominator,
op2_labor_cph_atat_hours_denominator,
op2_labor_cph_all_hours_denominator,
quarterly_total_cph_atat_hours_denominator,
quarterly_total_cph_all_hours_denominator,
quarterly_labor_cph_atat_hours_denominator,
quarterly_labor_cph_all_hours_denominator,
normalized_opex_total_cph_atat_hours_denominator,
normalized_opex_total_cph_all_hours_denominator,
normalized_opex_labor_cph_atat_hours_denominator,
normalized_opex_labor_cph_all_hours_denominator,
manifest_average_da_hourly_wage,
manifest_opex_total_cph_atat_hours_denominator,
manifest_opex_total_cph_all_hours_denominator,
manifest_opex_labor_cph_atat_hours_denominator,
manifest_opex_labor_cph_all_hours_denominator,
manifest_op2_total_cph_atat_hours_denominator,
manifest_op2_total_cph_all_hours_denominator,
manifest_op2_labor_cph_atat_hours_denominator,
manifest_op2_labor_cph_all_hours_denominator,
manifest_quarterly_total_cph_atat_hours_denominator,
manifest_quarterly_total_cph_all_hours_denominator,
manifest_quarterly_labor_cph_atat_hours_denominator,
manifest_quarterly_labor_cph_all_hours_denominator,
manifest_normalized_opex_total_cph_atat_hours_denominator,
manifest_normalized_opex_total_cph_all_hours_denominator,
manifest_normalized_opex_labor_cph_atat_hours_denominator,
manifest_normalized_opex_labor_cph_all_hours_denominator,
atat_mercury_cost,
opex,
atat_mercury_labor_cost,
opex_labor,
manifest_atat_mercury_cost,
manifest_opex,
manifest_atat_mercury_labor_cost,
manifest_opex_labor
)
as
(SELECT 
        finance_deck.*,
        e.actual_site, e.rollup_site, e.amzn_loc_desc, 
        ----get customers START----
        CASE WHEN (lower(task_queue_name) LIKE '%certify%' or lower(task_queue_name) LIKE '%gsr-verify-verify%' 
             or lower(task_queue_name) LIKE '%verify-verify-verify%') then 'ADS-Quality'
             
        WHEN (LOWER(queue_type) LIKE '%training%' OR LOWER(queue_type) LIKE '%accuracy%' 
             or LOWER(queue_type) LIKE '%error categorization%') THEN 'ADS-WFM'
             
        WHEN LOWER([priority]) LIKE '%accuracy%' THEN 'ADS-Quality'
        WHEN LOWER([priority]) LIKE '%training%' THEN 'ADS-WFM'
        WHEN (lower(task_queue_name) LIKE '%transcription audit%' or LOWER(queue_type)='quality') THEN 'ADS-Quality'
        WHEN LOWER(queue_type) LIKE '%training%' THEN 'ADS-WFM'
        WHEN lower(task_queue_name) LIKE '%ads-wfm%' then 'ADS-WFM'
        
        WHEN (customer_grp_value is null and vertical_or_sub_initiative='Other' And (lower(queue_type) LIKE '%eeep%' 
            or lower(task_queue_name) like '%fx-clue%' or task_queue_name LIKE '%FX-CLUE%' or lower(task_queue_name) like '%ramp%' 
            or task_queue_name like '% FX-CLUE%' or task_queue_name LIKE '%FX-CLUE %' or lower(task_queue_name) LIKE '%clue%'))
                    then 'ADS-RAMP'
        WHEN (lower(task_queue_name) LIKE '%privacy_enhancements%' AND (lower(vertical_or_sub_initiative) LIKE '%other%' 
            or lower(vertical_or_sub_initiative) LIKE '%ads-wfm%' or lower(vertical_or_sub_initiative) LIKE '%ads-science%')) then 'ADSC Tech'
-------------------------above should be fixed------------------
        When (customer_grp_value like 'ADS Internal%') then 'ADS Internal'--RAMP data ERROR
        WHEN (customer_grp_value is null and vertical_or_sub_initiative='Alexa Fitness') then 'Echo Software Experience'
        WHEN (customer_grp_value = 'ADS-Science' and vertical_or_sub_initiative= 'ADS-Science')then 'ADS-Product'        
        WHEN (customer_grp_value is null and LOWER(transformation) LIKE '%wake word%') then 'Wakeword'    
        --WHEN (customer_grp_value is null and vertical_or_sub_initiative='Echo Devices') then 'Echo Devices'
        WHEN (customer_grp_value is null and lower(vertical_or_sub_initiative) LIKE '%1p%') then 'Echo Devices'
        
        WHEN (customer_grp_value is null and lower(vertical_or_sub_initiative) LIKE '%2p%') then 'Fire TV Devices and Experience'
        WHEN (customer_grp_value is null and vertical_or_sub_initiative='Alexa Enabled Amazon Devices') then 'Fire TV Devices and Experience'
        WHEN (customer_grp_value = '2P Devices' and vertical_or_sub_initiative = '2P Devices') then 'Fire TV Devices and Experience'
        WHEN (customer_grp_value='Alexa Enabled Amazon Devices' and vertical_or_sub_initiative='Alexa Enabled Amazon Devices') then 'Fire TV Devices and Experience'
       
        WHEN ((customer_grp_value='NLU 2.0' and vertical_or_sub_initiative='NLU 2.0')
              or (customer_grp_value is null and vertical_or_sub_initiative='Other' and device_or_initiative= 'NLU 2.0')) then 'Complex Utterances'
        WHEN (customer_grp_value='Auto/Navigation' or customer_grp_value='Automotive' or customer_grp_value='Auto'
              or (customer_grp_value is null and (vertical_or_sub_initiative='Other' and device_or_initiative='Auto'))) then 'Alexa Automotive'
              
        when (customer_grp_value=' Alexa Experience and Devices' or customer_grp_value='Alexa Alexa Experience and Devices' --RAMP ERROR
              or customer_grp_value='Alexa Experience & Devices (EAD)' or customer_grp_value LIKE 'Alexa Experience%'
              or vertical_or_sub_initiative='Alexa Experience and Devices (EAD)' or vertical_or_sub_initiative='Alexa Experience & Devices (EAD)'
              or vertical_or_sub_initiative LIKE 'Alexa Experience%' )
              then 'Alexa Experience and Devices (EAD)'
              
        WHEN (customer_grp_value is null and vertical_or_sub_initiative in ('Alexa Brain')) then 'Alexa Intelligent Decisions'
        WHEN (customer_grp_value is null and device_or_initiative in ('Alexa Brain')) then 'Alexa Intelligent Decisions'
        WHEN (customer_grp_value='ID') then 'Alexa Intelligent Decisions'
        WHEN (customer_grp_value='Alexa Communication') then 'Communication'
        WHEN (customer_grp_value is null and device_or_initiative='Other' and vertical_or_sub_initiative='Dylan VBM') then 'VBM'
        WHEN (customer_grp_value is null and vertical_or_sub_initiative='Other' and device_or_initiative='Dylan VBM') then 'VBM'
        when (customer_grp_value= 'Household Organization (HHO)' or 
             (customer_grp_value is null and vertical_or_sub_initiative='Household Organization (HHO)') or
             (customer_grp_value is null and vertical_or_sub_initiative='HHO')) then 'HHO'
        when (vertical_or_sub_initiative='Information non-QA') then 'Information'
        when (customer_grp_value='SmartHome') then 'Smart Home'
        when (customer_grp_value='Health & Wellness') then 'Health and Wellness'
        when (customer_grp_value is null and device_or_initiative='Hybrid') then 'Alexa Hybrid'
        when (customer_grp_value is null and vertical_or_sub_initiative='Other' and device_or_initiative='Presence Detection') then 'Audio Event Detection (AED)'
        WHEN (customer_grp_value is null and vertical_or_sub_initiative<>'Other') then vertical_or_sub_initiative
        WHEN (customer_grp_value is null and vertical_or_sub_initiative='Other') then device_or_initiative
        else customer_grp_value
        end as customer,
        ----get customers END----
        d.parent_work_type, 
        d.work_type, 
        d.workflow_app, 
        d.base_verify_workflow_app, 
        d.core_non_core_workflow_app,

        b.average_da_hourly_wage,
        b.opex_total_cph_atat_hours_denominator,
        b.opex_total_cph_all_hours_denominator,
        b.opex_labor_cph_atat_hours_denominator,
        b.opex_labor_cph_all_hours_denominator,
        b.op2_total_cph_atat_hours_denominator,
        b.op2_total_cph_all_hours_denominator,
        b.op2_labor_cph_atat_hours_denominator,
        b.op2_labor_cph_all_hours_denominator,
        b.quarterly_total_cph_atat_hours_denominator,
        b.quarterly_total_cph_all_hours_denominator,
        b.quarterly_labor_cph_atat_hours_denominator,
        b.quarterly_labor_cph_all_hours_denominator,
        b.normalized_opex_total_cph_atat_hours_denominator,
        b.normalized_opex_total_cph_all_hours_denominator,
        b.normalized_opex_labor_cph_atat_hours_denominator,
        b.normalized_opex_labor_cph_all_hours_denominator,
        c.average_da_hourly_wage as manifest_average_da_hourly_wage,
        c.opex_total_cph_atat_hours_denominator as manifest_opex_total_cph_atat_hours_denominator,
        c.opex_total_cph_all_hours_denominator as manifest_opex_total_cph_all_hours_denominator,
        c.opex_labor_cph_atat_hours_denominator as manifest_opex_labor_cph_atat_hours_denominator,
        c.opex_labor_cph_all_hours_denominator as manifest_opex_labor_cph_all_hours_denominator,
        c.op2_total_cph_atat_hours_denominator as manifest_op2_total_cph_atat_hours_denominator,
        c.op2_total_cph_all_hours_denominator as manifest_op2_total_cph_all_hours_denominator,
        c.op2_labor_cph_atat_hours_denominator as manifest_op2_labor_cph_atat_hours_denominator,
        c.op2_labor_cph_all_hours_denominator as manifest_op2_labor_cph_all_hours_denominator,
        c.quarterly_total_cph_atat_hours_denominator as manifest_quarterly_total_cph_atat_hours_denominator,
        c.quarterly_total_cph_all_hours_denominator as manifest_quarterly_total_cph_all_hours_denominator,
        c.quarterly_labor_cph_atat_hours_denominator as manifest_quarterly_labor_cph_atat_hours_denominator,
        c.quarterly_labor_cph_all_hours_denominator as manifest_quarterly_labor_cph_all_hours_denominator,
        c.normalized_opex_total_cph_atat_hours_denominator as manifest_normalized_opex_total_cph_atat_hours_denominator,
        c.normalized_opex_total_cph_all_hours_denominator as manifest_normalized_opex_total_cph_all_hours_denominator,
        c.normalized_opex_labor_cph_atat_hours_denominator as manifest_normalized_opex_labor_cph_atat_hours_denominator,
        c.normalized_opex_labor_cph_all_hours_denominator as manifest_normalized_opex_labor_cph_all_hours_denominator,
        case when cost_center='5140' then (production_hours*b.opex_total_cph_atat_hours_denominator) end as ATAT_Mercury_Cost,
        case when cost_center='5140' then (production_hours*b.opex_total_cph_all_hours_denominator) end as OPEX,
        case when cost_center='5140' then (production_hours*b.opex_labor_cph_atat_hours_denominator) end as ATAT_Mercury_Labor_Cost,
        case when cost_center='5140' then (production_hours*b.opex_labor_cph_all_hours_denominator) end as OPEX_Labor,
        case when cost_center='5140' then (production_hours*c.opex_total_cph_atat_hours_denominator) end as manifest_ATAT_Mercury_Cost,
        case when cost_center='5140' then (production_hours*c.opex_total_cph_all_hours_denominator) end as manifest_OPEX,
        case when cost_center='5140' then (production_hours*c.opex_labor_cph_atat_hours_denominator) end as manifest_ATAT_Mercury_Labor_Cost,
        case when cost_center='5140' then (production_hours*c.opex_labor_cph_all_hours_denominator) end as manifest_OPEX_Labor

FROM
--------------------------------finance_deck START-------------------------------------------
--------------------ATAT Part START----------------------------------------------------------
(select
'ATAT/Mercury' as ads_process_type,
analyst,
badge_color,
supervisor_id,
supervisor_login_name,
calendar_week as week,
calendar_mth as month,
calendar_qtr as quarter,
calendar_year as year,
reporting_year,
reporting_week,
country,
building as site, 
cost_center, 
priority,
queue_type, 
task_data_language,
transformation, 
root_transformation, 
transformation_type,
task_queue_type, 
device_environment, 
ADS_service, 
order_name,
service_order_id,
customer_grp_value,
program,
vertical_or_sub_initiative, 
device_or_initiative,
task_queue_id, 
task_queue_name,
sla_planned_flg,
--convention, 
--owner, 
owner_id,
--aim,
--reason_code,
rolling_month,
rolling_week,
rolling_qtr,
rolling_year,
fiscal_mth,
fiscal_qtr,
fiscal_week,
fiscal_year,
period_name_mth,
period_name_qtr,
period_name_week,
period_name_year,
rpt_period_name_week,
rpt_period_name_year,
sum(Auto_verified_volume) as Auto_verified_volume,
sum(Manual_verified_volume) as Manual_verified_volume,
sum(auto_processed_volume) as Auto_processed_volume,
sum(Verified_volume) as Verified_volume,
cast(avg(weekly_staff_hrs) as integer) as weekly_staff_hrs, 
cast(avg(weekly_allocated_hrs) as integer) as weekly_allocated_hrs, 
cast(avg(total_working_hrs) as integer) as total_working_hrs,
sum(op2_volume) as op2_volume,
sum(target_volume) as target_volume,
sum(committed_volume) as committed_volume,
sum(ingested_count) as ingested_count,
sum(discard_count) as discard_count,
sum(auto_skipped_count) as auto_skipped_count,
sum(overturned_count) as overturned_count,
sum(processed) as processed_volume, 
sum(hours) as production_hours,
sum(sla_volume) as sla_volume

from
	( --------------------ATAT Base table start--------------------------------------------------------
	SELECT
  fqat.analyst,
  b.badge_color,
  deh.supervisor_id,
  deh.supervisor_login_name,
  deh.department_id cost_center,
  deh.building,
  deh.country,
  t0.priority,--nvrpt.daily_queue_analyst_metrics table vs.view
  dtq.task_queue_id,
  dtq.task_queue_name,
  dtq.task_queue_type,
  fqat.sla_planned_flg,  
  dtq.queue_type_tag as queue_type,
  dtq.task_data_language,
  dtq.transformation,
  --fqat.convention,
  --fqat.owner,
  fqat.owner_id,
  --fqat.aim,
  --fqat.reason_code,
  dtq2.transformation as root_transformation,
  dtq2.device_environment, --from root_task_queue_id
  dtq.vertical_or_sub_initiative,
  dtq.device_or_initiative,
  fqat.calendar_date,
  fso2.ADS_service,
  fso2.order_name,
  "date dimension".reporting_year,
  "date dimension".calendar_week,
  "date dimension".calendar_mth,
  "date dimension".calendar_qtr,
  "date dimension".reporting_week,
  "date dimension".calendar_year,
  wtqd.transformation_type,
  adsc.service_order_id,
  cust.customer_grp_value,
  fso.program,
  "date dimension".rolling_month,
  "date dimension".rolling_week,
  "date dimension".rolling_qtr,
  "date dimension".rolling_year,
  "date dimension".fiscal_mth,
  "date dimension".fiscal_qtr,
  "date dimension".fiscal_week,
  "date dimension".fiscal_year,
  "date dimension".period_name_mth,
  "date dimension".period_name_qtr,
  "date dimension".period_name_week,
  "date dimension".period_name_year,
  "date dimension".rpt_period_name_week,
  "date dimension".rpt_period_name_year,
  b.weekly_staff_hrs,
  b.weekly_allocated_hrs,
  b.total_working_hrs,
  ttp.auto_verified_volume as Auto_verified_volume,
  (fqat.verified_count - nvl(ttp.auto_verified_volume,0)) AS Manual_verified_volume,
  nvl(ttp_prc.auto_processed_count,0) AS auto_processed_volume,
  sum(fqat.verified_count) Verified_volume,
  SUM(fqat.processed_count) processed,
  SUM(fqat.production_hrs) hours,
  sum(fqat.sla_volume) as sla_volume,
  sum(fqat.op2_volume) as op2_volume,
  sum(fqat.target_volume) as target_volume,
  sum(fqat.committed_volume) as committed_volume,
  sum(fqat.ingested_count) as ingested_count,
  sum(fqat.discard_count) as discard_count,
  sum(fqat.auto_skipped_count) as auto_skipped_count,
  sum(fqat.overturned_count) as overturned_count
 
  FROM nvrpt.daily_queue_analyst_metrics fqat
  JOIN nvdev.date_dim "date dimension" ON fqat.calendar_date = "date dimension".calendar_date AND "date dimension".rolling_year >= -1
  LEFT JOIN (SELECT "weekly sla dimension".task_queue_id, "weekly sla dimension".week_month_start_date,  "weekly sla dimension".priority
           FROM nvrpt.weekly_sla_dim "weekly sla dimension") t0 ON fqat.task_queue_id::text = t0.task_queue_id::text AND fqat.week_month_start_date = t0.week_month_start_date
  Left Join nvdev.wbr_task_queue_dim wtqd on fqat.task_queue_id::text = wtqd.task_queue_id_key::text
  LEFT JOIN  nvads.dim_task_queue dtq
        on fqat.task_queue_id = dtq.task_queue_id
  LEFT JOIN nvads.task_queue_dim_adsc adsc --service_order_id
        ON dtq.task_queue_id::text = adsc.task_queue_id::text
  left join nvads.dim_task_queue_chain dtqc --root_task_queue_id
        on dtq.task_queue_id=dtqc.task_queue_id
  left join nvads.dim_task_queue dtq2  --root task queue information
        on dtqc.root_task_queue_id=dtq2.task_queue_id
  left join--customer id
        --(select distinct order_id, max(CUST_GRP_ID) as cust_grp_id from nvads.fact_service_order group by 1) fso
        (select order_id, cust_grp_id, program
         from
            (select distinct order_id, cust_grp_id, program, rank() over (partition by order_id order by order_update_dt desc) as rank 
            from nvads.fact_service_order)
        where rank=1) fso                            
        on adsc.service_order_id = fso.order_id
  left join  nvads.dim_customer_group cust--customer group value
         on fso.cust_grp_id = cust.cust_grp_id
        --(select distinct customer_grp_key, customer_grp_value, max(cust_grp_id) as cust_grp_id from nvads.dim_customer_group group by 1,2) cust
--         (select customer_grp_key, customer_grp_value, cust_grp_id
--         from
--         (select distinct customer_grp_key, customer_grp_value, cust_grp_id, rank() over (partition by CUST_GRP_ID order by insert_date desc) as rank 
--         from nvads.dim_customer_group)
--         where rank=1) cust
        
left join
        (select distinct order_id, order_create_dt, order_update_dt, order_name, aim_work_type as ADS_Service
        from 
              (select distinct order_id, order_create_dt, order_update_dt, order_name, aim_work_type, 
                      rank() over (partition by order_id order by order_update_dt desc) as rank
              from nvads.fact_service_order
              where (lower(order_name) not like '%%[closed]%%' or lower(order_name) not like '%%[closded]%%'))
        where rank = 1) fso2            
        on adsc.service_order_id = fso2.order_id      
        
LEFT JOIN  
        (select deh.supervisor_id,
        deh.supervisor_login_name,
        deh.login_name,
        deh.department_id,
        deh.building,
        deh.country,
        effective_start_day,
        deh.effective_end_day,
        deh.termination_day
        from nvads.dim_employee_history deh
        where deh.effective_start_day>=sysdate-750) deh
        

        on fqat.analyst = deh.login_name
        and fqat.calendar_date >= deh.effective_start_day
        and fqat.calendar_date < deh.effective_end_day
        and (fqat.calendar_date < deh.termination_day
        or deh.termination_day is null)


left join 
        (select analyst, week_start_date, badge_color, max(analyst_expected_hrs) as analyst_expected_hrs , 
                max(weekly_staff_hrs)as weekly_staff_hrs , max(weekly_allocated_hrs) as weekly_allocated_hrs , max(total_working_hrs) as total_working_hrs
        from nvdev.wbr_associate_skill_map_fact
        where date_part('year',week_start_date) >= datepart(year,sysdate)-1
        group by analyst, week_start_date, badge_color
        ) b
        on fqat.analyst=b.analyst and fqat.week_month_start_date=b.week_start_date


LEFT JOIN (SELECT ttp.processed_date,
                    ttp.task_queue_id,
                    ttp.analyst,
                    SUM(ttp.verif_auto_saved_cnt) AS auto_verified_volume
                    FROM nvads.fact_ttop_audit_summary ttp
                    where date_part('year',processed_date) >= datepart(year,sysdate)-1 
                    
             GROUP BY 1,
                      2,
                      3) ttp
         ON fqat.task_queue_id = ttp.task_queue_id
        AND fqat.analyst = ttp.analyst
        AND fqat.calendar_date = ttp.processed_date

  LEFT JOIN (SELECT verif_analyst AS analyst,
                    verif_date AS processed_date,
                    verif_task_queue_id AS task_queue_id,
                    COUNT(DISTINCT verif_utterance_id) AS processed_count,
                    COUNT(DISTINCT CASE WHEN verif_auto_save = 1 THEN verif_utterance_id END) AS auto_processed_count
             FROM nvads.fact_ttop_utterance
             WHERE verif_utterance_Id IS NOT NULL and date_part('year',processed_date) >= datepart(year,sysdate)-1
             -- AND   dedup_rn = 1
             GROUP BY verif_analyst,
                      verif_date,
                      verif_task_queue_id) ttp_prc
         ON fqat.task_queue_id = ttp_prc.task_queue_id
        AND fqat.analyst = ttp_prc.analyst
        AND fqat.calendar_date = ttp_prc.processed_date
        

  where 1=1 
  and date_part('year',fqat.calendar_date) >= datepart(year,sysdate)-1
	and date_trunc('week',fqat.calendar_date) <> date_trunc('week',SYSDATE)

	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,
	49,50,51,52,53)
-----------------------------------------------ATAT base table end----------------------------------------------------------------------------
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,
45,46,47
--------------------------------------------ATAT Part End----------------------------------------------------------------------------------------


union all

--------------------------------------------RVD Part Start----------------------------------------------------------------------------------------
select 
  'RVD' as ads_process_type,
  user_login as analyst,
  'RVD' as badge_color,
  'RVD' as supervisor_id,
  'RVD' as supervisor_login_name,
  calendar_week as week,
  calendar_mth as month,
  calendar_qtr as quarter,
  calendar_year as year,
  reporting_year,
  reporting_week,
  case when country is null then 'Total' else country end as country,
  site, 
  cost_center, 
  'RVD' as priority,
  'RVD' as queue_type, 
  REPLACE(lang_code, '-', '_')  as task_data_language,
  transformation, 
  'RVD' as root_transformation, 
  transformation as transformation_type,
  'RVD' as task_queue_type, 
  'RVD' as device_environment, 
  'RVD' as ADS_service, 
  Cycle_name as order_name,
  'null' as service_order_id,
  cycle_customer as customer_grp_value,
  'null' as program,
  cycle_customer as vertical_or_sub_initiative, 
  cycle_customer as device_or_initiative,
  cycle_id :: text as task_queue_id, 
  cycle_name as task_queue_name,
  'Y' as sla_planned_flg,
  --'RVD' as convention, 
  --Case when cost_center='5140 ' then 'ADS' else null end as owner, 
  null :: int as owner_id,
  --'RVD' as aim,
  --'RVD' as reason_code,
  rolling_month,
  rolling_week,
  rolling_qtr,
  rolling_year,
  fiscal_mth,
  fiscal_qtr,
  fiscal_week,
  fiscal_year,
  period_name_mth,
  period_name_qtr,
  period_name_week,
  period_name_year,
  rpt_period_name_week,
  rpt_period_name_year,
  0 as Auto_Verified_Volume, 
  0 as Manual_Verified_Volume, 
  0 as Auto_processed_volume, 
  0 as Verified_volume,
  0 as weekly_staff_hrs, 
  0 as weekly_allocated_hrs, 
  0 as total_working_hrs,
  0 as op2_volume,
  0 as target_volume,
  0 as committed_volume,
  0 as ingested_count,
  0 as discard_count,
  0 as auto_skipped_count,
  0 as overturned_count,
  testcase_count as processed_volume,
  processed_hrs as production_hours,
  0 as sla_volume
from
  (------------------RVD Fifth Base table rvd+deh START----------------------------------------------------------------------------
  select rvd.*, deh.country, deh.building as site, deh.department_id as cost_center,
  cycle_type || ' ~ work state = ' || cycle_state as transformation

  from
  (------------------RVD Fourth Base table rvd--test case count, date_dim, tap_cyc START-------------------------------------------
    SELECT "Test Case Counts"."cycle_id" AS "cycle_id",
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
    FROM (
-------------------------------RVD Third Base table "Test Case Counts" with processed count and production hours START--------------------------
          SELECT cycle_id,
                       user_id,
                       user_login,
                       processed_date,
                       resulting_testcase_completeness_status AS status,
                       COUNT(DISTINCT testcase_id) AS testcase_count,
                       SUM(total_duration_seconds) /(60*60::FLOAT) AS processed_hrs,
                       SUM(total_duration_seconds_lex) /(60*60::FLOAT) AS processed_hrs_rr
                --to get most recent submission at cycle,testcase,user,date and status level(duration is added)       
           FROM (
                  ---------------RVD Second base table from RVD base table-----to get the total time for total_duration_seconds_lex START----------
                  SELECT *,
                  CASE WHEN resulting_testcase_completeness_status IN ('FINISHED') 
                  THEN SUM(
                            CASE WHEN resulting_testcase_completeness_status IN ('FINISHED','NEED_REVIEW') 
                            THEN total_duration_seconds
                            ELSE 0
                            END) 
                            
                            OVER (PARTITION BY cycle_id,testcase_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following
                           )
                  ELSE 0
                  END AS total_duration_seconds_lex
                  FROM (
                        -------------RVD base table from nvrvd.tap_submission----- to get the total time for each submission START-----------------
                        SELECT DISTINCT cycle_id,
                               testcase_id,
                               user_id,
                               user_login,
                               DATE (created_date) AS processed_date,
                               resulting_testcase_completeness_status,
                               -- ROW_NUMBER() OVER (PARTITION BY cycle_id,DATE (created_date),user_id,testcase_id,resulting_testcase_completeness_status ORDER BY created_date DESC) AS rn,
                               FIRST_VALUE(submission_id IGNORE NULLS) OVER (PARTITION BY cycle_id, DATE(created_date),user_id,testcase_id,
                               resulting_testcase_completeness_status ORDER BY created_date DESC 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS latest_submission_id,
                               -- FIRST_VALUE(created_date IGNORE NULLS) OVER (PARTITION BY cycle_id,DATE (created_date),user_id,testcase_id,resulting_testcase_completeness_status ORDER BY created_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS processed_datetime,
                               SUM(submission_duration_seconds) OVER (PARTITION BY cycle_id,testcase_id,user_id,resulting_testcase_completeness_status 
                               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS total_duration_seconds
                        FROM nvrvd.tap_submission
                        WHERE date_part('year',created_date) >= datepart(year,sysdate)-1)
                        -------------RVD base table from nvrvd.tap_submission----- to get the total time for each submission END----------------------
                   )
                   ---------------RVD Second base table from RVD base table-----to get the total time for total_duration_seconds_lex END------------

      --where 1=1
     -- and processed_date>=sysdate-750

       GROUP BY cycle_id,
                 user_id,
                 user_login,
                 processed_date,
                 resulting_testcase_completeness_status) "Test Case Counts"
-------------------------------RVD Third Base table with processed count and production hours END---------------------------------------------------

  INNER JOIN "nvdev"."date_dim" "date_dim" ON ("Test Case Counts"."processed_date" = "date_dim"."calendar_date")
  LEFT JOIN "nvrvd"."tap_cycle" "tap_cycle" ON ("Test Case Counts"."cycle_id" = "tap_cycle"."cycle_id")
 )rvd
------------------RVD Fourth Base table rvd--test case count, date_dim, tap_cycle END-------------------------------------------

	LEFT JOIN 
	(select deh.supervisor_id,
        deh.supervisor_login_name,
        deh.login_name,
        deh.department_id,
        deh.building,
        deh.country,
        effective_start_day,
        deh.effective_end_day,
        deh.termination_day
        from nvads.dim_employee_history deh
        where deh.effective_start_day>=sysdate-750) deh

	on rvd.user_login = deh.login_name
	and rvd.processed_date >= deh.effective_start_day
	and rvd.processed_date < deh.effective_end_day
	and (rvd.processed_date < deh.termination_day
	or deh.termination_day is null))


------------------RVD Fifth Base table rvd+deh END----------------------------------------------------------------------------
----------------------------------------------------RVD Part END------------------------------------------------------------------------
union all
----------------------------------------------------RAMP Part START------------------------------------------------------------------------
select
'RAMP (non-ATAT and non-Mercury)' as ads_process_type,
'null' as analyst,
'RAMP' as badge_color,
'RAMP' as supervisor_id,
'RAMP' as supervisor_login_name,
cast (week as numeric) as week,
cast (actual_month as numeric) as month,
cast (quarter as numeric) as quarter,
cast (actual_year as numeric) as year,
cast (a.reporting_year as numeric) as reporting_year,
cast (b.reporting_week as numeric) as reporting_week,
country,
site, 
'5140' as cost_center, 
'RAMP' as priority,
'RAMP' as queue_type, 
task_data_language,
transformation, 
'RAMP' as root_transformation, 
transformation_type,
'RAMP' as task_queue_type, 
'RAMP' as device_environment, 
'RAMP' as ADS_service, 
task_queue_name as order_name,
'null' as service_order_id,
customer_grp_value,
'null' as program,
customer_grp_value as vertical_or_sub_initiative, 
customer_grp_value as device_or_initiative,
task_queue_id, 
task_queue_name,
'Y' as sla_planned_flg,
--'RAMP' as convention, 
--'ADS' as owner, 
null :: int as owner_id,
--'RAMP' as aim,
--'RAMP' as reason_code,
rolling_month,
rolling_week,
rolling_qtr,
rolling_year,
fiscal_mth,
fiscal_qtr,
fiscal_week,
fiscal_year,
period_name_mth,
period_name_qtr,
period_name_week,
period_name_year,
rpt_period_name_week,
rpt_period_name_year,
0 as Auto_Verified_Volume, 
0 as Manual_Verified_Volume, 
0 as Auto_processed_volume, 
0 as Verified_volume,
0 as weekly_staff_hrs, 
0 as weekly_allocated_hrs, 
0 as total_working_hrs,
0 as op2_volume,
0 as target_volume,
0 as committed_volume,
0 as ingested_count,
0 as discard_count,
0 as auto_skipped_count,
0 as overturned_count,
sum(cast(processed_count as double precision)) as processed_volume,
sum(cast(production_hours as double PRECISION)) as production_hours,
Sum(cast(sla_volume as double precision)) as sla_volume

From nvads.RAMP_edx a
left Join nvdev.date_dim b
On date(a.week_start_date)=b.calendar_date
where date_part('year',date(a.week_start_date)) >= datepart(year,sysdate)-1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,
42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61
----------------------------------------------------RAMP Part END------------------------------------------------------------------------
) finance_deck


left join 
(select distinct country, year, month, 
Cast(b.average_da_hourly_wage as decimal(14,10)),
Cast(b.opex_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.opex_total_cph_all_hours_denominator as decimal(14,10)),
Cast(b.opex_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.opex_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(b.op2_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.op2_total_cph_all_hours_denominator as decimal(14,10)),
Cast(b.op2_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.op2_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(b.quarterly_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.quarterly_total_cph_all_hours_denominator as decimal(14,10)),
Cast(b.quarterly_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.quarterly_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(b.normalized_opex_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.normalized_opex_total_cph_all_hours_denominator as decimal(14,10)),
Cast(b.normalized_opex_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(b.normalized_opex_labor_cph_all_hours_denominator as decimal(14,10))
from nvads.cph_edx b) b
on finance_deck.country=b.country and finance_deck.month=b.month and finance_deck.year=b.year

left join 
(select distinct country, manifest_year, manifest_month, 
Cast(c.average_da_hourly_wage as decimal(14,10)),
Cast(c.opex_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.opex_total_cph_all_hours_denominator as decimal(14,10)),
Cast(c.opex_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.opex_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(c.op2_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.op2_total_cph_all_hours_denominator as decimal(14,10)),
Cast(c.op2_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.op2_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(c.quarterly_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.quarterly_total_cph_all_hours_denominator as decimal(14,10)),
Cast(c.quarterly_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.quarterly_labor_cph_all_hours_denominator as decimal(14,10)),
Cast(c.normalized_opex_total_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.normalized_opex_total_cph_all_hours_denominator as decimal(14,10)),
Cast(c.normalized_opex_labor_cph_atat_hours_denominator as decimal(14,10)),
Cast(c.normalized_opex_labor_cph_all_hours_denominator as decimal(14,10))
from nvads.cph_edx c) c
on finance_deck.country=c.country and finance_deck.month=c.manifest_month and finance_deck.year=c.manifest_year


left join 
(select distinct parent_work_type, work_type, workflow_app, base_verify_workflow_app, core_non_core_workflow_app
from nvads.ads_workflows_categorical_mappings) d
on finance_deck.transformation=d.workflow_app

left join nvdev.wbr_analyst_dim e 
ON finance_deck.analyst = e.login_name

)
WITH NO SCHEMA binding;

GRANT SELECT ON nvrpt.finance_deck_cp TO nvort;

