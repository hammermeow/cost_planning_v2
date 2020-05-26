
CREATE OR REPLACE VIEW public.queues_by_customer_entity 
AS
SELECT 
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
       task_queue_id,
       service_order_id,
       aim_work_type,
       order_name,
       customer_grp_key,
       program
FROM (SELECT DISTINCT customer_grp_value,
             wtqd.vertical_or_sub_initiative,
             wtqd.device_or_initiative,
             wtqd.atat_transformation AS transformation,
             wtqd.task_queue_name,
             wtqd.task_queue_id_key AS task_queue_id,
             wtqd.queue_type,
             priority,
             dtq.service_order_id,
             fso.aim_work_type,
             fso.order_name,
             fso.program,
             customer_grp_key::VARCHAR(1000) AS customer_grp_key
      FROM nvdev.wbr_task_queue_dim wtqd
        LEFT JOIN nvads.dim_task_queue_view dtq ON wtqd.task_queue_id_key = dtq.task_queue_id
        LEFT JOIN (SELECT order_id,
                          cust_grp_id,
                          program,
                          aim_work_type,
                          order_name
                   FROM (SELECT DISTINCT order_id,
                                cust_grp_id,
                                program,
                                aim_work_type,
                                order_name,
                                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_update_dt DESC) AS RNK
                         FROM nvads.fact_service_order)
                   WHERE RNK = 1) fso ON dtq.service_order_id = fso.order_id
        LEFT JOIN (SELECT customer_grp_key,
                          customer_grp_value,
                          cust_grp_id
                   FROM (SELECT DISTINCT customer_grp_key,
                                customer_grp_value,
                                cust_grp_id,
                                ROW_NUMBER() OVER (PARTITION BY CUST_GRP_ID ORDER BY insert_date DESC) AS RNK
                         FROM nvads.dim_customer_group)
                   WHERE RNK = 1) cust ON fso.cust_grp_id = cust.cust_grp_id
        LEFT JOIN (SELECT DISTINCT order_id,
                          order_create_dt,
                          order_update_dt,
                          order_name,
                          aim_work_type AS ADS_Service
                   FROM (SELECT DISTINCT order_id,
                                order_create_dt,
                                order_update_dt,
                                order_name,
                                aim_work_type,
                                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_update_dt DESC) AS RNK
                         FROM nvads.fact_service_order
                         WHERE (LOWER(order_name) NOT LIKE '%%[closed]%%' OR LOWER(order_name) NOT LIKE '%%[closded]%%'))
                   WHERE RNK = 1) fso2 ON dtq.service_order_id = fso2.order_id
      UNION ALL
      SELECT DISTINCT customer_grp_value,
             customer_grp_value AS vertical_or_sub_initiative,
             customer_grp_value AS device_or_initiative,
             transformation,
             task_queue_name,
             task_queue_id,
             'RAMP' AS queue_type,
             'RAMP' AS priority,
             NULL AS service_order_id,
             NULL AS aim_work_type,
             NULL AS order_name,
             Null AS program,
             NULL AS customer_grp_key
      FROM nvads.ramp_edx
      UNION ALL
      SELECT DISTINCT cycle_customer AS customer_grp_value,
             cycle_customer AS vertical_or_sub_initiative,
             cycle_type || ' ~ work state = ' || cycle_state AS transformation,
             cycle_customer AS device_or_initiative,
             cycle_name AS task_queue_name,
             cycle_id::TEXT AS task_queue_id,
             'RVD' AS queue_type,
             'RVD' AS priority,
             NULL AS service_order_id,
             NULL AS aim_work_type,
             NULL AS order_name,
             NULL As program,
             NULL AS customer_grp_key
      FROM nvrvd.tap_cycle) WITH NO SCHEMA BINDING;

GRANT SELECT ON public.queues_by_customer_entity TO nvort_dml;
GRANT RULE, SELECT, INSERT, TRIGGER, UPDATE, DELETE, REFERENCES ON public.queues_by_customer_entity TO nvort_dev;
GRANT SELECT ON public.queues_by_customer_entity TO nvort;

